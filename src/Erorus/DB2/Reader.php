<?php

namespace Erorus\DB2;

class Reader
{
    const EMBEDDED_STRING_FIELDS = [
        0xEFBEADDE => [2], // tests/wdb5/EmbedStrings.db2
        0x27909DB0 => [13,14,15,16,17], // item-sparse.db2 as of 7.0.3.22522 and earlier
        0xF62C72EE => [13,14,15,16,17], // item-sparse.db2 as of 7.1.0.22900
    ];

    const FIELD_TYPE_UNKNOWN = 0;
    const FIELD_TYPE_INT = 1;
    const FIELD_TYPE_FLOAT = 2;
    const FIELD_TYPE_STRING = 3;

    const DISTINCT_STRINGS_REQUIRED = 5;

    private $fileHandle;
    private $fileFormat = '';
    private $fileName = '';
    private $fileSize = 0;

    private $headerSize = 0;
    private $recordCount = 0;
    private $fieldCount = 0;
    private $recordSize = 0;
    private $stringBlockSize = 0;
    private $tableHash = 0;
    private $layoutHash = 0;
    private $timestamp = 0;
    private $build = 0;
    private $minId = 0;
    private $maxId = 0;
    private $locale = 0;
    private $copyBlockSize = 0;
    private $flags = 0;
    private $totalFieldCount = 0;
    private $nonzeroBlockSize = 0;
    private $idField = -1;

    private $hasEmbeddedStrings = false;
    private $hasIdBlock = false;
    private $hasIdsInIndexBlock = false;

    private $stringBlockPos = 0;
    private $indexBlockPos = 0;
    private $idBlockPos = 0;
    private $copyBlockPos = 0;
    private $nonzeroBlockPos = 0;

    private $recordFormat = [];
    
    private $idMap = [];
    private $recordOffsets = null;

    private $nonzeroLookup = [];

    function __construct($db2path, $arg = null) {
        if (is_string($db2path)) {
            $this->fileHandle = @fopen($db2path, 'rb');
            if ($this->fileHandle === false) {
                throw new \Exception("Error opening ".$db2path);
            }
            $this->fileName = strtolower(basename($db2path));
        } else {
            throw new \Exception("Must supply path to DB2 file");
        }

        $fstat = fstat($this->fileHandle);
        $this->fileSize = $fstat['size'];
        $this->fileFormat = fread($this->fileHandle, 4);
        if (is_a($arg, Reader::class)) {
            switch ($this->fileFormat) {
                case 'WCH7':
                case 'WCH8':
                    $this->openWch7($arg);
                    break;
                default:
                    throw new \Exception("Unknown ADB format: ".$this->fileFormat);
            }
        } else {
            switch ($this->fileFormat) {
                case 'WDBC':
                case 'WDB2':
                    $this->openWdb2();
                    break;
                case 'WDB5':
                case 'WDB6':
                    if (!is_null($arg) && !is_array($arg)) {
                        throw new \Exception("You may only pass an array of string fields when loading a DB2");
                    }
                    $this->openWdb5($arg);
                    break;
                default:
                    throw new \Exception("Unknown DB2 format: ".$this->fileFormat);
            }
        }
    }

    function __destruct() {
        fclose($this->fileHandle);
    }

    ///// initialization

    private function openWdb2() {
        fseek($this->fileHandle, 4);
        $wdbc = $this->fileFormat == 'WDBC';
        $headerFieldCount = $wdbc ? 4 : 11;
        $parts = array_values(unpack('V'.$headerFieldCount.'x',fread($this->fileHandle, 4 * $headerFieldCount)));

        $this->recordCount      = $parts[0];
        $this->fieldCount       = $parts[1];
        $this->recordSize       = $parts[2];
        $this->stringBlockSize  = $parts[3];
        $this->tableHash        = $wdbc ? 0 : $parts[4];
        $this->build            = $wdbc ? 0 : $parts[5];
        $this->timestamp        = $wdbc ? 0 : $parts[6];
        $this->minId            = $wdbc ? 0 : $parts[7];
        $this->maxId            = $wdbc ? 0 : $parts[8];
        $this->locale           = $wdbc ? 0 : $parts[9];
        $this->copyBlockSize    = $wdbc ? 0 : $parts[10];

        $this->headerSize = 4 * ($headerFieldCount + 1);

        $this->hasEmbeddedStrings = false;
        $this->totalFieldCount = $this->fieldCount;

        $this->hasIdBlock = $this->maxId > 0;
        $this->idBlockPos = $this->headerSize;

        if ($this->hasIdBlock) {
            $this->headerSize += 6 * ($this->maxId - $this->minId + 1);
        }

        $this->stringBlockPos = $this->headerSize + ($this->recordCount * $this->recordSize);
        $this->copyBlockPos = $this->stringBlockPos + $this->stringBlockSize;

        $eof = $this->copyBlockPos + $this->copyBlockSize;
        if ($eof != $this->fileSize) {
            throw new \Exception("Expected size: $eof, actual size: ".$this->fileSize);
        }

        $this->recordFormat = [];
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $this->recordFormat[$fieldId] = [
                'bitShift' => 0,
                'offset' => $fieldId * 4,
                'valueLength' => 4,
                'valueCount' => 1,
                'type' => static::FIELD_TYPE_UNKNOWN,
                'signed' => false,
            ] ;
        }

        $this->idField = 0;

        $this->populateIdMap();
        $this->guessFieldTypes();
    }

    private function openWdb5($stringFields) {
        $wdbVersion = intval(substr($this->fileFormat, 3));

        if ($wdbVersion >= 6) {
            $preambleLength = 56;
            $headerFormat = 'V10x/v2y/V2z';
        } else {
            $preambleLength = 48;
            $headerFormat = 'V10x/v2y';
        }

        fseek($this->fileHandle, 4);
        $parts = array_values(unpack($headerFormat, fread($this->fileHandle, $preambleLength - 4)));

        $this->recordCount      = $parts[0];
        $this->fieldCount       = $parts[1];
        $this->recordSize       = $parts[2];
        $this->stringBlockSize  = $parts[3];
        $this->tableHash        = $parts[4];
        $this->layoutHash       = $parts[5];
        $this->minId            = $parts[6];
        $this->maxId            = $parts[7];
        $this->locale           = $parts[8];
        $this->copyBlockSize    = $parts[9];
        $this->flags            = $parts[10];
        $this->idField          = $parts[11];
        $this->totalFieldCount  = $wdbVersion >= 6 ? $parts[12] : $this->fieldCount;
        $this->nonzeroBlockSize = $wdbVersion >= 6 ? $parts[13] : 0;

        $this->headerSize = $preambleLength + $this->fieldCount * 4;

        $this->hasEmbeddedStrings = ($this->flags & 1) > 0;
        $this->hasIdBlock = ($this->flags & 4) > 0;

        if ($this->hasEmbeddedStrings) {
            if (!$this->hasIdBlock) {
                throw new \Exception("File has embedded strings and no ID block, which was not expected, aborting");
            }
            $this->stringBlockPos = $this->fileSize - $this->copyBlockSize - $this->nonzeroBlockSize - ($this->recordCount * 4);
            $this->indexBlockPos = $this->stringBlockSize;
            $this->stringBlockSize = 0;

            if (is_null($stringFields)) {
                if (array_key_exists($this->layoutHash, static::EMBEDDED_STRING_FIELDS)) {
                    $stringFields = static::EMBEDDED_STRING_FIELDS[$this->layoutHash];
                } else {
                    throw new \Exception($this->fileName." has embedded strings, but string fields were not supplied during instantiation");
                }
            }
        } else {
            $this->stringBlockPos = $this->headerSize + ($this->recordCount * $this->recordSize);
        }
        $this->idBlockPos = $this->stringBlockPos + $this->stringBlockSize;

        $this->copyBlockPos = $this->idBlockPos + ($this->hasIdBlock ? $this->recordCount * 4 : 0);

        $this->nonzeroBlockPos = $this->copyBlockPos + $this->copyBlockSize;

        $eof = $this->nonzeroBlockPos + $this->nonzeroBlockSize;
        if ($eof != $this->fileSize) {
            throw new \Exception("Expected size: $eof, actual size: ".$this->fileSize);
        }

        fseek($this->fileHandle, $preambleLength);
        $this->recordFormat = [];
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $this->recordFormat[$fieldId] = unpack('vbitShift/voffset', fread($this->fileHandle, 4));
            $this->recordFormat[$fieldId]['valueLength'] = ceil((32 - $this->recordFormat[$fieldId]['bitShift']) / 8);
            $this->recordFormat[$fieldId]['type'] = ($this->recordFormat[$fieldId]['valueLength'] != 4) ? static::FIELD_TYPE_INT : static::FIELD_TYPE_UNKNOWN;
            if ($this->hasEmbeddedStrings && $this->recordFormat[$fieldId]['type'] == static::FIELD_TYPE_UNKNOWN && in_array($fieldId, $stringFields)) {
                $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_STRING;
            }
            $this->recordFormat[$fieldId]['signed'] = false;
            if ($fieldId > 0) {
                $this->recordFormat[$fieldId - 1]['valueCount'] =
                    floor(($this->recordFormat[$fieldId]['offset'] - $this->recordFormat[$fieldId - 1]['offset']) / $this->recordFormat[$fieldId - 1]['valueLength']);
            }
        }

        $fieldId = $this->fieldCount - 1;
        $remainingBytes = $this->recordSize - $this->recordFormat[$fieldId]['offset'];
        $this->recordFormat[$fieldId]['valueCount'] = max(1, floor($remainingBytes / $this->recordFormat[$fieldId]['valueLength']));
        if ($this->recordFormat[$fieldId]['valueCount'] > 1 &&    // we're guessing the last field is an array
            (($this->recordSize % 4 == 0 && $remainingBytes <= 4) // records may be padded to word length and the last field size <= word size
            || (!$this->hasIdBlock && $this->idField == $fieldId))) {  // or the reported ID field is the last field
            $this->recordFormat[$fieldId]['valueCount'] = 1;      // assume the last field is scalar, and the remaining bytes are just padding
        }

        if (!$this->hasIdBlock) {
            if ($this->idField >= $this->fieldCount) {
                throw new \Exception("Expected ID field " . $this->idField . " does not exist. Only found " . $this->fieldCount . " fields.");
            }
            if ($this->recordFormat[$this->idField]['valueCount'] != 1) {
                throw new \Exception("Expected ID field " . $this->idField . " reportedly has " . $this->recordFormat[$this->idField]['valueCount'] . " values per row");
            }
        }

        $this->findNonzeroFields();

        $this->populateIdMap();

        if ($this->hasEmbeddedStrings) {
            for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
                unset($this->recordFormat[$fieldId]['offset']); // just to make sure we don't use them later, because they're meaningless now
            }
            $this->populateRecordOffsets();
        }

        $this->guessFieldTypes();
    }

    private function openWch7(Reader $sourceReader) {
        fseek($this->fileHandle, 4);
        $parts = array_values(unpack('V12x',fread($this->fileHandle, 4 * 12)));

        $this->recordCount      = $parts[0];
        $unkTableSize           = $parts[1];
        $this->fieldCount       = $parts[2];
        $this->recordSize       = $parts[3];
        $this->stringBlockSize  = $parts[4];
        $this->tableHash        = $parts[5];
        $this->layoutHash       = $parts[6];
        $this->build            = $parts[7];
        $this->timestamp        = $parts[8];
        $this->minId            = $parts[9];
        $this->maxId            = $parts[10];
        $this->locale           = $parts[11];
        $this->copyBlockSize    = 0;
        $this->flags            = $sourceReader->flags;
        $this->idField          = $sourceReader->idField;

        $this->headerSize = 52;

        $this->totalFieldCount = $this->fieldCount;
        $this->hasEmbeddedStrings = ($this->flags & 1) > 0;
        $this->hasIdBlock = ($this->flags & 4) > 0;

        foreach (['tableHash', 'layoutHash', 'fieldCount'] as $headerField) {
            if ($this->$headerField != $sourceReader->$headerField) {
                throw new \Exception("$headerField of {$this->fileName} ({$this->$headerField}) does not match $headerField of {$sourceReader->fileName} ({$sourceReader->$headerField})");
            }
        }
        if (($sourceReader->locale & $this->locale) != $this->locale) {
            $headerField = 'locale';
            throw new \Exception("$headerField of {$this->fileName} ({$this->$headerField}) does not match $headerField of {$sourceReader->fileName} ({$sourceReader->$headerField})");
        }

        if ($this->hasEmbeddedStrings) {
            // this could get messy
            $this->hasIdsInIndexBlock = true;
            $this->hasIdBlock = !$this->hasIdsInIndexBlock;

            $this->indexBlockPos = $this->headerSize + $this->stringBlockSize; // stringBlockSize is really just the offset after the header to the index block

            $this->stringBlockPos = $this->idBlockPos = $this->fileSize;
            $this->stringBlockSize = 0;
        } else {
            $this->stringBlockPos = $this->headerSize + ($this->recordCount * $this->recordSize);
            $this->idBlockPos = $unkTableSize * 4 + $this->stringBlockPos + $this->stringBlockSize;
        }

        $this->copyBlockPos = $this->idBlockPos + ($this->hasIdBlock ? $this->recordCount * 4 : 0);

        $eof = $this->copyBlockPos + $this->copyBlockSize;
        if ($eof != $this->fileSize) {
            throw new \Exception("Expected size: $eof, actual size: ".$this->fileSize);
        }

        $this->recordFormat = $sourceReader->recordFormat;

        if ($this->hasEmbeddedStrings) {
            $this->populateRecordOffsets(); // also populates idMap
        } else {
            $this->populateIdMap();
        }
    }

    private function guessFieldTypes() {
        foreach ($this->recordFormat as $fieldId => &$format) {
            if ($format['type'] != static::FIELD_TYPE_UNKNOWN || $format['valueLength'] != 4) {
                continue;
            }

            $couldBeFloat = true;
            $couldBeString = !$this->hasEmbeddedStrings;
            $recordOffset = 0;
            $distinctValues = [];
            while (($couldBeString || $couldBeFloat) && $recordOffset < $this->recordCount) {
                $data = $this->getRawRecord($recordOffset);
                if (!$this->hasEmbeddedStrings) {
                    $byteOffset = $format['offset'];
                } else {
                    // format offsets mean nothing
                    $byteOffset = 0;
                    for ($offsetFieldId = 0; $offsetFieldId < $fieldId; $offsetFieldId++) {
                        if ($this->recordFormat[$offsetFieldId]['type'] == static::FIELD_TYPE_STRING) {
                            for ($offsetFieldValueId = 0; $offsetFieldValueId < $this->recordFormat[$offsetFieldId]['valueCount']; $offsetFieldValueId++) {
                                $byteOffset = strpos($data, "\x00", $byteOffset);
                                if ($byteOffset === false) {
                                    throw new \Exception("Could not find end of embedded string $offsetFieldId x $offsetFieldValueId in record $recordOffset");
                                }
                                $byteOffset++; // skip nul byte
                            }
                        } else {
                            $byteOffset += $this->recordFormat[$offsetFieldId]['valueLength'] * $this->recordFormat[$offsetFieldId]['valueCount'];
                        }
                    }
                }
                $data = substr($data, $byteOffset, $format['valueLength'] * $format['valueCount']);
                $values = unpack('V*', $data);
                foreach ($values as $value) {
                    if ($value == 0) {
                        continue; // can't do much with this
                    }
                    if (count($distinctValues) < static::DISTINCT_STRINGS_REQUIRED) {
                        $distinctValues[$value] = true;
                    }
                    if ($couldBeString) {
                        if ($value > $this->stringBlockSize) {
                            $couldBeString = false;
                        } else {
                            // offset should be the start of a string
                            // so the char immediately before should be the null terminator of the prev string
                            fseek($this->fileHandle, $this->stringBlockPos + $value - 1);
                            if (fread($this->fileHandle, 1) !== "\x00") {
                                $couldBeString = false;
                            }
                        }
                    }
                    if ($couldBeFloat) {
                        $exponent = ($value >> 23) & 0xFF;
                        if ($exponent == 0 || $exponent == 0xFF) {
                            $couldBeFloat = false;
                        } else {
                            $asFloat = current(unpack('f', pack('V', $value)));
                            if (round($asFloat, 6) == 0) {
                                $couldBeFloat = false;
                            }
                        }
                    }
                }
                $recordOffset++;
            }

            if ($couldBeString && ($this->recordCount < static::DISTINCT_STRINGS_REQUIRED * 2 || count($distinctValues) >= static::DISTINCT_STRINGS_REQUIRED)) {
                $format['type'] = static::FIELD_TYPE_STRING;
                $format['signed'] = false;
            } elseif ($couldBeFloat) {
                $format['type'] = static::FIELD_TYPE_FLOAT;
                $format['signed'] = true;
            } else {
                $format['type'] = static::FIELD_TYPE_INT;
            }
        }
        unset($format);
    }
    
    private function populateIdMap() {
        $this->idMap = [];
        if (!$this->hasIdBlock) {
            $this->recordFormat[$this->idField]['signed'] = false; // in case it's a 32-bit int
            $fieldFormat = $this->recordFormat[$this->idField];
            for ($x = 0; $x < $this->recordCount; $x++) {
                fseek($this->fileHandle, $this->headerSize + $x * $this->recordSize + $fieldFormat['offset']);
                $this->idMap[current(unpack('V', str_pad(fread($this->fileHandle, $fieldFormat['valueLength']), 4, "\x00", STR_PAD_RIGHT)))] = $x;
            }
        } else {
            fseek($this->fileHandle, $this->idBlockPos);
            if ($this->fileFormat == 'WDB2') {
                for ($x = $this->minId; $x <= $this->maxId; $x++) {
                    $record = current(unpack('V', fread($this->fileHandle, 4)));
                    if ($record) {
                        $this->idMap[$x] = $record - 1;
                    }
                    fseek($this->fileHandle, 2, SEEK_CUR); // ignore embed string length in this record
                }
            } else {
                for ($x = 0; $x < $this->recordCount; $x++) {
                    $this->idMap[current(unpack('V', fread($this->fileHandle, 4)))] = $x;
                }
            }
        }

        if ($this->copyBlockSize) {
            fseek($this->fileHandle, $this->copyBlockPos);
            $entryCount = floor($this->copyBlockSize / 8);
            for ($x = 0; $x < $entryCount; $x++) {
                list($newId, $existingId) = array_values(unpack('V*', fread($this->fileHandle, 8)));
                if (!isset($this->idMap[$existingId])) {
                    throw new \Exception("Copy block referenced ID $existingId which does not exist");
                }
                $this->idMap[$newId] = $this->idMap[$existingId];
            }
            ksort($this->idMap, SORT_NUMERIC);
        }
    }

    private function populateRecordOffsets() {
        // only required when hasEmbeddedStrings,
        // since it has the index block to map back into the data block

        fseek($this->fileHandle, $this->indexBlockPos);
        $this->recordOffsets = [];
        if ($this->hasIdsInIndexBlock) {
            $this->idMap = [];
            $lowerBound = 0;
            $upperBound = $this->recordCount - 1;
        } else {
            $lowerBound = $this->minId;
            $upperBound = $this->maxId;
        }
        $seenBefore = [];
        for ($x = $lowerBound; $x <= $upperBound; $x++) {
            if ($this->hasIdsInIndexBlock) {
                $bytes = fread($this->fileHandle, 10);
                $pointer = unpack('Vid/Vpos/vsize', $bytes);
                $bytes = substr($bytes, 4); // crop off ID in front to match other case
                $this->idMap[$pointer['id']] = $x;
            } else {
                $bytes = fread($this->fileHandle, 6);
                $pointer = unpack('Vpos/vsize', $bytes);
                $pointer['id'] = $x;
            }
            if ($pointer['size'] > 0) {
                if (!isset($seenBefore[$pointer['pos']])) {
                    $seenBefore[$pointer['pos']] = [];
                }
                if (!isset($this->idMap[$pointer['id']])) {
                    //echo "Found {$pointer['pos']} x {$pointer['size']} in index block for $pointer['id'] which isn't in id block\n";
                    foreach ($seenBefore[$pointer['pos']] as $anotherId) {
                        if (isset($this->idMap[$anotherId])) {
                            //echo "Mapping $pointer['id'] to match previously seen $anotherId\n";
                            $this->idMap[$pointer['id']] = $this->idMap[$anotherId];
                        }
                    }
                }
                if (isset($this->idMap[$pointer['id']])) {
                    $this->recordOffsets[$this->idMap[$pointer['id']]] = $bytes;
                    foreach ($seenBefore[$pointer['pos']] as $anotherId) {
                        if (!isset($this->idMap[$anotherId])) {
                            //echo "Mapping previously seen $anotherId to match $pointer['id']\n";
                            $this->idMap[$anotherId] = $this->idMap[$pointer['id']];
                        }
                    }
                }
                $seenBefore[$pointer['pos']][] = $pointer['id'];
            }
        }

        ksort($this->idMap);
    }

    private function findNonzeroFields() {
        $this->nonzeroLookup = [];
        if ($this->nonzeroBlockSize == 0) {
            return;
        }

        fseek($this->fileHandle, $this->nonzeroBlockPos);
        $fieldCount = current(unpack('V', fread($this->fileHandle, 4)));
        if ($fieldCount != $this->totalFieldCount) {
            throw new \Exception(sprintf("Expected %d fields in nonzero block, found %d", $this->totalFieldCount, $fieldCount));
        }

        for ($field = 0; $field < $this->totalFieldCount; $field++) {
            list($entryCount, $enumType) = array_values(unpack('V1x/C1y', fread($this->fileHandle, 5)));
            if ($field < $this->fieldCount) {
                if ($entryCount > 0) {
                    throw new \Exception(sprintf("Expected 0 entries in nonzero block field %d, instead found %d", $field, $entryCount));
                }
                continue;
            }

            $size = 4;
            $type = Reader::FIELD_TYPE_INT;

            switch ($enumType) {
                case 0: // string
                    $type = Reader::FIELD_TYPE_STRING;
                    break;
                case 1: // short
                    $size = 2;
                    break;
                case 2: // byte
                    $size = 1;
                    break;
                case 3: // float
                    $type = Reader::FIELD_TYPE_FLOAT;
                    break;
                case 4: // 4-byte int
                    break;
                default:
                    throw new \Exception("Unknown nonzero field type: $enumType");
            }

            $this->recordFormat[$field] = [
                'valueCount'  => 1,
                'valueLength' => $size,
                'type'        => $type,
                'signed'      => false,
                'zero'        => str_repeat("\x00", $size),
            ];
            $this->nonzeroLookup[$field] = [];

            $embeddedStrings = false;
            if ($this->hasEmbeddedStrings && $type == Reader::FIELD_TYPE_STRING) {
                // @codeCoverageIgnoreStart
                // file with both embedded strings and nonzero block not found in wild, this is just a guess
                $embeddedStrings = true;
                $this->recordFormat[$field]['zero'] = "\x00";
                // @codeCoverageIgnoreEnd
            }

            for ($entry = 0; $entry < $entryCount; $entry++) {
                $id = current(unpack('V', fread($this->fileHandle, 4)));
                if ($embeddedStrings) {
                    // @codeCoverageIgnoreStart
                    // file with both embedded strings and nonzero block not found in wild, this is just a guess
                    $maxLength = $this->nonzeroBlockSize - (ftell($this->fileHandle) - $this->nonzeroBlockPos);
                    $this->nonzeroLookup[$field][$id] = stream_get_line($this->fileHandle, $maxLength, "\x00") . "\x00";
                    // @codeCoverageIgnoreEnd
                } else {
                    $this->nonzeroLookup[$field][$id] = fread($this->fileHandle, $size);
                }
            }
        }
    }

    // general purpose for internal use

    private function getRawRecord($recordOffset, $id = false) {
        if (!is_null($this->recordOffsets)) {
            $pointer = unpack('Vpos/vsize', $this->recordOffsets[$recordOffset]);
            if ($pointer['size'] == 0) {
                // @codeCoverageIgnoreStart
                throw new \Exception("Requested record offset $recordOffset which is empty");
                // @codeCoverageIgnoreEnd
            }
            fseek($this->fileHandle, $pointer['pos']);
            $data = fread($this->fileHandle, $pointer['size']);
        } else {
            fseek($this->fileHandle, $this->headerSize + $recordOffset * $this->recordSize);
            $data = fread($this->fileHandle, $this->recordSize);
        }
        if ($id !== false) {
            foreach ($this->nonzeroLookup as $field => $lookup) {
                if (isset($lookup[$id])) {
                    $data .= $lookup[$id];
                } else {
                    $data .= $this->recordFormat[$field]['zero'];
                }
            }
        }
        return $data;
    }

    private function getString($stringBlockOffset) {
        if ($stringBlockOffset >= $this->stringBlockSize) {
            // @codeCoverageIgnoreStart
            throw new \Exception("Asked to get string from $stringBlockOffset, string block size is only ".$this->stringBlockSize);
            // @codeCoverageIgnoreEnd
        }
        $maxLength = $this->stringBlockSize - $stringBlockOffset;

        fseek($this->fileHandle, $this->stringBlockPos + $stringBlockOffset);
        return stream_get_line($this->fileHandle, $maxLength, "\x00");
    }

    private function getRecordByOffset($recordOffset, $id) {
        if ($recordOffset < 0 || $recordOffset >= $this->recordCount) {
            // @codeCoverageIgnoreStart
            throw new \Exception("Requested record offset $recordOffset out of bounds: 0-".$this->recordCount);
            // @codeCoverageIgnoreEnd
        }

        $record = $this->getRawRecord($recordOffset, $id);

        $runningOffset = 0;
        $row = [];
        for ($fieldId = 0; $fieldId < $this->totalFieldCount; $fieldId++) {
            $field = [];
            $format = $this->recordFormat[$fieldId];
            for ($valueId = 0; $valueId < $format['valueCount']; $valueId++) {
                if ($this->hasEmbeddedStrings && $format['type'] == static::FIELD_TYPE_STRING) {
                    $rawValue = substr($record, $runningOffset, strpos($record, "\x00", $runningOffset) - $runningOffset);
                    $runningOffset += strlen($rawValue) + 1;
                    $field[] = $rawValue;
                    continue;
                } else {
                    $rawValue = substr($record, $runningOffset, $format['valueLength']);
                    $runningOffset += $format['valueLength'];
                }
                switch ($format['type']) {
                    case static::FIELD_TYPE_UNKNOWN:
                    case static::FIELD_TYPE_INT:
                        if ($format['signed']) {
                            switch ($format['valueLength']) {
                                case 4:
                                    $field[] = current(unpack('l', $rawValue));
                                    break;
                                case 3:
                                    $field[] = current(unpack('l', $rawValue . (ord(substr($rawValue, -1)) & 0x80 ? "\xFF" : "\x00")));
                                    break;
                                case 2:
                                    $field[] = current(unpack('s', $rawValue));
                                    break;
                                case 1:
                                    $field[] = current(unpack('c', $rawValue));
                                    break;
                            }
                        } else {
                            $field[] = current(unpack('V', str_pad($rawValue, 4, "\x00", STR_PAD_RIGHT)));
                        }
                        break;
                    case static::FIELD_TYPE_FLOAT:
                        $field[] = round(current(unpack('f', $rawValue)), 6);
                        break;
                    case static::FIELD_TYPE_STRING:
                        $field[] = $this->getString(current(unpack('V', $rawValue)));
                        break;
                }
            }
            if (count($field) == 1) {
                $field = $field[0];
            }
            $row[isset($format['name']) ? $format['name'] : $fieldId] = $field;
        }

        return $row;
    }

    // standard usage

    public function getFieldCount() {
        return $this->totalFieldCount;
    }

    public function getRecord($id) {
        if (!isset($this->idMap[$id])) {
            return null;
        }
        
        return $this->getRecordByOffset($this->idMap[$id], $id);
    }

    public function getIds() {
        return array_keys($this->idMap);
    }

    public function generateRecords() {
        foreach ($this->idMap as $id => $offset) {
            yield $id => $this->getRecordByOffset($offset, $id);
        }
    }

    public function getFieldTypes($byName = true) {
        $fieldTypes = [];
        foreach ($this->recordFormat as $fieldId => $format) {
            if ($byName && isset($format['name'])) {
                $fieldId = $format['name'];
            }
            $fieldTypes[$fieldId] = $format['type'];
        }
        return $fieldTypes;
    }

    public function getLayoutHash() {
        return $this->layoutHash;
    }

    public function loadAdb($adbPath) {
        return new Reader($adbPath, $this);
    }

    // user preferences

    public function setFieldsSigned(Array $fields) {
        foreach ($fields as $fieldId => $isSigned) {
            if ($fieldId < 0 || $fieldId >= $this->totalFieldCount) {
                throw new \Exception("Field ID $fieldId out of bounds: 0-".($this->totalFieldCount - 1));
            }
            if (!$this->hasIdBlock && $this->idField == $fieldId) {
                continue;
            }
            if ($this->recordFormat[$fieldId]['type'] != static::FIELD_TYPE_INT) {
                continue;
            }
            $this->recordFormat[$fieldId]['signed'] = !!$isSigned;
        }

        $signedFields = [];
        foreach ($this->recordFormat as $fieldId => $format) {
            $signedFields[$fieldId] = $format['signed'];
        }
        return $signedFields;
    }

    public function setFieldNames(Array $names) {
        foreach ($names as $fieldId => $name) {
            if (!is_numeric($fieldId)) {
                throw new \Exception("Field ID $fieldId must be numeric");
            }
            if (is_numeric($name)) {
                throw new \Exception("Field $fieldId Name ($name) must NOT be numeric");
            }
            if ($fieldId < 0 || $fieldId >= $this->totalFieldCount) {
                throw new \Exception("Field ID $fieldId out of bounds: 0-".($this->totalFieldCount - 1));
            }
            if (!$name) {
                unset($this->recordFormat[$fieldId]['name']);
            } else {
                $this->recordFormat[$fieldId]['name'] = $name;
            }
        }

        $namedFields = [];
        foreach ($this->recordFormat as $fieldId => $format) {
            if (isset($format['name'])) {
                $namedFields[$fieldId] = $format['name'];
            }
        }
        return $namedFields;
    }

    // static utils

    public static function flattenRecord(Array $record) {
        $result = [];
        foreach ($record as $k => $v) {
            if (!is_array($v)) {
                $result[$k] = $v;
                continue;
            }
            $idx = 0;
            foreach ($v as $vv) {
                $result["$k-" . $idx++] = $vv;
            }
        }
        return $result;
    }
}