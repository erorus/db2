<?php

namespace Erorus\DB2;

class Reader
{
    const FIELD_TYPE_UNKNOWN = 0;
    const FIELD_TYPE_INT = 1;
    const FIELD_TYPE_FLOAT = 2;
    const FIELD_TYPE_STRING = 3;

    const DISTINCT_STRINGS_REQUIRED = 5;

    const FIELD_COMPRESSION_NONE = 0;
    const FIELD_COMPRESSION_BITPACKED = 1;
    const FIELD_COMPRESSION_COMMON = 2;
    const FIELD_COMPRESSION_BITPACKED_INDEXED = 3;
    const FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY = 4;
    const FIELD_COMPRESSION_BITPACKED_SIGNED = 5;

    private $fileHandle;
    private $fileFormat = '';
    private $fileName = '';
    private $fileSize = 0;

    private $headerSize = 0;
    private $recordCount = 0;
    private $fieldCount = 0;
    private $totalFieldCount = 0;
    private $recordSize = 0;
    private $stringBlockPos = 0;
    private $stringBlockSize = 0;
    private $tableHash = 0;
    private $layoutHash = 0;
    private $timestamp = 0;
    private $build = 0;
    private $minId = 0;
    private $maxId = 0;
    private $locale = 0;
    private $copyBlockPos = 0;
    private $copyBlockSize = 0;
    private $flags = 0;

    private $commonBlockPos = 0;
    private $commonBlockSize = 0;
    private $bitpackedDataPos = 0;
    private $lookupColumnCount = 0;
    private $idBlockSize = 0;
    private $fieldStorageInfoPos = 0;
    private $fieldStorageInfoSize = 0;
    private $palletDataPos = 0;
    private $palletDataSize = 0;
    private $relationshipDataPos = 0;
    private $relationshipDataSize = 0;

    private $sectionCount = 0;
    private $sectionHeaders = [];

    private $idField = -1;

    private $hasEmbeddedStrings = false;
    private $hasIdBlock = false;
    private $hasIdsInIndexBlock = false;

    private $indexBlockPos = 0;
    private $idBlockPos = 0;

    private $recordFormat = [];
    
    private $idMap = [];
    private $recordOffsets = null;

    private $commonLookup = [];

    function __construct($db2path, $arg = null) {
        if (is_string($db2path)) {
            $this->fileHandle = @fopen($db2path, 'rb');
            if ($this->fileHandle === false) {
                throw new \Exception("Error opening ".$db2path);
            }
            $this->fileName = basename($db2path);
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
                case 'XFTH':
                    $this->openHotfix($arg);
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
                case 'WDC1':
                    if (!is_null($arg) && !is_array($arg)) {
                        throw new \Exception("You may only pass an array of string fields when loading a DB2");
                    }
                    $this->openWdc1($arg);
                    break;
                case 'WDC2':
                case 'WDC3':
                case '1SLC':
                    if (!is_null($arg) && !is_array($arg)) {
                        throw new \Exception("You may only pass an array of string fields when loading a DB2");
                    }
                    $this->openWdc2($arg);
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
                'size' => 4,
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
        $this->commonBlockSize = $wdbVersion >= 6 ? $parts[13] : 0;

        $this->headerSize = $preambleLength + $this->fieldCount * 4;

        $this->hasEmbeddedStrings = ($this->flags & 1) > 0;
        $this->hasIdBlock = ($this->flags & 4) > 0;

        if ($this->hasEmbeddedStrings) {
            if (!$this->hasIdBlock) {
                throw new \Exception("File has embedded strings and no ID block, which was not expected, aborting");
            }
            $this->stringBlockPos = $this->fileSize - $this->copyBlockSize - $this->commonBlockSize - ($this->recordCount * 4);
            $this->indexBlockPos = $this->stringBlockSize;
            $this->stringBlockSize = 0;
        } else {
            $this->stringBlockPos = $this->headerSize + ($this->recordCount * $this->recordSize);
        }
        $this->idBlockPos = $this->stringBlockPos + $this->stringBlockSize;

        $this->copyBlockPos = $this->idBlockPos + ($this->hasIdBlock ? $this->recordCount * 4 : 0);

        $this->commonBlockPos = $this->copyBlockPos + $this->copyBlockSize;

        $eof = $this->commonBlockPos + $this->commonBlockSize;
        if ($eof != $this->fileSize) {
            throw new \Exception("Expected size: $eof, actual size: ".$this->fileSize);
        }

        fseek($this->fileHandle, $preambleLength);
        $this->recordFormat = [];
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $this->recordFormat[$fieldId] = unpack('vbitShift/voffset', fread($this->fileHandle, 4));
            $this->recordFormat[$fieldId]['valueLength'] = (int)ceil((32 - $this->recordFormat[$fieldId]['bitShift']) / 8);
            $this->recordFormat[$fieldId]['size'] = $this->recordFormat[$fieldId]['valueLength'];
            $this->recordFormat[$fieldId]['type'] = ($this->recordFormat[$fieldId]['size'] != 4) ? static::FIELD_TYPE_INT : static::FIELD_TYPE_UNKNOWN;
            if ($this->hasEmbeddedStrings && $this->recordFormat[$fieldId]['type'] == static::FIELD_TYPE_UNKNOWN
                && !is_null($stringFields) && in_array($fieldId, $stringFields)) {
                $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_STRING;
            }
            $this->recordFormat[$fieldId]['signed'] = false;
            if ($fieldId > 0) {
                $this->recordFormat[$fieldId - 1]['valueCount'] =
                    (int)floor(($this->recordFormat[$fieldId]['offset'] - $this->recordFormat[$fieldId - 1]['offset']) / $this->recordFormat[$fieldId - 1]['valueLength']);
            }
        }

        $fieldId = $this->fieldCount - 1;
        $remainingBytes = $this->recordSize - $this->recordFormat[$fieldId]['offset'];
        $this->recordFormat[$fieldId]['valueCount'] = max(1, (int)floor($remainingBytes / $this->recordFormat[$fieldId]['valueLength']));
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

        $this->findCommonFields();

        $this->populateIdMap();

        if ($this->hasEmbeddedStrings) {
            for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
                unset($this->recordFormat[$fieldId]['offset']); // just to make sure we don't use them later, because they're meaningless now
            }
            $this->populateRecordOffsets();

            if (is_null($stringFields)) {
                $this->detectEmbeddedStringFields();
            }
        }

        $this->guessFieldTypes();
    }

    private function openWdc1($stringFields) {
        $headerLength = 84;
        $headerFormat = 'V10x/v2y/V9z';

        fseek($this->fileHandle, 4);
        $parts = array_values(unpack($headerFormat, fread($this->fileHandle, $headerLength - 4)));

        $this->recordCount          = $parts[0];
        $this->fieldCount           = $parts[1];
        $this->recordSize           = $parts[2];
        $this->stringBlockSize      = $parts[3];
        $this->tableHash            = $parts[4];
        $this->layoutHash           = $parts[5];
        $this->minId                = $parts[6];
        $this->maxId                = $parts[7];
        $this->locale               = $parts[8];
        $this->copyBlockSize        = $parts[9];
        $this->flags                = $parts[10];
        $this->idField              = $parts[11];
        $this->totalFieldCount      = $parts[12];
        $this->bitpackedDataPos     = $parts[13];
        $this->lookupColumnCount    = $parts[14];
        $this->indexBlockPos        = $parts[15];
        $this->idBlockSize          = $parts[16];
        $this->fieldStorageInfoSize = $parts[17];
        $this->commonBlockSize      = $parts[18];
        $this->palletDataSize       = $parts[19];
        $this->relationshipDataSize = $parts[20];

        $this->headerSize = $headerLength + $this->fieldCount * 4;

        $this->hasEmbeddedStrings = ($this->flags & 1) > 0;
        $this->hasIdBlock = ($this->flags & 4) > 0;

        if ($this->fieldStorageInfoSize != $this->totalFieldCount * 24) {
            throw new \Exception(sprintf('Expected %d bytes for storage info, instead found %d', $this->totalFieldCount * 24, $this->fieldStorageInfoSize));
        }

        if ($this->hasEmbeddedStrings) {
            if (!$this->hasIdBlock) {
                throw new \Exception("File has embedded strings and no ID block, which was not expected, aborting");
            }

            $this->stringBlockSize = 0;
            $this->stringBlockPos = $this->indexBlockPos + 6 * ($this->maxId - $this->minId + 1);
        } else {
            $this->stringBlockPos = $this->headerSize + ($this->recordCount * $this->recordSize);
        }
        $this->idBlockPos = $this->stringBlockPos + $this->stringBlockSize;

        $this->copyBlockPos = $this->idBlockPos + ($this->hasIdBlock ? $this->recordCount * 4 : 0);

        $this->fieldStorageInfoPos = $this->copyBlockPos + $this->copyBlockSize;

        $this->palletDataPos = $this->fieldStorageInfoPos + $this->fieldStorageInfoSize;

        $this->commonBlockPos = $this->palletDataPos + $this->palletDataSize;

        $this->relationshipDataPos = $this->commonBlockPos + $this->commonBlockSize;

        $eof = $this->relationshipDataPos + $this->relationshipDataSize;
        if ($eof != $this->fileSize) {
            throw new \Exception("Expected size: $eof, actual size: ".$this->fileSize);
        }

        fseek($this->fileHandle, $headerLength);
        $this->recordFormat = [];
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $this->recordFormat[$fieldId] = unpack('sbitShift/voffset', fread($this->fileHandle, 4));
            $this->recordFormat[$fieldId]['valueLength'] = max(1, (int)ceil((32 - $this->recordFormat[$fieldId]['bitShift']) / 8));
            $this->recordFormat[$fieldId]['size'] = $this->recordFormat[$fieldId]['valueLength'];
            $this->recordFormat[$fieldId]['type'] = ($this->recordFormat[$fieldId]['size'] != 4) ? static::FIELD_TYPE_INT : static::FIELD_TYPE_UNKNOWN;
            if ($this->hasEmbeddedStrings && $this->recordFormat[$fieldId]['type'] == static::FIELD_TYPE_UNKNOWN
                && !is_null($stringFields) && in_array($fieldId, $stringFields)) {
                $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_STRING;
            }
            $this->recordFormat[$fieldId]['signed'] = false;
            if ($fieldId > 0) {
                $this->recordFormat[$fieldId - 1]['valueCount'] =
                    (int)floor(($this->recordFormat[$fieldId]['offset'] - $this->recordFormat[$fieldId - 1]['offset']) / $this->recordFormat[$fieldId - 1]['valueLength']);
            }
        }

        $fieldId = $this->fieldCount - 1;
        $remainingBytes = $this->recordSize - $this->recordFormat[$fieldId]['offset'];
        $this->recordFormat[$fieldId]['valueCount'] = max(1, (int)floor($remainingBytes / $this->recordFormat[$fieldId]['valueLength']));
        if ($this->recordFormat[$fieldId]['valueCount'] > 1 &&    // we're guessing the last field is an array
            (($this->recordSize % 4 == 0 && $remainingBytes <= 4) // records may be padded to word length and the last field size <= word size
             || (!$this->hasIdBlock && $this->idField == $fieldId))) {  // or the reported ID field is the last field
            $this->recordFormat[$fieldId]['valueCount'] = 1;      // assume the last field is scalar, and the remaining bytes are just padding
        }

        $commonBlockPointer = 0;
        $palletBlockPointer = 0;

        fseek($this->fileHandle, $this->fieldStorageInfoPos);
        $storageInfoFormat = 'voffsetBits/vsizeBits/VadditionalDataSize/VstorageType/VbitpackOffsetBits/VbitpackSizeBits/VarrayCount';
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $parts = unpack($storageInfoFormat, fread($this->fileHandle, 24));

            switch ($parts['storageType']) {
                case static::FIELD_COMPRESSION_COMMON:
                    $this->recordFormat[$fieldId]['size'] = 4;
                    $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_INT;
                    $this->recordFormat[$fieldId]['valueCount'] = 1;
                    $parts['defaultValue'] = pack('V', $parts['bitpackOffsetBits']);
                    $parts['bitpackOffsetBits'] = 0;
                    $parts['blockOffset'] = $commonBlockPointer;
                    $commonBlockPointer += $parts['additionalDataSize'];
                    break;
                case static::FIELD_COMPRESSION_BITPACKED:
                    $this->recordFormat[$fieldId]['size'] = 4;
                    $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_INT;
                    $this->recordFormat[$fieldId]['offset'] = (int)floor($parts['offsetBits'] / 8);
                    $this->recordFormat[$fieldId]['valueLength'] = (int)ceil(($parts['offsetBits'] + $parts['sizeBits']) / 8) - $this->recordFormat[$fieldId]['offset'] + 1;
                    $this->recordFormat[$fieldId]['valueCount'] = 1;
                    break;
                case static::FIELD_COMPRESSION_BITPACKED_INDEXED:
                case static::FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY:
                    $this->recordFormat[$fieldId]['size'] = 4;
                    $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_INT;
                    $this->recordFormat[$fieldId]['offset'] = (int)floor($parts['offsetBits'] / 8);
                    $this->recordFormat[$fieldId]['valueLength'] = (int)ceil(($parts['offsetBits'] + $parts['sizeBits']) / 8) - $this->recordFormat[$fieldId]['offset'] + 1;
                    $this->recordFormat[$fieldId]['valueCount'] = $parts['arrayCount'] > 0 ? $parts['arrayCount'] : 1;
                    $parts['blockOffset'] = $palletBlockPointer;
                    $palletBlockPointer += $parts['additionalDataSize'];
                    break;
                case static::FIELD_COMPRESSION_NONE:
                    if ($parts['arrayCount'] > 0) {
                        $this->recordFormat[$fieldId]['valueCount'] = $parts['arrayCount'];
                    }
                    break;
                default:
                    throw new \Exception(sprintf("Unknown field compression type ID: %d", $parts['storageType']));
            }

            $this->recordFormat[$fieldId]['storage'] = $parts;
        }

        if (!$this->hasIdBlock) {
            if ($this->idField >= $this->fieldCount) {
                throw new \Exception("Expected ID field " . $this->idField . " does not exist. Only found " . $this->fieldCount . " fields.");
            }
            if ($this->recordFormat[$this->idField]['valueCount'] != 1) {
                throw new \Exception("Expected ID field " . $this->idField . " reportedly has " . $this->recordFormat[$this->idField]['valueCount'] . " values per row");
            }
        }

        if ($this->relationshipDataSize) {
            $this->recordFormat[$this->totalFieldCount++] = [
                'valueLength' => 4,
                'size' => 4,
                'offset' => $this->recordSize,
                'type' => static::FIELD_TYPE_INT,
                'valueCount' => 1,
                'signed' => false,
                'isRelationshipData' => true,
                'storage' => [
                    'storageType' => static::FIELD_COMPRESSION_NONE
                ]
            ];
        }

        $this->populateIdMap();

        if ($this->hasEmbeddedStrings) {
            for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
                if ($this->recordFormat[$fieldId]['storage']['storageType'] != static::FIELD_COMPRESSION_NONE) {
                    throw new \Exception("DB2 with Embedded Strings has compressed field $fieldId");
                }
                unset($this->recordFormat[$fieldId]['offset']); // just to make sure we don't use them later, because they're meaningless now
            }
            $this->populateRecordOffsets();

            if (is_null($stringFields)) {
                $this->detectEmbeddedStringFields();
            }
        }

        $this->guessFieldTypes();
    }

    private function openWdc2($stringFields) {
        $headerFormat = 'V9x/v2y/V7z';

        $isWdc2 = $this->fileFormat == 'WDC2' || $this->fileFormat == '1SLC';

        fseek($this->fileHandle, 4);
        $parts = array_values(unpack($headerFormat, fread($this->fileHandle, 68)));

        $this->recordCount          = $parts[0];
        $this->fieldCount           = $parts[1];
        $this->recordSize           = $parts[2];
        $this->stringBlockSize      = $parts[3];
        $this->tableHash            = $parts[4];
        $this->layoutHash           = $parts[5];
        $this->minId                = $parts[6];
        $this->maxId                = $parts[7];
        $this->locale               = $parts[8];
        //$this->copyBlockSize        = 0;
        $this->flags                = $parts[9];
        $this->idField              = $parts[10];
        $this->totalFieldCount      = $parts[11];
        $this->bitpackedDataPos     = $parts[12];
        $this->lookupColumnCount    = $parts[13];
        //$this->indexBlockPos        = 0;
        //$this->idBlockSize          = 0;
        $this->fieldStorageInfoSize = $parts[14];
        $this->commonBlockSize      = $parts[15];
        $this->palletDataSize       = $parts[16];
        //$this->relationshipDataSize = 0;
        $this->sectionCount         = $parts[17];

        $this->hasEmbeddedStrings = ($this->flags & 1) > 0;
        $this->hasIdBlock = ($this->flags & 4) > 0;

        $eof = 0;
        $hasRelationshipData = false;
        $recordCountSum = 0;

        for ($x = 0; $x < $this->sectionCount; $x++) {
            if ($isWdc2) {
                $section = unpack('a8tactkey/Voffset/VrecordCount/VstringBlockSize/VcopyBlockSize/VindexBlockPos/VidBlockSize/VrelationshipDataSize', fread($this->fileHandle, 4 * 9));
            } else {
                $section = unpack('a8tactkey/Voffset/VrecordCount/VstringBlockSize/VindexRecordsEnd/VidBlockSize/VrelationshipDataSize/VindexBlockCount/VcopyBlockCount', fread($this->fileHandle, 4*10));
            }

            $section['tactkey'] = bin2hex($section['tactkey']);
            if (!$this->hasEmbeddedStrings) {
                $section['stringBlockPos'] = $section['offset'] + ($section['recordCount'] * $this->recordSize);
            } else {
                // Essentially set up id block to start after a non-existent string block
                if ($isWdc2) {
                    // indexBlockPos in section headers
                    $section['indexBlockSize'] = 6 * ($this->maxId - $this->minId + 1);

                    $section['stringBlockPos'] = $section['indexBlockPos'] + $section['indexBlockSize']; // indexBlockPos is absolute position in file
                } else {
                    $section['stringBlockPos'] = $section['indexRecordsEnd']; // - $section['offset'] ?
                }
                $section['stringBlockSize'] = 0;
            }

            $section['idBlockPos'] = $section['stringBlockPos'] + $section['stringBlockSize'];
            // isBlockSize in section headers

            $section['copyBlockPos'] = $section['idBlockPos'] + $section['idBlockSize'];
            if ($isWdc2) {
                // copyBlockSize in section headers
            } else {
                $section['copyBlockSize'] = $section['copyBlockCount'] * 8;
            }

            if ($isWdc2) {
                $section['relationshipDataPos'] = $section['copyBlockPos'] + $section['copyBlockSize'];
            } else {
                $section['indexBlockPos'] = $section['copyBlockPos'] + $section['copyBlockSize'];
                $section['indexBlockSize'] = $section['indexBlockCount'] * 6;

                $section['relationshipDataPos'] = $section['indexBlockPos'] + $section['indexBlockSize'];
            }
            // relationshipDataSize in section headers

            if ($isWdc2) {
                $eof += $section['size'] = $section['relationshipDataPos'] + $section['relationshipDataSize'] - $section['offset'];
            } else {
                $section['indexIdListPos'] = $section['relationshipDataPos'] + $section['relationshipDataSize'];
                $section['indexIdListSize'] = $section['indexBlockCount'] * 4;

                $eof += $section['size'] = $section['indexIdListPos'] + $section['indexIdListSize'] - $section['offset'];
            }

            $section['encrypted'] = false;
            if ($section['tactkey'] != '0000000000000000') {
                // Determine whether this section is available if it starts with any non-zero bytes
                $workingPos = ftell($this->fileHandle);
                fseek($this->fileHandle, $section['offset']);
                $section['encrypted'] = !trim(fread($this->fileHandle, min(1024, $section['size'])), "\0");
                fseek($this->fileHandle, $workingPos);
            }

            ksort($section);

            $hasRelationshipData |= $section['relationshipDataSize'] > 0;
            $recordCountSum += $section['recordCount'];

            $section['relationshipDataLookup'] = [];
            if (!$section['encrypted'] && $section['relationshipDataSize'] > 0) {
                $workingPos = ftell($this->fileHandle);
                fseek($this->fileHandle, $section['relationshipDataPos']);
                $relationshipHeader = unpack('Vcount/Vmin/Vmax', fread($this->fileHandle, 4 * 3));
                for ($relX = 0; $relX < $relationshipHeader['count']; $relX++) {
                    $relationshipData = fread($this->fileHandle, 4);
                    $section['relationshipDataLookup'][unpack('V', fread($this->fileHandle, 4))[1]] = $relationshipData;
                }
                fseek($this->fileHandle, $workingPos);
            }

            $this->sectionHeaders[] = $section;
        }

        $this->headerSize = ftell($this->fileHandle) + $this->fieldCount * 4;

        if ($this->recordCount != $recordCountSum) {
            throw new \Exception(sprintf('Expected %d records, found %d records in %d sections', $this->recordCount, $recordCountSum, $this->sectionCount));
        }

        if ($this->recordCount == 0) {
            return;
        }

        if ($this->fieldStorageInfoSize != $this->totalFieldCount * 24) {
            throw new \Exception(sprintf('Expected %d bytes for storage info, instead found %d', $this->totalFieldCount * 24, $this->fieldStorageInfoSize));
        }

        if ($this->hasEmbeddedStrings) {
            if (!$this->hasIdBlock) {
                throw new \Exception("File has embedded strings and no ID block, which was not expected, aborting");
            }
        }

        $this->fieldStorageInfoPos = $this->headerSize;

        $this->palletDataPos = $this->fieldStorageInfoPos + $this->fieldStorageInfoSize;

        $this->commonBlockPos = $this->palletDataPos + $this->palletDataSize;

        $eof += $this->commonBlockPos + $this->commonBlockSize;
        if ($eof != $this->fileSize) {
            throw new \Exception("Expected size: $eof, actual size: ".$this->fileSize);
        }

        $this->recordFormat = [];
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $this->recordFormat[$fieldId] = unpack('sbitShift/voffset', fread($this->fileHandle, 4));
            $this->recordFormat[$fieldId]['valueLength'] = max(1, (int)ceil((32 - $this->recordFormat[$fieldId]['bitShift']) / 8));
            $this->recordFormat[$fieldId]['size'] = $this->recordFormat[$fieldId]['valueLength'];
            $this->recordFormat[$fieldId]['type'] = ($this->recordFormat[$fieldId]['size'] != 4) ? static::FIELD_TYPE_INT : static::FIELD_TYPE_UNKNOWN;
            if ($this->hasEmbeddedStrings && $this->recordFormat[$fieldId]['type'] == static::FIELD_TYPE_UNKNOWN
                && !is_null($stringFields) && in_array($fieldId, $stringFields)) {
                $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_STRING;
            }
            $this->recordFormat[$fieldId]['signed'] = false;
            if ($fieldId > 0) {
                $this->recordFormat[$fieldId - 1]['valueCount'] =
                    (int)floor(($this->recordFormat[$fieldId]['offset'] - $this->recordFormat[$fieldId - 1]['offset']) / $this->recordFormat[$fieldId - 1]['valueLength']);
            }
        }

        $fieldId = $this->fieldCount - 1;
        $remainingBytes = $this->recordSize - $this->recordFormat[$fieldId]['offset'];
        $this->recordFormat[$fieldId]['valueCount'] = max(1, (int)floor($remainingBytes / $this->recordFormat[$fieldId]['valueLength']));
        if ($this->recordFormat[$fieldId]['valueCount'] > 1 &&    // we're guessing the last field is an array
            (($this->recordSize % 4 == 0 && $remainingBytes <= 4) // records may be padded to word length and the last field size <= word size
             || (!$this->hasIdBlock && $this->idField == $fieldId)// or the reported ID field is the last field
             || $this->hasEmbeddedStrings)) {                     // or we have embedded strings
            $this->recordFormat[$fieldId]['valueCount'] = 1;      // assume the last field is scalar, and the remaining bytes are just padding
        }

        $commonBlockPointer = 0;
        $palletBlockPointer = 0;

        fseek($this->fileHandle, $this->fieldStorageInfoPos);
        $storageInfoFormat = 'voffsetBits/vsizeBits/VadditionalDataSize/VstorageType/VbitpackOffsetBits/VbitpackSizeBits/VarrayCount';
        for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
            $parts = unpack($storageInfoFormat, fread($this->fileHandle, 24));

            switch ($parts['storageType']) {
                case static::FIELD_COMPRESSION_COMMON:
                    $this->recordFormat[$fieldId]['size'] = 4;
                    $this->recordFormat[$fieldId]['valueCount'] = 1;
                    $parts['defaultValue'] = pack('V', $parts['bitpackOffsetBits']);

                    $this->recordFormat[$fieldId]['type'] = static::canBeFloat($parts['bitpackOffsetBits']) ?
                        static::guessCommonFieldType($commonBlockPointer, $parts['additionalDataSize']) :
                        static::FIELD_TYPE_INT;

                    $parts['bitpackOffsetBits'] = 0;
                    $parts['blockOffset'] = $commonBlockPointer;
                    $commonBlockPointer += $parts['additionalDataSize'];
                    break;
                case static::FIELD_COMPRESSION_BITPACKED_SIGNED:
                    $this->recordFormat[$fieldId]['signed'] = true;
                    // fall through
                case static::FIELD_COMPRESSION_BITPACKED:
                    $this->recordFormat[$fieldId]['size'] = 4;
                    $this->recordFormat[$fieldId]['type'] = static::FIELD_TYPE_INT;
                    $this->recordFormat[$fieldId]['offset'] = (int)floor($parts['offsetBits'] / 8);
                    $this->recordFormat[$fieldId]['valueLength'] = (int)ceil(($parts['offsetBits'] + $parts['sizeBits']) / 8) - $this->recordFormat[$fieldId]['offset'] + 1;
                    $this->recordFormat[$fieldId]['valueCount'] = 1;
                    break;
                case static::FIELD_COMPRESSION_BITPACKED_INDEXED:
                case static::FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY:
                    $this->recordFormat[$fieldId]['size'] = static::guessPalletFieldSize($palletBlockPointer, $parts['additionalDataSize']);
                    $this->recordFormat[$fieldId]['type'] =
                        $this->recordFormat[$fieldId]['size'] == 4 ?
                            static::guessPalletFieldType($palletBlockPointer, $parts['additionalDataSize']) :
                            static::FIELD_TYPE_INT;
                    $this->recordFormat[$fieldId]['offset'] = (int)floor($parts['offsetBits'] / 8);
                    $this->recordFormat[$fieldId]['valueLength'] = (int)ceil(($parts['offsetBits'] + $parts['sizeBits']) / 8) - $this->recordFormat[$fieldId]['offset'] + 1;
                    $this->recordFormat[$fieldId]['valueCount'] = $parts['arrayCount'] > 0 ? $parts['arrayCount'] : 1;
                    $parts['blockOffset'] = $palletBlockPointer;
                    $palletBlockPointer += $parts['additionalDataSize'];
                    break;
                case static::FIELD_COMPRESSION_NONE:
                    if ($parts['arrayCount'] > 0) {
                        $this->recordFormat[$fieldId]['valueCount'] = $parts['arrayCount'];
                    }
                    break;
                default:
                    throw new \Exception(sprintf("Unknown field compression type ID: %d", $parts['storageType']));
            }

            $this->recordFormat[$fieldId]['storage'] = $parts;
        }

        if (!$this->hasIdBlock) {
            if ($this->idField >= $this->fieldCount) {
                throw new \Exception("Expected ID field " . $this->idField . " does not exist. Only found " . $this->fieldCount . " fields.");
            }
            if ($this->recordFormat[$this->idField]['valueCount'] != 1) {
                throw new \Exception("Expected ID field " . $this->idField . " reportedly has " . $this->recordFormat[$this->idField]['valueCount'] . " values per row");
            }
        }

        if ($hasRelationshipData) {
            $this->recordFormat[$this->totalFieldCount++] = [
                'valueLength' => 4,
                'size' => 4,
                'offset' => $this->recordSize,
                'type' => static::FIELD_TYPE_INT,
                'valueCount' => 1,
                'signed' => false,
                'isRelationshipData' => true,
                'storage' => [
                    'storageType' => static::FIELD_COMPRESSION_NONE
                ]
            ];
        }

        $this->populateIdMap();

        if ($this->hasEmbeddedStrings) {
            for ($fieldId = 0; $fieldId < $this->fieldCount; $fieldId++) {
                if ($this->recordFormat[$fieldId]['storage']['storageType'] != static::FIELD_COMPRESSION_NONE) {
                    throw new \Exception("DB2 with Embedded Strings has compressed field $fieldId");
                }
                unset($this->recordFormat[$fieldId]['offset']); // just to make sure we don't use them later, because they're meaningless now
            }

            $this->populateRecordOffsets();

            if (is_null($stringFields)) {
                $this->detectEmbeddedStringFields();
            }
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

    private function openHotfix(Reader $sourceReader) {
        fseek($this->fileHandle, 4);
        $parts = array_values(unpack('V2x',fread($this->fileHandle, 4 * 2)));

        $hotfixVersion          = $parts[0];
        $this->build            = $parts[1];

        if ($hotfixVersion >= 5) {
            fseek($this->fileHandle, 32, SEEK_CUR); // sha256 hash of file
        }

        $this->fieldCount       = $sourceReader->fieldCount;
        $this->totalFieldCount  = $sourceReader->totalFieldCount;
        $this->tableHash        = $sourceReader->tableHash;
        $this->layoutHash       = $sourceReader->layoutHash;
        $this->minId            = null;
        $this->maxId            = null;
        $this->locale           = $sourceReader->locale;
        $this->flags            = $sourceReader->flags | 1; // force embedded strings
        $this->idField          = $sourceReader->idField;

        $this->headerSize       = 12;

        $this->hasEmbeddedStrings = ($this->flags & 1) > 0;
        $this->hasIdBlock = ($this->flags & 4) > 0;

        $def = $sourceReader->getDBDef();
        $def = array_values(array_filter($def, function($a) { return !isset($a['annotations']['noninline']); }));

        $alternateRelationshipColumn = false;

        $this->recordFormat = $sourceReader->recordFormat;
        foreach ($this->recordFormat as $fieldId => &$fieldAttributes) {
            // Only modify the sizes of ints
            if ($fieldAttributes['type'] == self::FIELD_TYPE_INT) {
                if (isset($fieldAttributes['isRelationshipData'])) {
                    $fieldAttributes['size'] = 0;
                    if ($alternateRelationshipColumn) {
                        $fieldAttributes['alternateRelationshipColumnTarget'] = true;
                    }
                } elseif (isset($def[$fieldId]['size'])) {
                    $fieldAttributes['size'] = (int)ceil($def[$fieldId]['size'] / 8);
                    if (isset($def[$fieldId]['annotations']['relation'])) {
                        $fieldAttributes['alternateRelationshipColumnSource'] = true;
                        $alternateRelationshipColumn = true;
                    }
                } elseif (in_array($fieldAttributes['size'], [3, 4, 8])) {
                    $fieldAttributes['size'] = 4 * (int)ceil($fieldAttributes['size'] / 4);
                } elseif (in_array($fieldAttributes['size'], [1, 2])) {
                    // Assume this stays the same?
                } else {
                    throw new \Exception(
                        sprintf(
                            "Could not determine field size for field index %d in table %s (Original size %d)",
                            $fieldId,
                            str_pad(strtoupper(dechex($this->layoutHash)), 8, '0', STR_PAD_LEFT),
                            $fieldAttributes['size']
                        )
                    );
                }
            }
            $fieldAttributes['valueLength'] = $fieldAttributes['size'];

            unset($fieldAttributes['offset']); // just to make sure we don't use them later, because they're meaningless now
            unset($fieldAttributes['storage']); // no field compression in use
        }
        unset($fieldAttributes);

        $this->recordOffsets = [];
        if ($hotfixVersion < 7) {
            $recordHeaderSize = 4 * 7;
            $unpackFormat = 'a4magic/Vunk1/Vunk2/Vsize/Vtable/Vid/Vunk3';
        } else {
            $recordHeaderSize = 4 * 6;
            $unpackFormat = 'a4magic/Vunk1/Vtable/Vid/Vsize/Vunk2';
        }

        while (ftell($this->fileHandle) + $recordHeaderSize < $this->fileSize) {
            $recordHeader = unpack($unpackFormat, fread($this->fileHandle, $recordHeaderSize));
            if ($recordHeader['magic'] != 'XFTH') {
                throw new \Exception(sprintf("Missing expected XFTH record header at position %d", ftell($this->fileHandle) - $recordHeaderSize));
            }
            if ($recordHeader['size'] == 0) {
                continue;
            }
            if ($recordHeader['table'] == $this->tableHash) {
                $this->minId = is_null($this->minId) ? $recordHeader['id'] : min($this->minId, $recordHeader['id']);
                $this->maxId = is_null($this->maxId) ? $recordHeader['id'] : max($this->maxId, $recordHeader['id']);
                $this->idMap[$recordHeader['id']] = $this->recordCount;
                $this->recordOffsets[$this->recordCount] = ['pos' => ftell($this->fileHandle), 'size' => $recordHeader['size']];
                $this->recordCount++;
            }
            fseek($this->fileHandle, $recordHeader['size'], SEEK_CUR);
        }
    }

    /**
     * Fetch the table definition from https://github.com/wowdev/WoWDBDefs
     *
     * @return array
     */
    private function getDBDef() {
        $baseName = preg_replace('/\.[\w\W]*/', '', $this->fileName);
        $url = sprintf('https://raw.githubusercontent.com/wowdev/WoWDBDefs/master/definitions/%s.dbd', $baseName);

        $ch = curl_init();
        curl_setopt_array($ch, [
            CURLOPT_URL            => $url,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_MAXREDIRS      => 3,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => 8,
            CURLOPT_CONNECTTIMEOUT => 6,
            CURLOPT_ENCODING       => 'gzip',
        ]);
        $data = curl_exec($ch);
        $responseCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if (!preg_match('/^2\d\d/', $responseCode) || !$data) {
            return [];
        }

        $result = [];
        $foundLayout = false;
        $layoutHex = str_pad(strtoupper(dechex($this->layoutHash)), 8, '0', STR_PAD_LEFT);

        $lines = explode("\n", $data);
        foreach ($lines as $line) {
            if (!$foundLayout) {
                if (preg_match('/^LAYOUT /', $line)) {
                    $layouts = explode(',', str_replace(' ', '', substr($line, 7)));
                    $foundLayout = in_array($layoutHex, $layouts);
                }
            } else {
                if (!trim($line)) {
                    break;
                }
                if (preg_match('/^(?:BUILD|COMMENT) /', $line)) {
                    continue;
                }
                $col = [];
                if (preg_match('/\/\/([\w\W]+)/', $line, $res)) {
                    $col['comment'] = $res[1];
                    $line = trim(str_replace($res[0], '', $line));
                }
                if (preg_match('/\$([^\$]+)\$/', $line, $res)) {
                    $col['annotations'] = array_fill_keys(explode(',', $res[1]), true);
                    $line = trim(str_replace($res[0], '', $line));
                }
                if (preg_match('/<(u)?(\d+)>/', $line, $res)) {
                    if ($res[1]) {
                        $col['unsigned'] = true;
                    }
                    $col['size'] = $res[2];
                    $line = trim(str_replace($res[0], '', $line));
                }
                if (preg_match('/\[(\d+)\]/', $line, $res)) {
                    $col['count'] = $res[1];
                    $line = trim(str_replace($res[0], '', $line));
                }
                $col['name'] = $line;
                $result[] = $col;
            }
        }

        return $result;
    }

    private function detectEmbeddedStringFields()
    {
        $stringFields = [];
        foreach ($this->recordFormat as $fieldId => &$format) {
            if ($format['type'] != static::FIELD_TYPE_UNKNOWN || $format['size'] != 4) {
                continue;
            }

            $couldBeString = true;
            $maxLength = 0;

            $recordOffset = 0;
            while ($couldBeString && $recordOffset < $this->recordCount) {
                $data = $this->getRawRecord($recordOffset);
                if (is_null($data)) {
                    $recordOffset++;
                    continue;
                }

                $byteOffset = 0;
                for ($offsetFieldId = 0; $offsetFieldId < $fieldId; $offsetFieldId++) {
                    if ($this->recordFormat[$offsetFieldId]['type'] == static::FIELD_TYPE_STRING) {
                        for ($offsetFieldValueId = 0; $offsetFieldValueId < $this->recordFormat[$offsetFieldId]['valueCount']; $offsetFieldValueId++) {
                            $byteOffset = strpos($data, "\x00", $byteOffset);
                            if ($byteOffset === false) {
                                // should never happen, we just assigned this field as a string in a prior loop!
                                // @codeCoverageIgnoreStart
                                throw new \Exception("Could not find end of embedded string $offsetFieldId x $offsetFieldValueId in record $recordOffset");
                                // @codeCoverageIgnoreEnd
                            }
                            $byteOffset++; // skip nul byte
                        }
                    } else {
                        $byteOffset += $this->recordFormat[$offsetFieldId]['valueLength'] * $this->recordFormat[$offsetFieldId]['valueCount'];
                    }
                }
                for ($valuePosition = 0; $valuePosition < $format['valueCount']; $valuePosition++) {
                    $nextEnd = strpos($data, "\x00", $byteOffset);
                    if ($nextEnd === false) {
                        // only happens in last field in a record, which probably isn't an embedded string anyway
                        $couldBeString = false;
                        break;
                    }
                    $testLength = $nextEnd - $byteOffset;
                    $stringToTest = substr($data, $byteOffset, $testLength);
                    $maxLength = max($maxLength, $testLength);
                    $byteOffset = $nextEnd + 1;
                    if ($testLength > 0 && mb_detect_encoding($stringToTest, 'UTF-8', true) === false) {
                        $couldBeString = false;
                    }
                }
                $recordOffset++;
            }
            if ($couldBeString && ($maxLength > 2 || in_array($fieldId - 1, $stringFields))) { // string fields tend to group together, with any empties towards the end
                $stringFields[] = $fieldId;
                $format['type'] = static::FIELD_TYPE_STRING;
            }
        }
        unset($format);
    }

    private function guessFieldTypes() {
        foreach ($this->recordFormat as $fieldId => &$format) {
            if ($format['type'] != static::FIELD_TYPE_UNKNOWN || $format['size'] != 4) {
                continue;
            }

            $couldBeFloat = true;
            $couldBeString = !$this->hasEmbeddedStrings;
            $recordOffset = 0;
            $distinctValues = [];

            while (($couldBeString || $couldBeFloat) && $recordOffset < $this->recordCount) {
                $data = $this->getRawRecord($recordOffset);
                if (is_null($data)) {
                    $recordOffset++;
                    continue;
                }
                if ($this->sectionCount) {
                    $sectionRecordsSkipped = 0;
                    for ($sectionId = 0; $sectionId < $this->sectionCount; $sectionId++) {
                        if ($recordOffset - $sectionRecordsSkipped < $this->sectionHeaders[$sectionId]['recordCount']) {
                            break;
                        }
                        $sectionRecordsSkipped += $this->sectionHeaders[$sectionId]['recordCount'];
                    }
                }

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
                $values = array_values(unpack('V*', $data));
                foreach ($values as $valueId => $value) {
                    if ($value == 0) {
                        continue; // can't do much with this
                    }
                    if ($couldBeString) {
                        if ($this->sectionCount) {
                            // Start with our odd offset
                            $stringPos = $value;
                            // Move back to first value of field
                            $stringPos += 4 * $valueId;
                            // Move back to start of row
                            $stringPos += $byteOffset;
                            // Move back to start of first record
                            $stringPos += $recordOffset * $this->recordSize;
                            // Advance past all data records
                            $stringPos -= $this->recordCount * $this->recordSize;

                            if ($stringPos < 0 || $stringPos >= $this->stringBlockSize) {
                                $couldBeString = false;
                            } else {
                                try {
                                    $sectionPos = $stringPos;
                                    $stringPos  = $this->getStringFileOffset($sectionPos);
                                } catch (\Exception $e) {
                                    $couldBeString = false;
                                }
                            }
                        } else {
                            $stringPos = $this->stringBlockPos + $value;
                            if ($value >= $this->stringBlockSize) {
                                $couldBeString = false;
                            }
                        }
                        if (count($distinctValues) < static::DISTINCT_STRINGS_REQUIRED) {
                            $distinctValues[$value] = true;
                        }
                        if ($couldBeString && (!$this->sectionCount || $sectionPos > 0)) {
                            // offset should be the start of a string
                            // so the char immediately before should be the null terminator of the prev string
                            fseek($this->fileHandle, $stringPos - 1);
                            if (fread($this->fileHandle, 1) !== "\x00") {
                                $couldBeString = false;
                            }
                        }
                    }
                    if ($couldBeFloat) {
                        $couldBeFloat = static::canBeFloat($value);
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

    private function guessCommonFieldType($commonBlockOffset, $commonBlockSize) {
        $oldPos = ftell($this->fileHandle);

        fseek($this->fileHandle, $this->commonBlockPos + $commonBlockOffset);

        $couldBeFloat = true;
        for ($x = 0; $x < $commonBlockSize; $x += $chunkSize) {
            $chunkSize = min(256, $commonBlockSize - $x);
            if ($chunkSize < 4) {
                break;
            }

            $values = array_values(unpack('V*', fread($this->fileHandle, $chunkSize)));
            foreach ($values as $index => $value) {
                if ($index % 2 === 1) {
                    if (!static::canBeFloat($value)) {
                        $couldBeFloat = false;
                        break 2;
                    }
                }
            }
        }

        fseek($this->fileHandle, $oldPos);

        return $couldBeFloat ? static::FIELD_TYPE_FLOAT : static::FIELD_TYPE_INT;
    }

    private function guessPalletFieldType($palletBlockOffset, $palletBlockSize) {
        $oldPos = ftell($this->fileHandle);

        fseek($this->fileHandle, $this->palletDataPos + $palletBlockOffset);

        $couldBeFloat = true;
        for ($x = 0; $x < $palletBlockSize; $x += $chunkSize) {
            $chunkSize = min(64, $palletBlockSize - $x);
            if ($chunkSize < 4) {
                break;
            }

            $values = array_values(unpack('V*', fread($this->fileHandle, $chunkSize)));
            foreach ($values as $value) {
                if (!static::canBeFloat($value)) {
                    $couldBeFloat = false;
                    break 2;
                }
            }
        }

        fseek($this->fileHandle, $oldPos);

        return $couldBeFloat ? static::FIELD_TYPE_FLOAT : static::FIELD_TYPE_INT;
    }

    private function guessPalletFieldSize($palletBlockOffset, $palletBlockSize) {
        $oldPos = ftell($this->fileHandle);

        fseek($this->fileHandle, $this->palletDataPos + $palletBlockOffset);

        // each value in the pallet takes up 4 bytes, but sometimes fewer bytes represent the value and the rest are junk
        // this tries to guess how many bytes represent the value by seeing what junk bytes exist in all pallet locations

        // note: this will be incorrect if all the values in the pallet share the same upper non-zero bytes

        $first = str_split(fread($this->fileHandle, 4));
        $sameCount = 3;
        for ($x = 4; $x < $palletBlockSize; $x += 4) {
            $test = str_split(fread($this->fileHandle, 4));
            for ($y = 3; $y >= 4 - $sameCount; $y--) {
                if ($test[$y] != $first[$y]) {
                    $sameCount = 3 - $y;
                    break;
                }
            }
            if ($sameCount == 0) {
                break;
            }
        }

        fseek($this->fileHandle, $oldPos);

        return 4 - $sameCount;
    }

    private function populateIdMap() {
        $this->idMap = [];
        if (!$this->hasIdBlock) {
            $this->recordFormat[$this->idField]['signed'] = false; // in case it's a 32-bit int

            $idOffset = !isset($this->recordFormat[$this->idField]['storage']) || $this->recordFormat[$this->idField]['storage']['storageType'] == static::FIELD_COMPRESSION_NONE;
            if ($idOffset) {
                $idOffset = $this->recordFormat[$this->idField]['offset'];
            }

            $sectionCount = $this->sectionCount ?: 1;

            $recIndex = 0;
            $recordCount = $this->recordCount;

            for ($z = 0; $z < $sectionCount; $z++) {
                if ($this->sectionCount) {
                    $recordCount = $this->sectionHeaders[$z]['recordCount'];
                    if ($this->sectionHeaders[$z]['encrypted']) {
                        $recIndex += $recordCount;
                        continue;
                    }
                }
                if ($idOffset !== false) {
                    // attempt shortcut so we don't have to parse the whole record

                    if ($this->sectionCount) {
                        fseek($this->fileHandle, $this->sectionHeaders[$z]['offset'] + $idOffset);
                    } else {
                        fseek($this->fileHandle, $this->headerSize + $idOffset);
                    }

                    for ($x = 0; $x < $recordCount; $x++) {
                        $id = unpack('V', str_pad(fread($this->fileHandle, $this->recordFormat[$this->idField]['size']), 4, "\x00", STR_PAD_RIGHT))[1];
                        $this->idMap[$id] = $recIndex++;
                        fseek($this->fileHandle, $this->recordSize - $this->recordFormat[$this->idField]['size'], SEEK_CUR); // subtract for the bytes we just read
                    }
                } else {
                    for ($x = 0; $x < $recordCount; $x++) {
                        $rec = $this->getRecordByOffset($recIndex, false);
                        $id  = $rec[$this->idField];
                        $this->idMap[$id] = $recIndex++;
                    }
                }
            }
        } else {
            if ($this->sectionCount) {
                $recIndex = 0;
                for ($z = 0; $z < $this->sectionCount; $z++) {
                    if ($this->sectionHeaders[$z]['encrypted']) {
                        $recIndex += $this->sectionHeaders[$z]['recordCount'];
                        continue;
                    }
                    fseek($this->fileHandle, $this->sectionHeaders[$z]['idBlockPos']);
                    for ($x = 0; $x < $this->sectionHeaders[$z]['recordCount']; $x++) {
                        $this->idMap[unpack('V', fread($this->fileHandle, 4))[1]] = $recIndex++;
                    }
                }
            } else {
                fseek($this->fileHandle, $this->idBlockPos);
                if ($this->fileFormat == 'WDB2') {
                    for ($x = $this->minId; $x <= $this->maxId; $x++) {
                        $record = unpack('V', fread($this->fileHandle, 4))[1];
                        if ($record) {
                            $this->idMap[$x] = $record - 1;
                        }
                        fseek($this->fileHandle, 2, SEEK_CUR); // ignore embed string length in this record
                    }
                } else {
                    for ($x = 0; $x < $this->recordCount; $x++) {
                        $this->idMap[unpack('V', fread($this->fileHandle, 4))[1]] = $x;
                    }
                }
            }
        }

        $sections = $this->sectionHeaders;
        if (!$sections) {
            $sections = [[
                'copyBlockSize' => $this->copyBlockSize,
                'copyBlockPos' => $this->copyBlockPos,
            ]];
        }

        foreach ($sections as &$section) {
            if ($section['copyBlockSize'] && (!isset($section['encrypted']) || !$section['encrypted'])) {
                fseek($this->fileHandle, $section['copyBlockPos']);
                $entryCount = (int)floor($section['copyBlockSize'] / 8);
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
        unset($section);
    }

    private function populateRecordOffsets() {
        // only required when hasEmbeddedStrings,
        // since it has the index block to map back into the data block

        $idLists = [];
        foreach ($this->sectionHeaders as $sectionId => $section) {
            if (isset($section['indexIdListSize']) && $section['indexIdListSize'] && !$section['encrypted']) {
                fseek($this->fileHandle, $section['indexIdListPos']);
                $idLists[$sectionId] = array_values(unpack('V*', fread($this->fileHandle, $section['indexIdListSize'])));
            }
        }

        $this->recordOffsets = [];
        if (!$idLists) {
            if ($this->hasIdsInIndexBlock) {
                $this->idMap = [];
                $idLists[0] = range(0, $this->recordCount - 1);
            } else {
                $idLists[0] = range($this->minId, $this->maxId);
            }
        }
        foreach ($idLists as $sectionId => $idList) {
            $pos = $this->indexBlockPos;
            if (isset($this->sectionHeaders[$sectionId]['indexBlockPos'])) {
                $pos = $this->sectionHeaders[$sectionId]['indexBlockPos'];
            }
            fseek($this->fileHandle, $pos);
            foreach ($idList as $x) {
                if ($this->hasIdsInIndexBlock) {
                    $pointer = unpack('Vid/Vpos/vsize', fread($this->fileHandle, 10));
                    $this->idMap[$pointer['id']] = $x;
                } else {
                    $pointer = unpack('Vpos/vsize', fread($this->fileHandle, 6));
                    $pointer['id'] = $x;
                }
                if ($pointer['size'] > 0 && isset($this->idMap[$pointer['id']])) {
                    $this->recordOffsets[$this->idMap[$pointer['id']]] = $pointer;
                }
            }
        }

        ksort($this->idMap);
    }

    private function findCommonFields() {
        // WDB6 only

        $this->commonLookup = [];
        if ($this->commonBlockSize == 0) {
            return;
        }

        $commonBlockEnd = $this->commonBlockPos + $this->commonBlockSize;

        fseek($this->fileHandle, $this->commonBlockPos);
        $fieldCount = unpack('V', fread($this->fileHandle, 4))[1];
        if ($fieldCount != $this->totalFieldCount) {
            throw new \Exception(sprintf("Expected %d fields in common block, found %d", $this->totalFieldCount, $fieldCount));
        }

        // determine whether each table entry data is stored in 4 bytes (7.3 onward), or fewer bytes depending on type (pre-7.3)
        $fourBytesEveryType = true;
        for ($field = 0; $field < $this->totalFieldCount; $field++) {
            list($entryCount, $enumType) = array_values(unpack('V1x/C1y', fread($this->fileHandle, 5)));
            $mapSize = 8 * $entryCount;

            if (($enumType > 4) ||
                ($entryCount > $this->recordCount) ||
                (ftell($this->fileHandle) + $mapSize + ($field + 1 < $this->totalFieldCount ? 5 : 0) > $commonBlockEnd)) {
                $fourBytesEveryType = false;
                break;
            }
            fseek($this->fileHandle, $mapSize, SEEK_CUR); // skip this field's data, continue to the next
        }
        $fourBytesEveryType &= $commonBlockEnd - ftell($this->fileHandle) <= 8; // expect to be near the end of the common block if our assumptions held

        fseek($this->fileHandle, $this->commonBlockPos + 4); // return to first table entry
        for ($field = 0; $field < $this->totalFieldCount; $field++) {
            list($entryCount, $enumType) = array_values(unpack('V1x/C1y', fread($this->fileHandle, 5)));
            if ($field < $this->fieldCount) {
                if ($entryCount > 0) {
                    throw new \Exception(sprintf("Expected 0 entries in common block field %d, instead found %d", $field, $entryCount));
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
                    throw new \Exception("Unknown common field type: $enumType");
            }

            $this->recordFormat[$field] = [
                'valueCount'  => 1,
                'valueLength' => $size,
                'size'        => $size,
                'type'        => $type,
                'signed'      => false,
                'zero'        => str_repeat("\x00", $size),
            ];
            $this->commonLookup[$field] = [];

            $embeddedStrings = false;
            if ($this->hasEmbeddedStrings && $type == Reader::FIELD_TYPE_STRING) {
                // @codeCoverageIgnoreStart
                // file with both embedded strings and common block not found in wild, this is just a guess
                $embeddedStrings = true;
                $this->recordFormat[$field]['zero'] = "\x00";
                // @codeCoverageIgnoreEnd
            }

            for ($entry = 0; $entry < $entryCount; $entry++) {
                $id = unpack('V', fread($this->fileHandle, 4))[1];
                if ($embeddedStrings) {
                    // @codeCoverageIgnoreStart
                    // file with both embedded strings and common block not found in wild, this is just a guess
                    $maxLength = $this->commonBlockSize - (ftell($this->fileHandle) - $this->commonBlockPos);
                    $this->commonLookup[$field][$id] = stream_get_line($this->fileHandle, $maxLength, "\x00") . "\x00";
                    // @codeCoverageIgnoreEnd
                } else {
                    $this->commonLookup[$field][$id] = ($fourBytesEveryType && $size != 4) ?
                        substr(fread($this->fileHandle, 4), 0, $size) :
                        fread($this->fileHandle, $size);
                }
            }
        }
    }

    // general purpose for internal use

    private function getRawRecord($recordOffset, $id = false) {
        $relationshipRecordSize = 8;
        $relationshipDataSize = $this->relationshipDataSize;
        $relationshipDataPos = $this->relationshipDataPos;
        $relationshipDataLookup = null;
        $relationshipOffset = $recordOffset * $relationshipRecordSize + 12;
        $recordOffsetInSection = $recordOffset;

        if (!is_null($this->recordOffsets)) {
            if (!isset($this->recordOffsets[$recordOffset])) {
                // This is probably in an encrypted section. Double-check.
                if ($this->sectionCount) {
                    $offsetSearch = 0;
                    foreach ($this->sectionHeaders as $sectionHeader) {
                        if ($recordOffset - $offsetSearch >= $sectionHeader['recordCount']) {
                            $offsetSearch += $sectionHeader['recordCount'];
                            continue;
                        }
                        if ($sectionHeader['encrypted']) {
                            // As we expected.
                            return null;
                        }
                    }
                }
                throw new \Exception("Requested record offset $recordOffset which was not defined");
            }
            $pointer = $this->recordOffsets[$recordOffset];
            if ($pointer['size'] == 0) {
                // @codeCoverageIgnoreStart
                throw new \Exception("Requested record offset $recordOffset which is empty");
                // @codeCoverageIgnoreEnd
            }
            fseek($this->fileHandle, $pointer['pos']);
            $data = fread($this->fileHandle, $pointer['size']);
        } else {
            if ($this->sectionCount) {
                $offsetSearch = 0;
                foreach ($this->sectionHeaders as $sectionHeader) {
                    if ($recordOffset - $offsetSearch >= $sectionHeader['recordCount']) {
                        $offsetSearch += $sectionHeader['recordCount'];
                        $relationshipOffset -= $relationshipRecordSize * $sectionHeader['recordCount'];
                        $recordOffsetInSection -= $sectionHeader['recordCount'];
                        continue;
                    }

                    if ($sectionHeader['encrypted']) {
                        return null;
                    }

                    $relationshipDataSize = $sectionHeader['relationshipDataSize'];
                    $relationshipDataPos = $sectionHeader['relationshipDataPos'];
                    $relationshipDataLookup = $sectionHeader['relationshipDataLookup'];

                    fseek($this->fileHandle, $sectionHeader['offset'] + ($recordOffset - $offsetSearch) * $this->recordSize);
                    $offsetSearch = false;
                    break;
                }
                if ($offsetSearch !== false) {
                    throw new \Exception("Could not find record offset $recordOffset in {$this->recordCount} records");
                }
            } else {
                fseek($this->fileHandle, $this->headerSize + $recordOffset * $this->recordSize);
            }
            $data = fread($this->fileHandle, $this->recordSize);
        }

        if ($this->fileFormat == 'WDB6' && $id !== false && $this->commonBlockSize) {
            // must crop off any padding at end of standard record before appending common block fields
            $lastFieldFormat = $this->recordFormat[$this->fieldCount - 1];
            $data = substr($data, 0, $lastFieldFormat['offset'] + $lastFieldFormat['valueLength']);

            foreach ($this->commonLookup as $field => $lookup) {
                if (isset($lookup[$id])) {
                    $data .= $lookup[$id];
                } else {
                    $data .= $this->recordFormat[$field]['zero'];
                }
            }
        }

        if ($relationshipDataSize) {
            if (isset($relationshipDataLookup)) {
                // Preferred path, use lookup build during header read.
                $indexInSection = $recordOffset - $offsetSearch;
                if (isset($relationshipDataLookup[$indexInSection])) {
                    $data .= $relationshipDataLookup[$indexInSection];
                } else {
                    $data .= "\0\0\0\0";
                }
            } else {
                // Legacy path.
                if ($relationshipOffset >= $relationshipDataSize) {
                    throw new \Exception(sprintf("Attempted to read from offset %d in relationship map, size is only %d",
                        $relationshipOffset, $relationshipDataSize));
                }

                fseek($this->fileHandle, $relationshipDataPos + $relationshipOffset);
                $data .= fread($this->fileHandle, 4);

                $relationshipOffset = unpack('V', fread($this->fileHandle, 4))[1];
                if ($relationshipOffset != $recordOffsetInSection) {
                    throw new \Exception(sprintf("Record offset %d (section offset %d) attempted read of relationship offset %d",
                        $recordOffset, $recordOffsetInSection, $relationshipOffset));
                }
            }
        }
        return $data;
    }

    // returns signed 32-bit int from little endian
    private function extractValueFromBitstring($bitString, $bitOffset, $bitLength, $extendSign) {
        if ($bitOffset >= 8) {
            $bitString = substr($bitString, (int)floor($bitOffset / 8));
            $bitOffset &= 7;
        }

        $gmp = gmp_import($bitString, 1, GMP_LSW_FIRST | GMP_LITTLE_ENDIAN);

        $mask = ((gmp_init(1) << $bitLength) - 1);

        $gmp = gmp_and($gmp >> $bitOffset, $mask);

        // This is a signed field, and the msb is set, so it's negative. Flip the sign.
        if ($extendSign && gmp_and($gmp, 1 << ($bitLength - 1)) > 0) {
            $gmp = $gmp - $mask - 1;
        }

        return gmp_intval($gmp);
    }

    private function getPalletData($storage, $palletId, $valueId) {
        $recordSize = 4;
        $isArray = $storage['storageType'] == static::FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY;
        if ($isArray) {
            $recordSize *= $storage['arrayCount'];
        }

        $offset = $storage['blockOffset'] + $palletId * $recordSize + $valueId * 4;
        if ($offset > $this->palletDataSize) {
            throw new \Exception(sprintf("Requested pallet data offset %d which is beyond pallet data size %d", $offset, $this->palletDataSize));
        }
        fseek($this->fileHandle, $this->palletDataPos + $offset);

        return fread($this->fileHandle, 4);
    }

    private function getCommonData($fieldId, $id) {
        // WDC1 and up

        if (!isset($this->recordFormat[$fieldId]['commonCache'])) {
            $this->recordFormat[$fieldId]['commonCache'] = [];

            fseek($this->fileHandle, $this->commonBlockPos + $this->recordFormat[$fieldId]['storage']['blockOffset']);
            $numCommonRecs = (int)floor($this->recordFormat[$fieldId]['storage']['additionalDataSize'] / 8) - 1;

            for ($x = 0; $x < $numCommonRecs; $x++) {
                $commonId = unpack('V', fread($this->fileHandle, 4))[1];
                $this->recordFormat[$fieldId]['commonCache'][$commonId] = fread($this->fileHandle, 4);
            }
        }

        return isset($this->recordFormat[$fieldId]['commonCache'][$id]) ?
            $this->recordFormat[$fieldId]['commonCache'][$id] :
            $this->recordFormat[$fieldId]['storage']['defaultValue'];
    }

    /**
     * Given an offset into the combined string block across all sections, return the offset in this file to reach
     * that string. $stringBlockOffset is modified to the offset within the found string block, and $foundSectionId
     * returns the section ID where that string block is.
     *
     * @param int $stringBlockOffset
     * @param int|null $foundSectionId
     * @return int
     */
    private function getStringFileOffset(&$stringBlockOffset, &$foundSectionId = null) {
        foreach ($this->sectionHeaders as $sectionId => $section) {
            if ($stringBlockOffset < $section['stringBlockSize']) {
                $foundSectionId = $sectionId;
                return $section['stringBlockPos'] + $stringBlockOffset;
            }
            $stringBlockOffset -= $section['stringBlockSize'];
        }

        throw new \Exception("Searched past all string blocks");
    }

    private function getString($stringBlockOffset, $sectionId) {
        $stringBlockSize = $this->stringBlockSize;
        $stringBlockPos = $this->stringBlockPos;

        if ($sectionId >= 0) {
            $stringBlockSize = $this->sectionHeaders[$sectionId]['stringBlockSize'];
            $stringBlockPos = $this->sectionHeaders[$sectionId]['stringBlockPos'];
        }

        if ($stringBlockOffset >= $stringBlockSize) {
            // @codeCoverageIgnoreStart
            throw new \Exception("Asked to get string from $stringBlockOffset, string block size is only $stringBlockSize");
            // @codeCoverageIgnoreEnd
        }
        $maxLength = $stringBlockSize - $stringBlockOffset;

        fseek($this->fileHandle, $stringBlockPos + $stringBlockOffset);
        return stream_get_line($this->fileHandle, $maxLength, "\x00");
    }

    private function getRecordByOffset($recordOffset, $id) {
        if ($recordOffset < 0 || $recordOffset >= $this->recordCount) {
            // @codeCoverageIgnoreStart
            throw new \Exception("Requested record offset $recordOffset out of bounds: 0-".$this->recordCount);
            // @codeCoverageIgnoreEnd
        }

        $record = $this->getRawRecord($recordOffset, $id);
        if (is_null($record)) {
            throw new \Exception("Trying to read null record $recordOffset");
        }
        $sectionId = -1;
        $sectionRecordsSkipped = 0;
        if ($this->sectionCount) {
            for ($sectionId = 0; $sectionId < $this->sectionCount; $sectionId++) {
                if ($recordOffset - $sectionRecordsSkipped <= $this->sectionHeaders[$sectionId]['recordCount']) {
                    break;
                }
                $sectionRecordsSkipped += $this->sectionHeaders[$sectionId]['recordCount'];
            }
        }

        $fieldMax = $id === false ? $this->fieldCount : $this->totalFieldCount; // do not search wdb6 common table when we don't know IDs yet

        $relationshipValue = null;
        $runningOffset = 0;
        $row = [];
        for ($fieldId = 0; $fieldId < $fieldMax; $fieldId++) {
            $field = [];
            $format = $this->recordFormat[$fieldId];
            for ($valueId = 0; $valueId < $format['valueCount']; $valueId++) {
                if (isset($format['storage']) && !$this->hasEmbeddedStrings) {
                    switch ($format['storage']['storageType']) {
                        case static::FIELD_COMPRESSION_BITPACKED:
                        case static::FIELD_COMPRESSION_BITPACKED_INDEXED:
                        case static::FIELD_COMPRESSION_BITPACKED_INDEXED_ARRAY:
                        case static::FIELD_COMPRESSION_BITPACKED_SIGNED:
                            $rawValue = static::extractValueFromBitstring(
                                substr($record, $format['offset'], $format['valueLength']),
                                $format['storage']['offsetBits'] % 8,
                                $format['storage']['sizeBits'],
                                $format['storage']['storageType'] == static::FIELD_COMPRESSION_BITPACKED_SIGNED
                            );

                            if ($format['storage']['storageType'] == static::FIELD_COMPRESSION_BITPACKED ||
                                $format['storage']['storageType'] == static::FIELD_COMPRESSION_BITPACKED_SIGNED) {
                                $field[] = $rawValue;
                                continue 2; // we're done, rawvalue is actual value
                            }

                            $rawValue = substr($this->getPalletData($format['storage'], $rawValue, $valueId), 0, $format['size']);
                            break;

                        case static::FIELD_COMPRESSION_COMMON:
                            $rawValue = $this->getCommonData($fieldId, $id);
                            break;

                        case static::FIELD_COMPRESSION_NONE:
                            $rawValue = substr($record, $format['offset'] + $valueId * $format['valueLength'], $format['valueLength']);
                            break;

                        default:
                            throw new \Exception(sprintf("Field %d has an unknown storage type: %d", $fieldId, $format['storage']['storageType']));
                    }
                } else {
                    if ($this->hasEmbeddedStrings && $format['type'] == static::FIELD_TYPE_STRING) {
                        $rawValue = substr($record, $runningOffset,
                            strpos($record, "\x00", $runningOffset) - $runningOffset);
                        $runningOffset += strlen($rawValue) + 1;
                        $field[] = $rawValue;
                        continue;
                    } else {
                        $rawValue = substr($record, $runningOffset, $format['valueLength']);
                        $runningOffset += $format['valueLength'];
                    }
                }

                switch ($format['type']) {
                    case static::FIELD_TYPE_UNKNOWN:
                    case static::FIELD_TYPE_INT:
                        if ($format['signed']) {
                            switch ($format['size']) {
                                case 8:
                                    $field[] = unpack('q', $rawValue)[1];
                                    break;
                                case 4:
                                    $field[] = unpack('l', $rawValue)[1];
                                    break;
                                case 3:
                                    $field[] = unpack('l', $rawValue . (ord(substr($rawValue, -1)) & 0x80 ? "\xFF" : "\x00"))[1];
                                    break;
                                case 2:
                                    $field[] = unpack('s', $rawValue)[1];
                                    break;
                                case 1:
                                    $field[] = unpack('c', $rawValue)[1];
                                    break;
                                case 0:
                                    $field[] = 0;
                                    break;
                            }
                        } else {
                            if ($format['size'] == 8) {
                                $field[] = unpack('P', $rawValue)[1];
                            } else {
                                if ($format['size'] < 4) {
                                    $rawValue = str_pad($rawValue, 4, "\x00", STR_PAD_RIGHT);
                                }
                                $field[] = unpack('V', $rawValue)[1];
                            }
                        }
                        break;
                    case static::FIELD_TYPE_FLOAT:
                        $field[] = round(unpack('f', $rawValue)[1], 6);
                        break;
                    case static::FIELD_TYPE_STRING:
                        $stringPos = unpack('V', $rawValue)[1];
                        $stringSection = -1;

                        if ($sectionId >= 0) {
                            // Move back to first value of field
                            $stringPos += 4 * $valueId;
                            // Move back to start of row
                            $stringPos += $format['offset'];
                            // Move back to start of first record
                            $stringPos += $recordOffset * $this->recordSize;
                            // Advance past all data records
                            $stringPos -= $this->recordCount * $this->recordSize;

                            // Modify stringPos to offset within correct string block, and get section of that block
                            $this->getStringFileOffset($stringPos, $stringSection);
                        }

                        $field[] = $this->getString($stringPos, $stringSection);
                        break;
                }
            }
            if ($valueId == 1) {
                $field = $field[0];
            }
            if (isset($format['alternateRelationshipColumnSource'])) {
                $relationshipValue = $field;
            } elseif (isset($format['alternateRelationshipColumnTarget']) && !is_null($relationshipValue)) {
                $field = $relationshipValue;
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

    public function loadDBCache($dbCachePath) {
        return new Reader($dbCachePath, $this);
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

    private static function canBeFloat($value) {
        if ($value == 0) {
            return true;
        }

        $exponent = ($value >> 23) & 0xFF;
        if ($exponent === 0 || $exponent === 0xFF) {
            return false;
        } else {
            $asFloat = unpack('f', pack('V', $value))[1];
            if (abs($asFloat) > 1e19 || round($asFloat, 6) === 0.0) {
                return false;
            }
        }

        return true;
    }
}
