<?php

use Erorus\DB2\Reader;

class ReaderTest extends phpunit\framework\TestCase
{
    const WDB2_PATH = __DIR__.'/wdb2';
    const WDB5_PATH = __DIR__.'/wdb5';

    public function testInvalidDB2Param()
    {
        try {
            $reader = new Reader(0);
            $this->fail("Did not throw exception on invalid db2 param");
        } catch (Exception $e) {
            $this->assertEquals("Must supply path to DB2 file", $e->getMessage());
        }
    }

    public function testMissingFile()
    {
        $db2path = '/dev/null/notfound.db2';

        try {
            $reader = new Reader($db2path);
            $this->fail("Did not throw exception on missing file");
        } catch (Exception $e) {
            $this->assertEquals("Error opening $db2path", $e->getMessage());
        }
    }

    public function testInvalidFileFormat()
    {
        $format = substr(file_get_contents(static::WDB5_PATH.'/BadFormat.db2'), 0, 4);

        try {
            $reader = new Reader(static::WDB5_PATH.'/BadFormat.db2');
            $this->fail("Did not throw exception on unknown file format");
        } catch (Exception $e) {
            $this->assertEquals("Unknown DB2 format: $format", $e->getMessage());
        }
    }

    public function testWDB5FormatLoad()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdBlock2.db2');
        $this->assertEquals(1, $reader->getFieldCount());
    }

    public function testFileTooLong()
    {
        try {
            $reader = new Reader(static::WDB5_PATH.'/TooLong.db2');
            $this->fail("Did not notice db2 file was too long");
        } catch (Exception $e) {
            $this->assertRegExp('/^Expected size: \d+, actual size: '.filesize(static::WDB5_PATH.'/TooLong.db2').'$/', $e->getMessage());
        }
    }

    public function testFileTooShort()
    {
        try {
            $reader = new Reader(static::WDB5_PATH.'/TooShort.db2');
            $this->fail("Did not notice db2 file was too short");
        } catch (Exception $e) {
            $this->assertRegExp('/^Expected size: \d+, actual size: '.filesize(static::WDB5_PATH.'/TooShort.db2').'$/', $e->getMessage());
        }
    }

    public function testLoadRecordFromIDBlock()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdBlock.db2');
        $rec = $reader->getRecord(100);
        $this->assertEquals(200, $rec[0]);
    }

    public function testSignedByte()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');

        $ret = $reader->setFieldsSigned([true]);
        $this->assertNotFalse($ret[0]);

        $rec = $reader->getRecord(100);
        $this->assertEquals(-56, $rec[0]);

        $ret = $reader->setFieldsSigned([false]);
        $this->assertFalse($ret[0]);

        $rec = $reader->getRecord(100);
        $this->assertEquals(200, $rec[0]);
    }

    public function testSignedFieldOutOfBounds()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');

        try {
            $reader->setFieldsSigned([false, true]);
            $this->fail("Did not flag field 1 as out of bounds");
        } catch (Exception $e) {
            $this->assertEquals("Field ID 1 out of bounds: 0-0", $e->getMessage());
        }
    }

    public function testFieldNames()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');

        $ret = $reader->setFieldNames(['value']);
        $this->assertEquals('value', $ret[0]);

        $rec = $reader->getRecord(100);
        $this->assertEquals(200, $rec['value']);

        $ret = $reader->setFieldNames([false]);
        $this->assertEquals(0, count($ret));

        $rec = $reader->getRecord(100);
        $this->assertEquals(200, $rec[0]);
    }

    public function testFieldNameInvalidIndex()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');

        try {
            $reader->setFieldNames(['abc' => 'abc']);
            $this->fail("Did not flag field 'abc' as invalid");
        } catch (Exception $e) {
            $this->assertEquals("Field ID abc must be numeric", $e->getMessage());
        }
    }

    public function testFieldNameNumeric()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');

        try {
            $reader->setFieldNames([99]);
            $this->fail("Did not flag field name 99 as numeric");
        } catch (Exception $e) {
            $this->assertEquals("Field 0 Name (99) must NOT be numeric", $e->getMessage());
        }
    }

    public function testFieldOutOfBounds()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');

        try {
            $reader->setFieldNames([99=>'test']);
            $this->fail("Did not flag field 99 as out of bounds");
        } catch (Exception $e) {
            $this->assertEquals("Field ID 99 out of bounds: 0-0", $e->getMessage());
        }
    }

    public function testUnknownRecordID()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdBlock.db2');
        $rec = $reader->getRecord(999);

        $this->assertNotFalse(is_null($rec));
    }

    public function testIDList()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdBlock2.db2');
        $allIDs = $reader->getIds();

        $this->assertEquals([100,150], $allIDs);
    }

    public function testRecordIterator()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdBlock2.db2');
        $reader->setFieldNames(['value']);
        $reader->setFieldsSigned([true]);

        $recordCount = 0;
        foreach ($reader->generateRecords() as $id => $rec) {
            if (++$recordCount > 2) {
                break;
            }
            switch ($id) {
                case 100:
                    $this->assertEquals(["value" => -56], $rec);
                    break;
                case 150:
                    $this->assertEquals(["value" => -6], $rec);
                    break;
                default:
                    $this->fail("Returned unknown ID $id from IdBlock2.db2");
                    break;
            }
        }
        if ($recordCount != 2) {
            $this->fail("Returned $recordCount records instead of 2 from IdBlock2.db2 in iterator");
        }
    }

    public function testFlattenString()
    {
        try {
            $result = Reader::flattenRecord("abc");
            $this->fail("Returned something when trying to flatten string abc");
        } catch (TypeError $e) {
            $this->assertFalse(strpos($e->getMessage(), 'must be of the type array, string given') === false);
        }
    }

    public function testFlattenNormalRecord()
    {
        $result = Reader::flattenRecord([2,4,1,3]);
        $this->assertEquals([2,4,1,3], $result);
    }

    public function testFlattenNamedRecord()
    {
        $result = Reader::flattenRecord(['b' => 2, 'd' => 4, 'a' => 1, 'c' => 3]);
        $this->assertEquals(['b' => 2, 'd' => 4, 'a' => 1, 'c' => 3], $result);
    }

    public function testFlattenRecordWithArrays()
    {
        $result = Reader::flattenRecord([2,[10,20],1,[40,30]]);
        $this->assertEquals(["0" => 2, "1-0" => 10, "1-1" => 20, "2" => 1, "3-0" => 40, "3-1" => 30], $result);
    }

    public function testFieldTypeDetection()
    {
        $reader = new Reader(static::WDB5_PATH.'/FieldTypes.db2');

        $reader->setFieldsSigned([false,false,false,false]);

        $rec = $reader->getRecord(100);
        $this->assertEquals(10,         $rec[0]); // 1-byte
        $this->assertEquals(2000,       $rec[1]); // 2-byte
        $this->assertEquals(200000,     $rec[2]); // 3-byte
        $this->assertEquals(10,         $rec[3]); // 4-byte
        $this->assertEquals(2.5,        $rec[4]); // float
        $this->assertEquals('Test',     $rec[5]); // string

        $rec = $reader->getRecord(150);
        $this->assertEquals(250,        $rec[0]);
        $this->assertEquals(65000,      $rec[1]);
        $this->assertEquals(9000000,    $rec[2]);
        $this->assertEquals(2500000000, $rec[3]);
        $this->assertEquals(-2.5,       $rec[4]);
        $this->assertEquals('Passed',   $rec[5]);

        $rec = $reader->getRecord(200);
        $this->assertEquals(0,          $rec[0]);
        $this->assertEquals(0,          $rec[1]);
        $this->assertEquals(0,          $rec[2]);
        $this->assertEquals(0,          $rec[3]);
        $this->assertEquals(0,          $rec[4]);
        $this->assertEquals('',         $rec[5]);
    }

    public function testSignedInts()
    {
        $reader = new Reader(static::WDB5_PATH . '/FieldTypes.db2');

        $ret = $reader->setFieldsSigned([true,true,true,true]);
        $this->assertNotFalse($ret[0]);
        $this->assertNotFalse($ret[1]);
        $this->assertNotFalse($ret[2]);
        $this->assertNotFalse($ret[3]);

        $rec = $reader->getRecord(150);
        $this->assertEquals(-6,          $rec[0]);
        $this->assertEquals(-536,        $rec[1]);
        $this->assertEquals(-7777216,    $rec[2]);
        $this->assertEquals(-1794967296, $rec[3]);

        $ret = $reader->setFieldsSigned([false,false,false,false]);
        $this->assertFalse($ret[0]);
        $this->assertFalse($ret[1]);
        $this->assertFalse($ret[2]);
        $this->assertFalse($ret[3]);

        $rec = $reader->getRecord(150);
        $this->assertEquals(250,        $rec[0]);
        $this->assertEquals(65000,      $rec[1]);
        $this->assertEquals(9000000,    $rec[2]);
        $this->assertEquals(2500000000, $rec[3]);
    }

    public function testIgnoreSignedOther()
    {
        $reader = new Reader(static::WDB5_PATH . '/FieldTypes.db2');

        $ret = $reader->setFieldsSigned([
            4 => false, // float field, always signed
            5 => true, // string field, never signed
        ]);
        $this->assertNotFalse($ret[4]);
        $this->assertFalse($ret[5]);
    }

    public function testIDField()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdField.db2');

        $rec = $reader->getRecord(150);
        $this->assertEquals(250,        $rec[0]);
        $this->assertEquals(2500,       $rec[1]);
        $this->assertEquals(250000,     $rec[2]);
        $this->assertEquals(25000000,   $rec[3]);
        $this->assertEquals(-2.5,       $rec[4]);
        $this->assertEquals('Pass',     $rec[5]);
        $this->assertEquals(150,        $rec[6]);
    }

    public function testIgnoreSignedIDField()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdField.db2');

        $ret = $reader->setFieldsSigned([6 => true]);
        $this->assertFalse($ret[6]);
    }

    public function testCopyBlock()
    {
        $reader = new Reader(static::WDB5_PATH.'/CopyBlock.db2');

        $from = $reader->getRecord(100);
        $this->assertEquals(100, $from[6]);

        $to = $reader->getRecord(105);
        $this->assertEquals($from, $to);
    }

    public function testArrayFields()
    {
        $reader = new Reader(static::WDB5_PATH.'/Arrays.db2');

        $rec = $reader->getRecord(100);
        $this->assertEquals([10,100],           $rec[0]); // 1-byte
        $this->assertEquals([2000,20000],       $rec[1]); // 2-byte
        $this->assertEquals([200000,2000000],   $rec[2]); // 3-byte
        $this->assertEquals([10,5],             $rec[3]); // 4-byte
        $this->assertEquals([2.5,1.25],         $rec[4]); // float
        $this->assertEquals(["One","Two"],      $rec[5]); // string
        $this->assertEquals(100,                $rec[6]); // 1-byte id

        $rec = $reader->getRecord(150);
        $this->assertEquals([250,205],              $rec[0]); // 1-byte
        $this->assertEquals([1250,2500],            $rec[1]); // 2-byte
        $this->assertEquals([250000,62500],         $rec[2]); // 3-byte
        $this->assertEquals([25000000,1234567890],  $rec[3]); // 4-byte
        $this->assertEquals([-2.5,-1.25],           $rec[4]); // float
        $this->assertEquals(["Three","Two"],        $rec[5]); // string
        $this->assertEquals(150,                    $rec[6]); // 1-byte id

        $reader->setFieldsSigned([true,true,true,true]);

        $rec = $reader->getRecord(150);
        $this->assertEquals([-6,-51],               $rec[0]); // 1-byte
        $this->assertEquals([1250,2500],            $rec[1]); // 2-byte
        $this->assertEquals([250000,62500],         $rec[2]); // 3-byte
        $this->assertEquals([25000000,1234567890],  $rec[3]); // 4-byte

        $rec = $reader->getRecord(200);
        $this->assertEquals([0,0],      $rec[0]);
        $this->assertEquals([0,0],      $rec[1]);
        $this->assertEquals([0,0],      $rec[2]);
        $this->assertEquals([0,0],      $rec[3]);
        $this->assertEquals([0,0],      $rec[4]);
        $this->assertEquals(["",""],    $rec[5]);
        $this->assertEquals(200,        $rec[6]);
    }

    public function testEmbedStringsWithoutIdBlock()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/EmbedStringsWithoutIdBlock.db2');
            $this->fail("No exception raised with file with embedded strings without id block");
        } catch (Exception $e) {
            $this->assertEquals("File has embedded strings and no ID block, which was not expected, aborting", $e->getMessage());
        }
    }

    public function testEmbedStrings()
    {
        $reader = new Reader(static::WDB5_PATH . '/EmbedStrings.db2', [2]);

        $rec = $reader->getRecord(100);
        $this->assertEquals(9000,       $rec[0]);
        $this->assertEquals(750,        $rec[1]);
        $this->assertEquals('Embedded', $rec[2]);
        $this->assertEquals(751,        $rec[3]);

        $this->assertEquals($rec, $reader->getRecord(101)); // 101 just points to 100's data in index block

        $rec = $reader->getRecord(103);
        $this->assertEquals(12345,          $rec[0]);
        $this->assertEquals(98765,          $rec[1]);
        $this->assertEquals('Strings Test', $rec[2]);
        $this->assertEquals(43210,          $rec[3]);

        $this->assertEquals($rec, $reader->getRecord(102)); // 102 just points to 103's data in index block
    }

    public function testUnknownEmbedStrings()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/EmbedStringsUnknownHash.db2');
            $this->fail("No exception raised with file with unknown embedded strings file");
        } catch (Exception $e) {
            $this->assertEquals("embedstringsunknownhash.db2 has embedded strings, but string fields were not supplied during instantiation", $e->getMessage());
        }
    }

    public function testKnownEmbedStrings()
    {
        $reader = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        $this->assertEquals(4, $reader->getFieldCount());
    }

    public function testBadIdField()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/BadIdField.db2');
            $this->fail("No exception raised with bad ID field in header");
        } catch (Exception $e) {
            $this->assertEquals("Expected ID field 88 does not exist. Only found 7 fields.", $e->getMessage());
        }
    }

    public function testBadIdFieldCount()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/BadIdFieldCount.db2');
            $this->fail("No exception raised with bad ID field count in header");
        } catch (Exception $e) {
            $this->assertEquals("Expected ID field 5 reportedly has 2 values per row", $e->getMessage());
        }
    }

    public function testEmbedStringNoEnd()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/EmbedStringsNoEnd.db2', [2]);
            $this->fail("No exception raised with bad embedded string record");
        } catch (Exception $e) {
            $this->assertEquals("Could not find end of embedded string 2 x 0 in record 0", $e->getMessage());
        }
    }

    public function testCopyBlockReferenceMissingId()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/BadCopyBlock.db2', [2]);
            $this->fail("No exception raised with bad copy block reference");
        } catch (Exception $e) {
            $this->assertEquals("Copy block referenced ID 10066329 which does not exist", $e->getMessage());
        }
    }

    public function testInvalidStringFields()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/EmbedStrings.db2', 'invalid');
            $this->fail("No exception raised during construction with bad string fields argument");
        } catch (Exception $e) {
            $this->assertEquals("You may only pass an array of string fields when loading a DB2", $e->getMessage());
        }
    }

    public function testLastFieldNotArray()
    {
        $reader = new Reader(static::WDB5_PATH . '/LastFieldNotArray.db2');
        $this->assertEquals([16,1], $reader->getRecord(16));
    }

    public function testAdbOpen()
    {
        $db2 = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        $adbViaConstructor = new Reader(static::WDB5_PATH . '/IdBlock.adb', $db2);
        $this->assertEquals([96], $adbViaConstructor->getRecord(150));

        $adbViaMethod = $db2->loadAdb(static::WDB5_PATH . '/IdBlock.adb');
        $this->assertEquals([96], $adbViaMethod->getRecord(150));
    }

    public function testAdbInvalidConstructor()
    {
        try {
            $reader = new Reader(static::WDB5_PATH . '/IdBlock.adb');
            $this->fail("No exception raised trying to open an ADB without a DB2");
        } catch (Exception $e) {
            $this->assertEquals("Unknown DB2 format: WCH7", $e->getMessage());
        }
    }

    public function testDb2IsNotAdb()
    {
        $db2 = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        try {
            $adbViaConstructor = new Reader(static::WDB5_PATH . '/IdBlock.db2', $db2);
            $this->fail("No exception raised trying to open an DB2 as an ADB via Constructor");
        } catch (Exception $e) {
            $this->assertEquals("Unknown ADB format: WDB5", $e->getMessage());
        }

        try {
            $adbViaMethod = $db2->loadAdb(static::WDB5_PATH . '/IdBlock.db2');
            $this->fail("No exception raised trying to open an DB2 as an ADB via Method");
        } catch (Exception $e) {
            $this->assertEquals("Unknown ADB format: WDB5", $e->getMessage());
        }
    }

    public function testBadAdbLength()
    {
        $db2 = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        try {
            $adb = new Reader(static::WDB5_PATH . '/BadLength.adb', $db2);
            $this->fail("No exception raised with a truncated ADB file");
        } catch (Exception $e) {
            $this->assertEquals("Expected size: 1329, actual size: 59", $e->getMessage());
        }
    }

    public function testAdbWithEmbeddedStrings()
    {
        $db2 = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        $adb = new Reader(static::WDB5_PATH . '/EmbedStrings.adb', $db2);
        $this->assertEquals([54321,987654321,'Amended',77777], $adb->getRecord(150));
        $this->assertEquals([1221,123321,'ADB',321123], $adb->getRecord(175));
    }

    public function testAdbLocaleMismatch()
    {
        $db2 = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        try {
            $adb = new Reader(static::WDB5_PATH . '/EmbedStringsWrongLocale.adb', $db2);
            $this->fail("No exception raised with the wrong locale in the ADB file");
        } catch (Exception $e) {
            $this->assertEquals("locale of embedstringswronglocale.adb (2) does not match locale of embedstrings.db2 (1)", $e->getMessage());
        }
    }

    public function testAdbHashMismatch()
    {
        $db2 = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        try {
            $adb = new Reader(static::WDB5_PATH . '/EmbedStringsWrongHash.adb', $db2);
            $this->fail("No exception raised with the wrong locale in the ADB file");
        } catch (Exception $e) {
            $this->assertEquals("layoutHash of embedstringswronghash.adb (2913643194) does not match layoutHash of embedstrings.db2 (4022250974)", $e->getMessage());
        }
    }

    public function testGetFieldTypes()
    {
        $db2 = new Reader(static::WDB5_PATH . '/Arrays.db2');
        $db2->setFieldNames(['byte','short','triple','long','float','string','id']);
        $fieldByName = [
            'byte'   => Reader::FIELD_TYPE_INT,
            'short'  => Reader::FIELD_TYPE_INT,
            'triple' => Reader::FIELD_TYPE_INT,
            'long'   => Reader::FIELD_TYPE_INT,
            'float'  => Reader::FIELD_TYPE_FLOAT,
            'string' => Reader::FIELD_TYPE_STRING,
            'id'     => Reader::FIELD_TYPE_INT,
        ];
        $this->assertEquals($fieldByName, $db2->getFieldTypes());
        $this->assertEquals($fieldByName, $db2->getFieldTypes(true));
        $this->assertEquals([
                Reader::FIELD_TYPE_INT,
                Reader::FIELD_TYPE_INT,
                Reader::FIELD_TYPE_INT,
                Reader::FIELD_TYPE_INT,
                Reader::FIELD_TYPE_FLOAT,
                Reader::FIELD_TYPE_STRING,
                Reader::FIELD_TYPE_INT,
            ], $db2->getFieldTypes(false));
    }

    public function testLayoutHash()
    {
        $reader = new Reader(static::WDB5_PATH . '/Arrays.db2');
        $this->assertEquals(0xEFBEADDE, $reader->getLayoutHash());

        $reader = new Reader(static::WDB5_PATH . '/EmbedStringsUnknownHash.db2', [2]);
        $this->assertEquals(0xADAAAABA, $reader->getLayoutHash());
    }

    public function testWDB2IdBlock()
    {
        $reader = new Reader(static::WDB2_PATH.'/IdBlock.db2');

        $this->assertEquals([200], $reader->getRecord(100));
        $this->assertEquals(1, count($reader->getIds()));
    }

    public function testWDB2IdField()
    {
        $reader = new Reader(static::WDB2_PATH.'/IdField.db2');

        $this->assertEquals([100,10,2000,200000,10,2.5,"Test"], $reader->getRecord(100));
        $this->assertEquals([150,250,2500,250000,25000000,-2.5,"Pass"], $reader->getRecord(150));
        $this->assertEquals([200,0,0,0,0,0,""], $reader->getRecord(200));

        $this->assertEquals(3, count($reader->getIds()));
    }

    public function testWDB2FieldsFromArrays()
    {
        $reader = new Reader(static::WDB2_PATH.'/Arrays.db2');

        $this->assertEquals([100,10,100,2000,20000,200000,2000000,10,5,2.5,1.25,"One","Two"], $reader->getRecord(100));
        $this->assertEquals([150,250,205,1250,2500,250000,62500,25000000,1234567890,-2.5,-1.25,"Three","Two"], $reader->getRecord(150));
        $this->assertEquals([200,0,0,0,0,0,0,0,0,0,0,"",""], $reader->getRecord(200));
        $this->assertEquals(3, count($reader->getIds()));
    }

    public function testWDB2FieldTypes()
    {
        $reader = new Reader(static::WDB2_PATH.'/FieldTypes.db2');

        $this->assertEquals([10,2000,200000,10,2.5,"Test"], $reader->getRecord(100));
        $this->assertEquals([250,65000,9000000,2500000000,-2.5,"Passed"], $reader->getRecord(150));
        $this->assertEquals([0,0,0,0,0,""], $reader->getRecord(200));
        $this->assertEquals(3, count($reader->getIds()));

        $reader->setFieldsSigned([3 => true]);
        $record = $reader->getRecord(150);
        $this->assertEquals(-1794967296, $record[3]);
    }

    public function testWDB2TooLong()
    {
        try {
            $reader = new Reader(static::WDB2_PATH.'/TooLong.db2');
            $this->fail("Did not notice db2 file was too long");
        } catch (Exception $e) {
            $this->assertEquals('Expected size: 739, actual size: '.filesize(static::WDB2_PATH.'/TooLong.db2'), $e->getMessage());
        }
    }

    public function testWDB2TooShort()
    {
        try {
            $reader = new Reader(static::WDB2_PATH.'/TooShort.db2');
            $this->fail("Did not notice db2 file was too short");
        } catch (Exception $e) {
            $this->assertEquals('Expected size: 739, actual size: '.filesize(static::WDB2_PATH.'/TooShort.db2'), $e->getMessage());
        }
    }

    public function testNonzeroFields()
    {
        $reader = new Reader(static::WDB5_PATH . '/FieldTypesWDB6.db2');

        $rec = $reader->getRecord(100);
        $this->assertEquals(10,         $rec[0]); // 1-byte
        $this->assertEquals(2000,       $rec[1]); // 2-byte
        $this->assertEquals(200000,     $rec[2]); // 3-byte
        $this->assertEquals(10,         $rec[3]); // 4-byte
        $this->assertEquals(2.5,        $rec[4]); // float
        $this->assertEquals('Test',     $rec[5]); // string
        $this->assertEquals(0,          $rec[6]); // nonzero 4-byte
        $this->assertEquals(1,          $rec[7]); // nonzero 1-byte
        $this->assertEquals(6,          $rec[8]); // nonzero 1-byte
        $this->assertEquals(0,          $rec[9]); // nonzero 2-byte
        $this->assertEquals(1.25,       $rec[10]); // nonzero float
        $this->assertEquals('',         $rec[11]); // nonzero string
        $this->assertEquals(666666666,  $rec[12]); // nonzero 4-byte
        $this->assertEquals(204,        $rec[13]); // nonzero 1-byte

        $rec = $reader->getRecord(150);
        $this->assertEquals(250,        $rec[0]);
        $this->assertEquals(65000,      $rec[1]);
        $this->assertEquals(9000000,    $rec[2]);
        $this->assertEquals(2500000000, $rec[3]);
        $this->assertEquals(-2.5,       $rec[4]);
        $this->assertEquals('Passed',   $rec[5]);
        $this->assertEquals(0,          $rec[6]); // nonzero 4-byte
        $this->assertEquals(2,          $rec[7]); // nonzero 1-byte
        $this->assertEquals(5,          $rec[8]); // nonzero 1-byte
        $this->assertEquals(2000,       $rec[9]); // nonzero 2-byte
        $this->assertEquals(0,          $rec[10]); // nonzero float
        $this->assertEquals('Passed',   $rec[11]); // nonzero string
        $this->assertEquals(999999999,  $rec[12]); // nonzero 4-byte
        $this->assertEquals(255,        $rec[13]); // nonzero 1-byte

        $rec = $reader->getRecord(200);
        $this->assertEquals(0, $rec[0]);
        $this->assertEquals(0, $rec[1]);
        $this->assertEquals(0, $rec[2]);
        $this->assertEquals(0, $rec[3]);
        $this->assertEquals(0, $rec[4]);
        $this->assertEquals('', $rec[5]);
        $this->assertEquals(0, $rec[6]); // nonzero 4-byte
        $this->assertEquals(3, $rec[7]); // nonzero 1-byte
        $this->assertEquals(4, $rec[8]); // nonzero 1-byte
        $this->assertEquals(0, $rec[9]); // nonzero 2-byte
        $this->assertEquals(0, $rec[10]); // nonzero float
        $this->assertEquals('', $rec[11]); // nonzero string
        $this->assertEquals(0, $rec[12]); // nonzero 4-byte
        $this->assertEquals(0, $rec[13]); // nonzero 1-byte
    }

    public function testNonzeroSigned()
    {
        $reader = new Reader(static::WDB5_PATH.'/FieldTypesWDB6.db2');

        $reader->setFieldsSigned([true,true,true,true,13 => true]);
        $rec = $reader->getRecord(150);
        $this->assertEquals(-6,          $rec[0]);
        $this->assertEquals(-536,        $rec[1]);
        $this->assertEquals(-7777216,    $rec[2]);
        $this->assertEquals(-1794967296, $rec[3]);
        $this->assertEquals(-1,          $rec[13]);

        $reader->setFieldsSigned([false,false,false,false,13 => false]);
        $rec = $reader->getRecord(150);
        $this->assertEquals(250,        $rec[0]);
        $this->assertEquals(65000,      $rec[1]);
        $this->assertEquals(9000000,    $rec[2]);
        $this->assertEquals(2500000000, $rec[3]);
        $this->assertEquals(255,        $rec[13]);
    }

    public function testNonzeroNames()
    {
        $reader = new Reader(static::WDB5_PATH.'/FieldTypesWDB6.db2');

        $reader->setFieldNames([4 => 'float', 10 => 'nonzeroFloat']);
        $rec = $reader->getRecord(100);

        $this->assertEquals(2.5, $rec['float']);
        $this->assertEquals(1.25, $rec['nonzeroFloat']);
    }

    public function testNonzeroFieldCountMismatch()
    {
        try {
            $reader = new Reader(static::WDB5_PATH.'/NonzeroFieldCountMismatch.db2');
            $this->fail("Did not notice nonzero field counts differ between header and nonzero block");
        } catch (Exception $e) {
            $this->assertEquals('Expected 14 fields in nonzero block, found 13', $e->getMessage());
        }
    }

    public function testNonzeroEntriesInRegularField()
    {
        try {
            $reader = new Reader(static::WDB5_PATH.'/NonzeroEntriesInRegularField.db2');
            $this->fail("Did not notice nonzero entries defined for regular field");
        } catch (Exception $e) {
            $this->assertEquals('Expected 0 entries in nonzero block field 1, instead found 3', $e->getMessage());
        }
    }

    public function testNonzeroUnknownFieldType()
    {
        try {
            $reader = new Reader(static::WDB5_PATH.'/NonzeroUnknownFieldType.db2');
            $this->fail("Did not notice unknown type of nonzero field");
        } catch (Exception $e) {
            $this->assertEquals('Unknown nonzero field type: 240', $e->getMessage());
        }
    }

}

