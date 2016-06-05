<?php

use Erorus\DB2\Reader;

class ReaderTest extends phpunit\framework\TestCase
{
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
            $this->assertEquals("Unknown format: $format", $e->getMessage());
        }
    }

    public function testWDB5FormatLoad()
    {
        $reader = new Reader(static::WDB5_PATH.'/Idblock2.db2');
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
        $reader = new Reader(static::WDB5_PATH.'/Idblock2.db2');
        $allIDs = $reader->getIds();

        $this->assertEquals('[100,150]', json_encode($allIDs));
    }

    public function testRecordIterator()
    {
        $reader = new Reader(static::WDB5_PATH.'/Idblock2.db2');
        $reader->setFieldNames(['value']);
        $reader->setFieldsSigned([true]);

        $recordCount = 0;
        foreach ($reader->generateRecords() as $id => $rec) {
            if (++$recordCount > 2) {
                break;
            }
            switch ($id) {
                case 100:
                    $this->assertEquals('{"value":-56}', json_encode($rec));
                    break;
                case 150:
                    $this->assertEquals('{"value":-6}', json_encode($rec));
                    break;
                default:
                    $this->fail("Returned unknown ID $id from Idblock2.db2");
                    break;
            }
        }
        if ($recordCount != 2) {
            $this->fail("Returned $recordCount records instead of 2 from Idblock2.db2 in iterator");
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
        $rec = [2,4,1,3];
        $result = Reader::flattenRecord($rec);
        $this->assertEquals(json_encode($rec), json_encode($result));
    }

    public function testFlattenNamedRecord()
    {
        $rec = ['b' => 2, 'd' => 4, 'a' => 1, 'c' => 3];
        $result = Reader::flattenRecord($rec);
        $this->assertEquals(json_encode($rec), json_encode($result));
    }

    public function testFlattenRecordWithArrays()
    {
        $rec = [2,[10,20],1,[40,30]];
        $result = Reader::flattenRecord($rec);
        $this->assertEquals('{"0":2,"1-0":10,"1-1":20,"2":1,"3-0":40,"3-1":30}', json_encode($result));
    }

    public function testFieldTypeDetection()
    {
        $reader = new Reader(static::WDB5_PATH.'/FieldTypes.db2');

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
        $this->assertEquals(json_encode($from), json_encode($to));
    }
}

