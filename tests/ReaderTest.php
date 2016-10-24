<?php
use Erorus\DB2\Reader;

class ReaderTest extends \PHPUnit\Framework\TestCase
{
    const WDB5_PATH = __DIR__.'/wdb5';

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Must supply path to DB2 file
     */
    public function testInvalidDB2Param()
    {
        new Reader(0);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Error opening /dev/null/notfound.db2 for reading.
     */
    public function testMissingFile()
    {
        new Reader('/dev/null/notfound.db2');
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Unknown DB2 format: XXXX in file badformat.db2.
     */
    public function testInvalidFileFormat()
    {
        $file = static::WDB5_PATH.'/BadFormat.db2';
        new Reader($file);
    }

    public function testWDB5FormatLoad()
    {
        $reader = new Reader(static::WDB5_PATH.'/IdBlock2.db2');
        $this->assertEquals(1, $reader->getFieldCount());
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Expected size: 59, actual size: 68
     */
    public function testFileTooLong()
    {
       new Reader(static::WDB5_PATH.'/TooLong.db2');
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Expected size: 374, actual size: 64
     */
    public function testFileTooShort()
    {
        new Reader(static::WDB5_PATH.'/TooShort.db2');
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

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Field ID 1 out of bounds: 0-0
     */
    public function testSignedFieldOutOfBounds()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        $reader->setFieldsSigned([false, true]);
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

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Field ID abc must be numeric
     */
    public function testFieldNameInvalidIndex()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        $reader->setFieldNames(['abc' => 'abc']);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Field 0 Name (99) must NOT be numeric
     */
    public function testFieldNameNumeric()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        $reader->setFieldNames([99]);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Field ID 99 out of bounds: 0-0
     */
    public function testFieldOutOfBounds()
    {
        $reader = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        $reader->setFieldNames([99=>'test']);
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

    /**
     * @expectedException \TypeError
     * @expectedExceptionMessage must be of the type array, string given
     */
    public function testFlattenString()
    {
        Reader::flattenRecord("abc");
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

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage embedstringswithoutidblock.db2 has embedded strings and no ID block, which was not expected, aborting
     */
    public function testEmbedStringsWithoutIdBlock()
    {
        new Reader(static::WDB5_PATH . '/EmbedStringsWithoutIdBlock.db2');
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

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage embedstringsunknownhash.db2 has embedded strings, but string fields were not supplied during instantiation
     */
    public function testUnknownEmbedStrings()
    {
        new Reader(static::WDB5_PATH . '/EmbedStringsUnknownHash.db2');
    }

    public function testKnownEmbedStrings()
    {
        $reader = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        $this->assertEquals(4, $reader->getFieldCount());
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage ID field 88 in file badidfield.db2 is out of bounds: 0-7
     */
    public function testBadIdField()
    {
        new Reader(static::WDB5_PATH . '/BadIdField.db2');
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Expected ID field 5 reportedly has 2 values per row
     */
    public function testBadIdFieldCount()
    {
        new Reader(static::WDB5_PATH . '/BadIdFieldCount.db2');
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Could not find end of embedded string 2 x 0 in record 0
     */
    public function testEmbedStringNoEnd()
    {
        new Reader(static::WDB5_PATH . '/EmbedStringsNoEnd.db2', [2]);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Copy block referenced ID 10066329 which does not exist
     */
    public function testCopyBlockReferenceMissingId()
    {
        new Reader(static::WDB5_PATH . '/BadCopyBlock.db2', [2]);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage You may only pass an array of string fields when loading a DB2
     */
    public function testInvalidStringFields()
    {
        new Reader(static::WDB5_PATH . '/EmbedStrings.db2', 'invalid');
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

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Unknown DB2 format: WCH7 in file idblock.adb.
     */
    public function testAdbInvalidConstructor()
    {
        new Reader(static::WDB5_PATH . '/IdBlock.adb');
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Unknown ADB format: WDB5 in file idblock.db2.
     */
    public function testOpenDb2AsAdb()
    {
        $db2 = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        new Reader(static::WDB5_PATH . '/IdBlock.db2', $db2);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Unknown ADB format: WDB5 in file idblock.db2.
     */
    public function testOpenDb2AsAdbViaMethod()
    {
        $db2 = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        $db2->loadAdb(static::WDB5_PATH . '/IdBlock.db2');
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage Expected size: 1329, actual size: 59
     */
    public function testBadAdbLength()
    {
        $db2 = new Reader(static::WDB5_PATH . '/IdBlock.db2');
        new Reader(static::WDB5_PATH . '/BadLength.adb', $db2);
    }

    public function testAdbWithEmbeddedStrings()
    {
        $db2 = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        $adb = new Reader(static::WDB5_PATH . '/EmbedStrings.adb', $db2);
        $this->assertEquals([54321,987654321,'Amended',77777], $adb->getRecord(150));
        $this->assertEquals([1221,123321,'ADB',321123], $adb->getRecord(175));
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage locale of embedstringswronglocale.adb (2) does not match locale of embedstrings.db2 (1)
     */
    public function testAdbLocaleMismatch()
    {
        $db2 = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        new Reader(static::WDB5_PATH . '/EmbedStringsWrongLocale.adb', $db2);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage layoutHash of embedstringswronghash.adb (2913643194) does not match layoutHash of embedstrings.db2 (4022250974)
     */
    public function testAdbHashMismatch()
    {
        $db2 = new Reader(static::WDB5_PATH . '/EmbedStrings.db2');
        new Reader(static::WDB5_PATH . '/EmbedStringsWrongHash.adb', $db2);
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

}

