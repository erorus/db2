<?php

use Erorus\DB2\Reader;

class ReaderTest extends phpunit\framework\TestCase
{
    const TEMP_FILE_PATH = 'php://memory';
    const WDB5_ID_BLOCK_PATH = __DIR__.'/wdb5/idblock.db2';

    public function testInvalidDB2Param()
    {
        try {
            $reader = new Reader(0);
            $this->fail("Did not throw exception on invalid db2 param");
        } catch (Exception $e) {
            $this->assertEquals("Must supply path to DB2 file, or stream", $e->getMessage());
        }
    }

    public function testInvalidResourceType()
    {
        $f = fopen(static::TEMP_FILE_PATH, 'w+');
        fclose($f);

        try {
            $reader = new Reader($f);
            $this->fail("Did not throw exception on invalid resource");
        } catch (Exception $e) {
            $this->assertEquals("Must supply path to DB2 file, or stream", $e->getMessage());
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
        $format = 'XXXX';

        $f = fopen(static::TEMP_FILE_PATH, 'w+');
        fwrite($f, $format);
        rewind($f);

        try {
            $reader = new Reader($f);
            $this->fail("Did not throw exception on unknown file format");
        } catch (Exception $e) {
            $this->assertEquals("Unknown format: $format", $e->getMessage());
        } finally {
            fclose($f);
        }
    }

    public function testWDB5FormatLoad()
    {
        $reader = new Reader(static::WDB5_ID_BLOCK_PATH);
        $this->assertEquals(1, $reader->getFieldCount());
    }

    public function testStreamStaysOpen()
    {
        $f = fopen(static::WDB5_ID_BLOCK_PATH, 'rb');
        $reader = new Reader($f);
        unset($reader);
        $this->assertFalse(ftell($f) === false);
        fclose($f);
    }
}