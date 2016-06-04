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
        $format = substr(file_get_contents(static::WDB5_PATH.'/badformat.db2'), 0, 4);

        try {
            $reader = new Reader(static::WDB5_PATH.'/badformat.db2');
            $this->fail("Did not throw exception on unknown file format");
        } catch (Exception $e) {
            $this->assertEquals("Unknown format: $format", $e->getMessage());
        }
    }

    public function testWDB5FormatLoad()
    {
        $reader = new Reader(static::WDB5_PATH.'/idblock.db2');
        $this->assertEquals(1, $reader->getFieldCount());
    }
}