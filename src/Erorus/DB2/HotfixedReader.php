<?php

namespace Erorus\DB2;

/**
 * Class HotfixedReader
 * @package Erorus\DB2
 *
 * This provides mostly the same interface as Reader, but allows you to specify the DBCache.bin file when you create
 * the reader, and hotfixes from that file will take precedence over the db2 data. New rows added in DBCache.bin
 * will also be iterated through.
 */
class HotfixedReader
{
    const SOURCE_DB2 = 0;
    const SOURCE_DBCACHE = 1;

    /** @var Reader */
    private $db2;
    /** @var Reader */
    private $dbcache;

    /** @var array */
    private $idSource;

    /**
     * HotfixedReader constructor.
     *
     * @param string $db2path Full path to something.db2
     * @param string $hotfixPath Full path to DBCache.bin
     *
     * @throws \Exception
     */
    public function __construct($db2path, $hotfixPath) {
        $this->db2     = new Reader($db2path);
        $this->dbcache = $this->db2->loadDBCache($hotfixPath);

        // Keys defined in dbcache will take precedence over those in db2
        $this->idSource = array_fill_keys($this->dbcache->getIds(), self::SOURCE_DBCACHE) +
                          array_fill_keys($this->db2->getIds(), self::SOURCE_DB2);

        ksort($this->idSource);
    }

    /**
     * Fetches column names from the table definition at WoWDBDefs and sets them with $this->setFieldNames()
     */
    public function fetchColumnNames() {
        $this->db2->fetchColumnNames();
    }

    public function getFieldCount() {
        return $this->db2->getFieldCount();
    }

    public function getRecord($id) {
        if (!isset($this->idSource[$id])) {
            return null;
        }
        return $this->idSource[$id] == self::SOURCE_DB2 ? $this->db2->getRecord($id) : $this->dbcache->getRecord($id);
    }

    public function getIds() {
        return array_keys($this->idSource);
    }

    public function generateRecords() {
        foreach ($this->idSource as $id => $source) {
            yield $id => $source == self::SOURCE_DB2 ? $this->db2->getRecord($id) : $this->dbcache->getRecord($id);
        }
    }

    public function getFieldTypes($byName = true) {
        return $this->db2->getFieldTypes($byName);
    }

    public function getLayoutHash() {
        return $this->db2->getLayoutHash();
    }

    // user preferences

    public function setFieldsSigned(Array $fields) {
        $ret = $this->db2->setFieldsSigned($fields);
        $this->dbcache->setFieldsSigned($fields);

        return $ret;
    }

    public function setFieldNames(Array $names) {
        $ret = $this->db2->setFieldNames($names);
        $this->dbcache->setFieldNames($names);

        return $ret;
    }

    // static utils

    public static function flattenRecord(Array $record) {
        return Reader::flattenRecord($record);
    }
}
