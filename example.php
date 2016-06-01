<?php

require_once __DIR__ . '/Erorus/DB2/Reader.php';

use \Erorus\DB2\Reader;

$path = isset($argv[1]) ? $argv[1] : 'Achievement.db2';

$reader = new Reader(__DIR__.'/db2/wdb5/' . $path);
//print_r($reader->setFieldsSigned([4=>true,11=>true]));
//print_r($reader->setFieldNames(['name','description','flags']));

//print_r($reader->getRecord(118852));

$recordNum = 0;
foreach ($reader->generateRecords() as $id => $record) {
    echo $id, ": "; // implode(',', Reader::flattenRecord($record));

    $colNum = 0;
    foreach ($record as $colName => $colVal) {
        if ($colNum++ > 0) {
            echo ",";
        }
        if (is_array($colVal)) {
            echo '[', implode(',', $colVal), ']';
        } else {
            echo $colVal;
        }
    }

    echo "\n";

    if (++$recordNum >= 20) {
        break;
    }
}

