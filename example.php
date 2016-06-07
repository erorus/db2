<?php

require_once __DIR__ . '/src/autoload.php';

use \Erorus\DB2\Reader;

if (isset($argv[1])) {
    $path = $argv[1];
} else {
    $path = __DIR__.'/tests/wdb5/FieldTypes.db2';
}

$reader = new Reader($path);
if (isset($argv[2])) {
    $fields = explode(',', $argv[2]);
    foreach ($fields as $field) {
        $reader->setFieldsSigned([$field => true]);
    }
}

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

