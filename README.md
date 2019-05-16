[![Build Status](https://travis-ci.org/erorus/db2.svg?branch=master)](https://travis-ci.org/erorus/db2) [![Coverage Status](https://coveralls.io/repos/github/erorus/db2/badge.svg?branch=master)](https://coveralls.io/github/erorus/db2?branch=master)

# DB2 Reader

This is a small library to read DB2 and ADB/DBCache files (data tables) from World of Warcraft.

## Requirements

This was developed using 64-bit PHP 7. Tests are also run against PHP 5.6. 64-bit versions are recommended to support unsigned 32-bit ints and 64-bit ints.

Mbstring extension is required for all formats, and gmp extension is required for WDC1 support.

## Usage

```php
// Instantiate an object with the path to a DB2 file.
$db2Reader = new Reader("Achievement.db2"); 
 
// Records are presented as simple arrays.
// Some fields are, themselves, arrays of values.
// Get individual records by ID with:
$record = $db2Reader->getRecord(17);
 
// Loop through records with:
foreach ($db2Reader->generateRecords() as $id => $record) { ... }
 
// All valid record IDs:
$ids = $db2Reader->getIds();
 
// You can set field names for convenience:
$db2Reader->setFieldNames([0 => "name", 1 => "note", ...]);
$record = $db2Reader->getRecord(17);
if ($record['name'] == "...") ...
 
// All integers are assumed to be unsigned, but you can change that by field:
$db2Reader->setFieldsSigned([2 => true, 7 => true]);
$record = $db2Reader->getRecord(17);
if ($record[2] < 0) ...
 
// You can get the calculated field types, 
// useful when dynamically creating database tables:
$types = $db2Reader->getFieldTypes();
if ($types['name'] == Reader::FIELD_TYPE_STRING) ...
 
// You can load an ADB or DBCache, as long as you have its parent DB2.
// That reader will only expose records in the Hotfix/DBCache file.
$adbReader = $db2Reader->loadAdb("Achievement.adb");
$dbCacheReader = $db2Reader->loadDBCache("DBCache.bin");

// Finally, you can specify both the DB2 and the DBCache file when constructing a HotfixedReader.
// This has the same interface as Reader, but will substitute hotfixed records when available.
$hotReader = new HotfixedReader("Achievement.db2", "DBCache.bin");
```

Also check out example.php, which is what I'm using during development.

## Compatibility

| Version | Format | Works | Unit Tests |
|---------|--------|-------|-------|
| 3.x - 6.x | WDB2 | ✓ | ✓ |
| 7.0.1 | WDB3 |  |  |
| 7.0.3 | WDB4 |  |  |
| 7.0.3 - 7.2.0 | WDB5 | ✓ | ✓ |
| 7.0.3 - 7.2.0 | WCH7/8 | ✓ |  |
| 7.2.0 - 7.3.2 | WDB6 | ✓ | ✓ |
| 7.2.0 - ? | DBCache.bin | ✓ |  |
| 7.3.5 | WDC1 | ✓ |  |
| 8.0.1 | WDC2 | ✓ |  |
| 1.13.2 | 1SLC | ✓ |  |
| 8.1.0 - ? | WDC3 | ✓ |  |

All features of DB2 files should be supported (offset map / embedded strings, copy blocks, common blocks, pallet data, etc).

Encrypted blocks are expected to be decrypted by your CASC tool before being read by this library. For blocks that cannot be decrypted (because the key is unknown, for example), your CASC tool should substitute NUL bytes for the length of the encrypted blocks. DB2Reader ignores blocks which are encrypted and are empty of data. 

ADBs/DBCache require their counterpart DB2 file for necessary metadata, and Internet access to get field sizes via [WoWDBDefs](https://github.com/wowdev/WoWDBDefs).

## Goals

This is used for The Undermine Journal ([Newsstand](https://github.com/erorus/newsstand/)) to datamine items, pets, and other entities.

## Disclaimers

This work is neither endorsed by nor affiliated with Blizzard Entertainment.

## Thanks

Most of the file format details were found by documentation at [the WoWDev wiki](https://wowdev.wiki/DB2). Thanks to those who contribute there!

Thanks to [WoWDBDefs](https://github.com/wowdev/WoWDBDefs) for providing the table formats used by the game executable so we can parse hotfixes.

## License

Copyright 2017 Gerard Dombroski

Licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
