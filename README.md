# DB2 Reader

This is a small library to read DB2 files (data tables) from World of Warcraft.

## Installation

Just copy the class files into your project and include them where you want to use them. Eventually I want to make it compatible with Composer, but that's not done yet.

**Note:** The interfaces and class structures are not finalized yet, and they may change during development.

## Usage

Check out the example.php, which is what I'm using during development.
* Instantiate an object with the path to a DB2 file. `$reader = new Reader("Achievement.db2");`
* Records are presented as simple arrays. Some fields are, themselves, arrays of values. Get individual records by ID with `$reader->getRecord(17)`
* You can loop through all available records with `foreach ($reader->generateRecords() as $id => $record)`
* There's a list of all IDs in the file at `$reader->getIds()`

You can also set field names with `setFieldNames([0 => "name", 1 => "note", ...])` if you don't want to keep track of the field IDs.

All integers are assumed to be unsigned, but you can use `setFieldsSigned([2 => true, 7 => true, ...])` to permit negative values.

## Compatibility

Currently it works only with the WDB5 format, which is currently used in the Legion beta. It should work reasonably well for most DB2 files already, with support for embedded IDs, ID blocks, the copy block, and including Item-sparse with its embedded strings. Other files with embedded strings will need you to identify the string fields.

Warlords of Draenor (and prior versions) uses WDB2, which is not compatible with this library. Support for WDB2 might be added later.

Legion already went through WDB3 and WDB4, and I do not intend to support those versions, as the record structure for those formats is stored in the WoW executable.

## Goals

This will eventually be used for The Undermine Journal (Newsstand) to datamine items, pets, and other entities for Legion.

I'm also separating this from the rest of Newsstand because I'd like to work on best practices with PSRs, unit tests, composer compatibility, etc. Eventually.

## Disclaimers

The interfaces and class structures are not finalized yet, and they may change during development.

This work is neither endorsed by nor affiliated with Blizzard Entertainment.

This work is developed with no involvement of ZAM Network, which is my current employer.

## License

Copyright 2016 Gerard Dombroski

Licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.