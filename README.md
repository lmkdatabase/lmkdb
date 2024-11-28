# lmkdb

## Usage

**> create \<name\> [attr...]** Create a table with name \<name\> and list of attribute names [attr...]

**> insert \<name\> [attr:val...]** Insert a row to a table \<name\> with values val for each attribute attr

**> read \<name\>** Read all rows from table \<name\>
**> read \<name\> idx:\<idx\>** Read row from table \<name\> with index \<idx\>

**> delete \<name\>** Delete all rows from table \<name\>
**> delete \<name\> idx:\<idx\>** Delete row with index \<idx\> from table \<name\>
**> delete \<name\> [attr:val...]** Delete rows matching _all_ attr:val combination from table \<name\>

**> update \<name\> idx:\<idx\> [attr:val...]** Update attributes attr with values val... for row with index \<idx\> from table \<name\>

**> join \<table1\>.\<attr1\> \<table2\>.\<attr2\> [\<table_n\>.\<attr_n\>...]** Join tables \<table1\> and \<table2\> (and up to \<table_n\>) on attributes \<attr1\> and \<attr2\> (up to \<attr_n\>), performs inner join

## Build Instructions

1. **Clone the Repository** (if not already done):
   ```bash
   git clone https://github.com/lmkdatabase/lmkdb.git
   cd lmkdb
   ```
2. **Run build script**

   ```bash
   ./run.sh
   ```

3. **Run the Executable**
   After building, an executable named lmkdb will be created in the newly created `build/` directory.
   Run it with:
   ```bash
   cd build
   ./lmkdb
   ```
