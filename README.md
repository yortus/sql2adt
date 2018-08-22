# sql2adt
Run SQL queries against ADT files in Node.js.


Supported SQL clauses (examples):
- `SELECT t1.Column1 AS col1 FROM table1 AS t1`
- `INNER JOIN table2 AS t2 ON t1.id = t2.id`
- `WHERE t1.x >= 'aaa' AND t1.x <= 'zzz'`
- `LIMIT 5`
- `OFFSET 10`


Restrictions:
- Table names, column names and aliases *must* conform to `[A-Za-z_][A-Za-z_0-9]*`.
- Columns in the `SELECT` clause *must* have aliases.
- The `WHERE` clause only supports basic relative operators and `AND`.
- `LIMIT` and `OFFSET` may only be used in queries on a single table with no `WHERE` clause.


For more examples of valid and invalid queries, consult the unit tests.
