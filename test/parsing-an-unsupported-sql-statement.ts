import {expect} from 'chai';
import {parseSQL} from 'sql2adt';





describe('Parsing an unsupported SQL statement', () => {

    let tests = [
        // SELECT columns *must* be qualified (i.e. 'table.colname')
        `SELECT colname FROM table`,

        // SELECT columns *must* have aliases (i.e. 'table.colname [AS] name')
        `SELECT table.colname FROM table`,

        // SELECT DISTINCT is *not* supported
        `SELECT DISTINCT col FROM table`,
        `SELECT DISTINCT table.colname AS alias FROM table`,

        // SELECT expressions are *not* supported
        `SELECT 42 AS val`,
        `SELECT 42 AS alias FROM table`,
        `SELECT COUNT(1) AS n FROM table`,

        // SELECT table/column delimiters are *not* supported
        `SELECT [table].colname AS alias FROM table`,
        `SELECT table.[colname] AS alias FROM table`,
        `SELECT table.colname AS [alias] FROM table`,
        `SELECT table.colname AS alias FROM [table]`,

        // Only string/integer literals are supported in WHERE comparisons
        `SELECT tbl.colname AS alias FROM table WHERE table.val > 3.14`,

        // Only INNER JOINs are supported
        `SELECT t1.c1 AS c1, t2.c2 AS c2 FROM t1, t2`,
        `SELECT t1.c1 AS c1 FROM table1 as t1 JOIN table2 as t2 ON t1.id = t2.id`,
        `SELECT t1.c1 AS c1 FROM table1 as t1 LEFT JOIN table2 as t2 ON t1.id = t2.id`,

        // Only INNER JOINs based on column equality are supported
        `SELECT t1.c1 AS c1 FROM table1 as t1 JOIN table2 as t2 ON t1.id >= t2.id`,

        // The WHERE clause supports only simple AND conjunctions with simple literal comparisons
        // NB: no parentheses, no non-relational operators, no functions, etc...
        `SELECT t1.c1 AS c1 FROM t1 WHERE t1.x = 'alpha' OR t1.x = 'beta'`,
        `SELECT t1.c1 AS c1 FROM t1 WHERE t1.x LIKE '%a%'`,
        `SELECT t1.c1 AS c1 FROM t1 WHERE (t1.x = 'alpha')`,
        `SELECT t1.c1 AS c1 FROM t1 WHERE SUBSTRING(t1.x, 3, 1) = 'n'`,

        // TODO: only SELECT, FROM and WHERE clauses are supported...
        // TODO: subqueries are not supported...
    ];

    tests.forEach(test => {
        it(test, () => {
            expect(() => parseSQL(test)).to.throw();
        });
    });
});
