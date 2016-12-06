import * as path from 'path';
import {expect} from 'chai';
import {execute} from 'sql2adt';





describe('Executing a query', () => {

    const DB_PATH = path.join(__dirname, './fixtures/ADS_DATA');

    let tests = [
        `SELECT a.NAME as NAME FROM animals a`, [
            `rows.length === 7`,
            `rows[0].NAME === 'Angel Fish'`,
        ],
    ];

    for (let i = 0; i < tests.length; i += 2) {
        let sql = <string> tests[i];
        let facts = <string[]> tests[i + 1];
        it(sql, () => {
            // TODO: ...
            let result = execute(DB_PATH, sql);
            result;

            expect(() => execute(DB_PATH, sql)).not.to.throw;
            facts;
        });

    }
});
