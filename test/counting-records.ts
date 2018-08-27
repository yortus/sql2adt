import * as path from 'path';
import {expect} from 'chai';
import {count} from 'sql2adt';




describe('Counting records', () => {
    const DB_PATH = path.join(__dirname, './fixtures/ADS_DATA');
    const tests = {
        animals: 7,
        orders: 205,
        HOLDINGS: 36,
        CUST_BAK: 0,
        customer: 55,
        vendors: 23,
    };

    let tableNames = Object.keys(tests) as (keyof typeof tests)[];
    for (let tableName of tableNames) {
        it(`${tableName} has ${tests[tableName]} rows`, async () => {
            let rows = await count(DB_PATH, tableName);
            expect(rows).to.equal(tests[tableName]);
        });
    }
});
