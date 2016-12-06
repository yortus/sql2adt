import * as path from 'path';
import {expect} from 'chai';
import {execute} from 'sql2adt';





describe('Executing a query', () => {

    const DB_PATH = path.join(__dirname, './fixtures/ADS_DATA');

    let tests: Array<string | Array<(rows: any[]) => void>>;
    tests = [
        `SELECT a.NAME as NAME FROM animals a`, [
            rows => expect(rows.length).to.equal(7),
            rows => expect(rows.some(row => row.NAME === 'Angel Fish')).is.true,
            rows => expect(Object.keys(rows[0]).sort()).deep.equal(['AREA', 'BMP', 'NAME', 'SIZE', 'WEIGHT']),
        ],
    ];

    for (let i = 0; i < tests.length; i += 2) {
        let sql = tests[i] as string;
        let assertions = tests[i + 1] as Array<(rows: any[]) => boolean>;
        it(sql, async () => {
            let rows = await execute(DB_PATH, sql);
            assertions.forEach(assertion => assertion(rows));
        });
    }
});
