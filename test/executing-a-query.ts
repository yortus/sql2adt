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
            rows => expect(Object.keys(rows[0]).sort()).deep.equal(['NAME']),
            rows => expect(rows).includes({NAME: 'Angel Fish'}),
            rows => expect(rows).includes({NAME: 'Ocelot'}),
        ],
        `
            SELECT o.OrderNo as OrderNo, o.ItemsTotal as total, i.Qty as Qty, p.Description as desc
            FROM orders o
                INNER JOIN items i ON o.OrderNo = i.OrderNo
                INNER JOIN parts p ON p.PartNo = i.PartNo
            WHERE p.PartNo = 1313
        `, [
            rows => expect(rows.length).to.equal(16),
            rows => expect(Object.keys(rows[0])).deep.equal(['OrderNo', 'total', 'Qty', 'desc']),
            rows => expect(rows.every(row => row.desc === 'Regulator System')).equals(true, `all descs are 'Regulator System'`),
        ]
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
