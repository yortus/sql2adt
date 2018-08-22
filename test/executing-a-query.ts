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
            rows => expect(rows).to.deep.include({NAME: 'Angel Fish'}),
            rows => expect(rows).to.deep.include({NAME: 'Ocelot'}),
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
        ],
        `
            SELECT h.ACCT_NBR as ACCT_NBR, h.SYMBOL as SYMBOL, h.SHARES as SHARES, h.PUR_PRICE as PUR_PRICE, h.PUR_DATE as PUR_DATE
            FROM HOLDINGS h
        `, [
            rows => expect(rows.length).to.equal(36),
            rows => expect(rows[1].PUR_DATE).is.a('Date'),
            rows => expect((rows.find(row => row.SYMBOL === 'VG').PUR_DATE as Date).getFullYear()).to.equal(1987)
        ],
        `SELECT t.Address as Addr FROM CUST_BAK t`, [
            rows => expect(rows.length).to.equal(0),
        ],
        `SELECT c.CustNo as CustNo, c.Addr1 as Addr, c.Contact as Contact FROM customer c LIMIT 7`, [
            rows => expect(rows.length).to.equal(7),
            rows => expect(rows[0].CustNo).to.equal(1221),
            rows => expect(rows[4].Contact).to.equal('Chris Thomas')
        ],
        `SELECT c.CustNo as CustNo, c.Addr1 as Addr, c.Contact as Contact FROM customer c OFFSET 11`, [
            rows => expect(rows.length).to.equal(44),
            rows => expect(rows[0].CustNo).to.equal(1563),
            rows => expect(rows[43].Contact).to.equal('Louise Franks')
        ],
        `SELECT c.CustNo as CustNo, c.Addr1 as Addr, c.Contact as Contact FROM customer c LIMIT 20 OFFSET 40`, [
            rows => expect(rows.length).to.equal(15), // there are 55 rows total, so we effectively get the last 15
            rows => expect(rows[0].CustNo).to.equal(4684),
            rows => expect(rows[4].Contact).to.equal('Isabelle Neece')
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
