import * as path from 'path';
import Adt = require('node_adt');





export default function fetchRecords(databasePath: string, tableName: string, filter: (row: any) => boolean = () => true): Promise<any[]> {
    let tablePath = path.join(databasePath, tableName + '.adt');

    return new Promise((resolve, reject) => {
        let adt = new Adt();
        adt.open(tablePath, 'ISO-8859-1', (err, table) => {
            if (err) return reject(err);

            let rows: any[] = [];
            let rowError: any = null;


            table.eachRecord(
                (err, row) => {
                    if (rowError) return;
                    if (err) return reject(rowError = err);
                    let tuple = {[tableName]: row};
                    if (filter(tuple)) rows.push(tuple);
                },
                err => {
                    if (rowError) return;
                    if (err) return reject(err);
                    return resolve(rows);
                }
            );
        });
    });
}
