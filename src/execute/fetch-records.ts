import Adt = require('node_adt');





export default function fetchRecords(path: string, filter: (row: any) => boolean = () => true): Promise<any> {
    return new Promise((resolve, reject) => {
        let adt = new Adt();
        adt.open(path, 'ISO-8859-1', (err, table) => {
            if (err) return reject(err);

            let rows: any[] = [];
            let rowError: any = null;


            table.eachRecord(
                (err, row) => {
                    if (rowError) return;
                    if (err) return reject(rowError = err);
                    if (filter(row)) rows.push(row);
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
