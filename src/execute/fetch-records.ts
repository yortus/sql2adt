import * as path from 'path';
import Adt = require('node_adt');





export default function fetchRecords(databasePath: string, tableName: string, options: FetchOptions): Promise<any[]> {
    let tablePath = path.join(databasePath, tableName + '.adt');

    return new Promise((resolve, reject) => {
        let adt = new Adt();
        adt.open(tablePath, 'ISO-8859-1', (err, table) => {
            if (err) return reject(err);

            let rows: any[] = [];
            let rowError: any = null;

            table.eachRecord(
                {limit: options.limit, offset: options.offset},
                (err, row) => {
                    // If an error occurred on a previous row, skip all callback calls after that.
                    if (rowError) return;

                    // An error occured on this row. Set the flag to skip further row processing, and return the error.
                    if (err) {
                        rowError = err;
                        table.close();
                        return reject(err);
                    }

                    // Add the row to the results.
                    let tuple = {[tableName]: row};
                    if (!options.filter || options.filter(tuple)) rows.push(tuple);
                },
                err => {
                    // If an error occurred on a row, then we've already handled it and returned. Nothing else to do.
                    if (rowError) return;

                    // Ensure we close the table, otherwise the file descriptor will remain open until the process dies.
                    table.close();

                    // Some non-row-specific error has occurred. Return the error.
                    if (err) {
                        return reject(err);
                    }

                    // All rows processed successfully. Return the rows.
                    return resolve(rows);
                }
            );
        });
    });
}





export interface FetchOptions {
    filter?: (row: any) => boolean;
    limit?: number;
    offset?: number;
}
