import * as path from 'path';
import {AdtFile} from './adt-file';




export async function fetchRecords(databasePath: string, tableName: string, options: FetchOptions) {
    let tablePath = path.join(databasePath, tableName + '.adt');
    let adt = await AdtFile.open(tablePath, 'ISO-8859-1');
    try {
        let records = await adt.fetchRecords();
        let rows = records.map(rec => ({[tableName]: rec}));
        if (options.filter) rows = rows.filter(options.filter);
        return rows;
    }
    finally {
        adt.close();
    }
}




export interface FetchOptions {
    filter?: (row: any) => boolean;
}
