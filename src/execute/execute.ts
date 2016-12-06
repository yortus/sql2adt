import * as path from 'path';
import parseSQL from '../parse';
import fetchRecords from './fetch-records';





export default async function execute(databasePath: string, sql: string) {

    // TODO: temp testing...
    let ast = parseSQL(sql);
    let tableName = ast.tables[0];
    let tablePath = path.join(databasePath, tableName + '.adt');

    let rows = await fetchRecords(tablePath);

    console.log(rows);
    console.log('DONE!');
}
