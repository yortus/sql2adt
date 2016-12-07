import * as path from 'path';
import parseSQL from '../parse';
import fetchRecords from './fetch-records';





export default async function execute(databasePath: string, sql: string) {

    // Parse the SQL statement to get an AST.
    let ast = parseSQL(sql);


    let tableName = ast.tables[0];
    let tablePath = path.join(databasePath, tableName + '.adt');

    let rows = await fetchRecords(tablePath);
    return rows;



    // 1. construct per-table simple constant filter functions
    // 2. fetch rows for all tables
    // 3. perform inner joins to create tuples
    //    3.1 for each join, make an 'index' for the smaller rowset (use ES6 Map)
    //    3.2 for each row in the larger rowset, either make a tuple or discard the row
    // 4. ensure ALL restrictions have been used (sanity check)
    // 5. perform projection

}
