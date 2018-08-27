import * as path from 'path';
import {AdtFile} from './execute/adt-file';




export async function count(databasePath: string, tableName: string): Promise<number> {
    let tablePath = path.join(databasePath, tableName + '.adt');
    let adt = await AdtFile.open(tablePath);
    let result = adt.recordCount;
    await adt.close();
    return result;
}
