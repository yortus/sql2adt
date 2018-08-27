import {parseSQL, Restriction, Join, EqValue, NeValue, GtValue, GeValue, LtValue, LeValue} from '../parse';
import * as path from 'path';
import {AdtFile} from './adt-file';




export async function execute(databasePath: string, sql: string): Promise<any[]> {

    // Parse the SQL statement to get an AST.
    let ast = parseSQL(sql);
    let tableNames = ast.tables;
    let joins = ast.restrictions.filter(r => r.type === 'join') as Join[];
    let predicates = ast.restrictions.filter(r => r.type !== 'join') as (EqValue|NeValue|GtValue|GeValue|LtValue|LeValue)[];
    let projs = ast.projections;
    let usedRestrictions = 0;

    // Handle simple case: single table query with no WHERE clause. LIMIT/OFFSET are only valid for this case.
    if (tableNames.length === 1 && predicates.length === 0) {
        // TODO: ...
        let tablePath = path.join(databasePath, tableNames[0] + '.adt');
        let adt = await AdtFile.open(tablePath);
        try {
            let columnNames = projs.map(p => p.column.split('.')[1]);
            let rowIndexName = ast.rowIndexAlias;
            let records = await adt.fetchRecords({limit: ast.limit, offset: ast.offset, columnNames, rowIndexName});

            // Skip performing projections if all aliases match their column name.
            // TODO: but there may be extra fields, is that OK?
            if (projs.every(p => p.column.split('.')[1] === p.alias)) return records;

            // Perform the projections.
            let kvps = projs.map(p => `${p.alias}: r.${p.column.split('.')[1]}`);
            let project: (record: object) => object = eval(`(r => ({${kvps.join(', ')}}))`);
            records = records.map(project);
            return records;
        }
        finally {
            await adt.close();
        }
    }

    // TODO: build a filter function for each table
    let filters = tableNames.map(tableName => {
        let localPredicates = predicates.filter((_, i) => predicates[i].column.slice(0, predicates[i].column.indexOf('.')) === tableName);
        usedRestrictions += localPredicates.length;
        let predicateSources = localPredicates.map(cond => `tuple.${cond.column} ${jsBinaryOperator(cond)} ${JSON.stringify(cond.value)}`);
        if (predicateSources.length === 0) predicateSources = ['true'];
        let filter: (row: any) => boolean = eval(`(tuple => (${predicateSources.join(') && (')}))`);
        return filter;
    });

    // TODO: fetch all rowsets with table-level filtering...
    let rowsets = await Promise.all(tableNames.map((tableName, i) => fetchRecords(databasePath, tableName, filters[i])));

    // TODO: consume each join until there is a single rowset left
    // TODO: Also if any rowset is empty then the final result is also empty
    while (rowsets.every(rowset => rowset.length > 0) && joins.length > 0) {

        // TODO: ...
        let join = joins.pop()!;
        ++usedRestrictions;
        let lhsTable = join.column.slice(0, join.column.indexOf('.'));
        let rhsTable = join.column2.slice(0, join.column2.indexOf('.'));
        let lhsRowset = rowsets.find(rowset => Object.keys(rowset[0]).indexOf(lhsTable) !== -1)!;
        let rhsRowset = rowsets.find(rowset => Object.keys(rowset[0]).indexOf(rhsTable) !== -1)!;

        // TODO: ...
        let joinedRowset: any[];
        try {
            // Try the join as-is. This will fail if the RHS column in the join does not reference a unique key.
            joinedRowset = joinRowsets(join, lhsRowset, rhsRowset);
        }
        catch (err) {
            // Try the equivalent join with reversed LHS and RHS column references. If that fails too, then we fail.
            joinedRowset = joinRowsets({type: 'join', column: join.column2, column2: join.column}, rhsRowset, lhsRowset);
        }

        // TODO: Update rowsets...
        rowsets = rowsets.filter(rowset => rowset !== lhsRowset && rowset !== rhsRowset).concat([joinedRowset]);
    }

    // TODO: ...
    let result = rowsets.length === 1 ? rowsets[0] : [];
    usedRestrictions += joins.length;

    // TODO: perform the projections
    let project: (row: any) => any = eval(`(tuple => ({${projs.map(p => `${p.alias}: tuple.${p.column}`)}}))`);
    result = result.map(project);

    // TODO: sanity check - were all restrictions consumed to produce the final result?
    if (usedRestrictions !== ast.restrictions.length) {
        throw new Error(`Internal error`);
    }

    // All done.
    return result;
}




// TODO: ...
function joinRowsets(join: Join, lhsRowset: any[], rhsRowset: any[]): any[] {

    let lhsTable = join.column.slice(0, join.column.indexOf('.'));
    let rhsTable = join.column2.slice(0, join.column2.indexOf('.'));
    let lhsField = join.column.slice(lhsTable.length + 1);
    let rhsField = join.column2.slice(rhsTable.length + 1);

    // TODO: make a lookup map for the RHS table, keyed by the join value
    let getRhsValue: Function = eval(`(tuple => tuple.${rhsTable}.${rhsField})`);
    let rhsTableLookup = rhsRowset.reduce(
        (map: Map<any, any>, tuple) => {
            let rhsValue = getRhsValue(tuple);
            if (map.has(rhsValue)) {
                throw new Error(`${rhsTable}.${rhsField} is not a unique key. In every join clause 'A JOIN B on A.C = B.D, SQL2ADT requires that either A.C or B.D references a unique key`);
            }
            map.set(rhsValue, tuple);
            return map;
        },
        new Map()
    );

    // Iterate over the LHS table's rows, adding in the matching RHS table rows or nullifying the whole thing if no match
    let getLhsValue: Function = eval(`(tuple => tuple.${lhsTable}.${lhsField})`);
    let joinedRowset = lhsRowset.map(tuple => {
        let lhsValue = getLhsValue(tuple);
        let matchingRhsTuple = rhsTableLookup.get(lhsValue);
        if (!matchingRhsTuple) return null;
        Object.keys(matchingRhsTuple).forEach(key => tuple[key] = matchingRhsTuple[key]); // NB: modified in place
        return tuple;
    }).filter(tuple => tuple !== null);

    // All done.
    return joinedRowset;
}




// TODO: ...
function jsBinaryOperator(r: Restriction) {
    switch (r.type) {
        case 'eq-value': return '===';
        case 'ne-value': return '!==';
        case 'gt-value': return '>';
        case 'ge-value': return '>=';
        case 'lt-value': return '<';
        case 'le-value': return '<=';
        default:
            // If we get here, the code hasn't accounted for all possible restriction types and needs updating
            throw new Error(`Internal error`);
    }
}




async function fetchRecords(databasePath: string, tableName: string, filter: (row: any) => boolean) {
    let tablePath = path.join(databasePath, tableName + '.adt');
    let adt = await AdtFile.open(tablePath);
    try {
        let records = await adt.fetchRecords();
        let rows = records.map(rec => ({[tableName]: rec})).filter(filter);
        return rows;
    }
    finally {
        adt.close();
    }
}
