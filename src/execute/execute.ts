import parseSQL, {Restriction, Join, EqValue, NeValue, GtValue, GeValue, LtValue, LeValue} from '../parse';
import fetchRecords from './fetch-records';





export default async function execute(databasePath: string, sql: string): Promise<any[]> {

    // Parse the SQL statement to get an AST.
    let ast = parseSQL(sql);
    let tableNames = ast.tables;
    let joins = ast.restrictions.filter(r => r.type === 'join') as Join[];
    let where = ast.restrictions.filter(r => r.type !== 'join') as (EqValue|NeValue|GtValue|GeValue|LtValue|LeValue)[];
    let projs = ast.projections;

    // TODO: build a filter function for each table
    let whereTables = where.map(w => w.column.slice(0, w.column.indexOf('.')));
    let filters = tableNames.map(tableName => {
        let conds = where.filter((_, i) => whereTables[i] === tableName);
        let condSources = conds.map(cond => `tuple.${cond.column} ${jsBinaryOperator(cond)} ${JSON.stringify(cond.value)}`);
        if (condSources.length === 0) condSources = ['true'];
        let filter = eval(`(tuple => (${condSources.join(') && (')}))`);
        return filter;
    });

    // TODO: fetch all rowsets with table-level filtering...
    let rowsets = await Promise.all(tableNames.map((tableName, i) => fetchRecords(databasePath, tableName, filters[i])));



    // TODO: consume each join until there is a single rowset left
    // Also if any rowset is empty then the final result is also empty
    while (rowsets.every(rowset => rowset.length > 0) && joins.length > 0) {
        let join = joins.pop()!;

        let lhsTable = join.column.slice(0, join.column.indexOf('.'));
        let rhsTable = join.column2.slice(0, join.column2.indexOf('.'));
        let lhsField = join.column.slice(lhsTable.length + 1);
        let rhsField = join.column2.slice(rhsTable.length + 1);
        let lhsRowset = rowsets.find(rowset => Object.keys(rowset[0]).indexOf(lhsTable) !== -1)!;
        let rhsRowset = rowsets.find(rowset => Object.keys(rowset[0]).indexOf(rhsTable) !== -1)!;

        // TODO: make a lookup map for the RHS table, keyed by the join value
        let getRhsValue: Function = eval(`(tuple => tuple.${rhsTable}.${rhsField})`);
        let rhsTableLookup = rhsRowset.reduce(
            (map: Map<any, any>, tuple) => {
                let rhsValue = getRhsValue(tuple);
                if (map.has(rhsValue)) {
                    // TODO: would be better to assume *one* of wither the LHS or RHS colref is a unique key. Try one then try the other. Will need to factor some code into a function...
                    throw new Error(`${rhsTable}.${rhsField} is not a unique key. In every join clause 'A JOIN B on A.C = B.D, SQL2ADT requires that B.D references a unique key`);
                }
                map.set(rhsValue, tuple[rhsTable]);
                return map;
            },
            new Map()
        );

        // Iterate over the LHS table's rows, adding in the matching RHS table rows or nullifying the whole thing if no match
        let getLhsValue: Function = eval(`(tuple => tuple.${lhsTable}.${lhsField})`);
        let newRowset = lhsRowset.map(tuple => {
            let lhsValue = getLhsValue(tuple);
            let matchingRhsTuple = rhsTableLookup.get(lhsValue);
            if (!matchingRhsTuple) return null;
            tuple[rhsTable] = matchingRhsTuple; // NB: modified in place
            return tuple;
        }).filter(tuple => tuple !== null);
        
        // The side rowset has now been subsumed into the main rowset, and can be discarded
        rowsets = rowsets.filter(rowset => rowset !== lhsRowset && rowset !== rhsRowset).concat([newRowset]);
    }
    let result = rowsets.length === 1 ? rowsets[0] : [];

    // TODO: perform the projections
    let project: Function = eval(`(tuple => ({${projs.map(p => `${p.alias}: tuple.${p.column}`)}}))`);
    result = result.map(tuple => project(tuple));

    return result;











    // [x] 1. construct per-table simple constant filter functions
    // [x] 2. fetch rows for all tables
    // [ ] 3. perform inner joins to create tuples
    // [x]    3.1 for each join, make an 'index' for the smaller rowset (use ES6 Map)
    // [x]    3.2 for each row in the larger rowset, either make a tuple or discard the row
    // [ ] 4. ensure ALL restrictions have been used (sanity check)
    // [x] 5. perform projection

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
