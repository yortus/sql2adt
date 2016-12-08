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

    // TODO: ...
    let rowsets = await Promise.all(tableNames.map((tableName, i) => fetchRecords(databasePath, tableName, filters[i])));

    // TODO: ...
    rowsets.sort((a, b) => a.length > b.length ? 1 : a.length < b.length ? -1 : 0);
    let main = rowsets.pop() as any[];

    // TODO: ...
    rowsets.forEach(side => {

        // If there are no main or side rows, the result will always be empty
        if (main.length === 0 || side.length === 0) {
            main = [];
            return;
        }

        // Find a 'join' restriction that relates any of the main tables to the side table
        let mainTables = Object.keys(main[0]);
        let sideTable = Object.keys(side[0])[0];
        let join = extractJoinFor(mainTables, sideTable, joins);

        // TODO: make a lookup map for the side table, keyed by the join value
        let getValue: Function = eval(`(row => row.${join.column})`);
        let getValue2: Function = eval(`(row => row.${join.column2})`);
        let lookup = side.reduce(
            (map: Map<any, any>, row) => {
                map.set(getValue2(row), row[sideTable]);
                return map;
            },
            new Map()
        );

        // Iterate over the main table rows, adding in the matching side table rows or nullifying the whole thing if no match
        main = main.map(tuple => {
            let val = getValue(tuple);
            let sideRow = lookup.get(val);
            if (!sideRow) return null;
            tuple[sideTable] = sideRow; // NB: modified in place
            return tuple;
        }).filter(tuple => tuple !== null);
    });

    // TODO: perform the projections
    let project: Function = eval(`(tuple => ({${projs.map(p => `${p.alias}: tuple.${p.column}`)}}))`);
    main = main.map(tuple => project(tuple));

    return main;





    // [x] 1. construct per-table simple constant filter functions
    // [x] 2. fetch rows for all tables
    // [ ] 3. perform inner joins to create tuples
    // [x]    3.1 for each join, make an 'index' for the smaller rowset (use ES6 Map)
    // [x]    3.2 for each row in the larger rowset, either make a tuple or discard the row
    // [ ] 4. ensure ALL restrictions have been used (sanity check)
    // [x] 5. perform projection

}





// TODO: ...
function extractJoinFor(mainTables: string[], sideTable: string, joins: Join[]) {

    let joinTables = joins.map(join => {
        return {
            a: join.column.slice(0, join.column.indexOf('.')),
            b: join.column2.slice(0, join.column2.indexOf('.'))
        };
    });

    for (let i = 0; i < joinTables.length; ++i) {
        let joinTable = joinTables[i];


        if (mainTables.indexOf(joinTable.a) !== -1 && sideTable === joinTable.b) {
            let result = joins[i];
            joins.splice(i, 1);
            return result;
        }
        else if (mainTables.indexOf(joinTable.b) !== -1 && sideTable === joinTable.a) {
            let result = {type: 'join', column: joins[i].column2, column2: joins[i].column} as Join;
            joins.splice(i, 1);
            return result;
        }
    }

    // TODO: if we get here something is very wrong (nothing joins side table to main tables)

    // BUG: see next comment

    // TODO: but this *can* happen when the rowsets become sorted such that eg the first two rowsets don't have a direct join, since the sorting is by rowset length, regardless of what joins what
    // how to fix?
    // - different sort order which guarantees the rest works fine?
    // - different algorithm?

    throw new Error(`Internal error`);
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
