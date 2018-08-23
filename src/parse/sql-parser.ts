const grammar: { parse(text: string): AST; } = require('./sql-grammar');




/**
 * Verifies that `sql` is a valid SQL statement and returns abstract syntax information about the statement.
 * @param {string} sql - the SQL statement to be parsed.
 * @returns {AST} an object containing details about the successfully parsed SQL statement.
 */
export function parseSQL(sql: string): AST {
    try {
        let ast = grammar.parse(sql);
        return ast;
    }
    catch (ex) {
        let startCol = ex.location.start.column;
        let endCol = ex.location.end.column;
        if (endCol <= startCol) endCol = startCol + 1;
        let indicator = Array(startCol).join(' ') + Array(endCol - startCol + 1).join('^');
        throw new Error(`${ex.message}:\n${sql}\n${indicator}`);
    }
}




/** Information associated with a successfully parsed SQL statement. */
export interface AST {
    tables: string[];
    restrictions: Restriction[];
    projections: Projection[];
    limit?: number;
    offset?: number;
    rowIndexAlias?: string;
}
export type Restriction = Join | EqValue | GtValue | LtValue | NeValue | GeValue | LeValue
export type Join = { type: 'join', column: string; column2: string; }
export type EqValue = { type: 'eq-value', column: string; value: string|number; }
export type GtValue = { type: 'gt-value', column: string; value: string|number; }
export type LtValue = { type: 'lt-value', column: string; value: string|number; }
export type NeValue = { type: 'ne-value', column: string; value: string|number; }
export type GeValue = { type: 'ge-value', column: string; value: string|number; }
export type LeValue = { type: 'le-value', column: string; value: string|number; }
export type Projection = { column: string; alias: string; }
