SelectStatement
=   WS   select:SelectClause   WS
    from:FromClause
    where:(WS   WhereClause)?
    lim:(WS   LimitClause)?
    off:(WS   OffsetClause)?
    WS   !.
    {
        let tables = from.tables.map(el => el.name);
        let tableAliases = from.tables.reduce((map, t) => (map[t.alias] = t.name, map), {});
        let restrictions = from.restrictions.concat(where ? where[1] : []).map(r => {
            if (r.type === 'join') {
                return {type: r.type, column: dealias(r.column), column2: dealias(r.column2)};
            }
            else {
                return {type: r.type, column: dealias(r.column), value: r.value};
            }
        });
        let rowIndexInfo = select.find(s => s.column === '%.ROWINDEX');
        let rowIndexAlias = (rowIndexInfo || {}).alias
        select = select.filter(s => s.column !== '%.ROWINDEX');
        let projections = select.map(s => ({column: dealias(s.column), alias: s.alias}));
        let limit = lim ? lim[1] : undefined;
        let offset = off ? off[1] : undefined;

        if ((tables.length > 1 || !!where) && (!!rowIndexAlias || !!lim || !!off)) {
            throw new Error('A query with a JOIN or WHERE clause cannot use ROWINDEX, LIMIT or OFFSET');
        }

        return {tables, restrictions, projections, limit, offset, rowIndexAlias};

        function dealias(s) {
            let i = s.indexOf('.');
            let tableName = s.slice(0, i);
            return (tableAliases[tableName] || tableName) + s.slice(i);
        }
    }

SelectClause
=   "SELECT"i   WS   cols:ResultColumns   { return cols; }

ResultColumns
=   first:ResultColumn   rest:(WS   ","   WS   ResultColumn)*
    { return [first].concat(rest.map(el => el[3])); }

ResultColumn
=   column:QualifiedColumnReference   alias:(WS   "AS"i?   WS   ID)
    { return {column, alias: alias && alias[3]}; }
/   "ROWINDEX"i   WS   "AS"i?   WS   alias:ID
    { return {column: '%.ROWINDEX', alias}; }

FromClause
=   "FROM"   WS   table:Table   joins:JoinClause*
    {
        let tables = [table].concat(joins.map(el => el.table));
        let restrictions = joins.map(el => el.on);
        return {tables, restrictions};
    }

Table
=   name:ID   alias:(WS   "AS"i?   WS   ID)?
    { return {name, alias: alias && alias[3]}; }

JoinClause
=   WS   "INNER JOIN"i   WS   table:Table   WS   "ON"i   WS    on:JoinRestriction
    { return {table, on}; }

JoinRestriction
=   column:QualifiedColumnReference   WS   "="   WS   column2:QualifiedColumnReference
    { return {type: 'join', column, column2 }; }

WhereClause
=   "WHERE"i   WS   first:LogicalExpr   rest:(WS   "AND"i   WS   LogicalExpr)*
    { return [first].concat(rest.map(el => el[3])); }

LogicalExpr
=   column:QualifiedColumnReference   WS   op:"="   WS   value:Literal   { return {type: 'eq-value', column, value}; }
/   column:QualifiedColumnReference   WS   op:">"   WS   value:Literal   { return {type: 'gt-value', column, value}; }
/   column:QualifiedColumnReference   WS   op:"<"   WS   value:Literal   { return {type: 'lt-value', column, value}; }
/   column:QualifiedColumnReference   WS   op:"<>"   WS   value:Literal   { return {type: 'ne-value', column, value}; }
/   column:QualifiedColumnReference   WS   op:"!="   WS   value:Literal   { return {type: 'ne-value', column, value}; }
/   column:QualifiedColumnReference   WS   op:">="   WS   value:Literal   { return {type: 'ge-value', column, value}; }
/   column:QualifiedColumnReference   WS   op:"<="   WS   value:Literal   { return {type: 'le-value', column, value}; }

QualifiedColumnReference
=   tableName:ID   "."   columnName:ID   { return text(); }

LimitClause
=   "LIMIT"i   WS   n:NumericLiteral   { return n; }

OffsetClause
=   "OFFSET"i   WS   n:NumericLiteral   { return n; }

Literal
=   StringLiteral
/   NumericLiteral

StringLiteral
=   "'"   (!['\r\n]   .)*   "'"
    { return text().slice(1, -1); }

NumericLiteral
=   [0-9]+   { return parseInt(text()); }

ID = !KEYWORD   [a-z_]i [a-z0-9_]i*   { return text(); }
KEYWORD = "SELECT"i / "FROM"i / "AS"i / "INNER"i / "JOIN"i / "ON"i / "WHERE"i / "AND"i / "LIMIT"i / "OFFSET"i
WS = [ \t\r\n]*
