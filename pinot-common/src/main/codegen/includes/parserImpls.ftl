<#--
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
-->

private void DataFileDef(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlNode uri;
}
{
    ( <FILE> | <ARCHIVE> )
    {
        pos = getPos();
        list.add(StringLiteral());
    }
}

SqlNodeList DataFileDefList() :
{
    SqlParserPos pos;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <FROM> { pos = getPos(); }
    { pos = getPos(); }
    DataFileDef(list)
    ( <COMMA> DataFileDef(list) )*
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

/// INSERT INTO [db_name.]table_name
/// FROM [ FILE | ARCHIVE ] 'file_uri' [, [ FILE | ARCHIVE ] 'file_uri' ]
SqlInsertFromFile SqlInsertFromFile() :
{
    SqlParserPos pos;
    SqlIdentifier dbName = null;
    SqlIdentifier tableName;
    SqlNodeList fileList = null;
}
{
    <INSERT> { pos = getPos(); }
    <INTO>
    [
        dbName = SimpleIdentifier()
        <DOT>
    ]

    tableName = SimpleIdentifier()
    [
        fileList = DataFileDefList()
    ]
    {
        return new SqlInsertFromFile(pos, dbName, tableName, fileList);
    }
}

void SqlAtTimeZone(List<Object> list, ExprContext exprContext, Span s) :
{
    List<Object> list2;
    SqlOperator op;
}
{
    {
        checkNonQueryExpression(exprContext);
        s.clear().add(this);
    }
    <AT> <TIME> <ZONE> { op = SqlAtTimeZone.INSTANCE; }
    list2 = Expression2(ExprContext.ACCEPT_SUB_QUERY) {
        list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
        list.addAll(list2);
    }
}

SqlNode SqlPhysicalExplain() :
{
    SqlNode stmt;
    SqlExplainLevel detailLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    SqlExplain.Depth depth = SqlExplain.Depth.PHYSICAL;
    final SqlExplainFormat format = SqlExplainFormat.TEXT;
}
{
    <EXPLAIN> <IMPLEMENTATION> <PLAN>
    [ detailLevel = ExplainDetailLevel() ]
    <FOR> stmt = SqlQueryOrDml() {
        return new SqlPhysicalExplain(getPos(),
            stmt,
            detailLevel.symbol(SqlParserPos.ZERO),
            depth.symbol(SqlParserPos.ZERO),
            format.symbol(SqlParserPos.ZERO),
            nDynamicParams);
    }
}

/// CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]name ( ... )
/// [ REFRESH [INTERVAL] EVERY <period> ]
/// [ PROPERTIES ( 'k' = 'v', ... ) ]
/// AS <query>
///
/// The optional REFRESH clause registers a per-table cron schedule for the MV
/// minion task. When omitted, the task scheduler picks up the MV under the
/// cluster-wide default cron (`controller.task.frequencyInSeconds` /
/// `controller.task.taskTypeFrequenciesInSeconds.MaterializedViewTask`).
///
/// The `AS <query>` clause accepts only SELECT/VALUES/WITH/UNION/etc. queries
/// — NOT DML (INSERT/UPDATE/DELETE/MERGE). We invoke `OrderedQueryOrExpr` with
/// `ACCEPT_QUERY` rather than `SqlQueryOrDml` (the default Calcite production
/// that also accepts DML) because a materialized view's body is always a
/// projection-style query whose output rows are persisted; routing a DML
/// statement here would either be silently ignored by the downstream MV
/// analyzer or fail with a non-actionable error well below the grammar.
SqlNode SqlPinotCreateMaterializedView() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    boolean ifNotExists = false;
    // Column list is optional. When absent, the DDL compiler infers the columns from
    // the AS <query> projection via SingleStageMaterializedViewSchemaInferer. We model
    // the absent case as an empty SqlNodeList so downstream consumers can branch on
    // emptiness rather than nullability — keeps the consumer's public API stable.
    SqlNodeList columns = SqlNodeList.EMPTY;
    SqlPinotRefreshClause refresh = null;
    SqlNodeList properties = null;
    SqlNode query;
}
{
    <CREATE> { pos = getPos(); }
    <MATERIALIZED> <VIEW>
    [ LOOKAHEAD(3) <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    name = CompoundIdentifier()
    [ LOOKAHEAD(2) columns = PinotColumnList() ]
    [ refresh = PinotRefreshClause() ]
    [ <PROPERTIES> properties = PinotPropertyList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlPinotCreateMaterializedView(pos, name, ifNotExists, columns, refresh,
            properties, query);
    }
}

SqlPinotRefreshClause PinotRefreshClause() :
{
    SqlParserPos pos;
    SqlLiteral refreshPeriod;
}
{
    <REFRESH> { pos = getPos(); }
    [ <INTERVAL> ]
    <EVERY> refreshPeriod = PinotRefreshEveryPeriod()
    {
        return new SqlPinotRefreshClause(pos, refreshPeriod);
    }
}

/// EVERY period. Two forms are accepted:
///   1. Quoted Pinot period literal, e.g. `'1d'`, `'6h'`, `'15m'` — passes through verbatim.
///   2. Sugared `N <UNIT>`, where `UNIT` is one of `MINUTE / MINUTES / HOUR / HOURS / DAY /
///      DAYS`, normalized to `Nm`, `Nh`, `Nd` respectively.
///
/// Common-but-unsupported units (`SECOND[S]`, `WEEK[S]`, `MONTH[S]`, `YEAR[S]`) are
/// explicitly matched and rejected here with a message listing the supported units, rather
/// than left to fall off the parser's default "Encountered ..." path which prints an empty
/// expected-tokens list and offers no hint about the supported set.
SqlLiteral PinotRefreshEveryPeriod() :
{
    SqlLiteral n;
    SqlParserPos pos;
    String unitSuffix = null;
    String unsupportedUnit = null;
}
{
    (
        n = NumericLiteral()
        (
            <DAY> { unitSuffix = "d"; pos = getPos(); }
        |   <DAYS> { unitSuffix = "d"; pos = getPos(); }
        |   <HOUR> { unitSuffix = "h"; pos = getPos(); }
        |   <HOURS> { unitSuffix = "h"; pos = getPos(); }
        |   <MINUTE> { unitSuffix = "m"; pos = getPos(); }
        |   <MINUTES> { unitSuffix = "m"; pos = getPos(); }
        |   <SECOND>  { unsupportedUnit = "SECOND";  pos = getPos(); }
        |   <SECONDS> { unsupportedUnit = "SECONDS"; pos = getPos(); }
        |   <WEEK>    { unsupportedUnit = "WEEK";    pos = getPos(); }
        |   <WEEKS>   { unsupportedUnit = "WEEKS";   pos = getPos(); }
        |   <MONTH>   { unsupportedUnit = "MONTH";   pos = getPos(); }
        |   <MONTHS>  { unsupportedUnit = "MONTHS";  pos = getPos(); }
        |   <YEAR>    { unsupportedUnit = "YEAR";    pos = getPos(); }
        |   <YEARS>   { unsupportedUnit = "YEARS";   pos = getPos(); }
        )
        {
            if (unsupportedUnit != null) {
                throw new RuntimeException(
                    "REFRESH EVERY: unit '" + unsupportedUnit + "' is not supported. "
                        + "Supported units: MINUTE / MINUTES, HOUR / HOURS, DAY / DAYS. "
                        + "Alternatively use the quoted Pinot period form, e.g. '15m', '6h', '1d'.");
            }
            return SqlLiteral.createCharString(n.toValue() + unitSuffix, pos);
        }
    |
        { return (SqlLiteral) StringLiteral(); }
    )
}

/// CREATE TABLE [IF NOT EXISTS] [db.]name (
/// col TYPE [NULL | NOT NULL] [DEFAULT literal]
/// [ DIMENSION | METRIC | DATETIME FORMAT 'fmt' GRANULARITY 'gran' ],
/// ...
/// )
/// TABLE_TYPE = OFFLINE | REALTIME
/// [ PROPERTIES ( 'k' = 'v', ... ) ]
///
/// or the options-defined form, with no column list and no TABLE_TYPE:
///
/// CREATE TABLE [IF NOT EXISTS] [db.]name WITH ( key = value, ... )
///
/// In the options-defined form the schema and table config are derived entirely from the WITH
/// options by a pluggable handler in the DDL compiler (e.g. a handler that connects to an
/// external catalog named by the options and infers the schema from it). The two forms are
/// disambiguated by the token after the table name: `WITH` vs `(`.
SqlNode SqlPinotCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    boolean ifNotExists = false;
    SqlNodeList columns;
    SqlNodeList primaryKeyColumns = null;
    SqlLiteral tableType;
    SqlNodeList properties = null;
    SqlNodeList withOptions;
}
{
    <CREATE> { pos = getPos(); }
    <TABLE>
    [ LOOKAHEAD(3) <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    name = CompoundIdentifier()
    (
        <WITH> withOptions = PinotWithOptionList()
        {
            return new SqlPinotCreateTable(pos, name, ifNotExists, withOptions);
        }
    |
        columns = PinotColumnList()
        [ LOOKAHEAD(2) primaryKeyColumns = PinotPrimaryKeyList() ]
        <TABLE_TYPE> <EQ>
        tableType = PinotTableTypeLiteral()
        [ <PROPERTIES> properties = PinotPropertyList() ]
        {
            return new SqlPinotCreateTable(pos, name, ifNotExists, columns, primaryKeyColumns,
                tableType, properties);
        }
    )
}

SqlNodeList PinotColumnList() :
{
    SqlParserPos pos;
    SqlPinotColumnDeclaration col;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { pos = getPos(); }
    col = PinotColumnDeclaration() { list.add(col); }
    ( <COMMA> col = PinotColumnDeclaration() { list.add(col); } )*
    <RPAREN>
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

SqlNodeList PinotPrimaryKeyList() :
{
    SqlParserPos pos;
    SqlIdentifier col;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <PRIMARY> { pos = getPos(); } <KEY>
    <LPAREN>
    col = SimpleIdentifier() { list.add(col); }
    ( <COMMA> col = SimpleIdentifier() { list.add(col); } )*
    <RPAREN>
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

SqlPinotColumnDeclaration PinotColumnDeclaration() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlDataTypeSpec type;
    boolean nullable = true;
    SqlNode defaultValue = null;
    String role = null;
    SqlNode fmtNode = null;
    SqlNode granNode = null;
    boolean multiValue = false;
}
{
    name = SimpleIdentifier() { pos = getPos(); }
    type = DataType()
    [
        LOOKAHEAD(2)
        <NOT> <NULL> { nullable = false; }
    |
        <NULL> { nullable = true; }
    ]
    [ <DEFAULT_> defaultValue = Literal() ]
    [
        <DIMENSION> { role = "DIMENSION"; } [ <ARRAY> { multiValue = true; } ]
    |
        <METRIC> { role = "METRIC"; }
    |
        <DATETIME> { role = "DATETIME"; }
        <FORMAT> fmtNode = StringLiteral()
        <GRANULARITY> granNode = StringLiteral()
    ]
    {
        return new SqlPinotColumnDeclaration(pos, name, type, nullable, defaultValue, role,
            (SqlLiteral) fmtNode, (SqlLiteral) granNode, multiValue);
    }
}

SqlLiteral PinotTableTypeLiteral() :
{
    String value;
    SqlParserPos pos;
}
{
    (
        <OFFLINE> { value = "OFFLINE"; pos = getPos(); }
    |
        <REALTIME> { value = "REALTIME"; pos = getPos(); }
    )
    {
        return SqlLiteral.createCharString(value, pos);
    }
}

SqlNodeList PinotPropertyList() :
{
    SqlParserPos pos;
    SqlPinotProperty prop;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { pos = getPos(); }
    [
        prop = PinotProperty() { list.add(prop); }
        ( <COMMA> prop = PinotProperty() { list.add(prop); } )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

SqlPinotProperty PinotProperty() :
{
    SqlParserPos pos;
    SqlNode keyNode;
    SqlNode valueNode;
}
{
    keyNode = StringLiteral() { pos = getPos(); }
    <EQ> valueNode = StringLiteral()
    {
        return new SqlPinotProperty(pos, (SqlLiteral) keyNode, (SqlLiteral) valueNode);
    }
}

/// Parenthesized, comma-separated option list for the options-defined CREATE TABLE form.
/// Unlike PROPERTIES (whose keys and values are always quoted string literals), WITH options
/// accept the option shape common to external-table DDL in other engines: unquoted — possibly
/// dotted — identifier keys and typed literal values, e.g. `catalog_type = 'rest'`,
/// `storage.region = 'us-west-2'`, `enable_schema_evolution = true`. Quoted keys remain
/// accepted, so the PROPERTIES style also works. Keys and values are normalized to character
/// literals at parse time so downstream consumers uniformly see string pairs
/// ([SqlPinotProperty]).
SqlNodeList PinotWithOptionList() :
{
    SqlParserPos pos;
    SqlPinotProperty option;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { pos = getPos(); }
    [
        option = PinotWithOption() { list.add(option); }
        ( <COMMA> option = PinotWithOption() { list.add(option); } )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

/// A single `key = value` WITH option. Key: quoted string literal or (dotted) compound
/// identifier — identifier keys are case-preserved (the parser runs with unquoted casing
/// UNCHANGED) and joined with `.`. Value: quoted string literal, TRUE / FALSE, or an unsigned
/// numeric literal.
SqlPinotProperty PinotWithOption() :
{
    SqlParserPos pos;
    SqlNode keyNode;
    SqlIdentifier keyId;
    SqlLiteral key;
    SqlNode valueNode;
    SqlLiteral value;
}
{
    (
        keyNode = StringLiteral()
        {
            pos = getPos();
            key = (SqlLiteral) keyNode;
        }
    |
        keyId = CompoundIdentifier()
        {
            pos = getPos();
            key = SqlLiteral.createCharString(String.join(".", keyId.names), keyId.getParserPosition());
        }
    )
    <EQ>
    (
        valueNode = StringLiteral() { value = (SqlLiteral) valueNode; }
    |
        <TRUE> { value = SqlLiteral.createCharString("true", getPos()); }
    |
        <FALSE> { value = SqlLiteral.createCharString("false", getPos()); }
    |
        valueNode = UnsignedNumericLiteral()
        {
            value = SqlLiteral.createCharString(((SqlLiteral) valueNode).toValue(), getPos());
        }
    )
    {
        return new SqlPinotProperty(pos, key, value);
    }
}

/// DROP TABLE [IF EXISTS] [db.]name [TYPE OFFLINE | REALTIME]
/// | DROP MATERIALIZED VIEW [IF EXISTS] [db.]name
///
/// Both branches share the leading `DROP` token; combining them into a single entry point keeps
/// the JavaCC choice unambiguous (no need for LOOKAHEAD across multiple statementParser methods
/// that all start with DROP).
///
/// `DROP MATERIALIZED VIEW` does NOT take a `TYPE` clause: an MV is always realized as an
/// OFFLINE physical table, so accepting `TYPE REALTIME` would be confusing and `TYPE OFFLINE`
/// would be redundant. The controller refuses the bare `DROP TABLE` form on an MV (see
/// `PinotDdlRestletResource#executeDrop`) and refuses the MV form on a plain table — the two
/// statements are strictly partitioned by underlying TableConfig shape (Q2=B contract).
SqlNode SqlPinotDrop() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    boolean ifExists = false;
    SqlLiteral tableType = null;
}
{
    <DROP> { pos = getPos(); }
    (
        <TABLE>
        [ LOOKAHEAD(2) <IF> <EXISTS> { ifExists = true; } ]
        name = CompoundIdentifier()
        [ <TYPE> tableType = PinotTableTypeLiteral() ]
        {
            return new SqlPinotDropTable(pos, name, ifExists, tableType);
        }
    |
        <MATERIALIZED> <VIEW>
        [ LOOKAHEAD(2) <IF> <EXISTS> { ifExists = true; } ]
        name = CompoundIdentifier()
        {
            return new SqlPinotDropMaterializedView(pos, name, ifExists);
        }
    )
}

/// SHOW TABLES [FROM db]
/// | SHOW MATERIALIZED VIEWS [FROM db]
/// | SHOW CREATE TABLE [db.]name [TYPE OFFLINE | REALTIME]
/// | SHOW CREATE MATERIALIZED VIEW [db.]name
///
/// All four grammar branches share a leading `SHOW` token; combining them into a single entry
/// point keeps the JavaCC choice unambiguous (no need for LOOKAHEAD across multiple
/// statementParser methods that all start with SHOW).
///
/// `SHOW CREATE MATERIALIZED VIEW` does NOT take a `TYPE` clause: an MV is always backed by an
/// OFFLINE table, so accepting `TYPE REALTIME` would be confusing and accepting `TYPE OFFLINE`
/// would be redundant. The controller refuses the bare `SHOW CREATE TABLE` form on an MV (see
/// `PinotDdlRestletResource#executeShowCreate`) — callers always use this dedicated form so the
/// reverse DDL output is unambiguous (`CREATE MATERIALIZED VIEW`, not `CREATE TABLE`).
///
/// `SHOW MATERIALIZED VIEWS` is the MV-listing peer of `SHOW TABLES`: it returns the raw names
/// (no `_OFFLINE` suffix) of every TableConfig in the resolved database whose
/// `isMaterializedView` flag is true (PR #18564 is the canonical MV identity contract).
/// Choosing the plural lexeme `VIEWS` keeps the form aligned with `SHOW TABLES` and matches
/// the established Snowflake-style catalog-listing convention; the resulting names are
/// directly reusable as input to `SHOW CREATE MATERIALIZED VIEW` / `DROP MATERIALIZED VIEW`.
/// `SHOW TABLES` and `SHOW MATERIALIZED VIEWS` are intentionally NOT mutually exclusive at the
/// listing level — an MV physically is an OFFLINE table, and a user who runs `SHOW TABLES`
/// today sees it. Separating the two views at the DDL surface lets callers pick the verb that
/// matches their intent without renaming what `SHOW TABLES` has always meant.
SqlNode SqlPinotShow() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlIdentifier database = null;
    SqlLiteral tableType = null;
}
{
    <SHOW> { pos = getPos(); }
    (
        <TABLES>
        [ <FROM> database = SimpleIdentifier() ]
        {
            return new SqlPinotShowTables(pos, database);
        }
    |
        <MATERIALIZED> <VIEWS>
        [ <FROM> database = SimpleIdentifier() ]
        {
            return new SqlPinotShowMaterializedViews(pos, database);
        }
    |
        <CREATE>
        (
            <TABLE>
            name = CompoundIdentifier()
            [ <TYPE> tableType = PinotTableTypeLiteral() ]
            {
                return new SqlPinotShowCreateTable(pos, name, tableType);
            }
        |
            <MATERIALIZED> <VIEW>
            name = CompoundIdentifier()
            {
                return new SqlPinotShowCreateMaterializedView(pos, name);
            }
        )
    )
}
