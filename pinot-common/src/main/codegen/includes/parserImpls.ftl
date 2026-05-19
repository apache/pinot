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

/// CREATE TABLE [IF NOT EXISTS] [db.]name (
/// col TYPE [NULL | NOT NULL] [DEFAULT literal]
/// [ DIMENSION | METRIC | DATETIME FORMAT 'fmt' GRANULARITY 'gran' ],
/// ...
/// )
/// TABLE_TYPE = OFFLINE | REALTIME
/// [ PROPERTIES ( 'k' = 'v', ... ) ]
SqlNode SqlPinotCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    boolean ifNotExists = false;
    SqlNodeList columns;
    SqlNodeList primaryKeyColumns = null;
    SqlLiteral tableType;
    SqlNodeList properties = null;
}
{
    <CREATE> { pos = getPos(); }
    <TABLE>
    [ LOOKAHEAD(3) <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    name = CompoundIdentifier()
    columns = PinotColumnList()
    [ LOOKAHEAD(2) primaryKeyColumns = PinotPrimaryKeyList() ]
    <TABLE_TYPE> <EQ>
    tableType = PinotTableTypeLiteral()
    [ <PROPERTIES> properties = PinotPropertyList() ]
    {
        return new SqlPinotCreateTable(pos, name, ifNotExists, columns, primaryKeyColumns,
            tableType, properties);
    }
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

/// DROP TABLE [IF EXISTS] [db.]name [TYPE OFFLINE | REALTIME]
SqlNode SqlPinotDropTable() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    boolean ifExists = false;
    SqlLiteral tableType = null;
}
{
    <DROP> { pos = getPos(); }
    <TABLE>
    [ LOOKAHEAD(2) <IF> <EXISTS> { ifExists = true; } ]
    name = CompoundIdentifier()
    [ <TYPE> tableType = PinotTableTypeLiteral() ]
    {
        return new SqlPinotDropTable(pos, name, ifExists, tableType);
    }
}

/// SHOW TABLES [FROM db]
/// | SHOW CREATE TABLE [db.]name [TYPE OFFLINE | REALTIME]
///
/// Both grammar branches share a leading `SHOW` token; combining them into a single entry point
/// keeps the JavaCC choice unambiguous (no need for LOOKAHEAD across multiple statementParser
/// methods that all start with SHOW).
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
        <CREATE> <TABLE>
        name = CompoundIdentifier()
        [ <TYPE> tableType = PinotTableTypeLiteral() ]
        {
            return new SqlPinotShowCreateTable(pos, name, tableType);
        }
    )
}
