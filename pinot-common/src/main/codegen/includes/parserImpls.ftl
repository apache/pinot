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
    List<SqlNode> list = Lists.newArrayList();
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

/**
 * INSERT INTO [db_name.]table_name
 *   FROM [ FILE | ARCHIVE ] 'file_uri' [, [ FILE | ARCHIVE ] 'file_uri' ]
 */
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

/**
 * define the rest of the sql into SqlStmtList
 */
private void SqlStatementList(SqlNodeList list) :
{
}
{
    {
        list.add(SqlStmt());
    }
}

SqlNodeList SqlStmtsEof() :
{
    SqlParserPos pos;
    SqlNodeList stmts;
}
{
    {
        pos = getPos();
        stmts = new SqlNodeList(pos);
        stmts.add(SqlStmt());
    }
    ( LOOKAHEAD(2, <SEMICOLON> SqlStmt()) <SEMICOLON> SqlStatementList(stmts) )*
    [ <SEMICOLON> ] <EOF>
    {
        return stmts;
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
