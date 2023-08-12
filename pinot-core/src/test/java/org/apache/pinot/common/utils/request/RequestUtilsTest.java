/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils.request;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestUtilsTest {
  @Test
  public void testNullLiteralParsing() {
    SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    Expression nullExpr = RequestUtils.getLiteralExpression(nullLiteral);
    Assert.assertEquals(nullExpr.getType(), ExpressionType.LITERAL);
    Assert.assertEquals(nullExpr.getLiteral().getNullValue(), true);
  }

  @Test
  public void testExtractTableName() {
    String query = "select * from myTable";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable"));

    query = "select * from myTable where foo = 'bar'";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable"));

    query = "select * from default.myTable where foo = 'bar'";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable"));

    query = "explain plan for select * from default.myTable where foo = 'bar'";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable"));

    query = "select * from myTable JOIN myTable2 ON myTable.foo = myTable2.bar";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable", "myTable2"));

    query = "select * from myTable,myTable2,myTable3 WHERE myTable.foo = myTable2.bar AND myTable3.foo = 'bar'";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable", "myTable2", "myTable3"));

    query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment\n"
        + "from part, supplier, partsupp, nation, region\n"
        + "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS'\n"
        + "  and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = (\n"
        + "    select min(ps_supplycost)\n"
        + "    from partsupp, supplier, nation, region\n"
        + "    where\n"
        + "      p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey\n"
        + "      and n_regionkey = r_regionkey and r_name = 'EUROPE')\n"
        + "    order by s_acctbal desc, n_name, s_name, p_partkey;";
    Assert.assertEquals(RequestUtils.getTableNames(query),
        ImmutableSet.of("part", "supplier", "partsupp", "nation", "region"));

    query = "select * from myTable UNION select * from myTable2 INTERSECT select * from myTable3";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable", "myTable2", "myTable3"));

    query =
        "WITH t1 AS (SELECT * FROM myTable), t2 AS (SELECT * FROM myTable2) SELECT * FROM t1 JOIN t2 ON t1.foo = t2"
            + ".bar";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable", "myTable2"));

    query =
        "WITH t1 AS (SELECT * FROM myTable), t2 AS (SELECT * FROM myTable2) SELECT * FROM t1 JOIN t2 ON t1.foo = t2"
            + ".bar JOIN myTable3 ON t1.foo = myTable3.bar";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("myTable", "myTable2", "myTable3"));

    // A simple filter query with one table
    query = "Select * from tbl1 where condition1 = filter1";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1"));

    // query with IN / NOT IN clause
    query = "SELECT COUNT(*) FROM tbl1 WHERE userUUID IN (SELECT userUUID FROM tbl2) "
        + "and uuid NOT IN (SELECT uuid from tbl3)";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    // query with two level IN / NOT IN clause
    query = "SELECT COUNT(*) FROM tbl1 WHERE userUUID IN "
        + "(SELECT userUUID FROM tbl2 WHERE uuid NOT IN (SELECT uuid from tbl3)) ";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    // query with JOIN clause
    query = "SELECT tbl1.col1, tbl2.col2 FROM tbl1 JOIN tbl2 ON tbl1.key = tbl2.key WHERE tbl1.col1 = value1";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2"));

    // query with WHERE clause JOIN
    query = "SELECT tbl1.col1, tbl2.col2 FROM tbl1, tbl2 WHERE tbl1.key = tbl2.key AND tbl1.col1 = value1";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2"));

    // query with JOIN clause and table alias
    query = "SELECT A.col1, B.col2 FROM tbl1 AS A JOIN tbl2 AS B ON A.key = B.key WHERE A.col1 = value1";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2"));

    // query with UNION clause
    query = "SELECT * FROM tbl1 UNION ALL SELECT * FROM tbl2 UNION ALL SELECT * FROM tbl3";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    // query with UNION clause and table alias
    query = "SELECT * FROM (SELECT * FROM tbl1) AS t1 UNION SELECT * FROM ( SELECT * FROM tbl2) AS t2";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2"));

    // query with UNION clause and table alias using WITH clause
    query = "WITH tmp1 AS (SELECT * FROM tbl1), \n"
        + "tmp2 AS (SELECT * FROM tbl2) \n"
        + "SELECT * FROM tmp1 UNION ALL SELECT * FROM tmp2";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2"));

    // query with UNION clause and table alias using WITH clause
    query = "WITH tmp1 AS (SELECT * FROM tbl1), \n"
        + "tmp2 AS (SELECT * FROM tbl2) \n"
        + "SELECT * FROM tmp1 UNION ALL SELECT * FROM tmp2 UNION ALL SELECT * FROM tbl3";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3"));

    // query with aliases, JOIN, IN/NOT-IN, group-by
    query = "with tmp as (select col1, count(*) from tbl1 where condition1 = filter1 group by col1), "
        + "tmp2 as (select A.col1, B.col2 from tbl2 as A JOIN tbl3 AS B on A.key = B.key) "
        + "select sum(col1) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from tbl4)";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3", "tbl4"));

    // query with aliases, JOIN, IN/NOT-IN, group-by
    query = "with tmp as (select col1, count(*) from tbl1 where condition1 = filter1 group by col1 order by col2), "
        + "tmp2 as (select A.col1, B.col2 from tbl2 as A JOIN tbl3 AS B on A.key = B.key) "
        + "select sum(col1) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from tbl4) "
        + "order by A.col1";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3", "tbl4"));

    // query with aliases, JOIN, IN/NOT-IN, group-by and explain
    query = "explain plan for with tmp as (select col1, count(*) from tbl1 where condition1 = filter1 group by col1), "
        + "tmp2 as (select A.col1, B.col2 from tbl2 as A JOIN tbl3 AS B on A.key = B.key) "
        + "select sum(col1) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from tbl4)";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1", "tbl2", "tbl3", "tbl4"));

    // test for self join queries
    query = "SELECT tbl1.a FROM tbl1 JOIN(SELECT a FROM tbl1) as self ON tbl1.a=self.a ";
    Assert.assertEquals(RequestUtils.getTableNames(query), ImmutableSet.of("tbl1"));
  }
}
