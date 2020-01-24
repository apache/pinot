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
package org.apache.pinot.broker.request;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.pinot.broker.requesthandler.BrokerRequestOptimizer;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.parsers.utils.BrokerRequestComparisonUtils;
import org.apache.pinot.pql.parsers.PinotQuery2BrokerRequestConverter;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Some tests for the SQL compiler.
 * Please note that this test will load test resources: `pql_queries.list` and `pql_queries.list` under `pinot-common` module.
 */
public class PqlAndCalciteSqlCompatibilityTest {

  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  // OPTIMIZER is used to flatten certain queries with filtering optimization.
  // The reason is that SQL parser will parse the structure into a binary tree mode.
  // PQL parser will flat the case of multiple children under AND/OR.
  // After optimization, both BrokerRequests from PQL and SQL should look the same and be easier to compare.
  private static final BrokerRequestOptimizer OPTIMIZER = new BrokerRequestOptimizer();
  private static final Logger LOGGER = LoggerFactory.getLogger(PqlAndCalciteSqlCompatibilityTest.class);

  @Test
  public void testSinglePqlAndSqlCompatible() {
    final String sql =
        "SELECT CarrierDelay, Origin, DayOfWeek FROM mytable WHERE ActualElapsedTime BETWEEN 163 AND 322 OR CarrierDelay IN (17, 266) OR AirlineID IN (19690, 20366) ORDER BY TaxiIn, TailNum LIMIT 1";
    final String pql =
        "SELECT CarrierDelay, Origin, DayOfWeek FROM mytable WHERE ActualElapsedTime BETWEEN 163 AND 322 OR CarrierDelay IN (17, 266) OR AirlineID IN (19690, 20366) ORDER BY TaxiIn, TailNum LIMIT 1";

    // PQL
    LOGGER.info("Trying to compile PQL: {}", pql);
    // NOTE: SQL is always using upper cases, so we need to make the string to upper case in order to match the parsed identifier name.
    final BrokerRequest unOptimizedBrokerRequestFromPQL = COMPILER.compileToBrokerRequest(pql);
    final BrokerRequest brokerRequestFromPQL = OPTIMIZER.optimize(unOptimizedBrokerRequestFromPQL, null);
    LOGGER.debug("Compiled PQL: PQL: {}, BrokerRequest: {}", pql, brokerRequestFromPQL);
    brokerRequestFromPQL.unsetPinotQuery();

    //SQL
    LOGGER.info("Trying to compile SQL: {}", sql);
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    final BrokerRequest brokerRequestFromSQL =
        OPTIMIZER.optimize(new PinotQuery2BrokerRequestConverter().convert(pinotQuery), null);
    LOGGER.debug("Compiled SQL: SQL: {}, PinotQuery: {}, BrokerRequest: {}", sql, pinotQuery, brokerRequestFromSQL);

    // Compare
    LOGGER.info("Trying to compare BrokerRequest -\nFrom PQL: {}\nFrom SQL: {}", brokerRequestFromPQL,
        brokerRequestFromSQL);
    Assert.assertTrue(BrokerRequestComparisonUtils.validate(brokerRequestFromPQL, brokerRequestFromSQL));
  }

  @Test
  public void testSinglePqlAndSqlGroupByOrderByCompatible() {
    final String sql =
        "select group_city, sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count), group_city DESC limit 100";
    final String pql =
        "select group_city, sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count), group_city DESC limit 100";

    // PQL
    LOGGER.info("Trying to compile PQL: {}", pql);
    // NOTE: SQL is always using upper cases, so we need to make the string to upper case in order to match the parsed identifier name.
    final BrokerRequest unOptimizedBrokerRequestFromPQL = COMPILER.compileToBrokerRequest(pql);
    final BrokerRequest brokerRequestFromPQL = OPTIMIZER.optimize(unOptimizedBrokerRequestFromPQL, null);
    LOGGER.debug("Compiled PQL: PQL: {}, BrokerRequest: {}", pql, brokerRequestFromPQL);
    brokerRequestFromPQL.unsetPinotQuery();

    //SQL
    LOGGER.info("Trying to compile SQL: {}", sql);
    final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    final BrokerRequest brokerRequestFromSQL =
        OPTIMIZER.optimize(new PinotQuery2BrokerRequestConverter().convert(pinotQuery), null);
    LOGGER.debug("Compiled SQL: SQL: {}, PinotQuery: {}, BrokerRequest: {}", sql, pinotQuery, brokerRequestFromSQL);

    // Compare
    LOGGER.info("Trying to compare BrokerRequest -\nFrom PQL: {}\nFrom SQL: {}", brokerRequestFromPQL,
        brokerRequestFromSQL);
    Assert.assertTrue(BrokerRequestComparisonUtils.validate(brokerRequestFromPQL, brokerRequestFromSQL));
  }

  @Test
  public void testPqlAndSqlCompatible()
      throws Exception {
    final BufferedReader brPql = new BufferedReader(new InputStreamReader(
        PqlAndCalciteSqlCompatibilityTest.class.getClassLoader().getResourceAsStream("pql_queries.list")));
    final BufferedReader brSql = new BufferedReader(new InputStreamReader(
        PqlAndCalciteSqlCompatibilityTest.class.getClassLoader().getResourceAsStream("sql_queries.list")));
    String sql;
    int seqId = 0;
    while ((sql = brSql.readLine()) != null) {
      final String pql = brPql.readLine();
      try {

        // PQL
        LOGGER.info("Trying to compile PQL Id - {}, PQL: {}", seqId, pql);
        final BrokerRequest brokerRequestFromPQL = OPTIMIZER.optimize(COMPILER.compileToBrokerRequest(pql), null);
        LOGGER.debug("Compiled PQL: Id - {}, PQL: {}, BrokerRequest: {}", seqId, pql, brokerRequestFromPQL);
        brokerRequestFromPQL.unsetPinotQuery();

        //SQL
        LOGGER.info("Trying to compile SQL Id - {}, SQL: {}", seqId, sql);
        final PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
        final BrokerRequest brokerRequestFromSQL =
            OPTIMIZER.optimize(new PinotQuery2BrokerRequestConverter().convert(pinotQuery), null);
        LOGGER.debug("Compiled SQL: Id - {}, SQL: {}, PinotQuery: {}, BrokerRequest: {}", seqId, sql, pinotQuery,
            brokerRequestFromSQL);

        // Compare
        LOGGER.info("Trying to compare BrokerRequest - Id: {}\nFrom PQL: {}\nFrom SQL: {}", seqId, brokerRequestFromPQL,
            brokerRequestFromSQL);
        Assert.assertTrue(BrokerRequestComparisonUtils.validate(brokerRequestFromPQL, brokerRequestFromSQL));
        seqId++;
      } catch (Exception e) {
        LOGGER.error("Failed to compare results from \n\tPQL: {}\nand\n\tSQL: {}", pql, sql, e);
        throw e;
      }
    }
  }
}
