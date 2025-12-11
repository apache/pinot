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
package org.apache.pinot.tsdb.m3ql.parser;

import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class M3qlParserTest {

  @Test
  public void testBasicFetchCommand() throws Exception {
    String query = "fetch{table=\"metrics\", ts_column=\"timestamp\", ts_unit=\"SECONDS\", value=\"cpu\"}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 1);
    assertEquals(commands.get(0).size(), 9);
    assertEquals(commands.get(0).get(0), "fetch");
    assertEquals(commands.get(0).get(1), "table");
    assertEquals(commands.get(0).get(2), "metrics");
    assertEquals(commands.get(0).get(3), "ts_column");
    assertEquals(commands.get(0).get(4), "timestamp");
    assertEquals(commands.get(0).get(5), "ts_unit");
    assertEquals(commands.get(0).get(6), "SECONDS");
    assertEquals(commands.get(0).get(7), "value");
    assertEquals(commands.get(0).get(8), "cpu");
  }

  @Test
  public void testFetchWithFilter() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"MILLISECONDS\", "
                 + "filter=\"host='web1'\", value=\"v\"}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 1);
    assertEquals(commands.get(0).get(0), "fetch");
    assertTrue(commands.get(0).contains("filter"));
    assertTrue(commands.get(0).contains("host='web1'"));
  }

  @Test
  public void testFetchWithSumNoGroupBy() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | sum{}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(0).get(0), "fetch");
    assertEquals(commands.get(1).get(0), "sum");
    assertEquals(commands.get(1).size(), 1);
  }

  @Test
  public void testFetchWithSumSingleGroupBy() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | sum{hostname}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "sum");
    assertEquals(commands.get(1).get(1), "hostname");
  }

  @Test
  public void testFetchWithSumMultipleGroupBy() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} "
                 + "| sum{hostname,region,datacenter}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "sum");
    assertEquals(commands.get(1).get(1), "hostname,region,datacenter");
  }

  @Test
  public void testMinAggregation() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | min{region}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "min");
    assertEquals(commands.get(1).get(1), "region");
  }

  @Test
  public void testMaxAggregation() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | max{}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "max");
    assertEquals(commands.get(1).size(), 1);
  }

  @Test
  public void testKeepLastValue() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | keepLastValue{}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "keepLastValue");
    assertEquals(commands.get(1).size(), 1);
  }

  @Test
  public void testTransformNullWithoutValue() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | transformNull{}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "transformNull");
    assertEquals(commands.get(1).size(), 1);
  }

  @Test
  public void testTransformNullWithValue() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | transformNull{0.0}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 2);
    assertEquals(commands.get(1).get(0), "transformNull");
    assertEquals(commands.get(1).get(1), "0.0");
  }

  @Test
  public void testFullPipeline() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} "
                 + "| sum{host} "
                 + "| keepLastValue{} "
                 + "| transformNull{0.0}";
    List<List<String>> commands = M3qlParser.parse(query);

    assertEquals(commands.size(), 4);
    assertEquals(commands.get(0).get(0), "fetch");
    assertEquals(commands.get(1).get(0), "sum");
    assertEquals(commands.get(2).get(0), "keepLastValue");
    assertEquals(commands.get(3).get(0), "transformNull");
  }

  @Test(expectedExceptions = ParseException.class)
  public void testMissingClosingBrace() throws Exception {
    String query = "fetch{table=\"m\"";
    M3qlParser.parse(query);
  }

  @Test(expectedExceptions = ParseException.class)
  public void testInvalidCommand() throws Exception {
    String query = "fetch{table=\"m\", ts_column=\"t\", ts_unit=\"SECONDS\", value=\"v\"} | invalid{}";
    M3qlParser.parse(query);
  }
}
