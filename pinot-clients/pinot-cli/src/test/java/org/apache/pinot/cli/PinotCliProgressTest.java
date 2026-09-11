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
package org.apache.pinot.cli;

import java.util.List;
import org.apache.pinot.spi.query.QueryProgressStats;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PinotCliProgressTest {
  @Test
  public void testSingleStageProgressUsesOneLine() {
    String output = PinotCli.Progress.formatProgress(new QueryProgressStats(5, 10), 123);

    assertFalse(output.contains("\n"));
    assertTrue(output.contains("50.0%"));
    assertTrue(output.contains("5/10"));
    assertTrue(output.endsWith("123 ms"));
  }

  @Test
  public void testMultiStageProgressUsesAggregateAndDetailRows() {
    QueryProgressStats broker = new QueryProgressStats("Broker", 1, 2, 0, -1, true);
    QueryProgressStats server = new QueryProgressStats("Server server-1", 3, 6, 2, 4, true);
    QueryProgressStats query = QueryProgressStats.aggregate(List.of(broker, server)).withLabel("Query")
        .withDetails(List.of(broker, server));

    String[] lines = PinotCli.Progress.formatProgress(query, 250).split("\n");

    assertEquals(lines.length, 3);
    assertTrue(lines[0].startsWith("Query"));
    assertTrue(lines[0].endsWith("250 ms"));
    assertTrue(lines[1].startsWith("Broker"));
    assertTrue(lines[2].startsWith("Server server-1"));
  }

  @Test
  public void testUnknownProgressShowsUnknownDenominator() {
    String output = PinotCli.Progress.formatProgress(QueryProgressStats.unknown("Server", true), 50);

    assertTrue(output.contains("?.?%"));
    assertTrue(output.contains("0/?"));
  }
}
