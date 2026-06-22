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
package org.apache.pinot.tools.pagecache;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.pagecache.WarmupQueryUtils.Candidate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QueryLogWarmupQuerySourceTest {
  private File _logFile;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _logFile = File.createTempFile("warmup-query-log", ".log");
    List<String> lines = List.of(
        // Single-stage query, raw table name.
        "INFO requestId=1,table=tableA,queryHash=h1,timeMs=10,docs=100/200,exceptions=0,query=SELECT * FROM tableA",
        // Single-stage query referencing the table-name-with-type; normalizes to tableA.
        "INFO requestId=2,table=tableA_OFFLINE,queryHash=h2,timeMs=20,docs=300/400,exceptions=0,"
            + "query=SELECT * FROM tableA_OFFLINE",
        // Multi-stage query touching two tables; must be skipped.
        "INFO requestId=3,table=[tableA, tableB],queryHash=h3,timeMs=30,docs=1/2,exceptions=0,"
            + "query=SELECT * FROM tableA JOIN tableB ON tableA.id = tableB.id",
        // A different table.
        "INFO requestId=4,table=tableB,queryHash=h4,timeMs=5,docs=10/20,exceptions=0,query=SELECT * FROM tableB",
        // An errored query; still collected (the selector filters on error code).
        "INFO requestId=5,table=tableA,queryHash=h5,timeMs=1,docs=0/0,exceptions=1,query=SELECT bad FROM tableA");
    Files.write(_logFile.toPath(), lines, StandardCharsets.UTF_8);
  }

  @AfterMethod
  public void tearDown() {
    if (_logFile != null) {
      _logFile.delete();
    }
  }

  @Test
  public void testGroupsByRawTableNormalizingTypeSuffixAndSkippingMultiStage()
      throws Exception {
    WarmupQuerySource.QueryLog source = new WarmupQuerySource.QueryLog(List.of(_logFile), 0);
    Map<String, List<Candidate>> byTable = source.fetchCandidatesByTable(Set.of("tableA"));

    List<Candidate> tableA = byTable.get("tableA");
    // The raw, the type-suffixed (normalized), and the errored row — but not the multi-stage list row.
    assertEquals(tableA.size(), 3);
    assertTrue(tableA.stream().anyMatch(c -> c.getQuery().equals("SELECT * FROM tableA")));
    assertTrue(tableA.stream().anyMatch(c -> c.getQuery().equals("SELECT * FROM tableA_OFFLINE")));
    assertFalse(tableA.stream().anyMatch(c -> c.getQuery().contains("JOIN")));
    // tableB was not requested.
    assertFalse(byTable.containsKey("tableB"));
  }

  @Test
  public void testFetchesMultipleRequestedTables()
      throws Exception {
    WarmupQuerySource.QueryLog source = new WarmupQuerySource.QueryLog(List.of(_logFile), 0);
    Map<String, List<Candidate>> byTable = source.fetchCandidatesByTable(Set.of("tableA", "tableB"));
    assertEquals(byTable.get("tableA").size(), 3);
    assertEquals(byTable.get("tableB").size(), 1);
  }
}
