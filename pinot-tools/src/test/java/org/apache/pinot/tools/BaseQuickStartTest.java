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
package org.apache.pinot.tools;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests for the sample-query guard that the merged quickstarts rely on. No cluster is started here; only the
/// bootstrap bookkeeping is exercised.
public class BaseQuickStartTest {
  private File _tmpDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tmpDir = Files.createTempDirectory("base-quick-start-test").toFile();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tmpDir);
  }

  /// A bootstrap request built from a directory must resolve its table name the same way [BootstrapTableTool] does,
  /// since that is the name the table is actually created under.
  @Test
  public void testTableNameIsDerivedFromTheBootstrapDirectory() {
    assertEquals(new QuickstartTableRequest("/tmp/quickstart/baseballStats").getTableName(), "baseballStats");
    assertEquals(new QuickstartTableRequest("/tmp/quickstart/lineorder", "TASK").getTableName(), "lineorder");
  }

  /// The sample-query guard must reflect the tables a quickstart actually bootstraps, not the union of the batch and
  /// stream directory maps it inherits. A quickstart that narrows its batch directories still inherits the full
  /// default stream map, and must not report those stream-only tables as present.
  @Test
  public void testBootstrapRequestsIgnoreInheritedStreamTables()
      throws Exception {
    // TPCH narrows the batch directories to the TPCH tables only.
    Set<String> tpchTables =
        tableNames(new TPCHQuickStart().bootstrapOfflineTableDirectories(new File(_tmpDir, "tpch")));
    assertTrue(tpchTables.containsAll(Set.of("customer", "lineitem")), tpchTables.toString());
    assertFalse(tpchTables.contains("baseballStats"), "TPCH does not bootstrap the default batch tables");
    // githubEvents and fineFoodReviews are in the inherited default stream map, which TPCH never bootstraps.
    assertFalse(tpchTables.contains("githubEvents"), "TPCH must not report stream-only tables as present");
    assertFalse(tpchTables.contains("fineFoodReviews"), "TPCH must not report stream-only tables as present");
  }

  /// Every table the merged stream sample queries guard on must actually be bootstrapped by the stream quickstart.
  /// The batch equivalent is covered by the quickstart CI job, which queries each of those tables; the streaming job
  /// only asserts on `meetupRsvp`, so the remaining stream tables are checked here.
  @Test
  public void testStreamQuickstartBootstrapsEveryTableItsSampleQueriesNeed()
      throws Exception {
    Set<String> tables =
        tableNames(new RealtimeQuickStart().bootstrapStreamTableDirectories(new File(_tmpDir, "stream")));
    assertTrue(tables.containsAll(Set.of("meetupRsvp", "meetupRsvpJson", "meetupRsvpComplexType", "upsertMeetupRsvp",
        "upsertJsonMeetupRsvp", "upsertPartialMeetupRsvp", "fineFoodReviews", "fineFoodReviews_part_0",
        "fineFoodReviews_part_1")), tables.toString());
    assertFalse(tables.contains("baseballStats"), "the stream quickstart does not bootstrap batch tables");
  }

  private static Set<String> tableNames(List<QuickstartTableRequest> requests) {
    return requests.stream().map(QuickstartTableRequest::getTableName).collect(Collectors.toSet());
  }
}
