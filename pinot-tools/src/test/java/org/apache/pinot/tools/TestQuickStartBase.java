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
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for the sample-query guard that the merged quickstarts rely on. No cluster is started here; only the
 * bootstrap bookkeeping is exercised.
 */
public class TestQuickStartBase {
  private File _tmpDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tmpDir = Files.createTempDirectory("quick-start-base-test").toFile();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tmpDir);
  }

  /**
   * {@code hasTables} must reflect what a quickstart actually bootstrapped, not the union of the batch and stream
   * directory maps it inherits. A quickstart that narrows its batch directories still inherits the full default
   * stream map, and must not report those stream-only tables as present.
   */
  @Test
  public void testHasTablesIgnoresInheritedStreamTables()
      throws Exception {
    TPCHQuickStart tpch = new TPCHQuickStart();
    Assert.assertFalse(tpch.hasTables("customer"), "nothing is bootstrapped before the bootstrap call");

    tpch.bootstrapOfflineTableDirectories(new File(_tmpDir, "tpch"));
    Assert.assertTrue(tpch.hasTables("customer", "lineitem"), "TPCH bootstraps its own batch tables");
    Assert.assertFalse(tpch.hasTables("baseballStats"), "TPCH does not bootstrap the default batch tables");
    // githubEvents and fineFoodReviews are in the inherited default stream map, which TPCH never bootstraps.
    Assert.assertFalse(tpch.hasTables("githubEvents"), "TPCH must not report stream-only tables as present");
    Assert.assertFalse(tpch.hasTables("fineFoodReviews"), "TPCH must not report stream-only tables as present");
  }

  /** Every table the merged batch sample queries guard on must actually be bootstrapped by the batch quickstart. */
  @Test
  public void testBatchQuickstartBootstrapsEveryTableItsSampleQueriesNeed()
      throws Exception {
    Quickstart batch = new Quickstart();
    batch.bootstrapOfflineTableDirectories(new File(_tmpDir, "batch"));
    Assert.assertTrue(batch.hasTables("baseballStats", "dimBaseballTeams", "githubEvents", "githubComplexTypeEvents",
        "airlineStats", "fineFoodReviews", "lineorder", "customer", "dates"));
    Assert.assertFalse(batch.hasTables("meetupRsvp"), "the batch quickstart does not bootstrap stream tables");
  }

  /** Every table the merged stream sample queries guard on must actually be bootstrapped by the stream quickstart. */
  @Test
  public void testStreamQuickstartBootstrapsEveryTableItsSampleQueriesNeed()
      throws Exception {
    RealtimeQuickStart stream = new RealtimeQuickStart();
    stream.bootstrapStreamTableDirectories(new File(_tmpDir, "stream"));
    Assert.assertTrue(stream.hasTables("meetupRsvp", "meetupRsvpJson", "meetupRsvpComplexType", "upsertMeetupRsvp",
        "upsertJsonMeetupRsvp", "upsertPartialMeetupRsvp", "fineFoodReviews", "fineFoodReviews_part_0",
        "fineFoodReviews_part_1"));
    Assert.assertFalse(stream.hasTables("baseballStats"), "the stream quickstart does not bootstrap batch tables");
  }
}
