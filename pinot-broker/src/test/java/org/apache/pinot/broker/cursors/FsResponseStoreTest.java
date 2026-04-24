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
package org.apache.pinot.broker.cursors;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;


public class FsResponseStoreTest {
  private static final String BROKER_ID_1 = "Broker_localhost_8099";
  private static final String BROKER_ID_2 = "Broker_otherhost_8099";

  private Path _tempDir;
  private FsResponseStore _store;
  private BrokerMetrics _brokerMetrics;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = java.nio.file.Files.createTempDirectory("fs-response-store-test");
    Path dataDir = _tempDir.resolve("data");
    Path tmpDir = _tempDir.resolve("temp");

    PinotFSFactory.register(PinotFSFactory.LOCAL_PINOT_FS_SCHEME, LocalPinotFS.class.getName(),
        new PinotConfiguration());

    _brokerMetrics = mock(BrokerMetrics.class);
    doNothing().when(_brokerMetrics).addMeteredGlobalValue(any(), anyLong());

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(FsResponseStore.DATA_DIR, dataDir.toUri().toString());
    config.setProperty(FsResponseStore.TEMP_DIR, tmpDir.toString());

    _store = new FsResponseStore();
    _store.init(config, "localhost", 8099, BROKER_ID_1, _brokerMetrics, "1h");
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir.toFile());
  }

  private void storeResponse(String requestId, long expirationTimeMs)
      throws Exception {
    storeResponse(requestId, expirationTimeMs, BROKER_ID_1);
  }

  private void storeResponse(String requestId, long expirationTimeMs, String brokerId)
      throws Exception {
    // Build a minimal BrokerResponseNative that storeResponse() expects
    DataSchema schema = new DataSchema(new String[]{"col1"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT});
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1});
    ResultTable resultTable = new ResultTable(schema, rows);

    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    brokerResponse.setRequestId(requestId);
    brokerResponse.setResultTable(resultTable);
    brokerResponse.setBrokerId(brokerId);
    brokerResponse.setNumRowsResultSet(1);

    _store.storeResponse(brokerResponse);

    // Override the expirationTimeMs by re-reading, mutating, and re-writing
    CursorResponse stored = _store.readResponse(requestId);
    stored.setExpirationTimeMs(expirationTimeMs);
    // Use reflection-free approach: write via the protected writeResponse
    // Since writeResponse is protected, we go through the FS directly
    overwriteExpiration(requestId, expirationTimeMs, brokerId);
  }

  /**
   * Overwrites the metadata file to set custom expirationTimeMs and brokerId,
   * since storeResponse() auto-calculates expiration from current time.
   */
  private void overwriteExpiration(String requestId, long expirationTimeMs, String brokerId)
      throws Exception {
    CursorResponse response = _store.readResponse(requestId);
    CursorResponseNative mutable = new CursorResponseNative();
    mutable.setRequestId(requestId);
    mutable.setBrokerHost(response.getBrokerHost());
    mutable.setBrokerPort(response.getBrokerPort());
    mutable.setBrokerId(brokerId);
    mutable.setSubmissionTimeMs(response.getSubmissionTimeMs());
    mutable.setExpirationTimeMs(expirationTimeMs);
    mutable.setOffset(response.getOffset());
    mutable.setNumRows(response.getNumRows());
    mutable.setNumRowsResultSet(response.getNumRowsResultSet());
    mutable.setBytesWritten(response.getBytesWritten());

    // Write metadata via the serde to the same path the store uses
    java.net.URI dataDir = _tempDir.resolve("data").toUri();
    java.net.URI queryDir =
        new java.net.URI(dataDir.getScheme(), dataDir.getHost(),
            dataDir.getPath().endsWith("/") ? dataDir.getPath() + requestId
                : dataDir.getPath() + "/" + requestId, null);
    java.net.URI metadataFile =
        new java.net.URI(queryDir.getScheme(), queryDir.getHost(), queryDir.getPath() + "/response.json", null);

    org.apache.pinot.spi.filesystem.PinotFS pinotFS = PinotFSFactory.create(dataDir.getScheme());
    Path tempFile = _tempDir.resolve("temp").resolve("overwrite_" + requestId);
    JsonResponseSerde serde = new JsonResponseSerde();
    try (java.io.OutputStream os = java.nio.file.Files.newOutputStream(tempFile)) {
      serde.serialize(mutable, os);
    }
    pinotFS.copyFromLocalFile(tempFile.toFile(), metadataFile);
    java.nio.file.Files.deleteIfExists(tempFile);
  }

  @Test
  public void testDeleteExpiredResponsesDeletesExpired()
      throws Exception {
    long now = System.currentTimeMillis();
    storeResponse("req-expired-1", now - 2000);
    storeResponse("req-expired-2", now - 1000);
    storeResponse("req-alive", now + 60_000);

    int deleted = _store.deleteExpiredResponses(now);

    Assert.assertEquals(deleted, 2);
    Assert.assertFalse(_store.exists("req-expired-1"));
    Assert.assertFalse(_store.exists("req-expired-2"));
    Assert.assertTrue(_store.exists("req-alive"));
  }

  @Test
  public void testDeleteExpiredResponsesReturnsZeroWhenNoneExpired()
      throws Exception {
    long now = System.currentTimeMillis();
    storeResponse("req-1", now + 60_000);
    storeResponse("req-2", now + 120_000);

    int deleted = _store.deleteExpiredResponses(now);

    Assert.assertEquals(deleted, 0);
    Assert.assertTrue(_store.exists("req-1"));
    Assert.assertTrue(_store.exists("req-2"));
  }

  @Test
  public void testDeleteExpiredResponsesOnEmptyStore()
      throws Exception {
    int deleted = _store.deleteExpiredResponses(System.currentTimeMillis());
    Assert.assertEquals(deleted, 0);
  }

  @Test
  public void testDeleteExpiredResponsesFiltersByBrokerId()
      throws Exception {
    long now = System.currentTimeMillis();
    long expiredTime = now - 5000;

    // Store a response owned by this broker (should be deleted)
    storeResponse("req-mine", expiredTime, BROKER_ID_1);
    // Store a response owned by a different broker (should NOT be deleted)
    storeResponse("req-other", expiredTime, BROKER_ID_2);

    int deleted = _store.deleteExpiredResponses(now);

    Assert.assertEquals(deleted, 1, "Only responses owned by this broker should be deleted");
    Assert.assertFalse(_store.exists("req-mine"));
    // The other broker's response still exists on disk (not deleted)
    Assert.assertTrue(_store.exists("req-other"));
  }

  @Test
  public void testDeleteExpiredResponsesExactBoundary()
      throws Exception {
    long cutoff = 50000L;
    storeResponse("req-at-boundary", cutoff);
    storeResponse("req-after-boundary", cutoff + 1);

    int deleted = _store.deleteExpiredResponses(cutoff);

    Assert.assertEquals(deleted, 1, "Response exactly at the boundary should be deleted");
    Assert.assertFalse(_store.exists("req-at-boundary"));
    Assert.assertTrue(_store.exists("req-after-boundary"));
  }

  @Test
  public void testDeleteExpiredResponsesSkipsDirectoriesWithoutMetadata()
      throws Exception {
    long now = System.currentTimeMillis();
    storeResponse("req-valid", now - 1000);

    // Create an orphan directory with no metadata file
    File orphanDir = _tempDir.resolve("data").resolve("orphan-dir").toFile();
    orphanDir.mkdirs();

    int deleted = _store.deleteExpiredResponses(now);

    Assert.assertEquals(deleted, 1);
    Assert.assertFalse(_store.exists("req-valid"));
    Assert.assertTrue(orphanDir.exists(), "Orphan dir should be untouched");
  }
}
