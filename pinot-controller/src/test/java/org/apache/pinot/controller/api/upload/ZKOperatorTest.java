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
package org.apache.pinot.controller.api.upload;

import java.io.File;
import java.net.URI;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ZKOperatorTest {
  private static final String TABLE_NAME = "operatorTestTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String SEGMENT_NAME = "testSegment";

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    ControllerTestUtils.getHelixResourceManager().addTable(tableConfig);
  }

  @Test
  public void testCompleteSegmentOperations()
      throws Exception {
    ZKOperator zkOperator = new ZKOperator(ControllerTestUtils.getHelixResourceManager(), mock(ControllerConf.class),
        mock(ControllerMetrics.class));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    when(segmentMetadata.getCrc()).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(123L);
    HttpHeaders httpHeaders = mock(HttpHeaders.class);

    // Test if Zk segment metadata is removed if exception is thrown when moving segment to final location.
    try {
      // Create mock finalSegmentLocationURI and currentSegmentLocation.
      URI finalSegmentLocationURI =
          URIUtils.getUri("mockPath", OFFLINE_TABLE_NAME, URIUtils.encode(segmentMetadata.getName()));
      File currentSegmentLocation = new File(new File("foo/bar"), "mockChild");

      zkOperator
          .completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, finalSegmentLocationURI,
              currentSegmentLocation, true, httpHeaders, "downloadUrl",
              true, "crypter", true, 10);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Wait for the segment Zk entry to be deleted.
    TestUtils.waitForCondition(aVoid -> {
      SegmentZKMetadata segmentZKMetadata =
          ControllerTestUtils.getHelixResourceManager().getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
      return segmentZKMetadata == null;
    }, 30_000L, "Failed to delete segmentZkMetadata.");

    zkOperator
        .completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, null, null, true, httpHeaders, "downloadUrl",
            false, "crypter", true, 10);

    SegmentZKMetadata segmentZKMetadata =
        ControllerTestUtils.getHelixResourceManager().getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getCrc(), 12345L);
    assertEquals(segmentZKMetadata.getCreationTime(), 123L);
    long pushTime = segmentZKMetadata.getPushTime();
    assertTrue(pushTime > 0);
    assertEquals(segmentZKMetadata.getRefreshTime(), Long.MIN_VALUE);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "downloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");
    assertEquals(segmentZKMetadata.getSegmentUploadStartTime(), -1);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);

    // Upload the same segment with allowRefresh = false. Validate that an exception is thrown.
    try {
      zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, null, null, false, httpHeaders,
          "otherDownloadUrl", false, "otherCrypter", false, 10);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Refresh the segment with unmatched IF_MATCH field
    when(httpHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("123");
    try {
      zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, null, null, true, httpHeaders,
          "otherDownloadUrl", false, null, true, 10);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Refresh the segment with the same segment (same CRC) with matched IF_MATCH field but different creation time,
    // downloadURL and crypter
    when(httpHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(456L);
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, null, null, true, httpHeaders,
        "otherDownloadUrl", false, "otherCrypter", true, 10);
    segmentZKMetadata =
        ControllerTestUtils.getHelixResourceManager().getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertEquals(segmentZKMetadata.getCrc(), 12345L);
    // Push time should not change
    assertEquals(segmentZKMetadata.getPushTime(), pushTime);
    // Creation time and refresh time should change
    assertEquals(segmentZKMetadata.getCreationTime(), 456L);
    long refreshTime = segmentZKMetadata.getRefreshTime();
    assertTrue(refreshTime > 0);
    // DownloadURL and crypter should not unchanged
    assertEquals(segmentZKMetadata.getDownloadUrl(), "downloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");
    assertEquals(segmentZKMetadata.getSegmentUploadStartTime(), -1);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);

    // Refresh the segment with a different segment (different CRC)
    when(segmentMetadata.getCrc()).thenReturn("23456");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(789L);
    // Add a tiny sleep to guarantee that refresh time is different from the previous round
    // 1 second delay to avoid "org.apache.helix.HelixException: Specified EXTERNALVIEW operatorTestTable_OFFLINE is
    // not found!" exception from being thrown sporadically.
    Thread.sleep(1000L);
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, null, null, true, httpHeaders,
        "otherDownloadUrl", false, "otherCrypter", true, 10);
    segmentZKMetadata =
        ControllerTestUtils.getHelixResourceManager().getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertEquals(segmentZKMetadata.getCrc(), 23456L);
    // Push time should not change
    assertEquals(segmentZKMetadata.getPushTime(), pushTime);
    // Creation time, refresh time, downloadUrl and crypter should change
    assertEquals(segmentZKMetadata.getCreationTime(), 789L);
    assertTrue(segmentZKMetadata.getRefreshTime() > refreshTime);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "otherDownloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "otherCrypter");
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
