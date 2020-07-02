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

import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class ZKOperatorTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();

    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    _helixResourceManager.addTable(tableConfig);
  }

  @Test
  public void testCompleteSegmentOperations()
      throws Exception {
    ZKOperator zkOperator =
        new ZKOperator(_helixResourceManager, mock(ControllerConf.class), mock(ControllerMetrics.class));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    when(segmentMetadata.getCrc()).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(123L);
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getHeaderString(FileUploadDownloadClient.CustomHeaders.CRYPTER)).thenReturn("crypter");
    zkOperator.completeSegmentOperations(RAW_TABLE_NAME, segmentMetadata, null, null, false, httpHeaders, "downloadUrl",
        false);

    OfflineSegmentZKMetadata segmentZKMetadata =
        _helixResourceManager.getOfflineSegmentZKMetadata(RAW_TABLE_NAME, SEGMENT_NAME);
    assertEquals(segmentZKMetadata.getCrc(), 12345L);
    assertEquals(segmentZKMetadata.getCreationTime(), 123L);
    long pushTime = segmentZKMetadata.getPushTime();
    assertTrue(pushTime > 0);
    assertEquals(segmentZKMetadata.getRefreshTime(), Long.MIN_VALUE);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "downloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");

    // Refresh the segment with unmatched IF_MATCH field
    when(httpHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("123");
    try {
      zkOperator.completeSegmentOperations(RAW_TABLE_NAME, segmentMetadata, null, null, false, httpHeaders,
          "otherDownloadUrl", false);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Refresh the segment with the same segment (same CRC) with matched IF_MATCH field but different creation time,
    // downloadURL and crypter
    when(httpHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(456L);
    when(httpHeaders.getHeaderString(FileUploadDownloadClient.CustomHeaders.CRYPTER)).thenReturn("otherCrypter");
    zkOperator
        .completeSegmentOperations(RAW_TABLE_NAME, segmentMetadata, null, null, false, httpHeaders, "otherDownloadUrl",
            false);
    segmentZKMetadata = _helixResourceManager.getOfflineSegmentZKMetadata(RAW_TABLE_NAME, SEGMENT_NAME);
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

    // Refresh the segment with a different segment (different CRC)
    when(segmentMetadata.getCrc()).thenReturn("23456");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(789L);
    when(httpHeaders.getHeaderString(FileUploadDownloadClient.CustomHeaders.CRYPTER)).thenReturn("otherCrypter");
    // Add a tiny sleep to guarantee that refresh time is different from the previous round
    Thread.sleep(10L);
    zkOperator
        .completeSegmentOperations(RAW_TABLE_NAME, segmentMetadata, null, null, false, httpHeaders, "otherDownloadUrl",
            false);
    segmentZKMetadata = _helixResourceManager.getOfflineSegmentZKMetadata(RAW_TABLE_NAME, SEGMENT_NAME);
    assertEquals(segmentZKMetadata.getCrc(), 23456L);
    // Push time should not change
    assertEquals(segmentZKMetadata.getPushTime(), pushTime);
    // Creation time, refresh time, downloadUrl and crypter should change
    assertEquals(segmentZKMetadata.getCreationTime(), 789L);
    assertTrue(segmentZKMetadata.getRefreshTime() > refreshTime);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "otherDownloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "otherCrypter");
  }

  @AfterClass
  public void tearDown() {
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);

    stopFakeInstances();
    stopController();
    stopZk();
  }
}
