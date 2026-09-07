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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.FileUploadDownloadClient.FileUploadType;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.utils.SegmentReplacementUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/// Managed replacement registration must expire without introducing metadata reads or upload locks of its own.
public class ZKOperatorReplacementTest {
  private static final String TABLE = "test_OFFLINE";
  private static final String SEGMENT = "segment";
  private static final TableConfig CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

  @DataProvider
  public Object[][] batchModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "batchModes")
  public void testExpiredUploadRejectedBeforeMetadataRead(boolean batch) throws Exception {
    PinotHelixResourceManager manager = mock(PinotHelixResourceManager.class);
    String uri = uri(1);
    ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
        () -> push(manager, uri, headers(), true, batch));
    assertEquals(failure.getResponse().getStatus(), Response.Status.GONE.getStatusCode());
    verifyNoInteractions(manager);
  }

  @Test
  public void testManagedUploadRequiresExistingLockAndRefreshGuards() throws Exception {
    PinotHelixResourceManager manager = mock(PinotHelixResourceManager.class);
    String uri = uri(System.currentTimeMillis() + 100_000);
    ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
        () -> push(manager, uri, headers(), false, false));
    assertEquals(failure.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    HttpHeaders missingGuard = headers();
    when(missingGuard.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn(null);
    expectThrows(ControllerApplicationException.class, () -> push(manager, uri, missingGuard, true, false));
    verifyNoInteractions(manager);
  }

  @Test(dataProvider = "batchModes")
  public void testSameCrcReplacementUpdatesUrlUsingExistingMetadataOperations(boolean batch) throws Exception {
    PinotHelixResourceManager manager = existingSegment();
    List<SegmentZKMetadata> updates = captureUpdates(manager);
    String uri = uri(System.currentTimeMillis() + 100_000);
    push(manager, uri, headers(), true, batch);
    assertEquals(updates.size(), 2, "Reuse the existing lock and metadata-update writes");
    assertEquals(updates.get(0).getDownloadUrl(), "s3://original/segment");
    assertTrue(updates.get(0).getSegmentUploadStartTime() > 0);
    assertEquals(updates.get(1).getDownloadUrl(), uri);
    assertEquals(updates.get(1).getCrc(), 100);
    assertEquals(updates.get(1).getSegmentUploadStartTime(), -1);
    verify(manager).getSegmentMetadataZnRecord(TABLE, SEGMENT);
    verify(manager, never()).getSegmentsZKMetadata(anyString());
  }

  @Test
  public void testExpiryAfterLockReleasesLockWithoutChangingUrl() throws Exception {
    PinotHelixResourceManager manager = existingSegment();
    List<SegmentZKMetadata> updates = captureUpdates(manager);
    String uri = uri(System.currentTimeMillis() + 100_000);
    try (MockedStatic<SegmentReplacementUtils> utils = mockStatic(SegmentReplacementUtils.class)) {
      utils.when(() -> SegmentReplacementUtils.registrationDeadline(uri, TABLE, SEGMENT))
          .thenReturn(System.currentTimeMillis() + 100_000, 1L);
      ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
          () -> push(manager, uri, headers(), true, false));
      assertEquals(failure.getResponse().getStatus(), Response.Status.GONE.getStatusCode());
    }
    assertEquals(updates.size(), 2);
    assertEquals(updates.get(1).getDownloadUrl(), "s3://original/segment");
    assertEquals(updates.get(1).getSegmentUploadStartTime(), -1);
  }

  private static String uri(long deadline) {
    return SegmentReplacementUtils.outputRoot(URI.create("s3://bucket/table"), TABLE)
        + Long.toString(deadline) + "/" + UUID.randomUUID() + "/" + SEGMENT + ".tar.gz";
  }

  private static HttpHeaders headers() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("100");
    when(headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY)).thenReturn("true");
    return headers;
  }

  private static PinotHelixResourceManager existingSegment() {
    PinotHelixResourceManager manager = mock(PinotHelixResourceManager.class);
    SegmentZKMetadata current = new SegmentZKMetadata(SEGMENT);
    current.setCrc(100);
    current.setDownloadUrl("s3://original/segment");
    when(manager.getSegmentMetadataZnRecord(TABLE, SEGMENT)).thenReturn(current.toZNRecord());
    IdealState idealState = mock(IdealState.class);
    when(idealState.getInstanceStateMap(SEGMENT)).thenReturn(Map.of("server", "ONLINE"));
    when(manager.getTableIdealState(TABLE)).thenReturn(idealState);
    return manager;
  }

  private static List<SegmentZKMetadata> captureUpdates(PinotHelixResourceManager manager) {
    List<SegmentZKMetadata> updates = new ArrayList<>();
    when(manager.updateZkMetadata(eq(TABLE), any(), anyInt())).thenAnswer(i -> {
      SegmentZKMetadata metadata = i.getArgument(1);
      updates.add(new SegmentZKMetadata(new ZNRecord(metadata.toZNRecord())));
      return true;
    });
    return updates;
  }

  private static void push(PinotHelixResourceManager manager, String uri, HttpHeaders headers, boolean parallel,
      boolean batch) throws Exception {
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(metadata.getName()).thenReturn(SEGMENT);
    when(metadata.getCrc()).thenReturn("100");
    ZKOperator operator = new ZKOperator(manager, new ControllerConf(), mock(ControllerMetrics.class));
    if (batch) {
      SegmentUploadMetadata upload = new SegmentUploadMetadata(uri, uri, null, 10L, metadata, Pair.of(null, null));
      operator.completeSegmentsOperations(CONFIG, FileUploadType.METADATA, parallel, true, headers, List.of(upload));
    } else {
      operator.completeSegmentOperations(CONFIG, metadata, FileUploadType.METADATA, null, null, uri, uri, null, 10,
          parallel, true, headers);
    }
  }
}
