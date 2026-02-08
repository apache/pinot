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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class RealtimeTableDataManagerTest {

  @Test
  public void testSetDefaultTimeValueIfInvalid() {
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    long currentTimeMs = System.currentTimeMillis();
    when(segmentZKMetadata.getCreationTime()).thenReturn(currentTimeMs);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("timeColumn").build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("timeColumn", FieldSpec.DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
    RealtimeTableDataManager.setDefaultTimeValueIfInvalid(tableConfig, schema, segmentZKMetadata);
    DateTimeFieldSpec timeFieldSpec = schema.getSpecForTimeColumn("timeColumn");
    assertNotNull(timeFieldSpec);
    assertEquals(timeFieldSpec.getDefaultNullValue(), currentTimeMs);

    schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addDateTime("timeColumn", FieldSpec.DataType.INT, "SIMPLE_DATE_FORMAT|yyyyMMdd", "1:DAYS").build();
    RealtimeTableDataManager.setDefaultTimeValueIfInvalid(tableConfig, schema, segmentZKMetadata);
    timeFieldSpec = schema.getSpecForTimeColumn("timeColumn");
    assertNotNull(timeFieldSpec);
    assertEquals(timeFieldSpec.getDefaultNullValue(),
        Integer.parseInt(DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC).print(currentTimeMs)));
  }

  @Test
  public void testStreamMetadataProviderCache() {
    class FakeRealtimeTableDataManager extends RealtimeTableDataManager {
      private boolean _cacheEntryRemovalNotified;
      public FakeRealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
        super(segmentBuildSemaphore);
        _streamMetadataProviderCache = getStreamMetadataProviderCache();
      }
      public void updateCache() {
        _streamMetadataProviderCache = CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofMillis(1))
            .removalListener((RemovalNotification<String, StreamMetadataProvider> notification) -> {
              StreamMetadataProvider provider = notification.getValue();
              if (provider != null) {
                try {
                  provider.close();
                  _cacheEntryRemovalNotified = true;
                } catch (Exception e) {
                  LOGGER.warn("Failed to close StreamMetadataProvider for key {}", notification.getKey(), e);
                }
              }
            })
            .build();
      }
      public Cache<String, StreamMetadataProvider> getCache() {
        return _streamMetadataProviderCache;
      }
    }
    FakeRealtimeTableDataManager fakeRealtimeTableDataManager = new FakeRealtimeTableDataManager(null);

    RealtimeSegmentDataManager mockRealtimeSegmentDataManager = Mockito.mock(RealtimeSegmentDataManager.class);
    when(mockRealtimeSegmentDataManager.getTableStreamName()).thenReturn("testTable-testTopic");
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    when(mockRealtimeSegmentDataManager.getStreamConsumerFactory()).thenReturn(
        StreamConsumerFactoryProvider.create(streamConfig));

    StreamMetadataProvider streamMetadataProvider =
        fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager);
    Assert.assertEquals(fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager),
        streamMetadataProvider);

    fakeRealtimeTableDataManager.updateCache();
    Assert.assertEquals(fakeRealtimeTableDataManager.getCache().size(), 0);

    StreamMetadataProvider streamMetadataProvider1 =
        fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager);
    Assert.assertNotEquals(streamMetadataProvider, streamMetadataProvider1);

    TestUtils.waitForCondition(
        aVoid -> !(fakeRealtimeTableDataManager.getStreamMetadataProvider(mockRealtimeSegmentDataManager)
            .equals(streamMetadataProvider1)), 5, 2000, "streamMetadataProvider returned from cache must be new.");
    Assert.assertTrue(fakeRealtimeTableDataManager._cacheEntryRemovalNotified);
  }
}
