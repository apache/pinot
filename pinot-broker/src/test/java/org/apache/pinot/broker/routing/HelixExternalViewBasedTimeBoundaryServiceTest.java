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
package org.apache.pinot.broker.routing;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.TimeBoundaryService.TimeBoundaryInfo;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.ZkStarter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class HelixExternalViewBasedTimeBoundaryServiceTest {
  private static final String TIME_COLUMN = "time";

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeClass
  public void setUp() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();

    _zkClient = new ZkClient(StringUtil.join("/", StringUtils.chomp(ZkStarter.DEFAULT_ZK_STR, "/")),
        ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String helixClusterName = "TestTimeBoundaryService";
    _zkClient.deleteRecursively("/" + helixClusterName + "/PROPERTYSTORE");
    _zkClient.createPersistent("/" + helixClusterName + "/PROPERTYSTORE", true);
    _propertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(_zkClient), "/" + helixClusterName + "/PROPERTYSTORE",
            null);
  }

  @AfterClass
  public void tearDown() {
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testExternalViewBasedTimeBoundaryService()
      throws Exception {
    HelixExternalViewBasedTimeBoundaryService timeBoundaryService =
        new HelixExternalViewBasedTimeBoundaryService(_propertyStore);

    int tableIndex = 1;
    for (TimeUnit timeUnit : TimeUnit.values()) {
      String rawTableName = "table" + tableIndex;
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
      addTableConfig(rawTableName, timeUnit, "daily");
      addSchema(rawTableName, timeUnit);

      int endTimeInDays = tableIndex;
      addSegmentZKMetadata(rawTableName, endTimeInDays, timeUnit, false);

      // Should skip real-time external view
      ExternalView externalView = constructRealtimeExternalView(realtimeTableName);
      timeBoundaryService.updateTimeBoundaryService(externalView);
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(rawTableName));
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(realtimeTableName));
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName));

      externalView = constructOfflineExternalView(offlineTableName);
      timeBoundaryService.updateTimeBoundaryService(externalView);
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(rawTableName));
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(realtimeTableName));
      TimeBoundaryInfo timeBoundaryInfo = timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName);
      assertNotNull(timeBoundaryInfo);
      assertEquals(timeBoundaryInfo.getTimeColumn(), TIME_COLUMN);
      assertEquals(Long.parseLong(timeBoundaryInfo.getTimeValue()),
          timeUnit.convert(endTimeInDays - 1, TimeUnit.DAYS) + 1);

      // Test HOURLY push frequency
      addTableConfig(rawTableName, timeUnit, "hourly");
      timeBoundaryService.updateTimeBoundaryService(externalView);
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(rawTableName));
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(realtimeTableName));
      timeBoundaryInfo = timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName);
      assertNotNull(timeBoundaryInfo);
      assertEquals(timeBoundaryInfo.getTimeColumn(), TIME_COLUMN);
      long timeValue = Long.parseLong(timeBoundaryInfo.getTimeValue());
      if (timeUnit == TimeUnit.DAYS) {
        assertEquals(timeValue, timeUnit.convert(endTimeInDays - 1, TimeUnit.DAYS) + 1);
      } else {
        assertEquals(timeValue,
            timeUnit.convert(TimeUnit.HOURS.convert(endTimeInDays, TimeUnit.DAYS) - 1, TimeUnit.HOURS) + 1);
      }

      // Test the same start/end time
      addSegmentZKMetadata(rawTableName, endTimeInDays, timeUnit, true);
      timeBoundaryService.updateTimeBoundaryService(externalView);
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(rawTableName));
      assertNull(timeBoundaryService.getTimeBoundaryInfoFor(realtimeTableName));
      timeBoundaryInfo = timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName);
      assertNotNull(timeBoundaryInfo);
      assertEquals(timeBoundaryInfo.getTimeColumn(), TIME_COLUMN);
      assertEquals(Long.parseLong(timeBoundaryInfo.getTimeValue()), timeUnit.convert(endTimeInDays, TimeUnit.DAYS));

      tableIndex++;
    }
  }

  private void addTableConfig(String rawTableName, TimeUnit timeUnit, String pushFrequency)
      throws Exception {
    ZKMetadataProvider.setOfflineTableConfig(_propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(rawTableName),
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(rawTableName)
            .setTimeColumnName(TIME_COLUMN).setTimeType(timeUnit.name()).setSegmentPushFrequency(pushFrequency).build()
            .toZNRecord());
  }

  private void addSchema(String rawTableName, TimeUnit timeUnit) {
    ZKMetadataProvider.setSchema(_propertyStore,
        new Schema.SchemaBuilder().setSchemaName(rawTableName).addTime(TIME_COLUMN, timeUnit, FieldSpec.DataType.LONG)
            .build());
  }

  private void addSegmentZKMetadata(String rawTableName, int endTimeInDays, TimeUnit timeUnit,
      boolean sameStartEndTime) {
    for (int i = 1; i <= endTimeInDays; i++) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setSegmentName(rawTableName + i);
      long segmentEndTime = i * timeUnit.convert(1L, TimeUnit.DAYS);
      if (sameStartEndTime) {
        offlineSegmentZKMetadata.setStartTime(segmentEndTime);
      } else {
        offlineSegmentZKMetadata.setStartTime(0L);
      }
      offlineSegmentZKMetadata.setEndTime(segmentEndTime);
      offlineSegmentZKMetadata.setTimeUnit(timeUnit);
      ZKMetadataProvider
          .setOfflineSegmentZKMetadata(_propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(rawTableName),
              offlineSegmentZKMetadata);
    }
  }

  private ExternalView constructOfflineExternalView(String offlineTableName) {
    ExternalView externalView = new ExternalView(offlineTableName);
    List<OfflineSegmentZKMetadata> segmentZKMetadataList =
        ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, offlineTableName);
    for (OfflineSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      externalView.setState(segmentZKMetadata.getSegmentName(), "localhost", "ONLINE");
    }
    return externalView;
  }

  private ExternalView constructRealtimeExternalView(String realtimeTableName) {
    return new ExternalView(realtimeTableName);
  }
}
