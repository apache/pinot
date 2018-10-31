/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.routing;

import com.linkedin.pinot.broker.routing.TimeBoundaryService.TimeBoundaryInfo;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.ZkStarter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TimeBoundaryServiceTest {
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  @BeforeTest
  public void beforeTest() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();

    _zkClient = new ZkClient(StringUtil.join("/", StringUtils.chomp(ZkStarter.DEFAULT_ZK_STR, "/")),
        ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String helixClusterName = "TestTimeBoundaryService";
    _zkClient.deleteRecursive("/" + helixClusterName + "/PROPERTYSTORE");
    _zkClient.createPersistent("/" + helixClusterName + "/PROPERTYSTORE", true);
    _propertyStore = new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<ZNRecord>(_zkClient),
        "/" + helixClusterName + "/PROPERTYSTORE", null);
  }

  @AfterTest
  public void afterTest() {
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testExternalViewBasedTimeBoundaryService() throws Exception {
    addingTableToPropertyStore("testResource0");
    addingTableToPropertyStore("testResource1");
    HelixExternalViewBasedTimeBoundaryService tbs = new HelixExternalViewBasedTimeBoundaryService(_propertyStore);
    addingSegmentsToPropertyStore(5, "testResource0");
    ExternalView externalView = constructExternalView("testResource0");

    tbs.updateTimeBoundaryService(externalView);
    TimeBoundaryInfo tbi = tbs.getTimeBoundaryInfoFor("testResource0");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "4");

    addingSegmentsToPropertyStore(50, "testResource1");
    externalView = constructExternalView("testResource1");
    tbs.updateTimeBoundaryService(externalView);
    tbi = tbs.getTimeBoundaryInfoFor("testResource1");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "49");

    addingSegmentsToPropertyStore(50, "testResource0");
    externalView = constructExternalView("testResource0");
    tbs.updateTimeBoundaryService(externalView);
    tbi = tbs.getTimeBoundaryInfoFor("testResource0");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "49");
  }

  private ExternalView constructExternalView(String tableName) {
    ExternalView externalView = new ExternalView(tableName);
    List<OfflineSegmentZKMetadata> offlineResourceZKMetadataListForResource =
        ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, tableName);
    for (OfflineSegmentZKMetadata segmentMetadata : offlineResourceZKMetadataListForResource) {
      externalView.setState(segmentMetadata.getSegmentName(), "localhost", "ONLINE");
    }
    return externalView;
  }

  private void addingSegmentsToPropertyStore(int numSegments, String tableName) {
    for (int i = 0; i < numSegments; ++i) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setSegmentName(tableName + "_" + System.currentTimeMillis() + "_" + i);
      offlineSegmentZKMetadata.setTableName(tableName);
      offlineSegmentZKMetadata.setStartTime(i - 1);
      offlineSegmentZKMetadata.setEndTime(i);
      offlineSegmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
      offlineSegmentZKMetadata.setCrc(-1);
      offlineSegmentZKMetadata.setCreationTime(-1);
      offlineSegmentZKMetadata.setIndexVersion("0");
      offlineSegmentZKMetadata.setPushTime(i + 5);
      offlineSegmentZKMetadata.setSegmentType(SegmentType.OFFLINE);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
    }
  }

  private void addingTableToPropertyStore(String tableName) throws Exception {
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
        .setTimeColumnName("timestamp")
        .setTimeType("DAYS")
        .build();
    ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableConfig.getTableName(),
        TableConfig.toZnRecord(tableConfig));
  }
}
