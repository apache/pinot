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
package com.linkedin.pinot.transport.common;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.routing.HelixExternalViewBasedTimeBoundaryService;
import com.linkedin.pinot.routing.TimeBoundaryService.TimeBoundaryInfo;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONObject;
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

    _zkClient =
        new ZkClient(StringUtil.join("/", StringUtils.chomp(ZkStarter.DEFAULT_ZK_STR, "/")),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String helixClusterName = "TestTimeBoundaryService";
    _zkClient.deleteRecursive("/" + helixClusterName + "/PROPERTYSTORE");
    _zkClient.createPersistent("/" + helixClusterName + "/PROPERTYSTORE", true);
    _propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkClient), "/" + helixClusterName
            + "/PROPERTYSTORE", null);

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
    addingSegmentsToPropertyStore(5, _propertyStore, "testResource0");
    ExternalView externalView = constructExternalView("testResource0");

    tbs.updateTimeBoundaryService(externalView);
    TimeBoundaryInfo tbi = tbs.getTimeBoundaryInfoFor("testResource0");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "4");

    addingSegmentsToPropertyStore(50, _propertyStore, "testResource1");
    externalView = constructExternalView("testResource1");
    tbs.updateTimeBoundaryService(externalView);
    tbi = tbs.getTimeBoundaryInfoFor("testResource1");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "49");

    addingSegmentsToPropertyStore(50, _propertyStore, "testResource0");
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

  private void addingSegmentsToPropertyStore(int numSegments, ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableName) {
    for (int i = 0; i < numSegments; ++i) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setSegmentName(tableName + "_" + System.currentTimeMillis() + "_" + i);
      offlineSegmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
      offlineSegmentZKMetadata.setEndTime(i);
      offlineSegmentZKMetadata.setCrc(-1);
      offlineSegmentZKMetadata.setCreationTime(-1);
      offlineSegmentZKMetadata.setStartTime(i - 1);
      offlineSegmentZKMetadata.setIndexVersion("0");
      offlineSegmentZKMetadata.setPushTime(i + 5);
      offlineSegmentZKMetadata.setTableName(tableName);
      offlineSegmentZKMetadata.setSegmentType(SegmentType.OFFLINE);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
    }
  }

  private void addingTableToPropertyStore(String tableName) throws Exception {

    JSONObject offlineTableConfigJson = new JSONObject();
    offlineTableConfigJson.put("tableName", tableName);

    JSONObject segmentsConfig = new JSONObject();
    segmentsConfig.put("retentionTimeUnit", "DAYS");
    segmentsConfig.put("retentionTimeValue", -1);
    segmentsConfig.put("segmentPushFrequency", "daily");
    segmentsConfig.put("segmentPushType", "APPEND");
    segmentsConfig.put("replication", 1);
    segmentsConfig.put("schemaName", "tableSchema");
    segmentsConfig.put("timeColumnName", "timestamp");
    segmentsConfig.put("timeType", "daysSinceEpoch");
    segmentsConfig.put("segmentAssignmentStrategy", "");
    offlineTableConfigJson.put("segmentsConfig", segmentsConfig);
    JSONObject tableIndexConfig = new JSONObject();
    JSONArray invertedIndexColumns = new JSONArray();
    invertedIndexColumns.put("column1");
    invertedIndexColumns.put("column2");
    tableIndexConfig.put("invertedIndexColumns", invertedIndexColumns);
    tableIndexConfig.put("loadMode", "HEAP");
    tableIndexConfig.put("lazyLoad", "false");
    offlineTableConfigJson.put("tableIndexConfig", tableIndexConfig);
    JSONObject tenants = new JSONObject();
    tenants.put("broker", "brokerTenant");
    tenants.put("server", "serverTenant");
    offlineTableConfigJson.put("tenants", tenants);
    offlineTableConfigJson.put("tableType", "OFFLINE");
    JSONObject metadata = new JSONObject();
    JSONObject customConfigs = new JSONObject();
    customConfigs.put("d2Name", "xlntBetaPinot");
    metadata.put("customConfigs", customConfigs);
    offlineTableConfigJson.put("metadata", metadata);
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(offlineTableConfigJson.toString());
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    ZKMetadataProvider.setOfflineTableConfig(_propertyStore, offlineTableName,
        AbstractTableConfig.toZnRecord(offlineTableConfig));
  }
}
