/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.routing.HelixExternalViewBasedTimeBoundaryService;
import com.linkedin.pinot.routing.TimeBoundaryService.TimeBoundaryInfo;


public class TestTimeBoundaryService {

  private String _zkBaseUrl = "localhost:2181";
  private String _helixClusterName = "TestTimeBoundaryService";
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeTest
  public void beforeTest() {

    _zkClient =
        new ZkClient(StringUtil.join("/", StringUtils.chomp(_zkBaseUrl, "/")),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    _zkClient.deleteRecursive("/" + _helixClusterName + "/PROPERTYSTORE");
    _zkClient.createPersistent("/" + _helixClusterName + "/PROPERTYSTORE", true);
    _propertyStore = new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkClient), "/" + _helixClusterName + "/PROPERTYSTORE", null);

  }

  @AfterTest
  public void afterTest() {
    _zkClient.deleteRecursive("/TestTimeBoundaryService");
    _zkClient.close();
  }

  @Test
  public void testExternalViewBasedTimeBoundaryService() {
    addingResourceToPropertyStore("testResource0");
    addingResourceToPropertyStore("testResource1");
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

  private ExternalView constructExternalView(String resourceName) {
    ExternalView externalView = new ExternalView(resourceName);
    List<OfflineSegmentZKMetadata> offlineResourceZKMetadataListForResource = ZKMetadataProvider.getOfflineResourceZKMetadataListForResource(_propertyStore, resourceName);
    for (OfflineSegmentZKMetadata segmentMetadata : offlineResourceZKMetadataListForResource) {
      externalView.setState(segmentMetadata.getSegmentName(), "localhost", "ONLINE");
    }
    return externalView;
  }

  private void addingSegmentsToPropertyStore(int numSegments, ZkHelixPropertyStore<ZNRecord> propertyStore, String resource) {
    String resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource);

    for (int i = 0; i < numSegments; ++i) {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setSegmentName(resourceName + "_" + System.currentTimeMillis() + "_" + i);
      offlineSegmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
      offlineSegmentZKMetadata.setEndTime(i);
      offlineSegmentZKMetadata.setCrc(-1);
      offlineSegmentZKMetadata.setCreationTime(-1);
      offlineSegmentZKMetadata.setStartTime(i - 1);
      offlineSegmentZKMetadata.setIndexVersion("0");
      offlineSegmentZKMetadata.setPushTime(i + 5);
      offlineSegmentZKMetadata.setResourceName(resourceName);
      offlineSegmentZKMetadata.setSegmentType(SegmentType.OFFLINE);
      offlineSegmentZKMetadata.setTableName(resourceName);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
    }
  }

  private void addingResourceToPropertyStore(String resource) {
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = new OfflineDataResourceZKMetadata();
    offlineDataResourceZKMetadata.setResourceName(resource);
    offlineDataResourceZKMetadata.setTimeColumnName("timestamp");
    offlineDataResourceZKMetadata.setTimeType("daysSinceEpoch");
    offlineDataResourceZKMetadata.setRetentionTimeUnit(TimeUnit.DAYS);
    offlineDataResourceZKMetadata.setRetentionTimeValue(-1);
    ZKMetadataProvider.setOfflineResourceZKMetadata(_propertyStore, offlineDataResourceZKMetadata);
  }
}
