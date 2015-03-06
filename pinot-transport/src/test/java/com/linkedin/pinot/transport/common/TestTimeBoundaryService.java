package com.linkedin.pinot.transport.common;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.AccessOption;
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

import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.routing.HelixExternalViewBasedTimeBoundaryService;
import com.linkedin.pinot.routing.TimeBoundaryService.TimeBoundaryInfo;


public class TestTimeBoundaryService {
  private final static String SEGMENT_TIME_COLUMN = "segment.time.column.name";
  private final static String SEGMENT_END_TIME = "segment.end.time";

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
    HelixExternalViewBasedTimeBoundaryService tbs = new HelixExternalViewBasedTimeBoundaryService(_propertyStore);
    addingSegmentsToPropertyStore(5, _propertyStore, "testResource0");
    ExternalView externalView = new ExternalView("testResource0");
    tbs.updateTimeBoundaryService(externalView);
    TimeBoundaryInfo tbi = tbs.getTimeBoundaryInfoFor("testResource0");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "4");

    addingSegmentsToPropertyStore(50, _propertyStore, "testResource1");
    externalView = new ExternalView("testResource1");
    tbs.updateTimeBoundaryService(externalView);
    tbi = tbs.getTimeBoundaryInfoFor("testResource1");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "49");

    addingSegmentsToPropertyStore(50, _propertyStore, "testResource0");
    externalView = new ExternalView("testResource0");
    tbs.updateTimeBoundaryService(externalView);
    tbi = tbs.getTimeBoundaryInfoFor("testResource0");
    Assert.assertEquals(tbi.getTimeColumn(), "timestamp");
    Assert.assertEquals(tbi.getTimeValue(), "49");
  }

  private void addingSegmentsToPropertyStore(int numSegments, ZkHelixPropertyStore<ZNRecord> propertyStore, String resource) {
    String resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource);
    propertyStore.set("/" + resourceName, null, AccessOption.PERSISTENT);
    for (int i = 0; i < numSegments; ++i) {
      String segmentName = resourceName + "_" + System.currentTimeMillis() + "_" + i;
      ZNRecord record = new ZNRecord(segmentName);
      record.setSimpleField(SEGMENT_TIME_COLUMN, "timestamp");
      record.setSimpleField(SEGMENT_END_TIME, i + "");
      propertyStore.create("/" + resourceName + "/" + segmentName, record, AccessOption.PERSISTENT);
    }

  }

}
