package com.linkedin.thirdeye.client.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;

public class CollectionMaxDataTimeCacheLoader extends CacheLoader<String, Long> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionMaxDataTimeCacheLoader.class);

  private String zkUrl;
  private String clusterName;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;

  public CollectionMaxDataTimeCacheLoader(PinotThirdEyeClientConfig pinotThirdEyeClientConfig) {

   zkUrl = pinotThirdEyeClientConfig.getZookeeperUrl();
   clusterName = pinotThirdEyeClientConfig.getClusterName();

   ZkClient zkClient = new ZkClient(zkUrl);
   zkClient.setZkSerializer(new ZNRecordSerializer());
   zkClient.waitUntilConnected();

   ZkBaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(zkClient);
   String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);

   propertyStore = new ZkHelixPropertyStore<ZNRecord>(baseAccessor, propertyStorePath, null);
  }

  @Override
  public Long load(String collection) throws Exception {
    long maxTime = 0;
    if (propertyStore != null) {
      List<Stat> stats = new ArrayList<>();
      int options = AccessOption.PERSISTENT;
      List<ZNRecord> zkSegmentDataList =
          propertyStore.getChildren("/SEGMENTS/" + collection + "_OFFLINE", stats, options);
      for (ZNRecord record : zkSegmentDataList) {
        try {
          long endTime = Long.parseLong(record.getSimpleField("segment.end.time"));
          TimeUnit timeUnit = TimeUnit.valueOf(record.getSimpleField("segment.time.unit"));
          long endTimeMillis = timeUnit.toMillis(endTime);
          if (endTimeMillis > maxTime) {
            maxTime = endTimeMillis;
          }
        } catch (Exception e) {
          LOGGER.warn("Exception parsing metadata:{}", record, e);
        }
      }
    } else {
      maxTime = System.currentTimeMillis();
    }
    return maxTime;
  }

}
