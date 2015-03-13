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
package com.linkedin.pinot.core.data.manager.realtime;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.DataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.ResourceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;


public class RealtimeResourceDataManager implements ResourceDataManager {
  private static final Logger logger = Logger.getLogger(RealtimeResourceDataManager.class);

  private final Object lock = new Object();

  private File indexDir;
  private ReadMode readMode;

  private ResourceDataManagerConfig resourceConfigs;

  private ExecutorService _queryExecutorService;

  private final Map<String, OfflineSegmentDataManager> segmentsMap = new HashMap<String, OfflineSegmentDataManager>();
  private final List<String> activeSegments = new ArrayList<String>();
  private final List<String> loadingSegments = new ArrayList<String>();
  private Map<String, AtomicInteger> referenceCounts = new HashMap<String, AtomicInteger>();
  private Map<String, Thread> activeIndexedThreadMap;
  private ZkHelixPropertyStore<ZNRecord> helixPropertyStore;

  @Override
  public void init(ResourceDataManagerConfig resourceDataManagerConfig) {
    resourceConfigs = resourceDataManagerConfig;
  }

  @Override
  public void start() {
    indexDir = new File(resourceConfigs.getDataDir());
    readMode = ReadMode.valueOf(resourceConfigs.getReadMode());
    if (!indexDir.exists()) {
      if (!indexDir.mkdir()) {
        logger.error("could not create data dir");
      }
    }
    activeIndexedThreadMap = new HashMap<String, Thread>();
  }

  @Override
  public void shutDown() {
    // TODO Auto-generated method stub

  }

  public void notify(RealtimeSegmentZKMetadata metadata) {
    ZNRecord rec = metadata.toZNRecord();
    helixPropertyStore.set("/" + metadata.getResourceName() + "/" + metadata.getSegmentName(), rec,
        AccessOption.PERSISTENT);
    // update property store for gve id
  }

  @Override
  public boolean isStarted() {
    // TODO Auto-generated method stub
    return false;
  }

  public void addSegment(SegmentZKMetadata segmentMetadata) throws Exception {
    throw new UnsupportedOperationException("Cannot add realtime segment with just SegmentZKMetadata");
  }

  @Override
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, DataResourceZKMetadata dataResourceZKMetadata, InstanceZKMetadata instanceZKMetadata,
      SegmentZKMetadata segmentZKMetadata) throws Exception {
    this.helixPropertyStore = propertyStore;
    String segmentId = segmentZKMetadata.getSegmentName();
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      if (new File(indexDir, segmentId).exists()) {
        IndexSegment segment = ColumnarSegmentLoader.load(new File(indexDir, segmentId), readMode);
        // segment already exists on file, simply load it and add it to the map
        synchronized (lock) {
          segmentsMap.put(segmentId, new OfflineSegmentDataManager(segment));
        }
      } else {
        // this is a new segment, lets create an instance of RealtimeSegmentDataManager
        SegmentDataManager manager =
            new RealtimeSegmentDataManager((RealtimeSegmentZKMetadata) segmentZKMetadata, (RealtimeDataResourceZKMetadata) dataResourceZKMetadata,
                instanceZKMetadata, this, indexDir.getAbsolutePath(), readMode);
      }
    }

  }

  public void updateStatus() {

  }

  @Override
  public void addSegment(IndexSegment indexSegmentToAdd) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addSegment(SegmentMetadata segmentMetaToAdd) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void removeSegment(String segmentToRemove) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<OfflineSegmentDataManager> getAllSegments() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<OfflineSegmentDataManager> getSegments(List<String> segmentList) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public OfflineSegmentDataManager getSegment(String segmentName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void returnSegmentReaders(List<String> segmentList) {
    // TODO Auto-generated method stub

  }

  @Override
  public ExecutorService getExecutorService() {
    // TODO Auto-generated method stub
    return null;
  }

}
