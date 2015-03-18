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
package com.linkedin.pinot.controller.helix.core;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;


/**
 * @author xiafu
 *
 */
public class SegmentDeletionManager {

  private static Logger LOGGER = Logger.getLogger(SegmentDeletionManager.class);

  private final ScheduledExecutorService _executorService;
  private final String _localDiskDir;
  private final String _helixClusterName;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  SegmentDeletionManager(String localDiskDir, HelixAdmin helixAdmin, String helixClusterName, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _localDiskDir = localDiskDir;
    _helixAdmin = helixAdmin;
    _helixClusterName = helixClusterName;
    _propertyStore = propertyStore;

    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotHelixResourceManagerExecutorService");
        return thread;
      }
    });
  }

  public void stop() {
    _executorService.shutdownNow();
  }

  public void deleteSegment(final String resourceName, final String segmentId) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        deleteSegmentFromPropertyStoreAndLocal(resourceName, segmentId);
      }
    }, 2, TimeUnit.SECONDS);
  }

  /**
   * Check if segment got deleted from IdealStates and ExternalView.
   * If segment got removed, then delete this segment from PropertyStore and local disk.
   *
   * @param resourceName
   * @param segmentId
   */
  private synchronized void deleteSegmentFromPropertyStoreAndLocal(String resourceName, String segmentId) {
    // Check if segment got removed from ExternalView and IdealStates
    if (_helixAdmin.getResourceExternalView(_helixClusterName, resourceName) == null ||
        _helixAdmin.getResourceIdealState(_helixClusterName, resourceName) == null) {
      return;
    }
    Map<String, String> segmentToInstancesMapFromExternalView = _helixAdmin.getResourceExternalView(_helixClusterName, resourceName).getStateMap(segmentId);
    Map<String, String> segmentToInstancesMapFromIdealStates =
        _helixAdmin.getResourceIdealState(_helixClusterName, resourceName).getInstanceStateMap(segmentId);
    if ((segmentToInstancesMapFromExternalView == null || segmentToInstancesMapFromExternalView.size() < 1)
        || (segmentToInstancesMapFromIdealStates == null || segmentToInstancesMapFromIdealStates.size() < 1)) {
      _propertyStore.remove(ZKMetadataProvider.constructPropertyStorePathForSegment(resourceName, segmentId), AccessOption.PERSISTENT);
      LOGGER.info("Delete segment : " + segmentId + " from Property store.");
      if (_localDiskDir != null) {
        File fileToDelete = new File(new File(_localDiskDir, resourceName), segmentId);
        FileUtils.deleteQuietly(fileToDelete);
        LOGGER.info("Delete segment : " + segmentId + " from local directory : " + fileToDelete.getAbsolutePath());
      } else {
        LOGGER.info("localDiskDir is not configured, won't delete anything from disk");
      }
    }
  }
}
