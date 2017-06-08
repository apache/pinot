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
package com.linkedin.pinot.server.starter.helix;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import com.linkedin.pinot.core.segment.index.loader.V3RemoveIndexException;
import java.io.File;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherAndLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherAndLoader.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final DataManager _dataManager;
  private final SegmentMetadataLoader _metadataLoader;
  private final String _instanceId;

  private final int _segmentLoadMaxRetryCount;
  private final long _segmentLoadMinRetryDelayMs; // Min delay (in msecs) between retries

  public SegmentFetcherAndLoader(DataManager dataManager, SegmentMetadataLoader metadataLoader,
      ZkHelixPropertyStore<ZNRecord> propertyStore, Configuration pinotHelixProperties, String instanceId) {
    _propertyStore = propertyStore;
    _dataManager = dataManager;
    _metadataLoader = metadataLoader;
    _instanceId = instanceId;
    int maxRetries = Integer.parseInt(CommonConstants.Server.DEFAULT_SEGMENT_LOAD_MAX_RETRY_COUNT);
    try {
      maxRetries = pinotHelixProperties
          .getInt(CommonConstants.Server.CONFIG_OF_SEGMENT_LOAD_MAX_RETRY_COUNT, maxRetries);
    } catch (Exception e) {
      // Keep the default value
    }
    _segmentLoadMaxRetryCount = maxRetries;

    long minRetryDelayMillis =
        Long.parseLong(CommonConstants.Server.DEFAULT_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS);
    try {
      minRetryDelayMillis = pinotHelixProperties.getLong(
          CommonConstants.Server.CONFIG_OF_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS,
          minRetryDelayMillis);
    } catch (Exception e) {
      // Keep the default value
    }
    _segmentLoadMinRetryDelayMs = minRetryDelayMillis;

    SegmentFetcherFactory.initSegmentFetcherFactory(pinotHelixProperties);
  }

  public void addOrReplaceOfflineSegment(String tableName, String segmentId, boolean retryOnFailure) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentId);

    // Try to load table schema from Helix property store.
    // This schema is used for adding default values for newly added columns.
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableName);

    LOGGER.info("Adding or replacing segment {} for table {}, metadata {}", segmentId, tableName, offlineSegmentZKMetadata);
    try {
      SegmentMetadata segmentMetadataForCheck = new SegmentMetadataImpl(offlineSegmentZKMetadata);

      // We lock the segment in order to get its metadata, and then release the lock, so it is possible
      // that the segment is dropped after we get its metadata.
      SegmentMetadata localSegmentMetadata = _dataManager.getSegmentMetadata(tableName, segmentId);

      if (localSegmentMetadata == null) {
        LOGGER.info("Segment {} of table {} is not loaded in memory, checking disk", segmentId, tableName);
        File indexDir = new File(getSegmentLocalDirectory(tableName, segmentId));
        // Restart during segment reload might leave segment in inconsistent state (index directory might not exist but
        // segment backup directory existed), need to first try to recover from reload failure before checking the
        // existence of the index directory and loading segment metadata from it
        LoaderUtils.reloadFailureRecovery(indexDir);
        if (indexDir.exists()) {
          LOGGER.info("Segment {} of table {} found on disk, attempting to load it", segmentId, tableName);
          try {
            localSegmentMetadata = new SegmentMetadataImpl(indexDir);
            LOGGER.info("Found segment {} of table {} with crc {} on disk", segmentId, tableName, localSegmentMetadata.getCrc());
          } catch (Exception e) {
            // The localSegmentDir should help us get the table name,
            LOGGER.error("Failed to load segment metadata from {}. Deleting it.", indexDir, e);
            FileUtils.deleteQuietly(indexDir);
            localSegmentMetadata = null;
          }
          try {
            if (!isNewSegmentMetadata(localSegmentMetadata, segmentMetadataForCheck, segmentId, tableName)) {
              LOGGER.info("Segment metadata same as before, loading {} of table {} (crc {}) from disk", segmentId,
                  tableName, localSegmentMetadata.getCrc());
              TableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
              _dataManager.addSegment(localSegmentMetadata, tableConfig, schema);
              // TODO Update zk metadata with CRC for this instance
              return;
            }
          } catch (V3RemoveIndexException e) {
            LOGGER.info(
                "Unable to remove local index from V3 format segment: {}, table: {}, try to reload it from controller.",
                segmentId, tableName, e);
            FileUtils.deleteQuietly(indexDir);
            localSegmentMetadata = null;
          } catch (Exception e) {
            LOGGER.error("Failed to load {} of table {} from local, will try to reload it from controller!", segmentId,
                tableName, e);
            FileUtils.deleteQuietly(indexDir);
            localSegmentMetadata = null;
          }
        }
      }
      // There is a very unlikely race condition that we may have gotten the metadata of a
      // segment that was not dropped when we checked, but was dropped after the check above.
      // That is possible only if we get two helix transitions (to drop, and then to add back) the
      // segment at the same, or very close to each other.If the race condition triggers, and the
      // two segments are same in metadata, then we may end up NOT adding back the segment
      // that is in the process of being dropped.

      // If we get here, then either it is the case that we have the segment loaded in memory (and therefore present
      // in disk) or, we need to load from the server. In the former case, we still need to check if the metadata
      // that we have is different from that in zookeeper.
      if (isNewSegmentMetadata(localSegmentMetadata, segmentMetadataForCheck, segmentId, tableName)) {
        if (localSegmentMetadata == null) {
          LOGGER.info("Loading new segment {} of table {} from controller", segmentId, tableName);
        } else {
          LOGGER.info("Trying to refresh segment {} of table {} with new data.", segmentId, tableName);
        }
        int retryCount;
        int maxRetryCount = 1;
        if (retryOnFailure) {
          maxRetryCount = _segmentLoadMaxRetryCount;
        }
        for (retryCount = 0; retryCount < maxRetryCount; ++retryCount) {
          long attemptStartTime = System.currentTimeMillis();
          try {
            TableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
            final String uri = offlineSegmentZKMetadata.getDownloadUrl();
            final String localSegmentDir = downloadSegmentToLocal(uri, tableName, segmentId);
            final SegmentMetadata segmentMetadata =
                _metadataLoader.loadIndexSegmentMetadataFromDir(localSegmentDir);
            _dataManager.addSegment(segmentMetadata, tableConfig, schema);
            LOGGER.info("Downloaded segment {} of table {} crc {} from controller", segmentId, tableName, segmentMetadata.getCrc());

            // Successfully loaded the segment, break out of the retry loop
            break;
          } catch (Exception e) {
            long attemptDurationMillis = System.currentTimeMillis() - attemptStartTime;
            LOGGER.warn("Caught exception while loading segment " + segmentId + "(table " + tableName + "), attempt "
                + (retryCount + 1) + " of " + maxRetryCount, e);

            // Do we need to wait for the next retry attempt?
            if (retryCount < maxRetryCount - 1) {
              // Exponentially back off, wait for (minDuration + attemptDurationMillis) *
              // 1.0..(2^retryCount)+1.0
              double maxRetryDurationMultiplier = Math.pow(2.0, (retryCount + 1));
              double retryDurationMultiplier = Math.random() * maxRetryDurationMultiplier + 1.0;
              long waitTime =
                  (long) ((_segmentLoadMinRetryDelayMs + attemptDurationMillis) * retryDurationMultiplier);

              LOGGER.warn("Waiting for " + TimeUnit.MILLISECONDS.toSeconds(waitTime)
                  + " seconds to retry(" + segmentId + " of table " + tableName);
              long waitEndTime = System.currentTimeMillis() + waitTime;
              while (System.currentTimeMillis() < waitEndTime) {
                try {
                  Thread.sleep(Math.max(System.currentTimeMillis() - waitEndTime, 1L));
                } catch (InterruptedException ie) {
                  // Ignore spurious wakeup
                }
              }
            }
          }
        }
        if (retryCount == maxRetryCount) {
          throw new RuntimeException(
              "Failed to download and load segment " + segmentId + " (table " + tableName + " after " + retryCount
                  + " retries");
        }
      } else {
        LOGGER.info("Got already loaded segment {} of table {} crc {} again, will do nothing.", segmentId, tableName, localSegmentMetadata.getCrc());
      }
    } catch (final Exception e) {
      LOGGER.error("Cannot load segment : " + segmentId + " for table " + tableName, e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  private boolean isNewSegmentMetadata(SegmentMetadata segmentMetadataFromServer,
      SegmentMetadata segmentMetadataForCheck, String segmentName, String tableName) {
    if (segmentMetadataFromServer == null || segmentMetadataForCheck == null) {
      LOGGER.info("segmentMetadataForCheck = null? {}, segmentMetadataFromServer = null? {} for {} of table {}",
          segmentMetadataForCheck == null, segmentMetadataFromServer == null, segmentName, tableName);
      return true;
    }
    LOGGER.info("segmentMetadataForCheck.crc={},segmentMetadataFromServer.crc={} for {} of table {}",
        segmentMetadataForCheck.getCrc(), segmentMetadataFromServer.getCrc(), segmentName, tableName);
    if ((!segmentMetadataFromServer.getCrc().equalsIgnoreCase("null"))
        && (segmentMetadataFromServer.getCrc().equals(segmentMetadataForCheck.getCrc()))) {
      return false;
    }
    return true;
  }

  private String downloadSegmentToLocal(String uri, String tableName, String segmentId)
      throws Exception {
    File tempSegmentFile = null;
    File tempFile = null;
    try {
      tempSegmentFile = new File(_dataManager.getSegmentFileDirectory() + "/"
          + tableName + "/temp_" + segmentId + "_" + System.currentTimeMillis());
      tempFile = new File(_dataManager.getSegmentFileDirectory(), segmentId + ".tar.gz");
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI(uri).fetchSegmentToLocal(uri, tempFile);
      LOGGER.info("Downloaded file from {} to {}; Length of downloaded file: {}; segmentName: {}; table: {}", uri, tempFile,
          tempFile.length(), segmentId, tableName);
      LOGGER.info("Trying to decompress segment tar file from {} to {} for table {}", tempFile, tempSegmentFile, tableName);

      TarGzCompressionUtils.unTar(tempFile, tempSegmentFile);
      FileUtils.deleteQuietly(tempFile);
      final File segmentDir = new File(new File(_dataManager.getSegmentDataDirectory(), tableName), segmentId);
      Thread.sleep(1000);
      if (segmentDir.exists()) {
        LOGGER.info("Deleting the directory {} and recreating it again table {} ", segmentDir.getAbsolutePath(), tableName);
        FileUtils.deleteDirectory(segmentDir);
      }
      LOGGER.info("Move the dir - " + tempSegmentFile.listFiles()[0] + " to "
          + segmentDir.getAbsolutePath() + " for " + segmentId + " of table " + tableName);
      FileUtils.moveDirectory(tempSegmentFile.listFiles()[0], segmentDir);
      FileUtils.deleteDirectory(tempSegmentFile);
      Thread.sleep(1000);
      LOGGER.info("Was able to succesfully rename the dir to match the segment {} for table {}", segmentId, tableName);

      new File(segmentDir, "finishedLoading").createNewFile();
      return segmentDir.getAbsolutePath();
    } catch (Exception e) {
      FileUtils.deleteQuietly(tempSegmentFile);
      FileUtils.deleteQuietly(tempFile);
      LOGGER.error("Caught exception downloading segment {} for table {}", segmentId, tableName, e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  public String getSegmentLocalDirectory(String tableName, String segmentId) {
    return _dataManager.getSegmentDataDirectory() + "/" + tableName + "/" + segmentId;
  }

  public void reloadAllSegments(@Nonnull String tableNameWithType)
      throws Exception {
    for (SegmentMetadata segmentMetadata : _dataManager.getAllSegmentsMetadata(tableNameWithType)) {
      reloadSegment(tableNameWithType, segmentMetadata);
    }
  }

  public void reloadSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName)
      throws Exception {
    SegmentMetadata segmentMetadata = _dataManager.getSegmentMetadata(tableNameWithType, segmentName);
    if (segmentMetadata == null) {
      LOGGER.warn("Cannot locate segment: {} in table: {]", segmentName, tableNameWithType);
      return;
    }
    reloadSegment(tableNameWithType, segmentMetadata);
  }

  private void reloadSegment(@Nonnull String tableNameWithType, @Nonnull SegmentMetadata segmentMetadata)
      throws Exception {
    String segmentName = segmentMetadata.getName();

    String indexDir = segmentMetadata.getIndexDir();
    if (indexDir == null) {
      LOGGER.info("Skip reloading REALTIME consuming segment: {} in table: {}", segmentName, tableNameWithType);
      return;
    }

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Schema schema = null;
    // For OFFLINE table, try to get schema for default columns
    if (TableNameBuilder.OFFLINE.tableHasTypeSuffix(tableNameWithType)) {
      schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    }
    _dataManager.reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema);
  }
}
