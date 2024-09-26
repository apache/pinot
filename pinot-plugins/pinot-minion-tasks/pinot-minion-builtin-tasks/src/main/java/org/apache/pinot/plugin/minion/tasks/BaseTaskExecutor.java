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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseTaskExecutor implements PinotTaskExecutor {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskExecutor.class);
  protected static final MinionContext MINION_CONTEXT = MinionContext.getInstance();

  protected boolean _cancelled = false;
  protected final MinionMetrics _minionMetrics = MinionMetrics.get();

  @Override
  public void cancel() {
    _cancelled = true;
  }

  /**
   * Returns the segment ZK metadata custom map modifier.
   */
  protected abstract SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult);

  protected TableConfig getTableConfig(String tableNameWithType) {
    TableConfig tableConfig =
        ZKMetadataProvider.getTableConfig(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
    return tableConfig;
  }

  protected Schema getSchema(String tableName) {
    Schema schema = ZKMetadataProvider.getTableSchema(MINION_CONTEXT.getHelixPropertyStore(), tableName);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableName);
    return schema;
  }

  protected long getSegmentCrc(String tableNameWithType, String segmentName) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(MINION_CONTEXT.getHelixPropertyStore(), tableNameWithType, segmentName);
    /*
     * If the segmentZKMetadata is null, it is likely that the segment has been deleted, return -1 as CRC in this case,
     * so that task can terminate early when verify CRC. If we throw exception, helix will keep retrying this forever
     * and task status would be left unchanged without proper cleanup.
     */
    return segmentZKMetadata == null ? -1 : segmentZKMetadata.getCrc();
  }

  protected void reportSegmentDownloadMetrics(File indexDir, String tableNameWithType, String taskType) {
    long downloadSegmentSize = FileUtils.sizeOfDirectory(indexDir);
    addTaskMeterMetrics(MinionMeter.SEGMENT_BYTES_DOWNLOADED, downloadSegmentSize, tableNameWithType, taskType);
    addTaskMeterMetrics(MinionMeter.SEGMENT_DOWNLOAD_COUNT, 1L, tableNameWithType, taskType);
  }

  protected void reportSegmentUploadMetrics(File indexDir, String tableNameWithType, String taskType) {
    long uploadSegmentSize = FileUtils.sizeOfDirectory(indexDir);
    addTaskMeterMetrics(MinionMeter.SEGMENT_BYTES_UPLOADED, uploadSegmentSize, tableNameWithType, taskType);
    addTaskMeterMetrics(MinionMeter.SEGMENT_UPLOAD_COUNT, 1L, tableNameWithType, taskType);
  }

  protected void reportTaskProcessingMetrics(String tableNameWithType, String taskType, int numRecordsProcessed,
      int numRecordsPurged) {
    reportTaskProcessingMetrics(tableNameWithType, taskType, numRecordsProcessed);
    addTaskMeterMetrics(MinionMeter.RECORDS_PURGED_COUNT, numRecordsPurged, tableNameWithType, taskType);
  }

  protected void reportTaskProcessingMetrics(String tableNameWithType, String taskType, int numRecordsProcessed) {
    addTaskMeterMetrics(MinionMeter.RECORDS_PROCESSED_COUNT, numRecordsProcessed, tableNameWithType, taskType);
  }

  private void addTaskMeterMetrics(MinionMeter meter, long unitCount, String tableName, String taskType) {
    _minionMetrics.addMeteredGlobalValue(meter, unitCount);
    _minionMetrics.addMeteredTableValue(tableName, meter, unitCount);
    _minionMetrics.addMeteredTableValue(tableName, taskType, meter, unitCount);
  }

  protected File downloadSegmentToLocalAndUntar(String tableNameWithType, String segmentName, String deepstoreURL,
      String taskType, File tempDataDir, String suffix)
      throws Exception {
    File tarredSegmentFile = new File(tempDataDir, "tarredSegmentFile" + suffix);
    File segmentDir = new File(tempDataDir, "segmentDir" + suffix);
    File indexDir;
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    String crypterName = tableConfig.getValidationConfig().getCrypterClassName();
    LOGGER.info("Downloading segment {} from {} to {}", segmentName, deepstoreURL, tarredSegmentFile.getAbsolutePath());

    try {
      // download from deepstore first
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(deepstoreURL, tarredSegmentFile, crypterName);
      // untar the segment file
      indexDir = TarCompressionUtils.untar(tarredSegmentFile, segmentDir).get(0);
    } catch (Exception e) {
      LOGGER.error("Segment download failed from deepstore for {}, crypter:{}", deepstoreURL, crypterName, e);
      String peerDownloadScheme = tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
      if (MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig, taskType,
          MINION_CONTEXT.isAllowDownloadFromServer()) && peerDownloadScheme != null) {
        // if allowDownloadFromServer is enabled, download the segment from a peer server as deepstore download failed
        LOGGER.info("Trying to download from servers for segment {} post deepstore download failed", segmentName);
        SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(segmentName, peerDownloadScheme, () -> {
          List<URI> uris =
              PeerServerSegmentFinder.getPeerServerURIs(MINION_CONTEXT.getHelixManager(), tableNameWithType,
                  segmentName, peerDownloadScheme);
          Collections.shuffle(uris);
          return uris;
        }, tarredSegmentFile, crypterName);
        // untar the segment file
        indexDir = TarCompressionUtils.untar(tarredSegmentFile, segmentDir).get(0);
      } else {
        throw e;
      }
    } finally {
      if (!FileUtils.deleteQuietly(tarredSegmentFile)) {
        LOGGER.warn("Failed to delete tarred input segment: {}", tarredSegmentFile.getAbsolutePath());
      }
    }
    return indexDir;
  }
}
