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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.http.client.utils.URIBuilder;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpsertCompactionTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskExecutor.class);
  private static HelixManager _helixManager = MINION_CONTEXT.getHelixManager();
  private static HelixAdmin _clusterManagementTool = _helixManager.getClusterManagmentTool();
  private static String _clusterName = _helixManager.getClusterName();

  private class CompactedRecordReader implements RecordReader {
    private final PinotSegmentRecordReader _pinotSegmentRecordReader;
    private final PeekableIntIterator _validDocIdsIterator;
    // Reusable generic row to store the next row to return
    GenericRow _nextRow = new GenericRow();
    // Flag to mark whether we need to fetch another row
    boolean _nextRowReturned = true;

    CompactedRecordReader(File indexDir, ImmutableRoaringBitmap validDocIds) {
      _pinotSegmentRecordReader = new PinotSegmentRecordReader();
      _pinotSegmentRecordReader.init(indexDir, null, null);
      _validDocIdsIterator = validDocIds.getIntIterator();
    }

    @Override
    public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      if (!_validDocIdsIterator.hasNext() && _nextRowReturned) {
        return false;
      }

      // If next row has not been returned, return true
      if (!_nextRowReturned) {
        return true;
      }

      // Try to get the next row to return
      if (_validDocIdsIterator.hasNext()) {
        int docId = _validDocIdsIterator.next();
        _nextRow.clear();
        _pinotSegmentRecordReader.getRecord(docId, _nextRow);
        _nextRowReturned = false;
        return true;
      }

      // Cannot find next row to return, return false
      return false;
    }

    @Override
    public GenericRow next() {
      return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      Preconditions.checkState(!_nextRowReturned);
      reuse.init(_nextRow);
      _nextRowReturned = true;
      return reuse;
    }

    @Override
    public void rewind() {
      _pinotSegmentRecordReader.rewind();
      _nextRowReturned = true;
    }

    @Override
    public void close()
        throws IOException {
      _pinotSegmentRecordReader.close();
    }
  }

  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    _eventObserver.notifyProgress(pinotTaskConfig, "Compacting segment: " + indexDir);
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String taskType = pinotTaskConfig.getTaskType();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    ImmutableRoaringBitmap validDocIds = getValidDocIds(tableNameWithType, configs);

    if (validDocIds.isEmpty()) {
      // prevents empty segment generation
      LOGGER.info("validDocIds is empty, skip the task. Table: {}, segment: {}", tableNameWithType, segmentName);
      if (indexDir.exists() && !FileUtils.deleteQuietly(indexDir)) {
        LOGGER.warn("Failed to delete input segment: {}", indexDir.getAbsolutePath());
      }
      if (!FileUtils.deleteQuietly(workingDir)) {
        LOGGER.warn("Failed to delete working directory: {}", workingDir.getAbsolutePath());
      }
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
          .build();
    }

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    try (CompactedRecordReader compactedRecordReader = new CompactedRecordReader(indexDir, validDocIds)) {
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, compactedRecordReader);
      driver.build();
    }

    File compactedSegmentFile = new File(workingDir, segmentName);
    SegmentConversionResult result =
        new SegmentConversionResult.Builder().setFile(compactedSegmentFile).setTableNameWithType(tableNameWithType)
            .setSegmentName(segmentName).build();

    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType, configs, (endMillis - startMillis));

    return result;
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
      SegmentMetadataImpl segmentMetadata, String segmentName) {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, segmentMetadata.getSchema());
    config.setOutDir(workingDir.getPath());
    config.setSegmentName(segmentName);
    // Keep index creation time the same as original segment because both segments use the same raw data.
    // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
    // identify if the new pushed segment has newer data than the existing one.
    config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

    // The time column type info is not stored in the segment metadata.
    // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
    if (segmentMetadata.getTimeInterval() != null) {
      config.setTimeColumnName(tableConfig.getValidationConfig().getTimeColumnName());
      config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
      config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
      config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
    }
    return config;
  }

  // TODO: Consider moving this method to a more appropriate class (eg ServerSegmentMetadataReader)
  private static ImmutableRoaringBitmap getValidDocIds(String tableNameWithType, Map<String, String> configs)
      throws URISyntaxException {
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String server = getServer(segmentName, tableNameWithType);

    // get the url for the validDocIds for the server
    InstanceConfig instanceConfig = _clusterManagementTool.getInstanceConfig(_clusterName, server);
    String endpoint = InstanceUtils.getServerAdminEndpoint(instanceConfig);
    String url =
        new URIBuilder(endpoint).setPath(String.format("/segments/%s/%s/validDocIds", tableNameWithType, segmentName))
            .toString();

    // get the validDocIds from that server
    Response response = ClientBuilder.newClient().target(url).request().get(Response.class);
    Preconditions.checkState(response.getStatus() == Response.Status.OK.getStatusCode(),
        "Unable to retrieve validDocIds from %s", url);
    byte[] snapshot = response.readEntity(byte[].class);
    ImmutableRoaringBitmap validDocIds = new ImmutableRoaringBitmap(ByteBuffer.wrap(snapshot));
    return validDocIds;
  }

  @VisibleForTesting
  public static String getServer(String segmentName, String tableNameWithType) {
    String server = null;
    ExternalView externalView = _clusterManagementTool.getResourceExternalView(_clusterName, tableNameWithType);
    if (externalView == null) {
      throw new IllegalStateException("External view does not exist for table: " + tableNameWithType);
    }
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      if (Objects.equals(segment, segmentName)) {
        server = entry.getValue().keySet().toArray()[0].toString();
        break;
      }
    }
    return server;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(MinionConstants.UpsertCompactionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
