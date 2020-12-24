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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.File;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.query.utils.Pair;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment Operations:
 * UPLOAD:
 *   Generates a segment for a table from the data in the input file.
 *   Uploads the segment, and verifies that the segments appear in ExternalView
 * DELETE:
 *   Deletes the segment from the table.
 *
 * TODO:
 *  - Maybe segment names can be auto-generated if the name is "AUTO".
 *  - We can add segmentGeneration config file as an option also
 *  - We can consider supporting different readers, starting with csv. Will help in easily scanning the data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentOp extends BaseOp {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOp.class);
  private static final FileFormat DEFAULT_FILE_FORMAT = FileFormat.CSV;
  private static final String STATE_ONLINE = "ONLINE";

  public enum Op {
    UPLOAD, DELETE
  }

  private Op _op;
  private String _inputDataFileName;
  private String _tableConfigFileName;
  private String _schemaFileName;
  private String _recordReaderConfigFileName;
  private String _tableName;
  private String _segmentName;

  public SegmentOp() {
    super(OpType.SEGMENT_OP);
  }

  public Op getOp() {
    return _op;
  }

  public void setOp(Op op) {
    _op = op;
  }

  public String getInputDataFileName() {
    return _inputDataFileName;
  }

  public void setInputDataFileName(String inputDataFileName) {
    _inputDataFileName = inputDataFileName;
  }

  public String getTableConfigFileName() {
    return _tableConfigFileName;
  }

  public void setTableConfigFileName(String tableConfigFileName) {
    _tableConfigFileName = tableConfigFileName;
  }

  public void setSchemaFileName(String schemaFileName) {
    _schemaFileName = schemaFileName;
  }

  public String getSchemaFileName() {
    return _schemaFileName;
  }

  public void setRecordReaderConfigFileName(String recordReaderConfigFileName) {
    _recordReaderConfigFileName = recordReaderConfigFileName;
  }

  public String getRecordReaderConfigFileName() {
    return _recordReaderConfigFileName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  boolean runOp() {
    switch (_op) {
      case UPLOAD:
        return createAndUploadSegments();
      case DELETE:
        return deleteSegment();
    }
    return true;
  }

  /**
   * Create Segment file, compress to TarGz, and upload the files to controller.
   * @return true if all successful, false in case of failure.
   */
  private boolean createAndUploadSegments() {
    File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-compat-test-" + UUID.randomUUID());
    File localOutputTempDir = new File(localTempDir, "output");
    try {
      FileUtils.forceMkdir(localOutputTempDir);
      File segmentTarFile = generateSegment(localOutputTempDir);
      uploadSegment(segmentTarFile);

      Pair<Long, Long> onlineSegmentCount = getOnlineSegmentCount(getTableExternalView());
      if (onlineSegmentCount.getFirst() <= 0 && onlineSegmentCount.getSecond() <= 0) {
        LOGGER.error("Uploaded segment {} not found or not in {} state.", _segmentName, STATE_ONLINE);
        return false;
      }
      LOGGER.info("Successfully verified segment {} and its current status is {}.", _segmentName, STATE_ONLINE);

      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to create and upload segment for input data file {}.", _inputDataFileName, e);
      return false;
    } finally {
      FileUtils.deleteQuietly(localTempDir);
    }
  }

  /**
   * Generate the Segment(s) and then compress to TarGz file. Supports generation of segment files for one input data
   * file.
   * @param outputDir to generate the Segment file(s).
   * @return File object of the TarGz compressed segment file.
   * @throws Exception while generating segment files and/or compressing to TarGz.
   */
  private File generateSegment(File outputDir)
      throws Exception {
    TableConfig tableConfig = JsonUtils.fileToObject(new File(_tableConfigFileName), TableConfig.class);
    _tableName = tableConfig.getTableName();

    Schema schema = JsonUtils.fileToObject(new File(_schemaFileName), Schema.class);
    RecordReaderConfig recordReaderConfig =
        RecordReaderFactory.getRecordReaderConfig(DEFAULT_FILE_FORMAT, _recordReaderConfigFileName);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(_inputDataFileName);
    segmentGeneratorConfig.setFormat(DEFAULT_FILE_FORMAT);
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
    segmentGeneratorConfig.setTableName(_tableName);
    segmentGeneratorConfig.setSegmentName(_segmentName);

    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
    String segmentName = driver.getSegmentName();
    File indexDir = new File(outputDir, segmentName);
    LOGGER.info("Successfully created segment: {} at directory: {}", segmentName, indexDir);
    File segmentTarFile = new File(outputDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(indexDir, segmentTarFile);
    LOGGER.info("Tarring segment from: {} to: {}", indexDir, segmentTarFile);

    return segmentTarFile;
  }

  /**
   * Upload the TarGz Segment file to the controller.
   * @param segmentTarFile TarGz Segment file
   * @throws Exception when upload segment fails.
   */
  private void uploadSegment(File segmentTarFile)
      throws Exception {
    URI controllerURI = FileUploadDownloadClient.getUploadSegmentURI(new URI(ClusterDescriptor.CONTROLLER_URL));
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.uploadSegment(controllerURI, segmentTarFile.getName(), segmentTarFile, _tableName);
    }
  }

  /**
   * Deletes the segment for the given segment name and table name.
   * @return true if delete successful, else false.
   */
  private boolean deleteSegment() {
    try {
      TableConfig tableConfig = JsonUtils.fileToObject(new File(_tableConfigFileName), TableConfig.class);
      _tableName = tableConfig.getTableName();

      ControllerTest.sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.CONTROLLER_URL)
          .forSegmentDelete(_tableName, _segmentName));

      Pair<Long, Long> onlineSegmentCount = getOnlineSegmentCount(getTableExternalView());
      if (onlineSegmentCount.getFirst() > 0 || onlineSegmentCount.getSecond() > 0) {
        LOGGER.error("Delete segment {} for the table {} is not successful and segment count is {}.", _segmentName,
            _tableName, (onlineSegmentCount.getFirst() + onlineSegmentCount.getSecond()));
        return false;
      }
    } catch (Exception e) {
      LOGGER.error("Request to delete the segment {} for the table {} failed.", _segmentName, _tableName, e);
      return false;
    }

    LOGGER.info("Successfully delete the segment {} for the table {}.", _segmentName, _tableName);
    return true;
  }

  /**
   * Delay of 5 seconds to get segment from controller.
   * @return Offline and Realtime segment for the table.
   */
  private TableViews.TableView getTableExternalView()
      throws ExecutionException, InterruptedException {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    Callable<TableViews.TableView> validationCallback = () -> {
      return JsonUtils.stringToObject(ControllerTest.sendGetRequest(
          ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.CONTROLLER_URL).forTableExternalView(_tableName)),
          TableViews.TableView.class);
    };
    Future<TableViews.TableView> futureTableView = executorService.schedule(validationCallback, 5, TimeUnit.SECONDS);
    return  futureTableView.get();
  }

  /**
   * Retrieve the number of segments for both OFFLINE and REALTIME which are in ONLINE state.
   * @param segmentTreeView External table view retrieved from controller.
   * @return Pair of counts for OFFLINE and REALTIME segments.
   */
  private Pair<Long, Long> getOnlineSegmentCount(TableViews.TableView segmentTreeView) {
    long offlineSegmentCount = segmentTreeView.offline != null ? segmentTreeView.offline.entrySet().stream()
        .filter(k -> k.getKey().equalsIgnoreCase(_segmentName))
        .filter(v -> v.getValue().values().contains(STATE_ONLINE)).count() : 0;
    long realtimeSegmentCount = segmentTreeView.realtime != null ? segmentTreeView.realtime.entrySet().stream()
        .filter(k -> k.getKey().equalsIgnoreCase(_segmentName))
        .filter(v -> v.getValue().values().contains(STATE_ONLINE)).count() : 0;

    return new Pair<>(offlineSegmentCount, realtimeSegmentCount);
  }
}
