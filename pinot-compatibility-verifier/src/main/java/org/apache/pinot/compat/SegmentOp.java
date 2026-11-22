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
package org.apache.pinot.compat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.TableView;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SqlResultComparator;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Segment Operations:
/// UPLOAD:
///   Generates a segment for a table from the data in the input file.
///   Uploads the segment, and verifies that the segments appear in ExternalView
/// DELETE:
///   Deletes the segment from the table.
///
/// TODO:
///  - Maybe segment names can be auto-generated if the name is "AUTO".
///  - We can add segmentGeneration config file as an option also
///  - We can consider supporting different readers, starting with csv. Will help in easily scanning the data.
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentOp extends BaseOp {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOp.class);
  private static final FileFormat DEFAULT_FILE_FORMAT = FileFormat.CSV;
  private static final int DEFAULT_MAX_SLEEP_TIME_MS = 120000;
  private static final int DEFAULT_SLEEP_INTERVAL_MS = 1000;

  public enum Op {
    UPLOAD, DELETE
  }

  /// Upload readiness for one OFFLINE segment. Values are immutable and safe to share across threads.
  enum SegmentStatus {
    PENDING, ONLINE, ERROR
  }

  private Op _op;
  private String _inputDataFileName;
  private String _tableConfigFileName;
  private String _schemaFileName;
  private String _recordReaderConfigFileName;
  private String _tableName;
  private String _segmentName;
  private int _generationNumber;

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
  boolean runOp(int generationNumber) {
    _generationNumber = generationNumber;
    switch (_op) {
      case UPLOAD:
        return createAndUploadSegments();
      case DELETE:
        return deleteSegment();
      default:
        return true;
    }
  }

  /// Create Segment file, compress to TarGz, upload the files to controller and verify segment upload.
  /// @return true if all successful, false in case of failure.
  private boolean createAndUploadSegments() {
    File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-compat-test-segment-op-" + UUID.randomUUID());
    localTempDir.deleteOnExit();
    File localOutputTempDir = new File(localTempDir, "output");
    try {
      FileUtils.forceMkdir(localOutputTempDir);
      // replace the placeholder in the data file.
      File localReplacedInputDataFile = new File(localTempDir, "replaced");
      Utils.replaceContent(new File(getAbsoluteFileName(_inputDataFileName)), localReplacedInputDataFile,
          GENERATION_NUMBER_PLACEHOLDER, String.valueOf(_generationNumber));

      File segmentTarFile = generateSegment(localOutputTempDir, localReplacedInputDataFile.getAbsolutePath());
      uploadSegment(segmentTarFile);
      return verifySegmentOnline() && verifyRoutingTableUpdated();
    } catch (Exception e) {
      LOGGER.error("Failed to create and upload segment for input data file {}.", _inputDataFileName, e);
      return false;
    } finally {
      FileUtils.deleteQuietly(localTempDir);
    }
  }

  /// Generate the Segment(s) and then compress to TarGz file. Supports generation of segment files for one input data
  /// file.
  /// @param outputDir to generate the Segment file(s).
  /// @return File object of the TarGz compressed segment file.
  /// @throws Exception while generating segment files and/or compressing to TarGz.
  private File generateSegment(File outputDir, String localReplacedInputDataFilePath)
      throws Exception {
    TableConfig tableConfig =
        JsonUtils.fileToObject(new File(getAbsoluteFileName(_tableConfigFileName)), TableConfig.class);
    _tableName = tableConfig.getTableName();
    // if user does not specify segmentName, use tableName_generationNumber
    if (_segmentName == null || _segmentName.isEmpty()) {
      _segmentName = _tableName + "_" + _generationNumber;
    }

    Schema schema = JsonUtils.fileToObject(new File(getAbsoluteFileName(_schemaFileName)), Schema.class);
    RecordReaderConfig recordReaderConfig = RecordReaderFactory.getRecordReaderConfig(DEFAULT_FILE_FORMAT,
        getAbsoluteFileName(_recordReaderConfigFileName));

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(localReplacedInputDataFilePath);
    segmentGeneratorConfig.setFormat(DEFAULT_FILE_FORMAT);
    segmentGeneratorConfig.setOutDir(outputDir.getAbsolutePath());
    segmentGeneratorConfig.setReaderConfig(recordReaderConfig);
    segmentGeneratorConfig.setTableName(_tableName);
    segmentGeneratorConfig.setSegmentName(_segmentName);

    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
    File indexDir = new File(outputDir, _segmentName);
    LOGGER.info("Successfully created segment: {} at directory: {}", _segmentName, indexDir);
    File segmentTarFile = new File(outputDir, _segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
    LOGGER.info("Tarring segment from: {} to: {}", indexDir, segmentTarFile);

    return segmentTarFile;
  }

  /// Upload the TarGz Segment file to the controller.
  /// @param segmentTarFile TarGz Segment file
  /// @throws Exception when upload segment fails.
  private void uploadSegment(File segmentTarFile)
      throws Exception {
    URI controllerURI =
        FileUploadDownloadClient.getUploadSegmentURI(new URI(ClusterDescriptor.getInstance().getControllerUrl()));
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.uploadSegment(controllerURI, segmentTarFile.getName(), segmentTarFile, _tableName);
    }
  }

  /// Wait until the uploaded OFFLINE segment has replicas and all of them are ONLINE.
  /// @return true when all replicas are ONLINE, false on an ERROR replica or timeout.
  /// @throws IOException when unable to get the external view for the table.
  /// @throws InterruptedException when thread sleep is interrupted.
  private boolean verifySegmentOnline()
      throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    while (true) {
      SegmentStatus status = getSegmentStatus(getExternalViewForTable(), _segmentName);
      if (status == SegmentStatus.ONLINE) {
        LOGGER.info("Verified all replicas of segment {} are ONLINE", _segmentName);
        return true;
      }
      if (status == SegmentStatus.ERROR) {
        LOGGER.error("Upload segment verification failed, segment {} has a replica in ERROR state", _segmentName);
        return false;
      }
      if ((System.currentTimeMillis() - startTime) > DEFAULT_MAX_SLEEP_TIME_MS) {
        LOGGER.error("Upload segment verification failed, segment {} is not ONLINE after max wait time {} ms",
            _segmentName, DEFAULT_MAX_SLEEP_TIME_MS);
        return false;
      }
      LOGGER.warn("Segment {} is not yet ONLINE, will retry after {} ms", _segmentName, DEFAULT_SLEEP_INTERVAL_MS);
      Thread.sleep(DEFAULT_SLEEP_INTERVAL_MS);
    }
  }

  // TODO: verify by getting the number of rows before adding the segment, and the number of rows after adding the
  //       segment, then make sure that it has increased by the number of rows in the segment.
  private boolean verifyRoutingTableUpdated()
      throws Exception {
    String query = "SELECT count(*) FROM " + _tableName;
    ClusterDescriptor clusterDescriptor = ClusterDescriptor.getInstance();
    JsonNode result = Utils.postSqlQuery(query, clusterDescriptor.getBrokerUrl());
    long startTime = System.currentTimeMillis();
    while (SqlResultComparator.isEmpty(result)) {
      if ((System.currentTimeMillis() - startTime) > DEFAULT_MAX_SLEEP_TIME_MS) {
        LOGGER.error(
            "Upload segment verification failed, routing table has not been updated after max wait time {} ms.",
            DEFAULT_MAX_SLEEP_TIME_MS);
        return false;
      }
      LOGGER.warn("Routing table has not been updated yet, will retry after {} ms.", DEFAULT_SLEEP_INTERVAL_MS);
      Thread.sleep(DEFAULT_SLEEP_INTERVAL_MS);
      result = Utils.postSqlQuery(query, clusterDescriptor.getBrokerUrl());
    }
    LOGGER.info("Routing table has been updated.");
    return true;
  }

  /// Deletes the segment for the given segment name and table name.
  /// @return true if delete successful, else false.
  private boolean deleteSegment() {
    try {
      TableConfig tableConfig =
          JsonUtils.fileToObject(new File(getAbsoluteFileName(_tableConfigFileName)), TableConfig.class);
      _tableName = tableConfig.getTableName();
      // if user does not specify segmentName, use tableName_generationNumber
      if (_segmentName == null || _segmentName.isEmpty()) {
        _segmentName = _tableName + "_" + _generationNumber;
      }

      ControllerTest.sendDeleteRequest(
          ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.getInstance().getControllerUrl())
              .forSegmentDelete(_tableName, _segmentName));
      return verifySegmentDeleted();
    } catch (Exception e) {
      LOGGER.error("Request to delete the segment {} for the table {} failed.", _segmentName, _tableName, e);
      return false;
    }
  }

  /// Verify given table name and segment name deleted from the controller.
  /// @return true if no segment found, else false.
  /// @throws IOException when unable to get the external view for the table.
  /// @throws InterruptedException when thread sleep is interrupted.
  private boolean verifySegmentDeleted()
      throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    while (isSegmentPresent()) {
      if ((System.currentTimeMillis() - startTime) > DEFAULT_MAX_SLEEP_TIME_MS) {
        LOGGER.error("Delete segment verification failed, segment {} is still present after max wait time {} ms",
            _segmentName, DEFAULT_MAX_SLEEP_TIME_MS);
        return false;
      }
      LOGGER.warn("Segment {} is still present, will retry after {} ms", _segmentName, DEFAULT_SLEEP_INTERVAL_MS);
      Thread.sleep(DEFAULT_SLEEP_INTERVAL_MS);
    }

    LOGGER.info("Verified segment {} was deleted from table {}", _segmentName, _tableName);
    return true;
  }

  /// Retrieve external view for the given table name.
  /// @return OFFLINE and REALTIME segment assignments, or null if the controller returns a null view.
  @Nullable
  private TableView getExternalViewForTable()
      throws IOException {
    return JsonUtils.stringToObject(ControllerTest.sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.getInstance().getControllerUrl())
            .forTableExternalView(_tableName)), TableView.class);
  }

  /// Classify an OFFLINE segment from one external-view snapshot without modifying it. Missing assignments are
  /// PENDING; any ERROR replica takes precedence; otherwise replicas must exist and all be ONLINE.
  static SegmentStatus getSegmentStatus(@Nullable TableView externalView, String segmentName) {
    if (externalView == null || externalView._offline == null) {
      return SegmentStatus.PENDING;
    }
    Map<String, String> segmentStates = externalView._offline.get(segmentName);
    if (segmentStates == null || segmentStates.isEmpty()) {
      return SegmentStatus.PENDING;
    }
    if (segmentStates.containsValue(SegmentStateModel.ERROR)) {
      return SegmentStatus.ERROR;
    }

    return segmentStates.values().stream().allMatch(SegmentStateModel.ONLINE::equals)
        ? SegmentStatus.ONLINE
        : SegmentStatus.PENDING;
  }

  /// Whether the OFFLINE external view still contains the segment, regardless of its replicas' states.
  private boolean isSegmentPresent()
      throws IOException {
    TableView externalView = getExternalViewForTable();
    return externalView != null && externalView._offline != null && externalView._offline.containsKey(_segmentName);
  }
}
