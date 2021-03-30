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
package org.apache.pinot.controller.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A driver for the ingestion process of the provided file.
 * Responsible for copying the file locally, building a segment and uploading it to the controller.
 */
public class FileIngestionHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileIngestionHelper.class);

  private static final String WORKING_DIR_PREFIX = "working_dir";
  private static final String INPUT_DATA_DIR = "input_data_dir";
  private static final String OUTPUT_SEGMENT_DIR = "output_segment_dir";
  private static final String SEGMENT_TAR_DIR = "segment_tar_dir";
  private static final String DATA_FILE_PREFIX = "data";

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final BatchConfig _batchConfig;
  private final URI _controllerUri;
  private final File _uploadDir;
  private final String _authToken;

  public FileIngestionHelper(TableConfig tableConfig, Schema schema, BatchConfig batchConfig, URI controllerUri,
      File uploadDir, String authToken) {
    _tableConfig = tableConfig;
    _schema = schema;
    _batchConfig = batchConfig;
    _controllerUri = controllerUri;
    _uploadDir = uploadDir;
    _authToken = authToken;
  }

  /**
   * Creates a segment using the provided data file/URI and uploads to Pinot
   */
  public SuccessResponse buildSegmentAndPush(DataPayload payload)
      throws Exception {
    String tableNameWithType = _tableConfig.getTableName();
    File workingDir = new File(_uploadDir,
        String.format("%s_%s_%d", WORKING_DIR_PREFIX, tableNameWithType, System.currentTimeMillis()));
    LOGGER.info("Starting ingestion of {} payload to table: {} using working dir: {}", payload._payloadType,
        tableNameWithType, workingDir.getAbsolutePath());

    // Setup working dir
    File inputDir = new File(workingDir, INPUT_DATA_DIR);
    File outputDir = new File(workingDir, OUTPUT_SEGMENT_DIR);
    File segmentTarDir = new File(workingDir, SEGMENT_TAR_DIR);
    try {
      Preconditions
          .checkState(inputDir.mkdirs(), "Could not create directory for downloading input file locally: %s", inputDir);
      Preconditions.checkState(segmentTarDir.mkdirs(), "Could not create directory for segment tar file: %s", inputDir);

      // Copy file to local working dir
      File inputFile = new File(inputDir,
          String.format("%s.%s", DATA_FILE_PREFIX, _batchConfig.getInputFormat().toString().toLowerCase()));
      if (payload._payloadType == PayloadType.URI) {
        FileIngestionUtils.copyURIToLocal(_batchConfig, payload._uri, inputFile);
        LOGGER.info("Copied from URI: {} to local file: {}", payload._uri, inputFile.getAbsolutePath());
      } else {
        FileIngestionUtils.copyMultipartToLocal(payload._multiPart, inputFile);
        LOGGER.info("Copied multipart payload to local file: {}", inputDir.getAbsolutePath());
      }

      // Build segment
      SegmentGeneratorConfig segmentGeneratorConfig =
          FileIngestionUtils.generateSegmentGeneratorConfig(_tableConfig, _batchConfig, _schema, inputFile, outputDir);
      String segmentName = FileIngestionUtils.buildSegment(segmentGeneratorConfig);
      LOGGER.info("Built segment: {}", segmentName);

      // Tar and push segment
      File segmentTarFile =
          new File(segmentTarDir, segmentName + org.apache.pinot.spi.ingestion.batch.spec.Constants.TAR_GZ_FILE_EXT);
      TarGzCompressionUtils.createTarGzFile(new File(outputDir, segmentName), segmentTarFile);
      FileIngestionUtils
          .uploadSegment(tableNameWithType, Lists.newArrayList(segmentTarFile), _controllerUri, _authToken);
      LOGGER.info("Uploaded tar: {} to {}", segmentTarFile.getAbsolutePath(), _controllerUri);

      return new SuccessResponse(
          "Successfully ingested file into table: " + tableNameWithType + " as segment: " + segmentName);
    } catch (Exception e) {
      LOGGER.error("Caught exception when ingesting file to table: {}", tableNameWithType, e);
      throw e;
    } finally {
      FileUtils.deleteQuietly(workingDir);
    }
  }

  /**
   * Enum to identify the source of ingestion file
   */
  private enum PayloadType {
    URI, FILE
  }

  /**
   * Wrapper around file payload
   */
  public static class DataPayload {
    PayloadType _payloadType;
    FormDataMultiPart _multiPart;
    URI _uri;

    public DataPayload(FormDataMultiPart multiPart) {
      _payloadType = PayloadType.FILE;
      _multiPart = multiPart;
    }

    public DataPayload(URI uri) {
      _payloadType = PayloadType.URI;
      _uri = uri;
    }
  }
}
