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
import org.apache.pinot.controller.ControllerConf;
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
  private final ControllerConf _controllerConf;

  public FileIngestionHelper(TableConfig tableConfig, Schema schema,
      BatchConfig batchConfig, ControllerConf controllerConf) {
    _tableConfig = tableConfig;
    _schema = schema;
    _batchConfig = batchConfig;
    _controllerConf = controllerConf;
  }

  /**
   * Creates a segment using the provided data file/URI and uploads to Pinot
   */
  public SuccessResponse buildSegmentAndPush(DataPayload payload)
      throws Exception {
    String tableNameWithType = _tableConfig.getTableName();

    // Setup working dir
    File workingDir = new File(FileUtils.getTempDirectory(),
        String.format("%s_%s_%d", WORKING_DIR_PREFIX, tableNameWithType, System.currentTimeMillis()));
    File inputDir = new File(workingDir, INPUT_DATA_DIR);
    File outputDir = new File(workingDir, OUTPUT_SEGMENT_DIR);
    File segmentTarDir = new File(workingDir, SEGMENT_TAR_DIR);
    try {
      Preconditions
          .checkState(inputDir.mkdirs(), "Could not create directory for downloading input file locally: %s", inputDir);
      Preconditions.checkState(segmentTarDir.mkdirs(), "Could not create directory for segment tar file: %s", inputDir);

      // Copy file to local working dir
      File inputFile =
          new File(inputDir, String.format("%s.%s", DATA_FILE_PREFIX, _batchConfig.getInputFormat().toString().toLowerCase()));
      if (payload._dataSource.equals(DataSource.URI)) {
        FileIngestionUtils.copyURIToLocal(_batchConfig, payload._uri, inputFile);
      } else {
        FileIngestionUtils.copyMultipartToLocal(payload._multiPart, inputFile);
      }

      // Build segment
      SegmentGeneratorConfig segmentGeneratorConfig =
          FileIngestionUtils.generateSegmentGeneratorConfig(_tableConfig, _batchConfig, _schema, inputFile, outputDir);
      String segmentName = FileIngestionUtils.buildSegment(segmentGeneratorConfig);

      // Tar and push segment
      File segmentTarFile =
          new File(segmentTarDir, segmentName + org.apache.pinot.spi.ingestion.batch.spec.Constants.TAR_GZ_FILE_EXT);
      TarGzCompressionUtils.createTarGzFile(new File(outputDir, segmentName), segmentTarFile);
      FileIngestionUtils
          .uploadSegment(tableNameWithType, Lists.newArrayList(segmentTarFile), _controllerConf.getControllerHost(),
              Integer.parseInt(_controllerConf.getControllerPort()));

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
  private enum DataSource {
    URI,
    FILE
  }

  /**
   * Wrapper around file payload
   */
  public static class DataPayload{
    DataSource _dataSource;
    FormDataMultiPart _multiPart;
    URI _uri;

    public DataPayload(FormDataMultiPart multiPart) {
      _dataSource = DataSource.FILE;
      _multiPart = multiPart;
    }

    public DataPayload(URI uri) {
      _dataSource = DataSource.URI;
      _uri = uri;
    }
  }
}
