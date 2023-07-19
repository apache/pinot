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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.segment.uploader.SegmentUploader;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A driver for the ingestion process of the provided file.
 * Responsible for copying the file locally, building a segment and uploading it to the controller.
 */
public class FileIngestionHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileIngestionHelper.class);
  private static final String SEGMENT_UPLOADER_CLASS = "org.apache.pinot.plugin.segmentuploader.SegmentUploaderDefault";

  private static final String WORKING_DIR_PREFIX = "working_dir";
  private static final String INPUT_DATA_DIR = "input_data_dir";
  private static final String OUTPUT_SEGMENT_DIR = "output_segment_dir";
  private static final String SEGMENT_TAR_DIR = "segment_tar_dir";
  private static final String DATA_FILE_PREFIX = "data";

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final Map<String, String> _batchConfigMap;
  private final URI _controllerUri;
  private final File _ingestionDir;
  private final AuthProvider _authProvider;

  public FileIngestionHelper(TableConfig tableConfig, Schema schema, Map<String, String> batchConfigMap,
      URI controllerUri, File ingestionDir, AuthProvider authProvider) {
    _tableConfig = tableConfig;
    _schema = schema;
    _batchConfigMap = batchConfigMap;
    _controllerUri = controllerUri;
    _ingestionDir = ingestionDir;
    _authProvider = authProvider;
  }

  /**
   * Creates a segment using the provided data file/URI and uploads to Pinot
   */
  public SuccessResponse buildSegmentAndPush(DataPayload payload)
      throws Exception {
    String tableNameWithType = _tableConfig.getTableName();
    // 1. append a timestamp for easy debugging
    // 2. append a random string to avoid using the same working directory when multiple tasks are running in parallel
    File workingDir = new File(_ingestionDir,
        String.format("%s_%s_%d_%s", WORKING_DIR_PREFIX, tableNameWithType, System.currentTimeMillis(),
            RandomStringUtils.random(10, true, false)));
    LOGGER.info("Starting ingestion of {} payload to table: {} using working dir: {}", payload._payloadType,
        tableNameWithType, workingDir.getAbsolutePath());

    if (!workingDir.getCanonicalPath().startsWith(_ingestionDir.getPath())) {
      throw new IllegalArgumentException("Invalid working dir path %s" + workingDir.getAbsolutePath());
    }

    // Setup working dir
    File inputDir = new File(workingDir, INPUT_DATA_DIR);
    File outputDir = new File(workingDir, OUTPUT_SEGMENT_DIR);
    File segmentTarDir = new File(workingDir, SEGMENT_TAR_DIR);
    try {
      Preconditions.checkState(inputDir.mkdirs(),
          "Could not create directory for downloading input file locally: %s", inputDir);
      Preconditions.checkState(segmentTarDir.mkdirs(),
          "Could not create directory for segment tar file: %s", inputDir);

      // Copy file to local working dir
      File inputFile = new File(inputDir, String.format(
          "%s.%s", DATA_FILE_PREFIX, _batchConfigMap.get(BatchConfigProperties.INPUT_FORMAT).toLowerCase()));
      if (payload._payloadType == PayloadType.URI) {
        copyURIToLocal(_batchConfigMap, payload._uri, inputFile);
        LOGGER.info("Copied from URI: {} to local file: {}", payload._uri, inputFile.getAbsolutePath());
      } else {
        copyMultipartToLocal(payload._multiPart, inputFile);
        LOGGER.info("Copied multipart payload to local file: {}", inputDir.getAbsolutePath());
      }

      // Update batch config map with values for file upload
      Map<String, String> batchConfigMapOverride = new HashMap<>(_batchConfigMap);
      batchConfigMapOverride.put(BatchConfigProperties.INPUT_DIR_URI, inputFile.getAbsolutePath());
      batchConfigMapOverride.put(BatchConfigProperties.OUTPUT_DIR_URI, outputDir.getAbsolutePath());
      batchConfigMapOverride.put(BatchConfigProperties.PUSH_CONTROLLER_URI, _controllerUri.toString());
      String segmentNamePostfixProp = String.format("%s.%s", BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX,
          BatchConfigProperties.SEGMENT_NAME_POSTFIX);
      if (StringUtils.isBlank(batchConfigMapOverride.get(segmentNamePostfixProp))) {
        // Default segmentNameGenerator is SIMPLE.
        // Adding this suffix to prevent creating a segment with the same name as an existing segment,
        // if a file with the same time range is received again
        batchConfigMapOverride.put(segmentNamePostfixProp, String.valueOf(System.currentTimeMillis()));
      }
      BatchIngestionConfig batchIngestionConfigOverride =
          new BatchIngestionConfig(Collections.singletonList(batchConfigMapOverride),
              IngestionConfigUtils.getBatchSegmentIngestionType(_tableConfig),
              IngestionConfigUtils.getBatchSegmentIngestionFrequency(_tableConfig));

      // Get SegmentGeneratorConfig
      SegmentGeneratorConfig segmentGeneratorConfig =
          IngestionUtils.generateSegmentGeneratorConfig(_tableConfig, _schema, batchIngestionConfigOverride);

      // Build segment
      String segmentName = IngestionUtils.buildSegment(segmentGeneratorConfig);
      LOGGER.info("Built segment: {}", segmentName);

      // Tar segment dir
      File segmentTarFile =
          new File(segmentTarDir, segmentName + org.apache.pinot.spi.ingestion.batch.spec.Constants.TAR_GZ_FILE_EXT);
      TarGzCompressionUtils.createTarGzFile(new File(outputDir, segmentName), segmentTarFile);

      // Upload segment
      IngestionConfig ingestionConfigOverride = new IngestionConfig();
      ingestionConfigOverride.setBatchIngestionConfig(batchIngestionConfigOverride);
      TableConfig tableConfigOverride =
          new TableConfigBuilder(_tableConfig.getTableType()).setTableName(_tableConfig.getTableName())
              .setIngestionConfig(ingestionConfigOverride).build();
      SegmentUploader segmentUploader = PluginManager.get().createInstance(SEGMENT_UPLOADER_CLASS);
      segmentUploader.init(tableConfigOverride);
      segmentUploader.uploadSegment(segmentTarFile.toURI(), _authProvider);
      LOGGER.info("Uploaded tar: {} to table: {}", segmentTarFile.getAbsolutePath(), tableNameWithType);

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
   * Copy the file from given URI to local file
   */
  public static void copyURIToLocal(Map<String, String> batchConfigMap, URI sourceFileURI, File destFile)
      throws Exception {
    String sourceFileURIScheme = sourceFileURI.getScheme();
    if (!PinotFSFactory.isSchemeSupported(sourceFileURIScheme)) {
      PinotFSFactory.register(sourceFileURIScheme, batchConfigMap.get(BatchConfigProperties.INPUT_FS_CLASS),
          IngestionConfigUtils.getInputFsProps(batchConfigMap));
    }
    PinotFSFactory.create(sourceFileURIScheme).copyToLocalFile(sourceFileURI, destFile);
  }

  /**
   * Copy the file from the uploaded multipart to a local file
   */
  public static void copyMultipartToLocal(FormDataMultiPart multiPart, File destFile)
      throws IOException {
    FormDataBodyPart formDataBodyPart = multiPart.getFields().values().iterator().next().get(0);
    try (InputStream inputStream = formDataBodyPart.getValueAs(InputStream.class);
        OutputStream outputStream = new FileOutputStream(destFile)) {
      IOUtils.copyLarge(inputStream, outputStream);
    } finally {
      multiPart.cleanup();
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
