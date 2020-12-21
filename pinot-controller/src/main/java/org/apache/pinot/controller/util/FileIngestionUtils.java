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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods for ingestion from file
 */
public final class FileIngestionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileIngestionUtils.class);
  private static final long DEFAULT_RETRY_WAIT_MS = 1000L;
  private static final int DEFAULT_ATTEMPTS = 3;
  private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

  private FileIngestionUtils() {
  }

  /**
   * Copy the file from given URI to local file
   */
  public static void copyURIToLocal(BatchConfig batchConfig, URI sourceFileURI, File destFile)
      throws Exception {
    String sourceFileURIScheme = sourceFileURI.getScheme();
    if (!PinotFSFactory.isSchemeSupported(sourceFileURIScheme)) {
      PinotFSFactory.register(sourceFileURIScheme, batchConfig.getInputFsClassName(),
          IngestionConfigUtils.getFsProps(batchConfig.getInputFsProps()));
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
   * Creates a {@link SegmentGeneratorConfig}
   * @param tableConfig Table config
   * @param batchConfig Batch config override provided by the user during upload
   * @param schema Table schema
   * @param inputFile The input file
   * @param outputSegmentDir The output dir
   */
  public static SegmentGeneratorConfig generateSegmentGeneratorConfig(TableConfig tableConfig, BatchConfig batchConfig,
      Schema schema, File inputFile, File outputSegmentDir)
      throws ClassNotFoundException, IOException {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(tableConfig.getTableName());
    segmentGeneratorConfig.setOutDir(outputSegmentDir.getAbsolutePath());
    segmentGeneratorConfig.setInputFilePath(inputFile.getAbsolutePath());

    FileFormat fileFormat = batchConfig.getInputFormat();
    segmentGeneratorConfig.setFormat(fileFormat);
    segmentGeneratorConfig.setRecordReaderPath(RecordReaderFactory.getRecordReaderClassName(fileFormat.toString()));
    Map<String, String> configs = batchConfig.getRecordReaderProps();
    segmentGeneratorConfig.setReaderConfig(
        RecordReaderFactory.getRecordReaderConfig(fileFormat, IngestionConfigUtils.getRecordReaderProps(configs)));
    // Using current time as postfix to prevent overwriting segments with same time ranges
    segmentGeneratorConfig.setSegmentNameGenerator(
        new SimpleSegmentNameGenerator(tableConfig.getTableName(), String.valueOf(System.currentTimeMillis())));
    return segmentGeneratorConfig;
  }

  /**
   * Builds a segment using given {@link SegmentGeneratorConfig}
   * @return segment name
   */
  public static String buildSegment(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
    return driver.getSegmentName();
  }

  /**
   * Uploads the segment tar files to the provided controller
   */
  public static void uploadSegment(String tableNameWithType, List<File> tarFiles, String controllerHost,
      int controllerPort)
      throws RetriableOperationException, AttemptsExceededException {
    for (File tarFile : tarFiles) {
      String fileName = tarFile.getName();
      Preconditions
          .checkArgument(fileName.endsWith(org.apache.pinot.spi.ingestion.batch.spec.Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0,
          fileName.length() - org.apache.pinot.spi.ingestion.batch.spec.Constants.TAR_GZ_FILE_EXT.length());

      RetryPolicies.exponentialBackoffRetryPolicy(DEFAULT_ATTEMPTS, DEFAULT_RETRY_WAIT_MS, 5).attempt(() -> {
        try (InputStream inputStream = new FileInputStream(tarFile)) {
          SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT
              .uploadSegment(FileUploadDownloadClient.getUploadSegmentHttpURI(controllerHost, controllerPort),
                  segmentName, inputStream, tableNameWithType);
          LOGGER.info("Response for pushing table {} segment {} - {}: {}", tableNameWithType, segmentName,
              response.getStatusCode(), response.getResponse());
          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode >= 500) {
            LOGGER.warn("Caught temporary exception while pushing table: {} segment: {}, will retry", tableNameWithType,
                segmentName, e);
            return false;
          } else {
            LOGGER
                .error("Caught permanent exception while pushing table: {} segment: {}, won't retry", tableNameWithType,
                    segmentName, e);
            throw e;
          }
        }
      });
    }
  }
}
