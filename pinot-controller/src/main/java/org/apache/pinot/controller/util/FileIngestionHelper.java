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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
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


/// A driver for the ingestion process of the provided file.
/// Responsible for copying the file locally, building a segment and uploading it to the controller.
public class FileIngestionHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileIngestionHelper.class);
  private static final String SEGMENT_UPLOADER_CLASS = "org.apache.pinot.plugin.segmentuploader.SegmentUploaderDefault";
  private static final String LOCAL_FILE_SYSTEM_DISABLED_MESSAGE =
      "Local filesystem sources are disabled for /ingestFromURI";
  private static final String INVALID_FILE_SYSTEM_MESSAGE = "Invalid filesystem source for /ingestFromURI";

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
  private final boolean _allowLocalFileSystemInUri;

  public FileIngestionHelper(TableConfig tableConfig, Schema schema, Map<String, String> batchConfigMap,
      URI controllerUri, File ingestionDir, AuthProvider authProvider, boolean allowLocalFileSystemInUri) {
    _tableConfig = tableConfig;
    _schema = schema;
    _batchConfigMap = batchConfigMap;
    _controllerUri = controllerUri;
    _ingestionDir = ingestionDir;
    _authProvider = authProvider;
    _allowLocalFileSystemInUri = allowLocalFileSystemInUri;
  }

  /// Creates a segment using the provided data file/URI and uploads to Pinot
  public SuccessResponse buildSegmentAndPush(DataPayload payload)
      throws Exception {
    // Resolve and validate the source before creating working directories or parsing input data. Keep the resolved
    // instance so a concurrent factory registration cannot replace it between validation and copying.
    try (ResolvedFileSystem sourceFileSystem = payload._payloadType == PayloadType.URI
        ? resolveSourceFileSystem(_batchConfigMap, payload._uri, _allowLocalFileSystemInUri) : null) {
      return buildSegmentAndPush(payload, sourceFileSystem);
    } catch (Exception | LinkageError e) {
      if (payload._payloadType == PayloadType.URI) {
        String tableNameWithType = _tableConfig != null ? _tableConfig.getTableName() : null;
        LOGGER.error("Failed URI ingestion for table: {}, exception type: {}", tableNameWithType,
            e.getClass().getName());
      }
      throw e;
    }
  }

  private SuccessResponse buildSegmentAndPush(DataPayload payload, ResolvedFileSystem sourceFileSystem)
      throws Exception {
    String tableNameWithType = _tableConfig.getTableName();
    // 1. append a timestamp for easy debugging
    // 2. append a random string to avoid using the same working directory when multiple tasks are running in parallel
    File workingDir = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(_ingestionDir,
        String.format("%s_%s_%d_%s", WORKING_DIR_PREFIX, tableNameWithType, System.currentTimeMillis(),
            RandomStringUtils.secure().next(10, true, false)), "Invalid table name: %S", tableNameWithType);
    LOGGER.info("Starting ingestion of {} payload to table: {}", payload._payloadType, tableNameWithType);

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
        sourceFileSystem._fileSystem.copyToLocalFile(payload._uri, inputFile);
        LOGGER.info("Copied URI source to a local staging file for table: {}", tableNameWithType);
      } else {
        copyMultipartToLocal(payload._multiPart, inputFile);
        LOGGER.info("Copied multipart payload to a local staging file for table: {}", tableNameWithType);
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
          new BatchIngestionConfig(List.of(batchConfigMapOverride),
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
      TarCompressionUtils.createCompressedTarFile(new File(outputDir, segmentName), segmentTarFile);

      // Upload segment
      IngestionConfig ingestionConfigOverride = new IngestionConfig();
      ingestionConfigOverride.setBatchIngestionConfig(batchIngestionConfigOverride);
      TableConfig tableConfigOverride =
          new TableConfigBuilder(_tableConfig.getTableType()).setTableName(_tableConfig.getTableName())
              .setIngestionConfig(ingestionConfigOverride).build();
      SegmentUploader segmentUploader = PluginManager.get().createInstance(SEGMENT_UPLOADER_CLASS);
      segmentUploader.init(tableConfigOverride);
      segmentUploader.uploadSegment(segmentTarFile.toURI(), _authProvider);
      LOGGER.info("Uploaded generated segment to table: {}", tableNameWithType);

      return new SuccessResponse(
          "Successfully ingested file into table: " + tableNameWithType + " as segment: " + segmentName);
    } catch (Exception e) {
      if (payload._payloadType == PayloadType.FILE) {
        LOGGER.error("Caught exception when ingesting file to table: {}", tableNameWithType, e);
      }
      throw e;
    } finally {
      FileUtils.deleteQuietly(workingDir);
    }
  }

  public static void copyURIToLocal(Map<String, String> batchConfigMap, URI sourceFileURI, File destFile,
      boolean allowLocalFileSystem)
      throws Exception {
    try (ResolvedFileSystem sourceFileSystem =
        resolveSourceFileSystem(batchConfigMap, sourceFileURI, allowLocalFileSystem)) {
      sourceFileSystem._fileSystem.copyToLocalFile(sourceFileURI, destFile);
    }
  }

  private static ResolvedFileSystem resolveSourceFileSystem(Map<String, String> batchConfigMap, URI sourceFileURI,
      boolean allowLocalFileSystem) {
    String sourceFileURIScheme = sourceFileURI.getScheme();
    Preconditions.checkArgument(allowLocalFileSystem || !isLocalSource(sourceFileURI),
        LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
    if (PinotFSFactory.isSchemeSupported(sourceFileURIScheme)) {
      PinotFS sourceFileSystem;
      try {
        sourceFileSystem = PinotFSFactory.create(sourceFileURIScheme);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(INVALID_FILE_SYSTEM_MESSAGE, e);
      }
      validateFileSystemInstance(sourceFileSystem, allowLocalFileSystem);
      return new ResolvedFileSystem(sourceFileSystem, null);
    }

    String inputFsClassName = batchConfigMap.get(BatchConfigProperties.INPUT_FS_CLASS);
    Preconditions.checkArgument(StringUtils.isNotBlank(inputFsClassName), INVALID_FILE_SYSTEM_MESSAGE);
    Class<? extends PinotFS> fileSystemClass = loadFileSystemClass(inputFsClassName, allowLocalFileSystem);
    PinotFS sourceFileSystem;
    try {
      sourceFileSystem = fileSystemClass.getConstructor().newInstance();
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      throw new IllegalArgumentException(INVALID_FILE_SYSTEM_MESSAGE, e);
    }
    try {
      validateFileSystemInstance(sourceFileSystem, allowLocalFileSystem);
    } catch (RuntimeException | LinkageError e) {
      closeRequestScopedFileSystem(sourceFileSystem);
      throw e;
    }
    try {
      sourceFileSystem.init(IngestionConfigUtils.getInputFsProps(batchConfigMap));
    } catch (RuntimeException | LinkageError e) {
      closeRequestScopedFileSystem(sourceFileSystem);
      throw new IllegalArgumentException(INVALID_FILE_SYSTEM_MESSAGE, e);
    }
    return new ResolvedFileSystem(sourceFileSystem, sourceFileSystem);
  }

  private static boolean isLocalSource(URI sourceFileURI) {
    String scheme = sourceFileURI.getScheme();
    if (StringUtils.isBlank(scheme) || PinotFSFactory.LOCAL_PINOT_FS_SCHEME.equalsIgnoreCase(scheme)) {
      return true;
    }
    // URI treats a Windows drive letter as a scheme (for example C:/data.csv).
    return scheme.length() == 1 && sourceFileURI.getAuthority() == null
        && (sourceFileURI.isOpaque() || sourceFileURI.getPath() != null && sourceFileURI.getPath().startsWith("/"));
  }

  private static Class<? extends PinotFS> loadFileSystemClass(String className, boolean allowLocalFileSystem) {
    Class<?> fileSystemClass;
    try {
      fileSystemClass = PluginManager.get().loadClass(className);
    } catch (Exception | LinkageError e) {
      throw new IllegalArgumentException(INVALID_FILE_SYSTEM_MESSAGE, e);
    }
    Preconditions.checkArgument(PinotFS.class.isAssignableFrom(fileSystemClass), INVALID_FILE_SYSTEM_MESSAGE);
    Preconditions.checkArgument(allowLocalFileSystem || !LocalPinotFS.class.isAssignableFrom(fileSystemClass),
        LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
    return fileSystemClass.asSubclass(PinotFS.class);
  }

  private static void validateFileSystemInstance(PinotFS fileSystem, boolean allowLocalFileSystem) {
    Preconditions.checkArgument(allowLocalFileSystem
            || !PinotFSFactory.isFileSystemInstanceOf(fileSystem, LocalPinotFS.class),
        LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
  }

  private static void closeRequestScopedFileSystem(PinotFS fileSystem) {
    try {
      fileSystem.close();
    } catch (Exception | LinkageError e) {
      LOGGER.warn("Failed to close request-scoped filesystem, exception type: {}", e.getClass().getName());
    }
  }

  private static class ResolvedFileSystem implements AutoCloseable {
    private final PinotFS _fileSystem;
    private final PinotFS _closeWhenDone;

    private ResolvedFileSystem(PinotFS fileSystem, PinotFS closeWhenDone) {
      _fileSystem = fileSystem;
      _closeWhenDone = closeWhenDone;
    }

    @Override
    public void close() {
      if (_closeWhenDone != null) {
        closeRequestScopedFileSystem(_closeWhenDone);
      }
    }
  }

  /// Copy the file from the uploaded multipart to a local file
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

  /// Enum to identify the source of ingestion file
  private enum PayloadType {
    URI, FILE
  }

  /// Wrapper around file payload
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
