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
package org.apache.pinot.segment.local.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.segment.local.constants.SegmentUploadConstants;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.creator.name.SegmentNameUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TlsSpec;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPushUtils implements Serializable {
  private SegmentPushUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPushUtils.class);
  private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();
  private static final String HTTP_CLIENT_CONNECTION_TIMEOUT_CONFIG = "http.client.connectionTimeoutMs";

  @VisibleForTesting
  static FileUploadDownloadClient getOrCreateFileUploadDownloadClient(SegmentGenerationJobSpec spec) {
    TlsSpec tlsSpec = spec.getTlsSpec();
    if (tlsSpec == null) {
      return FILE_UPLOAD_DOWNLOAD_CLIENT;
    }
    return new FileUploadDownloadClient(getHttpClientConfig(tlsSpec),
        TlsUtils.createSslContextWithoutAutoRenewal(tlsSpec.getKeyStoreType(), tlsSpec.getKeyStorePath(),
            tlsSpec.getKeyStorePassword(), tlsSpec.getTrustStoreType(), tlsSpec.getTrustStorePath(),
            tlsSpec.getTrustStorePassword()));
  }

  @VisibleForTesting
  static HttpClientConfig getHttpClientConfig(TlsSpec tlsSpec) {
    PinotConfiguration httpClientConfiguration = new PinotConfiguration();
    httpClientConfiguration.setProperty(HTTP_CLIENT_CONNECTION_TIMEOUT_CONFIG, tlsSpec.getConnectTimeout());
    return HttpClientConfig.newBuilder(httpClientConfiguration).build();
  }

  static void closeFileUploadDownloadClient(SegmentGenerationJobSpec spec,
      FileUploadDownloadClient fileUploadDownloadClient) {
    if (spec.getTlsSpec() == null) {
      return;
    }
    try {
      fileUploadDownloadClient.close();
    } catch (IOException e) {
      LOGGER.warn("Unable to close TLS-aware file upload/download client", e);
    }
  }

  static int getSocketTimeoutMs(SegmentGenerationJobSpec spec) {
    TlsSpec tlsSpec = spec.getTlsSpec();
    return tlsSpec != null ? tlsSpec.getReadTimeout() : HttpClient.DEFAULT_SOCKET_TIMEOUT_MS;
  }

  public static URI generateSegmentTarURI(URI dirURI, URI fileURI, String prefix, String suffix) {
    if (StringUtils.isEmpty(prefix) && StringUtils.isEmpty(suffix)) {
      // In case the FS doesn't provide scheme or host, will fill it up from dirURI.
      String scheme = fileURI.getScheme();
      if (StringUtils.isEmpty(fileURI.getScheme())) {
        scheme = dirURI.getScheme();
      }
      String host = fileURI.getHost();
      if (StringUtils.isEmpty(fileURI.getHost())) {
        host = dirURI.getHost();
      }
      int port = fileURI.getPort();
      if (port < 0) {
        port = dirURI.getPort();
      }
      try {
        return new URI(scheme, fileURI.getUserInfo(), host, port, fileURI.getPath(), fileURI.getQuery(),
            fileURI.getFragment());
      } catch (URISyntaxException e) {
        LOGGER.warn("Unable to generate push uri based from dir URI: {} and file URI: {}, directly return file URI.",
            dirURI, fileURI);
        return fileURI;
      }
    }
    return URI.create((prefix != null ? prefix : "") + fileURI.getRawPath() + (suffix != null ? suffix : ""));
  }

  public static void pushSegments(SegmentGenerationJobSpec spec, PinotFS fileSystem, List<String> tarFilePaths)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = spec.getTableSpec().getTableName();
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    List<Header> headers = AuthProviderUtils.toRequestHeaders(authProvider);
    List<NameValuePair> parameters = FileUploadDownloadClient.makeTableParam(tableName);
    pushSegments(spec, fileSystem, tarFilePaths, headers, parameters);
  }

  public static void sendSegmentUris(SegmentGenerationJobSpec spec, List<String> segmentUris)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = spec.getTableSpec().getTableName();
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    List<Header> headers = AuthProviderUtils.toRequestHeaders(authProvider);
    List<NameValuePair> parameters = FileUploadDownloadClient.makeTableParam(tableName);
    sendSegmentUris(spec, segmentUris, headers, parameters);
  }

  /// This method takes a map of segment downloadURI to corresponding tar file path, and push those segments in
  /// metadata mode.
  /// The steps are:
  /// 1. Download segment from tar file path;
  /// 2. Untar segment metadata and creation meta files from the tar file to a segment metadata directory;
  /// 3. Tar this segment metadata directory into a tar file
  /// 4. Generate a POST request with segmentDownloadURI in header to push tar file to Pinot controller.
  ///
  /// @param spec is the segment generation job spec
  /// @param fileSystem is the PinotFs used to copy segment tar file
  /// @param segmentUriToTarPathMap contains the map of segment DownloadURI to segment tar file path
  /// @throws Exception
  public static void sendSegmentUriAndMetadata(SegmentGenerationJobSpec spec, PinotFS fileSystem,
      Map<String, String> segmentUriToTarPathMap)
      throws Exception {
    String tableName = spec.getTableSpec().getTableName();
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    List<Header> headers = AuthProviderUtils.toRequestHeaders(authProvider);
    List<NameValuePair> parameters = FileUploadDownloadClient.makeTableParam(tableName);
    PushJobSpec pushJobSpec = spec.getPushJobSpec();
    parameters.add(FileUploadDownloadClient.makeParallelProtectionParam(pushJobSpec));
    if (pushJobSpec != null && pushJobSpec.isBatchSegmentUpload()) {
      // segments are uploaded in batch when batch mode is enabled.
      sendSegmentsUriAndMetadata(spec, fileSystem, segmentUriToTarPathMap, headers, parameters);
    } else {
      sendSegmentUriAndMetadata(spec, fileSystem, segmentUriToTarPathMap, headers, parameters);
    }
  }

  public static void pushSegments(SegmentGenerationJobSpec spec, PinotFS fileSystem, List<String> tarFilePaths,
      List<Header> headers, List<NameValuePair> parameters)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = spec.getTableSpec().getTableName();
    TableType tableType = tableName.endsWith("_" + TableType.REALTIME.name()) ? TableType.REALTIME : TableType.OFFLINE;
    boolean cleanUpOutputDir = spec.isCleanUpOutputDir();
    LOGGER.info("Start pushing segments: {}... to locations: {} for table {}",
        Arrays.toString(tarFilePaths.subList(0, Math.min(5, tarFilePaths.size())).toArray()),
        Arrays.toString(spec.getPinotClusterSpecs()), tableName);
    FileUploadDownloadClient fileUploadDownloadClient = getOrCreateFileUploadDownloadClient(spec);
    int socketTimeoutMs = getSocketTimeoutMs(spec);
    try {
      for (String tarFilePath : tarFilePaths) {
        URI tarFileURI = URI.create(tarFilePath);
        File tarFile = new File(tarFilePath);
        String fileName = tarFile.getName();
        Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
        String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
        for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
          URI controllerURI;
          try {
            controllerURI = new URI(pinotClusterSpec.getControllerURI());
          } catch (URISyntaxException e) {
            throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
          }
          LOGGER.info("Pushing segment: {} to location: {} for table {}", segmentName, controllerURI, tableName);
          int attempts = 1;
          if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushAttempts() > 0) {
            attempts = spec.getPushJobSpec().getPushAttempts();
          }
          long retryWaitMs = 1000L;
          if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushRetryIntervalMillis() > 0) {
            retryWaitMs = spec.getPushJobSpec().getPushRetryIntervalMillis();
          }
          RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
            try (InputStream inputStream = fileSystem.open(tarFileURI)) {
              SimpleHttpResponse response =
                  fileUploadDownloadClient.uploadSegment(FileUploadDownloadClient.getUploadSegmentURI(controllerURI),
                      segmentName, inputStream, headers, makeUploadSegmentParams(parameters, tableName, tableType),
                      socketTimeoutMs);
              LOGGER.info("Response for pushing table {} segment {} to location {} - {}: {}", tableName, segmentName,
                  controllerURI, response.getStatusCode(), response.getResponse());
              return true;
            } catch (HttpErrorStatusException e) {
              int statusCode = e.getStatusCode();
              if (statusCode >= 500) {
                // Temporary exception
                LOGGER.warn("Caught temporary exception while pushing table: {} segment: {} to {}, will retry",
                    tableName, segmentName, controllerURI, e);
                return false;
              } else {
                // Permanent exception
                LOGGER.error("Caught permanent exception while pushing table: {} segment: {} to {}, won't retry",
                    tableName, segmentName, controllerURI, e);
                throw e;
              }
            } finally {
              if (cleanUpOutputDir) {
                fileSystem.delete(tarFileURI, true);
              }
            }
          });
        }
      }
    } finally {
      closeFileUploadDownloadClient(spec, fileUploadDownloadClient);
    }
  }

  public static void sendSegmentUris(SegmentGenerationJobSpec spec, List<String> segmentUris,
      List<Header> headers, List<NameValuePair> parameters)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = spec.getTableSpec().getTableName();
    LOGGER.info("Start sending table {} segment URIs: {} to locations: {}", tableName,
        Arrays.toString(segmentUris.subList(0, Math.min(5, segmentUris.size())).toArray()),
        Arrays.toString(spec.getPinotClusterSpecs()));
    FileUploadDownloadClient fileUploadDownloadClient = getOrCreateFileUploadDownloadClient(spec);
    int socketTimeoutMs = getSocketTimeoutMs(spec);
    try {
      for (String segmentUri : segmentUris) {
        URI segmentURI = URI.create(segmentUri);
        PinotFS outputDirFS = PinotFSFactory.create(segmentURI.getScheme());
        for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
          URI controllerURI;
          try {
            controllerURI = new URI(pinotClusterSpec.getControllerURI());
          } catch (URISyntaxException e) {
            throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
          }
          LOGGER.info("Sending table {} segment URI: {} to location: {} for ", tableName, segmentUri, controllerURI);
          int attempts = 1;
          if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushAttempts() > 0) {
            attempts = spec.getPushJobSpec().getPushAttempts();
          }
          long retryWaitMs = 1000L;
          if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushRetryIntervalMillis() > 0) {
            retryWaitMs = spec.getPushJobSpec().getPushRetryIntervalMillis();
          }
          RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
            try {
              SimpleHttpResponse response = fileUploadDownloadClient
                  .sendSegmentUri(FileUploadDownloadClient.getUploadSegmentURI(controllerURI), segmentUri,
                      headers, parameters, socketTimeoutMs);
              LOGGER.info("Response for pushing table {} segment uri {} to location {} - {}: {}", tableName,
                  segmentUri, controllerURI, response.getStatusCode(), response.getResponse());
              return true;
            } catch (HttpErrorStatusException e) {
              int statusCode = e.getStatusCode();
              if (statusCode >= 500) {
                // Temporary exception
                LOGGER.warn("Caught temporary exception while pushing table: {} segment uri: {} to {}, will retry",
                    tableName, segmentUri, controllerURI, e);
                return false;
              } else {
                // Permanent exception
                LOGGER.error("Caught permanent exception while pushing table: {} segment uri: {} to {}, won't retry",
                    tableName, segmentUri, controllerURI, e);
                throw e;
              }
            } finally {
              if (spec.isCleanUpOutputDir()) {
                outputDirFS.delete(segmentURI, true);
              }
            }
          });
        }
      }
    } finally {
      closeFileUploadDownloadClient(spec, fileUploadDownloadClient);
    }
  }

  /// This method takes a map of segment downloadURI to corresponding tar file path, and push those segments in
  /// metadata mode.
  /// The steps are:
  /// 1. Download segment from tar file path;
  /// 2. Untar segment metadata and creation meta files from the tar file to a segment metadata directory;
  /// 3. Tar this segment metadata directory into a tar file
  /// 4. Generate a POST request with segmentDownloadURI in header to push tar file to Pinot controller.
  ///
  /// @param spec is the segment generation job spec
  /// @param fileSystem is the PinotFs used to copy segment tar file
  /// @param segmentUriToTarPathMap contains the map of segment DownloadURI to segment tar file path
  /// @throws Exception
  public static void sendSegmentUriAndMetadata(SegmentGenerationJobSpec spec, PinotFS fileSystem,
      Map<String, String> segmentUriToTarPathMap, List<Header> headers, List<NameValuePair> parameters)
      throws Exception {
    LOGGER.info("Start pushing segment metadata: {} to locations: {} for table {}", segmentUriToTarPathMap,
        Arrays.toString(spec.getPinotClusterSpecs()), spec.getTableSpec().getTableName());
    FileUploadDownloadClient fileUploadDownloadClient = getOrCreateFileUploadDownloadClient(spec);
    try {
      for (Map.Entry<String, String> entry : segmentUriToTarPathMap.entrySet()) {
        String tarFilePath = entry.getValue();
        String segmentName = getSegmentName(tarFilePath);
        File segmentMetadataFile = getSegmentMetadataFile(spec, fileSystem, tarFilePath, segmentName);
        try {
          pushSegmentMetadata(spec, fileUploadDownloadClient, entry.getKey(), segmentName, segmentMetadataFile,
              headers, parameters);
        } finally {
          FileUtils.deleteQuietly(segmentMetadataFile);
        }
      }
    } finally {
      closeFileUploadDownloadClient(spec, fileUploadDownloadClient);
    }
  }

  /// Same as above for segments whose metadata tars the caller already holds locally (see
  /// [#generateSegmentMetadataFile(File, File, String)]), so nothing is downloaded back from the output filesystem.
  /// Each file must be named `<segmentName>.metadata.tar.gz`, and the caller owns the files.
  public static void sendSegmentUriAndMetadata(SegmentGenerationJobSpec spec,
      Map<String, File> segmentUriToMetadataFileMap, List<Header> headers, List<NameValuePair> parameters)
      throws Exception {
    LOGGER.info("Start pushing local segment metadata for: {} to locations: {} for table {}",
        segmentUriToMetadataFileMap.keySet(), Arrays.toString(spec.getPinotClusterSpecs()),
        spec.getTableSpec().getTableName());
    FileUploadDownloadClient fileUploadDownloadClient = getOrCreateFileUploadDownloadClient(spec);
    try {
      for (Map.Entry<String, File> entry : segmentUriToMetadataFileMap.entrySet()) {
        File segmentMetadataFile = entry.getValue();
        pushSegmentMetadata(spec, fileUploadDownloadClient, entry.getKey(),
            getSegmentNameFromMetadataFile(segmentMetadataFile), segmentMetadataFile, headers, parameters);
      }
    } finally {
      closeFileUploadDownloadClient(spec, fileUploadDownloadClient);
    }
  }

  private static void pushSegmentMetadata(SegmentGenerationJobSpec spec,
      FileUploadDownloadClient fileUploadDownloadClient, String segmentUriPath, String segmentName,
      File segmentMetadataFile, List<Header> headers, List<NameValuePair> parameters)
      throws Exception {
    String tableName = spec.getTableSpec().getTableName();
    int socketTimeoutMs = getSocketTimeoutMs(spec);
    for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
      URI controllerURI;
      try {
        controllerURI = new URI(pinotClusterSpec.getControllerURI());
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
      }
      LOGGER.info("Pushing segment: {} to location: {} for table {}", segmentName, controllerURI, tableName);
      int attempts = 1;
      if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushAttempts() > 0) {
        attempts = spec.getPushJobSpec().getPushAttempts();
      }
      long retryWaitMs = 1000L;
      if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushRetryIntervalMillis() > 0) {
        retryWaitMs = spec.getPushJobSpec().getPushRetryIntervalMillis();
      }
      RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
        List<Header> reqHttpHeaders = new ArrayList<>(headers);
        try {
          reqHttpHeaders.add(
              new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI, segmentUriPath));
          reqHttpHeaders.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
              FileUploadDownloadClient.FileUploadType.METADATA.toString()));
          if (spec.getPushJobSpec() != null) {
            reqHttpHeaders.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE,
                String.valueOf(spec.getPushJobSpec().getCopyToDeepStoreForMetadataPush())));
          }

          SimpleHttpResponse response = fileUploadDownloadClient.uploadSegmentMetadata(
              FileUploadDownloadClient.getUploadSegmentURI(controllerURI), segmentName,
              segmentMetadataFile, reqHttpHeaders, parameters, socketTimeoutMs);
          LOGGER.info("Response for pushing table {} segment {} to location {} - {}: {}", tableName, segmentName,
              controllerURI, response.getStatusCode(), response.getResponse());
          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode >= 500) {
            // Temporary exception
            LOGGER.warn("Caught temporary exception while pushing table: {} segment: {} to {}, will retry",
                tableName, segmentName, controllerURI, e);
            return false;
          } else {
            // Permanent exception
            LOGGER.error("Caught permanent exception while pushing table: {} segment: {} to {}, won't retry",
                tableName, segmentName, controllerURI, e);
            throw e;
          }
        }
      });
    }
  }

  /// Metadata tar for a staged segment tar: the `<segmentName>.metadata.tar.gz` sidecar when the spec prefers it and
  /// it exists, otherwise extracted from the segment tar (downloaded first unless the filesystem is local).
  private static File getSegmentMetadataFile(SegmentGenerationJobSpec spec, PinotFS fileSystem, String tarFilePath,
      String segmentName)
      throws Exception {
    URI metadataTarGzFilePath = generateSegmentMetadataURI(tarFilePath, segmentName);
    LOGGER.info("Checking if metadata tar gz file {} exists", metadataTarGzFilePath);
    if (spec.getPushJobSpec().isPreferMetadataTarGz() && fileSystem.exists(metadataTarGzFilePath)) {
      File segmentMetadataFile = new File(FileUtils.getTempDirectory(),
          SegmentUploadConstants.SEGMENT_METADATA_DIR_PREFIX + UUID.randomUUID()
              + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
      if (segmentMetadataFile.exists()) {
        FileUtils.forceDelete(segmentMetadataFile);
      }
      fileSystem.copyToLocalFile(metadataTarGzFilePath, segmentMetadataFile);
      return segmentMetadataFile;
    }
    return generateSegmentMetadataFile(fileSystem, URI.create(tarFilePath));
  }

  /// Segments stored in the deep store do not have the .tar.gz extension.
  private static String getSegmentName(String tarFilePath) {
    String fileName = new File(tarFilePath).getName();
    String segmentName = fileName.endsWith(Constants.TAR_GZ_FILE_EXT)
        ? fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length()) : fileName;
    SegmentNameUtils.validatePartialOrFullSegmentName(segmentName);
    return segmentName;
  }

  private static String getSegmentNameFromMetadataFile(File segmentMetadataFile) {
    String fileName = segmentMetadataFile.getName();
    Preconditions.checkArgument(fileName.endsWith(Constants.METADATA_TAR_GZ_FILE_EXT),
        "Segment metadata file: %s must be named <segmentName>%s", fileName, Constants.METADATA_TAR_GZ_FILE_EXT);
    String segmentName = fileName.substring(0, fileName.length() - Constants.METADATA_TAR_GZ_FILE_EXT.length());
    SegmentNameUtils.validatePartialOrFullSegmentName(segmentName);
    return segmentName;
  }

  public static void sendSegmentsUriAndMetadata(SegmentGenerationJobSpec spec, PinotFS fileSystem,
      Map<String, String> segmentUriToTarPathMap, List<Header> headers, List<NameValuePair> parameters)
      throws Exception {
    LOGGER.info("Start pushing segment metadata: {} to locations: {} for table: {} with parallelism: {}",
        segmentUriToTarPathMap, Arrays.toString(spec.getPinotClusterSpecs()), spec.getTableSpec().getTableName(),
        spec.getPushJobSpec().getPushParallelism());
    ConcurrentHashMap<String, File> segmentMetadataFileMap = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> segmentURIs = new ConcurrentLinkedQueue<>();
    ExecutorService executor =
        Executors.newFixedThreadPool(spec.getPushJobSpec().getSegmentMetadataGenerationParallelism());
    try {
      generateSegmentMetadataFiles(spec, fileSystem, segmentUriToTarPathMap, segmentMetadataFileMap, segmentURIs,
          executor);
      pushSegmentsMetadata(spec, segmentURIs, segmentMetadataFileMap, headers, parameters);
    } finally {
      for (File segmentMetadataFile : segmentMetadataFileMap.values()) {
        FileUtils.deleteQuietly(segmentMetadataFile);
      }
      executor.shutdown();
    }
  }

  /// Batch variant of [#sendSegmentUriAndMetadata(SegmentGenerationJobSpec, Map, List, List)] for locally held
  /// metadata tars named `<segmentName>.metadata.tar.gz`. The caller owns the files.
  public static void sendSegmentsUriAndMetadata(SegmentGenerationJobSpec spec,
      Map<String, File> segmentUriToMetadataFileMap, List<Header> headers, List<NameValuePair> parameters)
      throws Exception {
    Map<String, File> segmentMetadataFileMap = new HashMap<>();
    List<String> segmentURIs = new ArrayList<>();
    for (Map.Entry<String, File> entry : segmentUriToMetadataFileMap.entrySet()) {
      String segmentName = getSegmentNameFromMetadataFile(entry.getValue());
      segmentMetadataFileMap.put(segmentName, entry.getValue());
      segmentURIs.add(segmentName);
      segmentURIs.add(entry.getKey());
    }
    pushSegmentsMetadata(spec, segmentURIs, segmentMetadataFileMap, headers, parameters);
  }

  private static void pushSegmentsMetadata(SegmentGenerationJobSpec spec, Collection<String> segmentURIs,
      Map<String, File> segmentMetadataFileMap, List<Header> headers, List<NameValuePair> parameters)
      throws Exception {
    String tableName = spec.getTableSpec().getTableName();
    FileUploadDownloadClient fileUploadDownloadClient = getOrCreateFileUploadDownloadClient(spec);
    int socketTimeoutMs = getSocketTimeoutMs(spec);
    File allSegmentsMetadataTarFile = createSegmentsMetadataTarFile(segmentURIs, segmentMetadataFileMap);
    // the key is unused in batch upload mode and hence 'noopKey'
    Map<String, File> allSegmentsMetadataMap = Map.of("noopKey", allSegmentsMetadataTarFile);
    try {
      for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
        URI controllerURI;
        try {
          controllerURI = new URI(pinotClusterSpec.getControllerURI());
        } catch (URISyntaxException e) {
          throw new RuntimeException("Got invalid controller uri: " + pinotClusterSpec.getControllerURI());
        }
        LOGGER.info("Pushing segments: {} to location: {} for table: {}",
            segmentMetadataFileMap.keySet(), controllerURI, tableName);
        int attempts = 1;
        if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushAttempts() > 0) {
          attempts = spec.getPushJobSpec().getPushAttempts();
        }
        long retryWaitMs = 1000L;
        if (spec.getPushJobSpec() != null && spec.getPushJobSpec().getPushRetryIntervalMillis() > 0) {
          retryWaitMs = spec.getPushJobSpec().getPushRetryIntervalMillis();
        }
        RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
          List<Header> reqHttpHeaders = new ArrayList<>(headers);
          try {
            addHeaders(spec, reqHttpHeaders);
            URI segmentUploadURI = getBatchSegmentUploadURI(controllerURI);
            SimpleHttpResponse response = fileUploadDownloadClient.uploadSegmentMetadataFiles(segmentUploadURI,
                allSegmentsMetadataMap, reqHttpHeaders, parameters, socketTimeoutMs);
            LOGGER.info("Response for pushing table {} segments {} to location {} - {}: {}", tableName,
                segmentMetadataFileMap.keySet(), controllerURI, response.getStatusCode(), response.getResponse());
            return true;
          } catch (HttpErrorStatusException e) {
            int statusCode = e.getStatusCode();
            if (statusCode >= 500) {
              // Temporary exception
              LOGGER.warn("Caught temporary exception while pushing table: {} segments: {} to {}, will retry",
                  tableName, segmentMetadataFileMap.keySet(), controllerURI, e);
              return false;
            } else {
              // Permanent exception
              LOGGER.error("Caught permanent exception while pushing table: {} segments: {} to {}, won't retry",
                  tableName, segmentMetadataFileMap.keySet(), controllerURI, e);
              throw e;
            }
          }
        });
      }
    } finally {
      FileUtils.deleteQuietly(allSegmentsMetadataTarFile);
      closeFileUploadDownloadClient(spec, fileUploadDownloadClient);
    }
  }

  @VisibleForTesting
  static void generateSegmentMetadataFiles(SegmentGenerationJobSpec spec, PinotFS fileSystem,
      Map<String, String> segmentUriToTarPathMap, ConcurrentHashMap<String, File> segmentMetadataFileMap,
      ConcurrentLinkedQueue<String> segmentURIs, ExecutorService executor) {

    List<Future<Void>> futures = new ArrayList<>();
    // Generate segment metadata files in parallel
    for (String segmentUriPath : segmentUriToTarPathMap.keySet()) {
      futures.add(
          executor.submit(() -> {
            String tarFilePath = segmentUriToTarPathMap.get(segmentUriPath);
            String segmentName = getSegmentName(tarFilePath);
            segmentMetadataFileMap.put(segmentName, getSegmentMetadataFile(spec, fileSystem, tarFilePath, segmentName));
            segmentURIs.add(segmentName);
            segmentURIs.add(segmentUriPath);
            return null;
          }));
    }
    int errorCount = 0;
    Exception exception = null;
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        errorCount++;
        exception = e;
      }
    }
    if (errorCount > 0) {
      throw new RuntimeException(
          String.format("%d out of %d segment metadata generation failed", errorCount, segmentUriToTarPathMap.size()),
          exception);
    }
  }

  private static URI getBatchSegmentUploadURI(URI controllerURI)
      throws URISyntaxException {
    return FileUploadDownloadClient.getBatchSegmentUploadURI(controllerURI);
  }

  private static void addHeaders(SegmentGenerationJobSpec jobSpec, List<Header> headers) {
    headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
        FileUploadDownloadClient.FileUploadType.METADATA.toString()));
    if (jobSpec.getPushJobSpec() != null) {
      headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE,
          String.valueOf(jobSpec.getPushJobSpec().getCopyToDeepStoreForMetadataPush())));
    }
  }

  private static List<NameValuePair> makeUploadSegmentParams(List<NameValuePair> parameters, String tableName,
      TableType tableType) {
    List<NameValuePair> requestParams = parameters == null ? new ArrayList<>() : new ArrayList<>(parameters);
    requestParams.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName));
    requestParams.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, tableType.name()));
    return requestParams;
  }

  // Method helps create an uber tar file which contains the metadata files for all segments that are to be uploaded.
  // Additionally, it contains a segmentName to segmentDownloadURI mapping file which allows us to avoid sending the
  // segmentDownloadURI as a header field as there are limitations on the number of headers allowed in the http request.
  @VisibleForTesting
  static File createSegmentsMetadataTarFile(Collection<String> segmentURIs, Map<String, File> segmentMetadataFileMap)
      throws IOException {
    String uuid = UUID.randomUUID().toString();
    File allSegmentsMetadataDir =
        new File(FileUtils.getTempDirectory(), SegmentUploadConstants.ALL_SEGMENTS_METADATA_DIR_PREFIX + uuid);
    FileUtils.forceMkdir(allSegmentsMetadataDir);
    for (Map.Entry<String, File> segmentMetadataTarFileEntry : segmentMetadataFileMap.entrySet()) {
      String segmentName = segmentMetadataTarFileEntry.getKey();
      File tarFile = segmentMetadataTarFileEntry.getValue();
      TarCompressionUtils.untarOneFile(tarFile, V1Constants.MetadataKeys.METADATA_FILE_NAME,
          new File(allSegmentsMetadataDir, segmentName + "." + V1Constants.MetadataKeys.METADATA_FILE_NAME));
      TarCompressionUtils.untarOneFile(tarFile, V1Constants.SEGMENT_CREATION_META,
          new File(allSegmentsMetadataDir, segmentName + "." + V1Constants.SEGMENT_CREATION_META));
    }
    File allSegmentsMetadataTarFile = new File(FileUtils.getTempDirectory(),
        SegmentUploadConstants.ALL_SEGMENTS_METADATA_TAR_FILE_PREFIX + uuid
            + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    if (allSegmentsMetadataTarFile.exists()) {
      FileUtils.forceDelete(allSegmentsMetadataTarFile);
    }
    // Add a file which contains the download URI of all the segments
    File segmentsURIFile = new File(allSegmentsMetadataDir, SegmentUploadConstants.ALL_SEGMENTS_METADATA_FILENAME);
    FileUtils.writeLines(segmentsURIFile, segmentURIs);
    try {
      TarCompressionUtils.createCompressedTarFile(allSegmentsMetadataDir, allSegmentsMetadataTarFile);
    } finally {
      FileUtils.deleteDirectory(allSegmentsMetadataDir);
    }
    return allSegmentsMetadataTarFile;
  }

  public static Map<String, String> getSegmentUriToTarPathMap(URI outputDirURI, PushJobSpec pushSpec,
      String[] files) {
    Map<String, String> segmentUriToTarPathMap = new HashMap<>();
    PathMatcher pushFilePathMatcher = null;
    if (pushSpec.getPushFileNamePattern() != null) {
      pushFilePathMatcher = FileSystems.getDefault().getPathMatcher(pushSpec.getPushFileNamePattern());
    }

    for (String file : files) {
      if (pushFilePathMatcher != null) {
        if (!pushFilePathMatcher.matches(Paths.get(file))) {
          continue;
        }
      }

      URI uri = URI.create(file);
      if (uri.getPath().endsWith(Constants.METADATA_TAR_GZ_FILE_EXT)) {
        // Skip segment metadata tar gz files
        continue;
      }
      if (uri.getPath().endsWith(Constants.TAR_GZ_FILE_EXT)) {
        URI updatedURI = SegmentPushUtils.generateSegmentTarURI(outputDirURI, uri, pushSpec.getSegmentUriPrefix(),
            pushSpec.getSegmentUriSuffix());
        segmentUriToTarPathMap.put(updatedURI.toString(), file);
      }
    }
    return segmentUriToTarPathMap;
  }

  /// Generate a segment metadata only tar file, which contains only metadata.properties and creation.meta file.
  /// The purpose of this is to create a lean tar to push to Pinot controller for adding segments without downloading
  /// the complete segment and untar the segment tarball.
  ///
  /// 1. Download segment tar file to temp dir;
  /// 2. Extract only metadata.properties and creation.meta files from the segment tar file;
  /// 3. Tar both files into a segment metadata file.
  public static File generateSegmentMetadataFile(PinotFS fileSystem, URI tarFileURI)
      throws Exception {
    String uuid = UUID.randomUUID().toString();
    File tarFile =
        new File(FileUtils.getTempDirectory(), "segmentTar-" + uuid + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    File segmentMetadataDir = new File(FileUtils.getTempDirectory(), "segmentMetadataDir-" + uuid);
    try {
      if (fileSystem instanceof LocalPinotFS) {
        // For local file system, we don't need to copy the tar file.
        tarFile = new File(URIUtils.decode(tarFileURI.getRawPath()));
      } else {
        // For other file systems, we need to download the file to local file system
        fileSystem.copyToLocalFile(tarFileURI, tarFile);
      }
      if (segmentMetadataDir.exists()) {
        FileUtils.forceDelete(segmentMetadataDir);
      }
      FileUtils.forceMkdir(segmentMetadataDir);

      // Extract metadata.properties
      LOGGER.info("Trying to untar Metadata file from: [{}] to [{}]", tarFile, segmentMetadataDir);
      TarCompressionUtils.untarOneFile(tarFile, V1Constants.MetadataKeys.METADATA_FILE_NAME,
          new File(segmentMetadataDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

      // Extract creation.meta
      LOGGER.info("Trying to untar CreationMeta file from: [{}] to [{}]", tarFile, segmentMetadataDir);
      TarCompressionUtils.untarOneFile(tarFile, V1Constants.SEGMENT_CREATION_META,
          new File(segmentMetadataDir, V1Constants.SEGMENT_CREATION_META));

      File segmentMetadataTarFile = new File(FileUtils.getTempDirectory(),
          "segmentMetadata-" + uuid + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
      if (segmentMetadataTarFile.exists()) {
        FileUtils.forceDelete(segmentMetadataTarFile);
      }
      LOGGER.info("Trying to tar segment metadata dir [{}] to [{}]", segmentMetadataDir, segmentMetadataTarFile);
      TarCompressionUtils.createCompressedTarFile(segmentMetadataDir, segmentMetadataTarFile);
      return segmentMetadataTarFile;
    } finally {
      if (!(fileSystem instanceof LocalPinotFS)) {
        // For local file system, we don't need to delete the tar file.
        FileUtils.deleteQuietly(tarFile);
      }
      FileUtils.deleteQuietly(segmentMetadataDir);
    }
  }

  /// Builds the metadata-only tar a METADATA push sends (`metadata.properties` and `creation.meta`) from a local
  /// segment directory, named `<segmentName>.metadata.tar.gz`, so the segment tar never has to be downloaded back.
  public static File generateSegmentMetadataFile(File segmentDir, File outputDir, String segmentName)
      throws IOException {
    File metadataDir = new File(outputDir, segmentName + "-metadata");
    File metadataTarFile = new File(outputDir, segmentName + Constants.METADATA_TAR_GZ_FILE_EXT);
    try {
      FileUtils.forceMkdir(metadataDir);
      FileUtils.copyFileToDirectory(SegmentDirectoryPaths.findMetadataFile(segmentDir), metadataDir);
      FileUtils.copyFileToDirectory(SegmentDirectoryPaths.findCreationMetaFile(segmentDir), metadataDir);
      TarCompressionUtils.createCompressedTarFile(metadataDir, metadataTarFile);
      return metadataTarFile;
    } finally {
      FileUtils.deleteQuietly(metadataDir);
    }
  }

  public static URI generateSegmentMetadataURI(String segmentTarPath, String segmentName)
      throws URISyntaxException {
    URI segmentTarURI = URI.create(segmentTarPath);
    URI metadataTarGzFilePath = new URI(
        segmentTarURI.getScheme(),
        segmentTarURI.getUserInfo(),
        segmentTarURI.getHost(),
        segmentTarURI.getPort(),
        new File(segmentTarURI.getPath()).getParentFile() + File.separator + segmentName
            + Constants.METADATA_TAR_GZ_FILE_EXT,
        segmentTarURI.getQuery(),
        segmentTarURI.getFragment());
    return metadataTarGzFilePath;
  }
}
