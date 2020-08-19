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
package org.apache.pinot.plugin.ingestion.batch.common;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPushUtils implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPushUtils.class);

  private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

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
        LOGGER.warn("Unable to generate push uri based from dir URI: {} and file URI: {}, directly return file URI.", dirURI, fileURI);
        return fileURI;
      }
    }
    return URI.create(String.format("%s%s%s", prefix, fileURI.getRawPath(), suffix));
  }

  public static void pushSegments(SegmentGenerationJobSpec spec, PinotFS fileSystem, List<String> tarFilePaths)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = spec.getTableSpec().getTableName();
    LOGGER.info("Start pushing segments: {}... to locations: {} for table {}",
        Arrays.toString(tarFilePaths.subList(0, Math.min(5, tarFilePaths.size())).toArray()),
        Arrays.toString(spec.getPinotClusterSpecs()), tableName);
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
            SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT
                .uploadSegment(FileUploadDownloadClient.getUploadSegmentURI(controllerURI), segmentName, inputStream,
                    tableName);
            LOGGER.info("Response for pushing table {} segment {} to location {} - {}: {}", tableName, segmentName,
                controllerURI, response.getStatusCode(), response.getResponse());
            return true;
          } catch (HttpErrorStatusException e) {
            int statusCode = e.getStatusCode();
            if (statusCode >= 500) {
              // Temporary exception
              LOGGER.warn("Caught temporary exception while pushing table: {} segment: {} to {}, will retry", tableName,
                  segmentName, controllerURI, e);
              return false;
            } else {
              // Permanent exception
              LOGGER
                  .error("Caught permanent exception while pushing table: {} segment: {} to {}, won't retry", tableName,
                      segmentName, controllerURI, e);
              throw e;
            }
          }
        });
      }
    }
  }

  public static void sendSegmentUris(SegmentGenerationJobSpec spec, List<String> segmentUris)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = spec.getTableSpec().getTableName();
    LOGGER.info("Start sending table {} segment URIs: {} to locations: {}", tableName,
        Arrays.toString(segmentUris.subList(0, Math.min(5, segmentUris.size())).toArray()),
        Arrays.toString(spec.getPinotClusterSpecs()));
    for (String segmentUri : segmentUris) {
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
            SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT
                .sendSegmentUri(FileUploadDownloadClient.getUploadSegmentURI(controllerURI), segmentUri, tableName);
            LOGGER.info("Response for pushing table {} segment uri {} to location {} - {}: {}", tableName, segmentUri,
                controllerURI, response.getStatusCode(), response.getResponse());
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
          }
        });
      }
    }
  }
}
