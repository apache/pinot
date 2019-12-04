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
package org.apache.pinot.ingestion.standalone;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.retry.AttemptsExceededException;
import org.apache.pinot.common.utils.retry.RetriableOperationException;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.ingestion.common.Constants;
import org.apache.pinot.ingestion.common.PinotClusterSpec;
import org.apache.pinot.ingestion.common.PinotFSSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationJobSpec;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentTarPushJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTarPushJobRunner.class);

  private SegmentGenerationJobSpec _spec;

  public SegmentTarPushJobRunner(SegmentGenerationJobSpec spec) {
    _spec = spec;
  }

  public void run() {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      Configuration config = new MapConfiguration(pinotFSSpec.getConfigs());
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), config);
    }

    //Get outputFS for writing output pinot segments
    URI outputDirURI;
    try {
      outputDirURI = new URI(_spec.getOutputDirURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException("outputDirURI is not valid - '" + _spec.getOutputDirURI() + "'");
    }
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());
    //Get list of files to process
    String[] files;
    try {
      files = outputDirFS.listFiles(outputDirURI, true);
    } catch (IOException e) {
      throw new RuntimeException("Unable to list all files under outputDirURI - '" + outputDirURI + "'");
    }

    List<String> segmentsToPush = new ArrayList<>();
    for (String file : files) {
      if (file.endsWith(Constants.TAR_GZ_FILE_EXT)) {
        segmentsToPush.add(file);
      }
    }
    try {
      pushSegments(outputDirFS, segmentsToPush);
    } catch (RetriableOperationException | AttemptsExceededException e) {
      throw new RuntimeException(e);
    }
  }

  public void pushSegments(PinotFS fileSystem, List<String> tarFilePaths)
      throws RetriableOperationException, AttemptsExceededException {
    String tableName = _spec.getTableSpec().getTableName();
    LOGGER.info("Start pushing segments: {}... to locations: {} for table {}",
        Arrays.toString(tarFilePaths.subList(0, Math.min(5, tarFilePaths.size())).toArray()),
        Arrays.toString(_spec.getPinotClusterSpecs()), tableName);
    FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient();
    for (String tarFilePath : tarFilePaths) {
      URI tarFileURI = URI.create(tarFilePath);
      File tarFile = new File(tarFilePath);
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
      for (PinotClusterSpec pinotClusterSpec : _spec.getPinotClusterSpecs()) {
        URI controllerURI;
        try {
          controllerURI = new URI(pinotClusterSpec.getControllerURI());
        } catch (URISyntaxException e) {
          throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
        }
        LOGGER.info("Pushing segment: {} to location: {} for table {}", segmentName, controllerURI, tableName);
        int attempts = 1;
        if (_spec.getPushJobSpec() != null && _spec.getPushJobSpec().getPushAttempts() > 0) {
          _spec.getPushJobSpec().getPushAttempts();
        }
        long retryWaitMs = 1000L;
        if (_spec.getPushJobSpec() != null && _spec.getPushJobSpec().getPushRetryIntervalMillis() > 0) {
          retryWaitMs = _spec.getPushJobSpec().getPushRetryIntervalMillis();
        }
        RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
          try (InputStream inputStream = fileSystem.open(tarFileURI)) {
            SimpleHttpResponse response =
                fileUploadDownloadClient.uploadSegment(controllerURI, segmentName, inputStream, tableName);
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
}
