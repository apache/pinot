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
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
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

  public void run()
      throws Exception {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      Configuration config = new MapConfiguration(pinotFSSpec.getConfigs());
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), config);
    }

    //Get outputFS for writing output pinot segments
    URI outputDirURI = new URI(_spec.getOutputDirURI());
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());
    outputDirFS.mkdir(outputDirURI);

    //Get list of files to process
    String[] files = outputDirFS.listFiles(outputDirURI, true);

    List<String> segmentsToPush = new ArrayList<>();
    for (String file : files) {
      if (file.endsWith(Constants.TAR_GZ_FILE_EXT)) {
        segmentsToPush.add(file);
      }
    }
    pushSegments(outputDirFS, segmentsToPush);
  }

  public void pushSegments(PinotFS fileSystem, List<String> tarFilePaths)
      throws RetriableOperationException, AttemptsExceededException {
    LOGGER.info("Start pushing segments: {} to locations: {}", tarFilePaths,
        Arrays.toString(_spec.getPinotClusterSpecs()));
    FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient();
    for (String tarFilePath : tarFilePaths) {
      URI tarFileURI = URI.create(tarFilePath);
      File tarFile = new File(tarFilePath);
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
      for (PinotClusterSpec pinotClusterSpec : _spec.getPinotClusterSpecs()) {
        LOGGER.info("Pushing segment: {} to location: {}", segmentName, pinotClusterSpec.getControllerURI());
        int retryCount = 1;
        if (_spec.getPushJobSpec() != null && _spec.getPushJobSpec().getRetryCount() > 0) {
          _spec.getPushJobSpec().getRetryCount();
        }
        long retryWaitMs = 1000L;
        if (_spec.getPushJobSpec() != null && _spec.getPushJobSpec().getRetryWaitMs() > 0) {
          retryWaitMs = _spec.getPushJobSpec().getRetryWaitMs();
        }
        RetryPolicies.exponentialBackoffRetryPolicy(retryCount, retryWaitMs, 5).attempt(() -> {
          try (InputStream inputStream = fileSystem.open(tarFileURI)) {
            SimpleHttpResponse response = fileUploadDownloadClient
                .uploadSegment(URI.create(pinotClusterSpec.getControllerURI()), segmentName, inputStream,
                    _spec.getTableSpec().getTableName());
            LOGGER.info("Response {}: {}", response.getStatusCode(), response.getResponse());
            return true;
          } catch (Exception e) {
            LOGGER.error("Caught exception while pushing segment: {} to location: {}", segmentName,
                pinotClusterSpec.getControllerURI(), e);
            return false;
          }
        });
      }
    }
  }
}
