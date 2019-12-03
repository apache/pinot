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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.ingestion.common.Constants;
import org.apache.pinot.ingestion.common.PinotClusterSpec;
import org.apache.pinot.ingestion.common.PinotFSSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationJobSpec;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentUriPushJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUriPushJobRunner.class);

  private SegmentGenerationJobSpec _spec;

  public SegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
    _spec = spec;
    if (_spec.getPushJobSpec() == null) {
      throw new RuntimeException("Missing PushJobSpec");
    }
  }

  public void run()
      throws Exception {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      Configuration config = new MapConfiguration(pinotFSSpec.getConfigs());
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), config);
    }

    //Get outputFS for writing output Pinot segments
    URI outputDirURI = new URI(_spec.getOutputDirURI());
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());
    outputDirFS.mkdir(outputDirURI);

    //Get list of files to process
    String[] files = outputDirFS.listFiles(outputDirURI, true);

    List<String> segmentUris = new ArrayList<>();
    for (String file : files) {
      URI uri = URI.create(file);
      if (uri.getPath().endsWith(Constants.TAR_GZ_FILE_EXT)) {
        segmentUris.add(_spec.getPushJobSpec().getSegmentUriPrefix() + uri.getRawPath() + _spec.getPushJobSpec()
            .getSegmentUriSuffix());
      }
    }
    sendSegmentUris(segmentUris);
  }

  public void sendSegmentUris(List<String> segmentUris) {
    LOGGER.info("Start sending segment URIs: {} to locations: {}", segmentUris,
        Arrays.toString(_spec.getPinotClusterSpecs()));
    FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient();
    for (String segmentUri : segmentUris) {
      for (PinotClusterSpec pinotClusterSpec : _spec.getPinotClusterSpecs()) {
        LOGGER.info("Sending segment URI: {} to location: {}", segmentUri, pinotClusterSpec.getControllerURI());
        try {
          SimpleHttpResponse response = fileUploadDownloadClient
              .sendSegmentUri(URI.create(pinotClusterSpec.getControllerURI()), segmentUri,
                  _spec.getTableSpec().getTableName());
          LOGGER.info("Response {}: {}", response.getStatusCode(), response.getResponse());
        } catch (Exception e) {
          LOGGER.error("Caught exception while sending segment URI: {} to location: {}", segmentUri,
              pinotClusterSpec.getControllerURI(), e);
          throw new RuntimeException(e);
        }
      }
    }
  }
}
