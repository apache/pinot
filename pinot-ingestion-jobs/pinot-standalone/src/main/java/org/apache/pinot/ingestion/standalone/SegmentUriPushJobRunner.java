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
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.DefaultControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.common.PinotClusterSpec;
import org.apache.pinot.ingestion.common.PinotFSSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationJobSpec;
import org.apache.pinot.ingestion.utils.PushLocation;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentUriPushJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUriPushJobRunner.class);

  private SegmentGenerationJobSpec _spec;

  public SegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
    _spec = spec;
    if (_spec.getUriPushJobSpec() == null) {
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

    //Get outputFS for writing output pinot segments
    URI outputDirURI = new URI(_spec.getOutputDirURI());
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());
    outputDirFS.mkdir(outputDirURI);

    //Get list of files to process
    String[] files = outputDirFS.listFiles(outputDirURI, true);

    List<String> segmentUris = new ArrayList<>();
    for (String file : files) {
      URI uri = URI.create(file);
      if (uri.getPath().endsWith(JobConfigConstants.TAR_GZ_FILE_EXT)) {
        segmentUris.add(_spec.getUriPushJobSpec().getSegmentUriPrefix() + uri.getRawPath() + _spec.getUriPushJobSpec()
            .getSegmentUriSuffix());
      }
    }
    ControllerRestApi controllerRestApi = getControllerRestApi();
    controllerRestApi.sendSegmentUris(segmentUris);
  }

  protected ControllerRestApi getControllerRestApi() {
    List<PushLocation> pushLocations = new ArrayList<>();
    for (PinotClusterSpec pinotClusterSpec : _spec.getPinotClusterSpecs()) {
      pushLocations.add(new PushLocation(pinotClusterSpec.getHost(), pinotClusterSpec.getPort()));
    }
    return new DefaultControllerRestApi(pushLocations, _spec.getTableSpec().getTableName());
  }
}
