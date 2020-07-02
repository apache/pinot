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
package org.apache.pinot.plugin.ingestion.batch.standalone;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentPushUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentUriPushJobRunner implements IngestionJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUriPushJobRunner.class);

  private SegmentGenerationJobSpec _spec;

  public SegmentUriPushJobRunner() {
  }

  public SegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;
    if (_spec.getPushJobSpec() == null) {
      throw new RuntimeException("Missing PushJobSpec");
    }
  }

  @Override
  public void run() {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      Configuration config = new MapConfiguration(pinotFSSpec.getConfigs());
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), config);
    }

    //Get outputFS for writing output Pinot segments
    URI outputDirURI;
    try {
      outputDirURI = new URI(_spec.getOutputDirURI());
      if (outputDirURI.getScheme() == null) {
        outputDirURI = new File(_spec.getOutputDirURI()).toURI();
      }
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
    List<String> segmentUris = new ArrayList<>();
    for (String file : files) {
      URI uri = URI.create(file);
      if (uri.getPath().endsWith(Constants.TAR_GZ_FILE_EXT)) {
        segmentUris.add(_spec.getPushJobSpec().getSegmentUriPrefix() + uri.getRawPath() + _spec.getPushJobSpec()
            .getSegmentUriSuffix());
      }
    }
    try {
      SegmentPushUtils.sendSegmentUris(_spec, segmentUris);
    } catch (RetriableOperationException | AttemptsExceededException e) {
      throw new RuntimeException(e);
    }
  }
}
