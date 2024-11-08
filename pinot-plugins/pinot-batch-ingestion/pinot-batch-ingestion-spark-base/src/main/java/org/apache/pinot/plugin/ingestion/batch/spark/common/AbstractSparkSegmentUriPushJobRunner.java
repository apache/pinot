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
package org.apache.pinot.plugin.ingestion.batch.spark.common;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


public abstract class AbstractSparkSegmentUriPushJobRunner implements IngestionJobRunner, Serializable {

  protected SegmentGenerationJobSpec _spec;

  public AbstractSparkSegmentUriPushJobRunner() {
  }

  public AbstractSparkSegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
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
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
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
        URI updatedURI =
            SegmentPushUtils.generateSegmentTarURI(outputDirURI, uri, _spec.getPushJobSpec().getSegmentUriPrefix(),
                _spec.getPushJobSpec().getSegmentUriSuffix());
        segmentUris.add(updatedURI.toString());
      }
    }

    int pushParallelism = _spec.getPushJobSpec().getPushParallelism();
    if (pushParallelism < 1) {
      pushParallelism = segmentUris.size();
    }
    if (pushParallelism == 1) {
      // Push from driver
      try {
        SegmentPushUtils.sendSegmentUris(_spec, segmentUris);
      } catch (RetriableOperationException | AttemptsExceededException e) {
        throw new RuntimeException(e);
      }
    } else {
      parallelizePushJob(pinotFSSpecs, segmentUris, pushParallelism);
    }
  }

  /**
   * Parallelizes the push job using Spark to distribute the work across multiple nodes.
   *
   * @param pinotFSSpecs the list of Pinot file system specifications to be registered
   * @param segmentUris the list of segment URIs to be pushed
   * @param pushParallelism the level of parallelism for the push job
   */
  public abstract void parallelizePushJob(List<PinotFSSpec> pinotFSSpecs,
      List<String> segmentUris, int pushParallelism);
}
