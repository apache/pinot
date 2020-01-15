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
package org.apache.pinot.spi.ingestion.batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Arrays;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class IngestionJobLauncher {

  public static final Logger LOGGER = LoggerFactory.getLogger(IngestionJobLauncher.class);

  private static final String USAGE = "usage: [jobSpec.yaml]";

  private static void usage() {
    System.err.println(USAGE);
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 1) {
      usage();
      System.exit(1);
    }
    String jobSpecFilePath = args[0];

    try (Reader reader = new BufferedReader(new FileReader(jobSpecFilePath))) {
      Yaml yaml = new Yaml();
      SegmentGenerationJobSpec spec = yaml.loadAs(reader, SegmentGenerationJobSpec.class);
      StringWriter sw = new StringWriter();
      yaml.dump(spec, sw);
      LOGGER.info("SegmentGenerationJobSpec: \n{}", sw.toString());

      ExecutionFrameworkSpec executionFramework = spec.getExecutionFrameworkSpec();
      PinotIngestionJobType jobType = PinotIngestionJobType.valueOf(spec.getJobType());
      switch (jobType) {
        case SegmentCreation:
          kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassName());
          break;
        case SegmentTarPush:
          kickoffIngestionJob(spec, executionFramework.getSegmentTarPushJobRunnerClassName());
          break;
        case SegmentUriPush:
          kickoffIngestionJob(spec, executionFramework.getSegmentUriPushJobRunnerClassName());
          break;
        case SegmentCreationAndTarPush:
          kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassName());
          kickoffIngestionJob(spec, executionFramework.getSegmentTarPushJobRunnerClassName());
          break;
        case SegmentCreationAndUriPush:
          kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassName());
          kickoffIngestionJob(spec, executionFramework.getSegmentUriPushJobRunnerClassName());
          break;
        default:
          LOGGER.error("Unsupported job type - {}. Support job types: {}", spec.getJobType(),
              Arrays.toString(PinotIngestionJobType.values()));
          throw new RuntimeException("Unsupported job type - " + spec.getJobType());
      }
    }
  }

  private static void kickoffIngestionJob(SegmentGenerationJobSpec spec, String ingestionJobRunnerClassName)
      throws Exception {
    IngestionJobRunner ingestionJobRunner =
        PluginManager.get().createInstance(ingestionJobRunnerClassName);
    ingestionJobRunner.init(spec);
    ingestionJobRunner.run();
  }

  enum PinotIngestionJobType {
    SegmentCreation, SegmentTarPush, SegmentUriPush, SegmentCreationAndTarPush, SegmentCreationAndUriPush,
  }
}
