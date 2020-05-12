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
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class IngestionJobLauncher {

  public static final Logger LOGGER = LoggerFactory.getLogger(IngestionJobLauncher.class);

  private static final String USAGE = "usage: [jobSpec.yaml] [template_key=template_value]...";

  private static void usage() {
    System.err.println(USAGE);
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length < 1) {
      usage();
      System.exit(1);
    }
    String jobSpecFilePath = args[0];
    List<String> valueList = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      valueList.add(args[i]);
    }
    SegmentGenerationJobSpec spec =
        getSegmentGenerationJobSpec(jobSpecFilePath, GroovyTemplateUtils.getTemplateContext(valueList));
    runIngestionJob(spec);
  }

  public static SegmentGenerationJobSpec getSegmentGenerationJobSpec(String jobSpecFilePath,
      Map<String, Object> context)
      throws IOException, ClassNotFoundException {
    String yamlTemplate = IOUtils.toString(new BufferedReader(new FileReader(jobSpecFilePath)));
    String yamlStr = GroovyTemplateUtils.renderTemplate(yamlTemplate, context);
    return new Yaml().loadAs(yamlStr, SegmentGenerationJobSpec.class);
  }

  public static void runIngestionJob(SegmentGenerationJobSpec spec)
      throws Exception {
    StringWriter sw = new StringWriter();
    new Yaml().dump(spec, sw);
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

  private static void kickoffIngestionJob(SegmentGenerationJobSpec spec, String ingestionJobRunnerClassName)
      throws Exception {
    LOGGER.info("Trying to create instance for class {}", ingestionJobRunnerClassName);
    IngestionJobRunner ingestionJobRunner = PluginManager.get().createInstance(ingestionJobRunnerClassName);
    ingestionJobRunner.init(spec);
    ingestionJobRunner.run();
  }

  enum PinotIngestionJobType {
    SegmentCreation, SegmentTarPush, SegmentUriPush, SegmentCreationAndTarPush, SegmentCreationAndUriPush,
  }
}
