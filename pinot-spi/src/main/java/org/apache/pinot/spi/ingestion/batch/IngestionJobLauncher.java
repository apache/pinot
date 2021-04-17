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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class IngestionJobLauncher {

  public static final Logger LOGGER = LoggerFactory.getLogger(IngestionJobLauncher.class);
  public static final String JOB_SPEC_FORMAT = "job-spec-format";
  public static final String JSON = "json";
  public static final String YAML = "yaml";

  public static SegmentGenerationJobSpec getSegmentGenerationJobSpec(String jobSpecFilePath, String propertyFilePath,
      Map<String, Object> context) {
    Properties properties = new Properties();
    if (propertyFilePath != null) {
      try {
        properties.load(FileUtils.openInputStream(new File(propertyFilePath)));
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Unable to read property file [%s] into properties.", propertyFilePath), e);
      }
    }
    Map<String, Object> propertiesMap = (Map) properties;
    if (context != null) {
      propertiesMap.putAll(context);
    }
    String jobSpecTemplate;
    try {
      jobSpecTemplate = IOUtils.toString(new BufferedReader(new FileReader(jobSpecFilePath)));
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to read ingestion job spec file [%s].", jobSpecFilePath), e);
    }
    String jobSpecStr;
    try {
      jobSpecStr = GroovyTemplateUtils.renderTemplate(jobSpecTemplate, propertiesMap);
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Unable to render templates on ingestion job spec template file - [%s] with propertiesMap - [%s].",
          jobSpecFilePath, Arrays.toString(propertiesMap.entrySet().toArray())), e);
    }

    String jobSpecFormat = (String) propertiesMap.getOrDefault(JOB_SPEC_FORMAT, YAML);
    if (jobSpecFormat.equals(JSON)) {
      try {
        return JsonUtils.stringToObject(jobSpecStr, SegmentGenerationJobSpec.class);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Unable to parse job spec - [%s] to JSON with propertiesMap - [%s]",
            jobSpecFilePath, Arrays.toString(propertiesMap.entrySet().toArray())), e);
      }
    }

    return new Yaml().loadAs(jobSpecStr, SegmentGenerationJobSpec.class);
  }

  public static void runIngestionJob(SegmentGenerationJobSpec spec) {
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
      case SegmentMetadataPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentMetadataPushJobRunnerClassName());
        break;
      case SegmentCreationAndTarPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassName());
        kickoffIngestionJob(spec, executionFramework.getSegmentTarPushJobRunnerClassName());
        break;
      case SegmentCreationAndUriPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassName());
        kickoffIngestionJob(spec, executionFramework.getSegmentUriPushJobRunnerClassName());
        break;
      case SegmentCreationAndMetadataPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassName());
        kickoffIngestionJob(spec, executionFramework.getSegmentMetadataPushJobRunnerClassName());
        break;
      default:
        LOGGER.error("Unsupported job type - {}. Support job types: {}", spec.getJobType(),
            Arrays.toString(PinotIngestionJobType.values()));
        throw new RuntimeException("Unsupported job type - " + spec.getJobType());
    }
  }

  private static void kickoffIngestionJob(SegmentGenerationJobSpec spec, String ingestionJobRunnerClassName) {
    LOGGER.info("Trying to create instance for class {}", ingestionJobRunnerClassName);
    IngestionJobRunner ingestionJobRunner;
    try {
      ingestionJobRunner = PluginManager.get().createInstance(ingestionJobRunnerClassName);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create IngestionJobRunner instance for class - " + ingestionJobRunnerClassName, e);
    }
    ingestionJobRunner.init(spec);
    try {
      ingestionJobRunner.run();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception during running - " + ingestionJobRunnerClassName, e);
    }
  }

  enum PinotIngestionJobType {
    SegmentCreation,
    SegmentTarPush,
    SegmentUriPush,
    SegmentMetadataPush,
    SegmentCreationAndTarPush,
    SegmentCreationAndUriPush,
    SegmentCreationAndMetadataPush,
  }
}
