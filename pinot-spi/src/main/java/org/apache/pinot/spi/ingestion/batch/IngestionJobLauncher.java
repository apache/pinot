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

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
  private IngestionJobLauncher() {
  }

  public static final Logger LOGGER = LoggerFactory.getLogger(IngestionJobLauncher.class);
  public static final String JOB_SPEC_FORMAT = "job-spec-format";
  public static final String JSON = "json";
  public static final String YAML = "yaml";

  public static SegmentGenerationJobSpec getSegmentGenerationJobSpec(String jobSpecFilePath, String propertyFilePath,
      Map<String, Object> context, Map<String, String> environmentValues) {
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
    if (environmentValues != null) {
      for (String propertyName: propertiesMap.keySet()) {
        if (environmentValues.get(propertyName) != null) {
          propertiesMap.put(propertyName, environmentValues.get(propertyName));
        }
      }
    }
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
      throw new RuntimeException(String
          .format("Unable to render templates on ingestion job spec template file - [%s] with propertiesMap - [%s].",
              jobSpecFilePath, Arrays.toString(propertiesMap.entrySet().toArray())), e);
    }

    String jobSpecFormat = (String) propertiesMap.getOrDefault(JOB_SPEC_FORMAT, YAML);
    if (jobSpecFormat.equals(JSON)) {
      try {
        return JsonUtils.stringToObject(jobSpecStr, SegmentGenerationJobSpec.class);
      } catch (IOException e) {
        throw new RuntimeException(String
            .format("Unable to parse job spec - [%s] to JSON with propertiesMap - [%s]", jobSpecFilePath,
                Arrays.toString(propertiesMap.entrySet().toArray())), e);
      }
    }

    return new Yaml().loadAs(jobSpecStr, SegmentGenerationJobSpec.class);
  }

  public static void runIngestionJob(SegmentGenerationJobSpec spec) {
    LOGGER.info("SegmentGenerationJobSpec: \n{}", spec.toJSONString(true));
    ExecutionFrameworkSpec executionFramework = spec.getExecutionFrameworkSpec();
    PinotIngestionJobType jobType = PinotIngestionJobType.fromString(spec.getJobType());
    switch (jobType) {
      case SegmentCreation:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassNameNotNull());
        break;
      case SegmentTarPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentTarPushJobRunnerClassNameNotNull());
        break;
      case SegmentUriPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentUriPushJobRunnerClassNameNotNull());
        break;
      case SegmentMetadataPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentMetadataPushJobRunnerClassNameNotNull());
        break;
      case SegmentCreationAndTarPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassNameNotNull());
        kickoffIngestionJob(spec, executionFramework.getSegmentTarPushJobRunnerClassNameNotNull());
        break;
      case SegmentCreationAndUriPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassNameNotNull());
        kickoffIngestionJob(spec, executionFramework.getSegmentUriPushJobRunnerClassNameNotNull());
        break;
      case SegmentCreationAndMetadataPush:
        kickoffIngestionJob(spec, executionFramework.getSegmentGenerationJobRunnerClassNameNotNull());
        kickoffIngestionJob(spec, executionFramework.getSegmentMetadataPushJobRunnerClassNameNotNull());

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

  /**
   * Ingestion Job type Enum.
   */
  public enum PinotIngestionJobType {
    SegmentCreation,
    SegmentTarPush,
    SegmentUriPush,
    SegmentMetadataPush,
    SegmentCreationAndTarPush,
    SegmentCreationAndUriPush,
    SegmentCreationAndMetadataPush;

    private static final Map<String, PinotIngestionJobType> VALUE_MAP = new HashMap<>();

    static {
      for (PinotIngestionJobType jobType : PinotIngestionJobType.values()) {
        // Use case-insensitive naming.
        VALUE_MAP.put(jobType.name().toLowerCase(), jobType);
      }
    }

    @JsonCreator
    public static PinotIngestionJobType fromString(String name) {
      PinotIngestionJobType jobType = VALUE_MAP.get(name.toLowerCase());

      if (jobType == null) {
        throw new IllegalArgumentException("No enum constant for: " + name);
      }
      return jobType;
    }
  }
}
