/*
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

package org.apache.pinot.thirdeye.detection.validators;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.swagger.util.Json;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.yaml.snakeyaml.Yaml;


/**
 * Validate a user supplied config like detection or subscription group config
 */
public abstract class ThirdEyeUserConfigValidator<T extends AbstractDTO> implements ConfigValidator<T> {

  public static final String USER_CONFIG_VALIDATION_FAILED_KEY = "Validation errors: ";
  protected static final String PROP_DISABLE_VALD = "disableValidation";

  private final JsonSchema schema;

  public ThirdEyeUserConfigValidator(String schemaPath) {
    final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    try {
      this.schema = factory.getJsonSchema(JsonLoader.fromResource(schemaPath));
    } catch (Exception e) {
      throw new RuntimeException("Unable to load the schema file", e);
    }
  }

  /**
   * Given a yaml configuration, validate it against the schema
   *
   * @param config the yaml configuration to validate
   * @throws ConfigValidationException configuration doesn't conform to the schema
   */
  protected void schemaValidation(String config) throws ConfigValidationException {
    ProcessingReport report;
    try {
      Map<String, Object> detectionConfigMap = ConfigUtils.getMap(new Yaml().load(config));
      JsonNode configNode = Json.mapper().convertValue(detectionConfigMap, JsonNode.class);
      report = this.schema.validate(configNode);
    } catch (ProcessingException e) {
      throw new RuntimeException("Unable to run schema validation on the config", e);
    }

    if (!report.isSuccess()) {
      throw new ConfigValidationException(report);
    }
  }
}
