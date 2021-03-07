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
package org.apache.pinot.core.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.LogLevel;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.utils.JsonUtils;

/**
* Validating JSON schema for [TableConfig,]
* */
public class PinotConfigSchemaValidator {

  private static final String TABLE_CONFIG_SCHEMA = "/schemas/tableConfig.json";
  private JsonSchema _schema;

  private PinotConfigSchemaValidator(String path)
      throws IOException, ProcessingException {
    loadSchema(path);
  }

  public static PinotConfigSchemaValidator forTableConfig()
      throws IOException, ProcessingException {
    return new PinotConfigSchemaValidator(TABLE_CONFIG_SCHEMA);
  }

  /**
   * Load schema from project resources
   * */
  private void loadSchema(String path)
      throws IOException, ProcessingException {
    JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    _schema = factory.getJsonSchema(JsonLoader.fromResource(path));
  }

  /**
  * Validate passed jsonStr schema against the loaded schema from resources
  * */
  public List<String> validate(String jsonStr)
      throws IOException, ProcessingException {
    JsonNode node = JsonUtils.stringToJsonNode(jsonStr);
    ProcessingReport report = _schema.validate(node);

    List<String> messages = new ArrayList<>();
    for (ProcessingMessage processingMessage : report) {
      if (processingMessage.getLogLevel() == LogLevel.ERROR)
        messages.add(processingMessage.getMessage());
    }

    return messages;
  }
}
