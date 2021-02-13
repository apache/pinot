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
package org.apache.pinot.core.util.validator;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.LogLevel;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.utils.JsonUtils;

/**
* Base implementation for validating JSON schemas
* */
abstract public class BasePinotConfigValidator implements PinotConfigValidator {

  protected JsonSchema _schema;
  protected ProcessingReport _report;

  /**
   * Load schema from project resources
   * */
  protected void loadSchema(String path)
      throws IOException, ProcessingException {
    JsonNode node = JsonLoader.fromResource(path);
    JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    _schema = factory.getJsonSchema(node);
  }

  /**
  * Validate passed jsonStr schema against the loaded schema from resources
  * */
  @Override
  public boolean validate(String jsonStr)
      throws ProcessingException {
    JsonNode node = null;
    try {
      node = JsonUtils.stringToJsonNode(jsonStr);
    } catch (IOException e) {
      e.printStackTrace();
    }
    _report = _schema.validate(node);
    return _report.isSuccess();
  }

  /**
   * Report validation messages if validation fail
   * */
  @Override
  public List<String> getValidationMessages() {
    List<String> messages = new ArrayList<>();
    for (ProcessingMessage processingMessage : _report) {
      if (processingMessage.getLogLevel() == LogLevel.ERROR)
        messages.add(processingMessage.getMessage());
    }
    return messages;
  }
}
