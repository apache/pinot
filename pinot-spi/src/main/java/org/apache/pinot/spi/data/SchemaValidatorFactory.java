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
package org.apache.pinot.spi.data;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.plugin.PluginManager;


public class SchemaValidatorFactory {
  private SchemaValidatorFactory() {
  }

  private static final Map<String, String> DEFAULT_RECORD_READER_TO_SCHEMA_VALIDATOR_MAP = new HashMap<>();

  private static final String DEFAULT_AVRO_RECORD_READER_CLASS = "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader";

  private static final String DEFAULT_AVRO_SCHEMA_VALIDATOR_CLASS = "org.apache.pinot.plugin.inputformat.avro.AvroIngestionSchemaValidator";

  //TODO: support schema validation for more data formats like ORC.

  static {
    DEFAULT_RECORD_READER_TO_SCHEMA_VALIDATOR_MAP.put(DEFAULT_AVRO_RECORD_READER_CLASS, DEFAULT_AVRO_SCHEMA_VALIDATOR_CLASS);
  }

  /**
   * Gets schema validator given the record recorder and the input file path
   * @param pinotSchema pinot schema
   * @param recordReaderClassName record reader class name
   * @param inputFilePath local input file path
   */
  public static IngestionSchemaValidator getSchemaValidator(Schema pinotSchema, String recordReaderClassName, String inputFilePath)
      throws Exception {
    String schemaValidatorClassName = DEFAULT_RECORD_READER_TO_SCHEMA_VALIDATOR_MAP.get(recordReaderClassName);
    if (schemaValidatorClassName == null) {
      return null;
    }
    IngestionSchemaValidator ingestionSchemaValidator = PluginManager.get().createInstance(schemaValidatorClassName);
    ingestionSchemaValidator.init(pinotSchema, inputFilePath);
    return ingestionSchemaValidator;
  }
}
