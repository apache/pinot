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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for command to convert avro schema to pinot schema. Given that it is not always possible to
 * automatically do this, the intention is to get most of the work done by this class, and require any
 * manual editing on top.
 */
public class AvroSchemaToPinotSchema extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaToPinotSchema.class);

  @Option(name = "-avroSchemaFile", forbids = {"-avroDataFile"}, metaVar = "<String>", usage = "Path to avro schema file.")
  String _avroSchemaFile;

  @Option(name = "-avroDataFile", forbids = {"-avroSchemaFile"}, metaVar = "<String>", usage = "Path to avro data file.")
  String _avroDataFile;

  @Option(name = "-outputDir", required = true, metaVar = "<string>", usage = "Path to output directory")
  String _outputDir;

  @Option(name = "-pinotSchemaName", required = true, metaVar = "<string>", usage = "Pinot schema name")
  String _pinotSchemaName;

  @Option(name = "-dimensions", metaVar = "<string>", usage = "Comma separated dimension column names.")
  String _dimensions;

  @Option(name = "-metrics", metaVar = "<string>", usage = "Comma separated metric column names.")
  String _metrics;

  @Option(name = "-timeColumnName", metaVar = "<string>", usage = "Name of the time column.")
  String _timeColumnName;

  @Option(name = "-timeUnit", metaVar = "<string>", usage = "Unit of the time column (default DAYS).")
  TimeUnit _timeUnit = TimeUnit.DAYS;

  @Option(name = "-unnestFields", metaVar = "<string>", usage = "Comma separated fields to unnest")
  String _unnestFields;

  @Option(name = "-delimiter", metaVar = "<string>", usage = "The delimiter separating components in nested structure, default to dot")
  String _delimiter;

  @Option(name = "-complexType", metaVar = "<boolean>", usage = "allow complex-type handling, default to false")
  boolean _complexType;

  @Option(name = "-collectionToJsonMode", metaVar = "<string>", usage = "The mode of converting collection to JSON string, can be NONE/NON_PRIMITIVE/ALL")
  String _collectionToJsonMode;

  @SuppressWarnings("FieldCanBeLocal")
  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute()
      throws Exception {
    if (_dimensions == null && _metrics == null && _timeColumnName == null) {
      LOGGER.error(
          "Error: Missing required argument, please specify at least one of -dimensions, -metrics, -timeColumnName");
      return false;
    }

    Schema schema;
    if (_avroSchemaFile != null) {
      schema = AvroUtils
          .getPinotSchemaFromAvroSchemaFile(new File(_avroSchemaFile), buildFieldTypesMap(), _timeUnit, _complexType,
              buildUnnestFields(), getDelimiter(), getCollectionToJsonMode());
    } else if (_avroDataFile != null) {
      schema = AvroUtils.getPinotSchemaFromAvroDataFile(new File(_avroDataFile), buildFieldTypesMap(), _timeUnit);
    } else {
      LOGGER.error("Error: Missing required argument, please specify either -avroSchemaFile, or -avroDataFile");
      return false;
    }

    schema.setSchemaName(_pinotSchemaName);

    File outputDir = new File(_outputDir);
    if (!outputDir.isDirectory()) {
      LOGGER.error("ERROR: Output directory: {} does not exist or is not a directory", _outputDir);
      return false;
    }
    File outputFile = new File(outputDir, _pinotSchemaName + ".json");
    LOGGER.info("Store Pinot schema to file: {}", outputFile.getAbsolutePath());

    try (FileWriter writer = new FileWriter(outputFile)) {
      writer.write(schema.toPrettyJsonString());
    }

    return true;
  }

  @Override
  public String description() {
    return "Extracting Pinot schema file from Avro schema or data file.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return "AvroSchemaToPinotSchema -avroSchemaFile " + _avroSchemaFile + " -avroDataFile " + _avroDataFile
        + " -outputDir " + _outputDir + " -pinotSchemaName " + _pinotSchemaName + " -dimensions " + _dimensions
        + " -metrics " + _metrics + " -timeColumnName " + _timeColumnName + " -timeUnit " + _timeUnit
        + " _unnestFields " + _unnestFields + " _delimiter " + _delimiter + " _complexType " + _complexType
        + " _collectionToJsonMode " + _collectionToJsonMode;
  }

  /**
   * Build a Map with column name as key and fieldType (dimension/metric/time) as value, from the
   * options list.
   *
   * @return The column <-> fieldType map.
   */
  private Map<String, FieldSpec.FieldType> buildFieldTypesMap() {
    Map<String, FieldSpec.FieldType> fieldTypes = new HashMap<>();
    if (_dimensions != null) {
      for (String column : _dimensions.split("\\s*,\\s*")) {
        fieldTypes.put(column, FieldSpec.FieldType.DIMENSION);
      }
    }
    if (_metrics != null) {
      for (String column : _metrics.split("\\s*,\\s*")) {
        fieldTypes.put(column, FieldSpec.FieldType.METRIC);
      }
    }
    if (_timeColumnName != null) {
      fieldTypes.put(_timeColumnName, FieldSpec.FieldType.TIME);
    }
    return fieldTypes;
  }

  private List<String> buildUnnestFields() {
    List<String> unnestFields = new ArrayList<>();
    if (_unnestFields != null) {
      for (String field : _unnestFields.split(",")) {
        unnestFields.add(field);
      }
    }
    return unnestFields;
  }

  private ComplexTypeConfig.CollectionToJsonMode getCollectionToJsonMode() {
    if (_collectionToJsonMode == null) {
      return ComplexTypeTransformer.DEFAULT_COLLECTION_TO_JSON_MODE;
    }
    return ComplexTypeConfig.CollectionToJsonMode.valueOf(_collectionToJsonMode);
  }

  private String getDelimiter() {
    return _delimiter == null ? ComplexTypeTransformer.DEFAULT_DELIMITER : _delimiter;
  }
}
