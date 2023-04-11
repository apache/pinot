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
import java.util.Arrays;
import java.util.Collections;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to convert avro schema to pinot schema. Given that it is not always possible to
 * automatically do this, the intention is to get most of the work done by this class, and require any
 * manual editing on top.
 */
@CommandLine.Command(name = "AvroSchemaToPinotSchema", description = "Extracting Pinot schema file from Avro schema "
                                                                     + "or data file.", mixinStandardHelpOptions = true)
public class AvroSchemaToPinotSchema extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaToPinotSchema.class);

  @CommandLine.Option(names = {"-avroSchemaFile"}, description = "Path to avro schema file.")
      // TODO: support forbids = {"-avroDataFile"},
  String _avroSchemaFile;

  @CommandLine.Option(names = {"-avroDataFile"}, description = "Path to avro data file.")
      // TODO: support forbids = {"-avroSchemaFile"},
  String _avroDataFile;

  @CommandLine.Option(names = {"-outputDir"}, required = true, description = "Path to output directory")
  String _outputDir;

  @CommandLine.Option(names = {"-pinotSchemaName"}, required = true, description = "Pinot schema name")
  String _pinotSchemaName;

  @CommandLine.Option(names = {"-dimensions"}, description = "Comma separated dimension column names.")
  String _dimensions;

  @CommandLine.Option(names = {"-metrics"}, description = "Comma separated metric column names.")
  String _metrics;

  @CommandLine.Option(names = {"-timeColumnName"}, description = "Name of the time column.")
  String _timeColumnName;

  @CommandLine.Option(names = {"-timeUnit"}, description = "Unit of the time column (default DAYS).")
  TimeUnit _timeUnit = TimeUnit.DAYS;

  @CommandLine.Option(names = {"-fieldsToUnnest"}, description = "Comma separated fields to unnest")
  String _fieldsToUnnest;

  @CommandLine.Option(names = {"-delimiter"}, description = "The delimiter separating components in nested "
      + "structure, default to dot")
  String _delimiter;

  @CommandLine.Option(names = {"-complexType"}, description = "allow complex-type handling, default to false")
  boolean _complexType;

  @CommandLine.Option(names = {"-collectionNotUnnestedToJson"}, description = "The mode of converting collection to "
      + "JSON string, can be NONE/NON_PRIMITIVE/ALL")
  String _collectionNotUnnestedToJson;

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
      schema = AvroUtils.getPinotSchemaFromAvroSchemaFile(new File(_avroSchemaFile), buildFieldTypesMap(), _timeUnit,
          _complexType, buildFieldsToUnnest(), getDelimiter(), getCollectionNotUnnestedToJson());
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
  public String toString() {
    return "AvroSchemaToPinotSchema -avroSchemaFile " + _avroSchemaFile + " -avroDataFile " + _avroDataFile
        + " -outputDir " + _outputDir + " -pinotSchemaName " + _pinotSchemaName + " -dimensions " + _dimensions
        + " -metrics " + _metrics + " -timeColumnName " + _timeColumnName + " -timeUnit " + _timeUnit
        + " _fieldsToUnnest " + _fieldsToUnnest + " _delimiter " + _delimiter + " _complexType " + _complexType
        + " _collectionNotUnnestedToJson " + _collectionNotUnnestedToJson;
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
      fieldTypes.put(_timeColumnName, FieldSpec.FieldType.DATE_TIME);
    }
    return fieldTypes;
  }

  private List<String> buildFieldsToUnnest() {
    if (_fieldsToUnnest != null) {
      return Arrays.asList(_fieldsToUnnest.split("\\s*,\\s*"));
    } else {
      return Collections.emptyList();
    }
  }

  private ComplexTypeConfig.CollectionNotUnnestedToJson getCollectionNotUnnestedToJson() {
    if (_collectionNotUnnestedToJson == null) {
      return ComplexTypeTransformer.DEFAULT_COLLECTION_TO_JSON_MODE;
    }
    return ComplexTypeConfig.CollectionNotUnnestedToJson.valueOf(_collectionNotUnnestedToJson);
  }

  private String getDelimiter() {
    return _delimiter == null ? ComplexTypeTransformer.DEFAULT_DELIMITER : _delimiter;
  }
}
