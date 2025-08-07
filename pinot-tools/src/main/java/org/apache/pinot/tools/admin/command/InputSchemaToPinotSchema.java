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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.plugin.inputformat.parquet.ParquetUtils;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Unified command to convert Avro/Parquet schemas into a Pinot schema.
 *
 * Supported input types:
 * - AVRO_SCHEMA (.avsc)
 * - AVRO_DATA (.avro)
 * - PARQUET (.parquet)
 */
@CommandLine.Command(name = "InputSchemaToPinotSchema", mixinStandardHelpOptions = true)
public class InputSchemaToPinotSchema extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(InputSchemaToPinotSchema.class);

  public enum InputType {
    AVRO_SCHEMA, AVRO_DATA, PARQUET
  }

  @CommandLine.Option(names = {"-inputFile"}, required = true, description = "Path to schema or data file (.avsc/.avro/.parquet)")
  String _inputFile;

  @CommandLine.Option(names = {"-inputType"}, description = "Explicit input type: AVRO_SCHEMA | AVRO_DATA | PARQUET. If omitted, inferred from file extension.")
  String _inputType;

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

  @CommandLine.Option(names = {"-delimiter"}, description = "The delimiter separating components in nested structure, default to dot")
  String _delimiter;

  @CommandLine.Option(names = {"-complexType"}, description = "allow complex-type handling, default to false")
  boolean _complexType;

  @CommandLine.Option(names = {"-collectionNotUnnestedToJson"}, description = "Mode for converting collections to JSON: NONE | NON_PRIMITIVE | ALL")
  String _collectionNotUnnestedToJson;

  @Override
  public boolean execute() throws Exception {
    if (_dimensions == null && _metrics == null && _timeColumnName == null) {
      LOGGER.error("Error: specify at least one of -dimensions, -metrics, -timeColumnName");
      return false;
    }

    File input = new File(_inputFile);
    if (!input.isFile()) {
      LOGGER.error("ERROR: Input file: {} does not exist or is not a file", _inputFile);
      return false;
    }

    InputType inputType = determineInputType(input, _inputType);
    LOGGER.info("Detected inputType={} for file={}", inputType, input.getAbsolutePath());

    Schema schema;
    switch (inputType) {
      case AVRO_SCHEMA: {
        if (!_complexType) {
          schema = AvroUtils.getPinotSchemaFromAvroSchemaFile(input, buildFieldTypesMap(), _timeUnit, false,
              Collections.emptyList(), getDelimiter(), getCollectionNotUnnestedToJson());
        } else {
          schema = AvroUtils.getPinotSchemaFromAvroSchemaFile(input, buildFieldTypesMap(), _timeUnit, true,
              buildFieldsToUnnest(), getDelimiter(), getCollectionNotUnnestedToJson());
        }
        break;
      }
      case AVRO_DATA: {
        schema = AvroUtils.getPinotSchemaFromAvroDataFile(input, buildFieldTypesMap(), _timeUnit);
        break;
      }
      case PARQUET: {
        Path parquetPath = new Path(input.getAbsolutePath());
        boolean hasEmbeddedAvro = ParquetUtils.hasAvroSchemaInFileMetadata(parquetPath);
        LOGGER.info("Reading Parquet schema (hasEmbeddedAvroSchemaMetadata={})", hasEmbeddedAvro);
        org.apache.avro.Schema avroSchema = ParquetUtils.getParquetAvroSchema(parquetPath);
        if (!_complexType) {
          schema = AvroUtils.getPinotSchemaFromAvroSchema(avroSchema, buildFieldTypesMap(), _timeUnit);
        } else {
          schema = AvroUtils.getPinotSchemaFromAvroSchemaWithComplexTypeHandling(avroSchema, buildFieldTypesMap(),
              _timeUnit, buildFieldsToUnnest(), getDelimiter(), getCollectionNotUnnestedToJson());
        }
        break;
      }
      default:
        LOGGER.error("Unsupported inputType: {}", inputType);
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

  private static InputType determineInputType(File input, String explicit) {
    if (explicit != null && !explicit.isEmpty()) {
      return InputType.valueOf(explicit.trim().toUpperCase(Locale.ROOT));
    }
    String name = input.getName().toLowerCase(Locale.ROOT);
    if (name.endsWith(".avsc")) {
      return InputType.AVRO_SCHEMA;
    } else if (name.endsWith(".avro")) {
      return InputType.AVRO_DATA;
    } else if (name.endsWith(".parquet")) {
      return InputType.PARQUET;
    }
    throw new IllegalArgumentException("Cannot infer input type from file extension: " + input.getName()
        + ". Use -inputType to specify explicitly.");
  }

  @Override
  public String description() {
    return "Extract Pinot schema from Avro schema/data or Parquet file.";
  }

  @Override
  public String toString() {
    return "InputSchemaToPinotSchema -inputFile " + _inputFile + " -inputType " + _inputType + " -outputDir "
        + _outputDir + " -pinotSchemaName " + _pinotSchemaName + " -dimensions " + _dimensions + " -metrics "
        + _metrics + " -timeColumnName " + _timeColumnName + " -timeUnit " + _timeUnit + " _fieldsToUnnest "
        + _fieldsToUnnest + " _delimiter " + _delimiter + " _complexType " + _complexType
        + " _collectionNotUnnestedToJson " + _collectionNotUnnestedToJson;
  }

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


