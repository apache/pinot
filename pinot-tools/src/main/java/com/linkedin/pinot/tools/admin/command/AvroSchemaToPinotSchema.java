/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for command to convert avro schema to pinot schema. Given that it is not always possible to
 * automatically do this, the intention is to get most of the work done by this class, and require any
 * manual editing on top.
 */
public class AvroSchemaToPinotSchema extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTenantCommand.class);

  @Option(name = "-avroSchemaFileName", required = false, forbids = {"-avroDataFileName"},
      metaVar = "<String>", usage = "Path to avro schema file.")
  String _avroSchemaFileName;

  @Option(name = "-avroDataFileName", required = false, forbids = {"-avroSchemaFileName"},
      metaVar = "<String>", usage = "Path to avro schema file.")
  String _avroDataFileName;

  @Option(name = "-pinotSchemaFileName", required = false, metaVar = "<string>", usage = "Path to pinot schema file.")
  String _pinotSchemaFileName;

  @Option(name = "-dimensions", required = false, metaVar = "<string>", usage = "Comma separated dimension columns.")
  String _dimensions;

  @Option(name = "-metrics", required = false, metaVar = "<string>", usage = "Comma separated metric columns.")
  String _metrics;

  @Option(name = "-timeColumnName", required = false, metaVar = "<string>", usage = "Name of Time Column.")
  String _timeColumnName;

  @Option(name = "-timeUnit", required = false, metaVar = "<string>", usage = "Unit for time column.")
  TimeUnit _timeUnit = TimeUnit.DAYS;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    Schema schema = null;

    if (_dimensions == null && _metrics == null) {
      LOGGER.error("Error: Missing required argument, please specify -dimensions, -metrics, or both");
      return false;
    }

    if (_avroSchemaFileName != null) {
      schema = AvroUtils.getPinotSchemaFromAvroSchemaFile(_avroSchemaFileName, buildFieldTypesMap(), _timeUnit);
    } else if (_avroDataFileName != null) {
      schema = AvroUtils.extractSchemaFromAvro(new File(_avroDataFileName));
    } else {
      LOGGER.error("Error: Missing required argument, please specify either -avroSchemaFileName, or -avroDataFileName");
      return false;
    }

    if (schema == null) {
      LOGGER.error("Error: Could not read avro schema from file.");
      return false;
    }

    String avroFileName = _avroSchemaFileName;
    if (avroFileName == null) {
      avroFileName = _avroDataFileName;
    }

    // Remove extension
    String schemaName = avroFileName.replaceAll("\\..*", "");
    schema.setSchemaName(schemaName);

    if (_pinotSchemaFileName == null) {
      _pinotSchemaFileName = schemaName + ".json";

      LOGGER.info("Using {} as the Pinot schema file name", _pinotSchemaFileName);
    }

    ObjectMapper objectMapper = new ObjectMapper();
    String schemaString = objectMapper.defaultPrettyPrintingWriter().writeValueAsString(schema);

    PrintWriter printWriter = new PrintWriter(_pinotSchemaFileName);
    printWriter.println(schemaString);
    printWriter.close();

    return true;
  }

  @Override
  public String description() {
    return "Converting Avro schema to Pinot schema.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return "AvroSchemaToPinotSchema -avroSchemaFileName " + _avroSchemaFileName + " -pinotSchemaFileName " +
        _pinotSchemaFileName + " -dimensions " + _dimensions + " -metrics " + _metrics + " -timeColumnName " +
        _timeColumnName + " -timeUnit " + _timeUnit;
  }

  /**
   * Build a Map with column name as key and fieldType (dimension/metric/time) as value, from the
   * options list.
   *
   * @return The column <-> fieldType map.
   */
  private Map<String, FieldSpec.FieldType> buildFieldTypesMap() {
    Map<String, FieldSpec.FieldType> fieldTypes = new HashMap<String, FieldSpec.FieldType>();
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
}
