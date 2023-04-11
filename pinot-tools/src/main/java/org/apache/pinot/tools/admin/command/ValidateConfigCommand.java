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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.config.validator.SchemaValidator;
import org.apache.pinot.tools.config.validator.TableConfigValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to validate configs.
 * <p>Validate the following configs:
 * <ul>
 *   <li>Table Config</li>
 *   <li>Schema</li>
 * </ul>
 */
@CommandLine.Command(name = "ValidateConfig", description = "Validate configs on the specified Zookeeper.",
    mixinStandardHelpOptions = true)
public class ValidateConfigCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidateConfigCommand.class);

  private static final String TABLE_CONFIG_PATH = "/CONFIGS/TABLE";
  private static final String SCHEMA_PATH = "/SCHEMAS";

  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;

  @CommandLine.Option(names = {"-zkAddress"}, required = true, description = "Zookeeper address")
  private String _zkAddress;

  @CommandLine.Option(names = {"-cluster"}, required = true, description = "Cluster name")
  private String _clusterName;

  @CommandLine.Option(names = {"-tableConfig"}, required = false, description = "Validate the table config")
  private boolean _validateTableConfig;

  @CommandLine.Option(names = {"-tableNames"}, required = false,
      description = "Space separated table names to be validated (default to validate ALL)")
  private String _tableNames;

  @CommandLine.Option(names = {"-schema"}, required = false, description = "Validate the schema")
  private boolean _validateSchema;

  @CommandLine.Option(names = {"-schemaNames"}, required = false,
      description = "Space separated schema names to be validated (default to validate ALL)")
  private String _schemaNames;

  @Override
  public String getName() {
    return "ValidateConfig";
  }

  @Override
  public boolean execute()
      throws Exception {
    if (!_validateTableConfig && !_validateSchema) {
      throw new RuntimeException("Need to specify at least one of -schema and -tableConfig");
    }

    LOGGER.info("Connecting to Zookeeper: {}, cluster: {}", _zkAddress, _clusterName);
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathBuilder.propertyStore(_clusterName);
    _helixPropertyStore = new ZkHelixPropertyStore<>(_zkAddress, serializer, path);

    LOGGER.info("\n\n-------------------- Starting Validation --------------------");
    if (_validateTableConfig) {
      validateTableConfig();
    }
    if (_validateSchema) {
      validateSchema();
    }

    return true;
  }

  private void validateTableConfig()
      throws Exception {
    List<String> tableNames = getTableNames();
    LOGGER.info("Validating table config for tables: " + tableNames);
    for (String tableName : tableNames) {
      LOGGER.info("  Validating table config for table: \"{}\"", tableName);
      try {
        ZNRecord record = _helixPropertyStore.get(TABLE_CONFIG_PATH + "/" + tableName, null, 0);
        TableConfig tableConfig = TableConfigUtils.fromZNRecord(record);
        if (!TableConfigValidator.validate(tableConfig)) {
          LOGGER.error("    Table config validation failed for table: \"{}\"", tableName);
        }
      } catch (Exception e) {
        LOGGER.error("    Caught exception while validating table config for table: \"{}\"", tableName, e);
      }
    }
  }

  private void validateSchema()
      throws Exception {
    List<String> schemaNames = getSchemaNames();
    LOGGER.info("Validating schemas: " + schemaNames);
    for (String schemaName : schemaNames) {
      LOGGER.info("  Validating schema: \"{}\"", schemaName);
      try {
        ZNRecord record = _helixPropertyStore.get(SCHEMA_PATH + "/" + schemaName, null, 0);
        Schema schema = SchemaUtils.fromZNRecord(record);
        SchemaValidator.validate(schema);
      } catch (Exception e) {
        LOGGER.error("    Caught exception while validating schema: \"{}\"", schemaName, e);
      }
    }
  }

  private List<String> getTableNames()
      throws Exception {
    if (_tableNames == null) {
      // Get all table names.
      return _helixPropertyStore.getChildNames(TABLE_CONFIG_PATH, 0);
    } else {
      // Extract space separated table names.
      Set<String> tableNames = new HashSet<>();
      for (String tableName : _tableNames.split(" ")) {
        if (!tableName.isEmpty()) {
          tableNames.add(tableName);
        }
      }
      if (tableNames.isEmpty()) {
        throw new RuntimeException("No table name specified.");
      }
      return new ArrayList<>(tableNames);
    }
  }

  private List<String> getSchemaNames()
      throws Exception {
    if (_schemaNames == null) {
      // Get all schema names.
      return _helixPropertyStore.getChildNames(SCHEMA_PATH, 0);
    } else {
      // Extract space separated schema names.
      Set<String> schemaNames = new HashSet<>();
      for (String schemaName : _schemaNames.split(" ")) {
        if (!schemaName.isEmpty()) {
          schemaNames.add(schemaName);
        }
      }
      if (schemaNames.isEmpty()) {
        throw new RuntimeException("No schema name specified.");
      }
      return new ArrayList<>(schemaNames);
    }
  }
}
