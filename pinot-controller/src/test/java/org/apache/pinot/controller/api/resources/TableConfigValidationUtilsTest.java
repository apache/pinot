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
package org.apache.pinot.controller.api.resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class TableConfigValidationUtilsTest {

  private TableConfig tableConfigWithCustomConfig(String key, String value) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("envVarTable")
        .setCustomConfig(new TableCustomConfig(Collections.singletonMap(key, value)))
        .build();
  }

  @Test
  public void testMissingEnvironmentVariableIsRejected() {
    TableConfig tableConfig = tableConfigWithCustomConfig("key", "${PINOT_NON_EXISTENT_ENV_VAR_XYZ}");
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> TableConfigValidationUtils.validateEnvironmentVariables(tableConfig));
    assertTrue(e.getMessage().contains("PINOT_NON_EXISTENT_ENV_VAR_XYZ"),
        "Expected message to reference the missing variable, got: " + e.getMessage());
    assertTrue(e.getMessage().contains("envVarTable"),
        "Expected message to reference the table name, got: " + e.getMessage());
  }

  @Test
  public void testMissingEnvironmentVariableWithDefaultIsAccepted() {
    // A template with a default value (${VAR:default}) resolves to the default and must not be rejected.
    TableConfig tableConfig = tableConfigWithCustomConfig("key", "${PINOT_NON_EXISTENT_ENV_VAR_XYZ:fallback}");
    TableConfigValidationUtils.validateEnvironmentVariables(tableConfig);
  }

  @Test
  public void testResolvableSystemPropertyIsAccepted() {
    String propertyName = "pinot.test.env.var.validation.prop";
    System.setProperty(propertyName, "resolvedValue");
    try {
      TableConfig tableConfig = tableConfigWithCustomConfig("key", "${" + propertyName + "}");
      TableConfigValidationUtils.validateEnvironmentVariables(tableConfig);
    } finally {
      System.clearProperty(propertyName);
    }
  }

  @Test
  public void testConfigWithoutTemplatesIsAccepted() {
    TableConfig tableConfig = tableConfigWithCustomConfig("key", "plainValue");
    TableConfigValidationUtils.validateEnvironmentVariables(tableConfig);
  }

  private static InstancePartitions createInstancePartitions(String name, int numPartitions, int numReplicaGroups,
      int numInstancesPerPartition) {
    InstancePartitions instancePartitions = new InstancePartitions(name);
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
        List<String> instances = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instances.add("Server_" + partitionId + "_" + replicaGroupId + "_" + i);
        }
        instancePartitions.setInstances(partitionId, replicaGroupId, instances);
      }
    }
    return instancePartitions;
  }

  @Test
  public void testValidateStrictReplicaGroupInstancePartitions() {
    RoutingConfig strictRoutingConfig =
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    InstancePartitions wideInstancePartitions = createInstancePartitions("table_OFFLINE", 4, 2, 2);

    // Tables without strict replica-group routing are not validated
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table").build();
    TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(tableConfig, wideInstancePartitions, null);

    // Dimension tables host every segment on every server
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setIsDimTable(true)
        .setRoutingConfig(strictRoutingConfig)
        .build();
    TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(tableConfig, wideInstancePartitions, null);

    // OFFLINE instance partitions with one instance per partition in each replica-group
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("table")
        .setRoutingConfig(strictRoutingConfig)
        .build();
    TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(tableConfig,
        createInstancePartitions("table_OFFLINE", 4, 2, 1), null);
    TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(tableConfig,
        createInstancePartitions("table_OFFLINE", 1, 2, 1), null);

    // OFFLINE instance partitions with multiple instances per partition
    TableConfig offlineTableConfig = tableConfig;
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(offlineTableConfig,
            wideInstancePartitions, null));
    assertEquals(e.getMessage(),
        "Table: table_OFFLINE using strict replica-group routing must have exactly one instance per partition in each "
            + "replica-group, found instances: [Server_0_0_0, Server_0_0_1] for partition_replicaGroup: 0_0 in "
            + "instance partitions: table_OFFLINE");

    // CONSUMING instance partitions with a single partition, or one instance per partition
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("table")
        .setRoutingConfig(strictRoutingConfig)
        .build();
    TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(tableConfig, null,
        createInstancePartitions("table_CONSUMING", 1, 2, 3));
    TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(tableConfig, null,
        createInstancePartitions("table_CONSUMING", 4, 2, 1));

    // CONSUMING instance partitions with multiple partitions and multiple instances per partition
    TableConfig realtimeTableConfig = tableConfig;
    e = expectThrows(IllegalStateException.class,
        () -> TableConfigValidationUtils.validateStrictReplicaGroupInstancePartitions(realtimeTableConfig, null,
            createInstancePartitions("table_CONSUMING", 4, 2, 2)));
    assertEquals(e.getMessage(),
        "Table: table_REALTIME using strict replica-group routing must have exactly one instance per partition in each "
            + "replica-group, found instances: [Server_0_0_0, Server_0_0_1] for partition_replicaGroup: 0_0 in "
            + "instance partitions: table_CONSUMING");
  }
}
