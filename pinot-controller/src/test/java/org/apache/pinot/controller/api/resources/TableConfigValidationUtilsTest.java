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

import java.util.Collections;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

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
}
