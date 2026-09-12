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
package org.apache.pinot.segment.local.utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigValidator;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SchemaUtilsTableConfigValidatorTest {
  @AfterMethod(alwaysRun = true)
  public void resetValidators() {
    TableConfigValidatorRegistry.reset();
  }

  @Test
  public void testSchemaValidationOnlyInvokesOptInTableConfigValidators() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("id", FieldSpec.DataType.LONG)
        .setPrimaryKeyColumns(List.of("id"))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    AtomicBoolean ordinaryValidatorInvoked = new AtomicBoolean();
    AtomicBoolean schemaValidatorInvoked = new AtomicBoolean();
    TableConfigValidatorRegistry.register((actualTableConfig, actualSchema) -> {
      ordinaryValidatorInvoked.set(true);
    });
    TableConfigValidatorRegistry.register(new TableConfigValidator() {
      @Override
      public void validate(TableConfig actualTableConfig, Schema actualSchema) {
        ordinaryValidatorInvoked.set(true);
      }

      @Override
      public void validateSchema(TableConfig actualTableConfig, Schema actualSchema) {
        assertTrue(actualTableConfig == tableConfig);
        assertTrue(actualSchema == schema);
        schemaValidatorInvoked.set(true);
      }
    });

    SchemaUtils.validate(schema, List.of(tableConfig));

    assertFalse(ordinaryValidatorInvoked.get());
    assertTrue(schemaValidatorInvoked.get());
  }

  @Test
  public void testSchemaValidationPropagatesRegisteredValidatorFailure() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("id", FieldSpec.DataType.LONG)
        .setPrimaryKeyColumns(List.of("id"))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    TableConfigValidatorRegistry.register(new TableConfigValidator() {
      @Override
      public void validate(TableConfig actualTableConfig, Schema actualSchema) {
      }

      @Override
      public void validateSchema(TableConfig actualTableConfig, Schema actualSchema) {
        throw new IllegalArgumentException("custom schema rejection");
      }
    });

    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> SchemaUtils.validate(schema, List.of(tableConfig)));

    assertTrue(e.getMessage().contains("custom schema rejection"));
  }
}
