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
package org.apache.pinot.controller.api;

import java.util.List;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminNotFoundException;
import org.apache.pinot.client.admin.PinotAdminValidationException;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotSchemaRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testPostJson()
      throws Exception {
    String schemaString = "{\n" + "  \"schemaName\" : \"transcript\",\n" + "  \"dimensionFieldSpecs\" : [ {\n"
        + "    \"name\" : \"studentID\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"firstName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"lastName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"gender\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"subject\",\n" + "    \"dataType\" : \"STRING\"\n" + "  } ],\n"
        + "  \"metricFieldSpecs\" : [ {\n" + "    \"name\" : \"score\",\n" + "    \"dataType\" : \"FLOAT\"\n"
        + "  } ]}";
    final String response = TEST_INSTANCE.getOrCreateAdminClient().getSchemaClient().createSchema(schemaString);
    assertEquals(response, "{\"unrecognizedProperties\":{},\"status\":\"transcript successfully added\"}");
  }

  @Test
  public void testCreateUpdateSchema()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    String schemaName = "testSchema";
    Schema schema = TEST_INSTANCE.createDummySchema(schemaName);

    // Add the schema
    adminClient.getSchemaClient().createSchema(schema.toSingleLineJsonString());

    // Add a new column
    DimensionFieldSpec newColumnFieldSpec = new DimensionFieldSpec("newColumn", DataType.STRING, true);
    schema.addField(newColumnFieldSpec);

    // Update the schema with addSchema api and override off
    expectValidationException(
        () -> adminClient.getSchemaClient().createSchema(schema.toSingleLineJsonString(), false, false));

    // Update the schema with addSchema api and override on
    adminClient.getSchemaClient().createSchema(schema.toSingleLineJsonString());

    // Get the schema and verify the new column exists
    Schema remoteSchema = adminClient.getSchemaClient().getSchema(schemaName);
    assertEquals(remoteSchema, schema);
    assertTrue(remoteSchema.hasColumn(newColumnFieldSpec.getName()));

    // Add another new column
    DimensionFieldSpec newColumnFieldSpec2 = new DimensionFieldSpec("newColumn2", DataType.STRING, true);
    schema.addField(newColumnFieldSpec2);

    // Update the schema with updateSchema api
    adminClient.getSchemaClient().updateSchema(schemaName, schema.toSingleLineJsonString());

    // Get the schema and verify both the new columns exist
    remoteSchema = adminClient.getSchemaClient().getSchema(schemaName);
    assertEquals(remoteSchema, schema);
    assertTrue(remoteSchema.hasColumn(newColumnFieldSpec.getName()));
    assertTrue(remoteSchema.hasColumn(newColumnFieldSpec2.getName()));

    // Change the column data type - backward-incompatible change
    newColumnFieldSpec.setDataType(DataType.INT);

    // Update the schema with addSchema api and override on
    expectValidationException(() -> adminClient.getSchemaClient().createSchema(schema.toSingleLineJsonString()));

    // Update the schema with updateSchema api
    expectValidationException(
        () -> adminClient.getSchemaClient().updateSchema(schemaName, schema.toSingleLineJsonString()));

    // Change the column data type from STRING to BOOLEAN
    newColumnFieldSpec.setDataType(DataType.BOOLEAN);

    // Update the schema with addSchema api and override on, force on
    adminClient.getSchemaClient().createSchema(schema.toSingleLineJsonString(), true, true);

    // Change another column max length from default 512 to 2000
    newColumnFieldSpec2.setMaxLength(2000);
    // Change another column default null value from default "null" to "0"
    newColumnFieldSpec2.setDefaultNullValue("0");

    // Update the schema with addSchema api and override on
    adminClient.getSchemaClient().createSchema(schema.toSingleLineJsonString());

    // Get the schema and verify the default null value and max length have been changed
    remoteSchema = adminClient.getSchemaClient().getSchema(schemaName);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getEffectiveMaxLength(), 2000);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getDefaultNullValue(), "0");

    // Change another column max length from 1000
    newColumnFieldSpec2.setMaxLength(1000);
    // Change another column default null value from default "null" to "1"
    newColumnFieldSpec2.setDefaultNullValue("1");

    // Update the schema with updateSchema api and override on
    adminClient.getSchemaClient().updateSchema(schemaName, schema.toSingleLineJsonString());

    // Get the schema and verify the default null value and max length have been changed
    remoteSchema = adminClient.getSchemaClient().getSchema(schemaName);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getEffectiveMaxLength(), 1000);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getDefaultNullValue(), "1");

    // Add a new BOOLEAN column
    DimensionFieldSpec newColumnFieldSpec3 = new DimensionFieldSpec("newColumn3", DataType.BOOLEAN, true);
    schema.addField(newColumnFieldSpec3);

    // Update the schema with updateSchema api
    adminClient.getSchemaClient().updateSchema(schemaName, schema.toSingleLineJsonString());

    // Get the schema and verify the new column has BOOLEAN data type
    remoteSchema = adminClient.getSchemaClient().getSchema(schemaName);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec3.getName()).getDataType(), DataType.BOOLEAN);

    // Post invalid schema string
    String invalidSchemaString = schema.toSingleLineJsonString().substring(1);
    expectValidationException(() -> adminClient.getSchemaClient().createSchema(invalidSchemaString));
    expectValidationException(() -> adminClient.getSchemaClient().updateSchema(schemaName, invalidSchemaString));

    // Update schema with null schema name
    schema.setSchemaName(null);
    expectValidationException(
        () -> adminClient.getSchemaClient().updateSchema(schemaName, schema.toSingleLineJsonString()));

    // Update schema with non-matching schema name
    String newSchemaName = "newSchemaName";
    schema.setSchemaName(newSchemaName);
    expectValidationException(
        () -> adminClient.getSchemaClient().updateSchema(schemaName, schema.toSingleLineJsonString()));

    // Update non-existing schema
    expectNotFoundException(
        () -> adminClient.getSchemaClient().updateSchema(newSchemaName, schema.toSingleLineJsonString()));
  }

  @Test
  public void testUnrecognizedProperties()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    String schemaStringWithExtraProps =
        "{\n" + "  \"schemaName\" : \"transcript2\",\"illegalKey1\" : 1, \n" + "  \"dimensionFieldSpecs\" : [ {\n"
            + "    \"name\" : \"studentID\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"firstName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"lastName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"gender\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"subject\",\n" + "    \"dataType\" : \"STRING\"\n" + "  } ],\n"
            + "  \"metricFieldSpecs\" : [ {\n" + "    \"name\" : \"score\",\n" + "    \"dataType\" : \"FLOAT\"\n"
            + "  } ]}";

    String response = adminClient.getSchemaClient().validateSchema(schemaStringWithExtraProps);
    assertTrue(response.contains("\"/illegalKey1\":1"));

    response = adminClient.getSchemaClient().createSchema(schemaStringWithExtraProps);
    assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");

    response = adminClient.getSchemaClient().updateSchema("transcript2", schemaStringWithExtraProps);
    assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");
  }

  @Test
  public void testUnrecognizedPropertiesFileEndpoints()
      throws Exception {
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    String schemaStringWithExtraProps =
        "{\n" + "  \"schemaName\" : \"transcript2\",\"illegalKey1\" : 1, \n" + "  \"dimensionFieldSpecs\" : [ {\n"
            + "    \"name\" : \"studentID\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"firstName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"lastName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"gender\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"subject\",\n" + "    \"dataType\" : \"STRING\"\n" + "  } ],\n"
            + "  \"metricFieldSpecs\" : [ {\n" + "    \"name\" : \"score\",\n" + "    \"dataType\" : \"FLOAT\"\n"
            + "  } ]}";

    String response = adminClient.getSchemaClient().validateSchema(schemaStringWithExtraProps);
    assertTrue(response.contains("\"/illegalKey1\":1"));

    response = adminClient.getSchemaClient().createSchema(schemaStringWithExtraProps);
    assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");

    response = adminClient.getSchemaClient().updateSchema("transcript2", schemaStringWithExtraProps);
    assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");
  }

  @Test
  public void testSchemaDeletionWithLogicalTable()
      throws Exception {
    String logicalTableName = "logical_table";
    String physicalTable = "physical_table";
    PinotAdminClient adminClient = TEST_INSTANCE.getOrCreateAdminClient();
    TEST_INSTANCE.addDummySchema(physicalTable);
    TEST_INSTANCE.addTableConfig(ControllerTest.createDummyTableConfig(physicalTable, TableType.OFFLINE));
    TEST_INSTANCE.addDummySchema(logicalTableName);

    // Create a logical table
    LogicalTableConfig logicalTableConfig =
        ControllerTest.getDummyLogicalTableConfig(logicalTableName,
            List.of(TableNameBuilder.OFFLINE.tableNameWithType(physicalTable)), "DefaultTenant");
    String response =
        adminClient.getLogicalTableClient().createLogicalTable(logicalTableConfig.toSingleLineJsonString());
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"logical_table logical table successfully added.\"}");

    // Delete schema should fail because logical table exists
    RuntimeException deleteException =
        expectThrows(RuntimeException.class, () -> adminClient.getSchemaClient().deleteSchema(logicalTableName));
    Throwable deleteCause = unwrap(deleteException);
    assertTrue(deleteCause instanceof PinotAdminValidationException);
    assertTrue(deleteCause.getMessage().contains("Cannot delete schema logical_table, as it is "
        + "associated with logical table"), deleteCause.getMessage());

    // Delete logical table
    response = adminClient.getLogicalTableClient().deleteLogicalTable(logicalTableName);
    assertEquals(response, "{\"status\":\"logical_table logical table successfully deleted.\"}");

    // Delete schema should succeed now
    response = adminClient.getSchemaClient().deleteSchema(logicalTableName);
    assertEquals(response, "{\"status\":\"Schema logical_table deleted\"}");
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run()
        throws Exception;
  }

  private void expectValidationException(ThrowingRunnable runnable) {
    RuntimeException runtimeException =
        expectThrows(RuntimeException.class, () -> runUnchecked(runnable));
    Throwable cause = unwrap(runtimeException);
    assertTrue(cause instanceof PinotAdminValidationException, "Unexpected exception: " + cause);
  }

  private void expectNotFoundException(ThrowingRunnable runnable) {
    RuntimeException runtimeException =
        expectThrows(RuntimeException.class, () -> runUnchecked(runnable));
    Throwable cause = unwrap(runtimeException);
    assertTrue(cause instanceof PinotAdminNotFoundException, "Unexpected exception: " + cause);
  }

  private void runUnchecked(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Throwable unwrap(Throwable throwable) {
    Throwable current = throwable;
    while (current.getCause() != null && current.getCause() != current) {
      current = current.getCause();
    }
    return current;
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
