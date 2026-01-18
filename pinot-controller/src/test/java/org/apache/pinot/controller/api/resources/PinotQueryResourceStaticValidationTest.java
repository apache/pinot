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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.config.provider.StaticTableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for the static table cache functionality in PinotQueryResource.
 */
public class PinotQueryResourceStaticValidationTest {

  private ObjectMapper _objectMapper;

  @BeforeClass
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _objectMapper = new ObjectMapper();
  }

  @Test
  public void testStaticTableCacheProvider() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("dimensionCol", FieldSpec.DataType.STRING)
        .addMetric("metricCol", FieldSpec.DataType.LONG).build();

    List<TableConfig> tableConfigs = Arrays.asList(tableConfig);
    List<Schema> schemas = Arrays.asList(schema);

    StaticTableCache provider = new StaticTableCache(tableConfigs, schemas, null, false);

    Assert.assertFalse(provider.isIgnoreCase());
    Assert.assertEquals(provider.getActualTableName("testTable_OFFLINE"), "testTable_OFFLINE");
    Assert.assertEquals(provider.getActualTableName("testTable"), "testTable");
    Assert.assertNotNull(provider.getTableConfig("testTable_OFFLINE"));
    Assert.assertNotNull(provider.getSchema("testTable"));
    Assert.assertNotNull(provider.getColumnNameMap("testTable"));
    Assert.assertEquals(provider.getColumnNameMap("testTable").size(), 6); // 2 columns + 4 built-in virtual columns

    Assert.assertTrue(provider.getTableNameMap().containsKey("testTable_OFFLINE"));
    Assert.assertTrue(provider.getTableNameMap().containsKey("testTable"));
  }
}
