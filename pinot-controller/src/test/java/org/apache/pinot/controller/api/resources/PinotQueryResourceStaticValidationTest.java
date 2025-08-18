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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.config.provider.TableCacheQueryValidator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for the static table cache functionality in PinotQueryResource.
 */
public class PinotQueryResourceStaticValidationTest {

  @Mock
  private HttpHeaders _httpHeaders;

  private ObjectMapper _objectMapper;

  @BeforeClass
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _objectMapper = new ObjectMapper();
  }

  @Test
  public void testStaticTableCacheProvider() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("dimensionCol", FieldSpec.DataType.STRING)
        .addMetric("metricCol", FieldSpec.DataType.LONG)
        .build();

    List<TableConfig> tableConfigs = Arrays.asList(tableConfig);
    List<Schema> schemas = Arrays.asList(schema);

    TableCacheQueryValidator provider = new TableCacheQueryValidator(tableConfigs, schemas, false);

    Assert.assertFalse(provider.isIgnoreCase());
    Assert.assertEquals(provider.getActualTableName("testTable_OFFLINE"), "testTable_OFFLINE");
    Assert.assertEquals(provider.getActualTableName("testTable"), "testTable");
    Assert.assertNotNull(provider.getTableConfig("testTable_OFFLINE"));
    Assert.assertNotNull(provider.getSchema("testTable"));
    Assert.assertNotNull(provider.getColumnNameMap("testTable"));
    Assert.assertEquals(provider.getColumnNameMap("testTable").size(), 4); // 2 columns + 2 built-in virtual columns

    Assert.assertTrue(provider.getTableNameMap().containsKey("testTable_OFFLINE"));
    Assert.assertTrue(provider.getTableNameMap().containsKey("testTable"));
  }

  @Test
  public void testRequestSerialization() throws Exception {
    Map<String, Object> request = new HashMap<>();
    request.put("sql", "SELECT * FROM testTable");

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();
    request.put("tableConfigs", Arrays.asList(tableConfig));

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .build();
    request.put("schemas", Arrays.asList(schema));
    request.put("logicalTableConfigs", Collections.emptyList());

    String json = _objectMapper.writeValueAsString(request);
    JsonNode jsonNode = _objectMapper.readTree(json);

    Assert.assertEquals(jsonNode.get("sql").asText(), "SELECT * FROM testTable");
    Assert.assertEquals(jsonNode.get("tableConfigs").size(), 1);
    Assert.assertEquals(jsonNode.get("schemas").size(), 1);
    Assert.assertEquals(jsonNode.get("logicalTableConfigs").size(), 0);
  }
}
