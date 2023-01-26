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

package org.apache.pinot.segment.local.segment.index.forward;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ForwardIndexTypeTest {



  @DataProvider(name = "allChunkCompressionType")
  public static Object[][] allChunkCompressionType() {
    return new String[][] {
        new String[] {"PASS_THROUGH"},
        new String[] {"SNAPPY"},
        new String[] {"ZSTANDARD"},
        new String[] {"LZ4"},
        new String[] {null}
    };
  }

  public class ConfTest {

    private Schema _schema = JsonUtils.stringToObject(""
        + "{\n"
        + "  \"schemaName\": \"transcript\",\n"
        + "  \"dimensionFieldSpecs\": [\n"
        + "    {\n"
        + "      \"name\": \"dimInt\",\n"
        + "      \"dataType\": \"INT\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"dimStr\",\n"
        + "      \"dataType\": \"STRING\"\n"
        + "    }\n"
        + "  ],\n"
        + "  \"metricFieldSpecs\": [\n"
        + "    {\n"
        + "      \"name\": \"metInt\",\n"
        + "      \"dataType\": \"INT\"\n"
        + "    }\n"
        + "  ]\n"
        + "}", Schema.class);
    private TableConfig _tableConfig;
    private final TypeReference<List<FieldConfig>> _fieldConfigListTypeRef = new TypeReference<>() {
    };

    public ConfTest() throws IOException {
    }

    @BeforeTest
    public void resetTableConfig()
        throws JsonProcessingException {
      _tableConfig = JsonUtils.stringToObject(""
          + "{\n"
          + "  \"tableName\": \"transcript\"\n,"
          + "  \"segmentsConfig\" : {\n"
          + "    \"replication\" : \"1\",\n"
          + "    \"schemaName\" : \"transcript\"\n"
          + "  },\n"
          + "  \"tableIndexConfig\" : {\n"
          + "  },\n"
          + "  \"tenants\" : {\n"
          + "    \"broker\":\"DefaultTenant\",\n"
          + "    \"server\":\"DefaultTenant\"\n"
          + "  },\n"
          + "  \"tableType\":\"OFFLINE\",\n"
          + "  \"metadata\": {}\n"
          + "}", TableConfig.class);
    }

    @Test
    public void oldConfNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.notDeclared(ForwardIndexType.INSTANCE);
      assertEquals(actual, expected);
    }

    @Test
    public void oldConfNotFound()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject("[]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.notDeclared(ForwardIndexType.INSTANCE);

      assertEquals(actual, expected);
    }

    @Test
    public void oldConfDisabled()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"properties\" : {"
                  + "      \"forwardIndexDisabled\": true"
                  + "    }\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declaredDisabled();

      assertEquals(actual, expected);
    }

    @Test
    public void oldConfEnableDefault()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\""
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.notDeclared(ForwardIndexType.INSTANCE);

      assertEquals(actual, expected);
    }

    @Test
    public void oldConfEnableDict()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"DICTIONARY\"\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.notDeclared(ForwardIndexType.INSTANCE);

      assertEquals(actual, expected);
    }

    @Test
    public void oldConfEnableRawDefault()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject("["
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\"\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declared(new ForwardIndexConfig.Builder()
              .withCompressionType(null)
              .withDeriveNumDocsPerChunk(false)
              .withRawIndexWriterVersion(2)
              .build());

      assertEquals(actual, expected);
    }

    @Test(dataProvider = "allChunkCompressionType", dataProviderClass = ForwardIndexTypeTest.class)
    public void oldConfEnableRawWithCompression(String compression)
        throws IOException {
      String valueJson = compression == null ? "null" : "\"" + compression + "\"";

      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\",\n"
                  + "    \"compressionCodec\": " + valueJson + "\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declared(new ForwardIndexConfig.Builder()
              .withCompressionType(compression == null ? null : ChunkCompressionType.valueOf(compression))
              .withDeriveNumDocsPerChunk(false)
              .withRawIndexWriterVersion(2)
              .build());

      assertEquals(actual, expected);
    }

    @Test
    public void oldConfEnableRawWithDeriveNumDocs()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\",\n"
                  + "    \"properties\" : {"
                  + "      \"deriveNumDocsPerChunkForRawIndex\": true"
                  + "    }\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declared(new ForwardIndexConfig.Builder()
              .withCompressionType(null)
              .withDeriveNumDocsPerChunk(true)
              .withRawIndexWriterVersion(2)
              .build());

      assertEquals(actual, expected);
    }

    @Test
    public void oldConfEnableRawWithRawIndexWriterVersion()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\",\n"
                  + "    \"properties\" : {"
                  + "      \"rawIndexWriterVersion\": 3"
                  + "    }\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      IndexDeclaration<ForwardIndexConfig> actual =
          ForwardIndexType.INSTANCE.deserializeSpreadConf(_tableConfig, _schema, "dimInt");
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declared(new ForwardIndexConfig.Builder()
              .withCompressionType(null)
              .withDeriveNumDocsPerChunk(false)
              .withRawIndexWriterVersion(3)
              .build());

      assertEquals(actual, expected);
    }

    @Test
    public void newConfigDisabled()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"indexes\" : {"
                  + "      \"forward\": null"
                  + "    }\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      Map<String, FieldIndexConfigs> confMap =
          FieldIndexConfigsUtil.createIndexConfigsByColName(_tableConfig, _schema, true);
      IndexDeclaration<ForwardIndexConfig> actual =
          confMap.get("dimInt").getConfig(ForwardIndexType.INSTANCE);
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declaredDisabled();

      assertEquals(actual, expected);
    }

    @Test
    public void newConfigDefault()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"indexes\" : {}\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      Map<String, FieldIndexConfigs> confMap =
          FieldIndexConfigsUtil.createIndexConfigsByColName(_tableConfig, _schema, true);
      IndexDeclaration<ForwardIndexConfig> actual =
          confMap.get("dimInt").getConfig(ForwardIndexType.INSTANCE);
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.notDeclared(ForwardIndexType.INSTANCE);

      assertEquals(actual, expected);
    }

    @Test(dataProvider = "allChunkCompressionType", dataProviderClass = ForwardIndexTypeTest.class)
    public void newConfigEnabled(String compression)
        throws IOException {
      String valueJson = compression == null ? "null" : "\"" + compression + "\"";
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject(""
                  + " [{\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"indexes\" : {"
                  + "      \"forward\": {"
                  + "        \"chunkCompressionType\": " + valueJson + ",\n"
                  + "        \"deriveNumDocsPerChunk\": true,\n"
                  + "        \"rawIndexWriterVersion\": 10\n"
                  + "      }"
                  + "    }\n"
                  + " }]", _fieldConfigListTypeRef)
      );
      Map<String, FieldIndexConfigs> confMap =
          FieldIndexConfigsUtil.createIndexConfigsByColName(_tableConfig, _schema, true);
      IndexDeclaration<ForwardIndexConfig> actual =
          confMap.get("dimInt").getConfig(ForwardIndexType.INSTANCE);
      IndexDeclaration<ForwardIndexConfig> expected =
          IndexDeclaration.declared(new ForwardIndexConfig.Builder()
              .withCompressionType(compression == null ? null : ChunkCompressionType.valueOf(compression))
              .withDeriveNumDocsPerChunk(true)
              .withRawIndexWriterVersion(10)
              .build());

      assertEquals(actual, expected);
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.forward(), ForwardIndexType.INSTANCE, "Standard index should use the same as "
        + "the ForwardIndexType static instance");
  }
}
