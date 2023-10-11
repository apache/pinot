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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


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

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(ForwardIndexConfig expected) {
      Assert.assertEquals(getActualConfig("dimInt", StandardIndexes.forward()), expected);
    }

    @Test
    public void oldConfNull()
        throws JsonProcessingException {
      _tableConfig.setIndexingConfig(null);

      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test
    public void oldConfNotFound()
        throws IOException {
      _tableConfig.setFieldConfigList(
          JsonUtils.stringToObject("[]", _fieldConfigListTypeRef)
      );

      assertEquals(ForwardIndexConfig.DEFAULT);
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

      assertEquals(ForwardIndexConfig.DISABLED);
    }

    @Test
    public void oldConfEnableDefault()
        throws IOException {
      addFieldIndexConfig(
          "{\n"
          + "    \"name\": \"dimInt\""
          + " }"
      );

      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test
    public void oldConfNoDictionaryConfig()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryConfig(
          JsonUtils.stringToObject("{\"dimInt\": \"RAW\"}",
              new TypeReference<Map<String, String>>() {
              })
      );
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"compressionCodec\": \"SNAPPY\"\n"
          + " }"
      );

      assertEquals(
          new ForwardIndexConfig.Builder()
              .withCompressionType(ChunkCompressionType.SNAPPY)
              .withDeriveNumDocsPerChunk(false)
              .withRawIndexWriterVersion(2)
              .build()
      );
    }

    @Test
    public void oldConfNoDictionaryColumns()
        throws IOException {
      _tableConfig.getIndexingConfig().setNoDictionaryColumns(
          JsonUtils.stringToObject("[\"dimInt\"]", _stringListTypeRef));
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"compressionCodec\": \"SNAPPY\"\n"
          + " }"
      );

      assertEquals(
          new ForwardIndexConfig.Builder()
              .withCompressionType(ChunkCompressionType.SNAPPY)
              .withDeriveNumDocsPerChunk(false)
              .withRawIndexWriterVersion(2)
              .build()
      );
    }

    @Test
    public void oldConfEnableDict()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"encodingType\": \"DICTIONARY\"\n"
          + " }"
      );
      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test
    public void oldConfEnableDictWithSnappyCompression()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"encodingType\": \"DICTIONARY\",\n"
          + "    \"compressionCodec\": \"SNAPPY\"\n"
          + " }"
      );
      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test
    public void oldConfEnableDictWithLZ4Compression()
        throws IOException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"encodingType\": \"DICTIONARY\",\n"
          + "    \"compressionCodec\": \"LZ4\"\n"
          + " }"
      );
      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test
    public void oldConfEnableRawDefault()
        throws IOException {
      addFieldIndexConfig(""
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\"\n"
                  + " }"
      );

      assertEquals(
          new ForwardIndexConfig.Builder()
              .withCompressionType(null)
              .withDeriveNumDocsPerChunk(false)
              .withRawIndexWriterVersion(ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION)
              .build()
      );
    }

    @Test(dataProvider = "allChunkCompressionType", dataProviderClass = ForwardIndexTypeTest.class)
    public void oldConfEnableRawWithCompression(String compression)
        throws IOException {
      String valueJson = compression == null ? "null" : "\"" + compression + "\"";

      addFieldIndexConfig(""
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\",\n"
                  + "    \"compressionCodec\": " + valueJson + "\n"
                  + " }"
      );

      assertEquals(
            new ForwardIndexConfig.Builder()
                .withCompressionType(compression == null ? null : ChunkCompressionType.valueOf(compression))
                .withDeriveNumDocsPerChunk(false)
                .withRawIndexWriterVersion(ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION)
                .build()
      );
    }

    @Test
    public void oldConfEnableRawWithDeriveNumDocs()
        throws IOException {
      addFieldIndexConfig(""
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\",\n"
                  + "    \"properties\" : {"
                  + "      \"deriveNumDocsPerChunkForRawIndex\": true"
                  + "    }\n"
                  + " }"
      );

      assertEquals(new ForwardIndexConfig.Builder()
          .withCompressionType(null)
          .withDeriveNumDocsPerChunk(true)
          .withRawIndexWriterVersion(ForwardIndexConfig.DEFAULT_RAW_WRITER_VERSION)
          .build());
    }

    @Test
    public void oldConfEnableRawWithRawIndexWriterVersion()
        throws IOException {
      addFieldIndexConfig(""
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"encodingType\": \"RAW\",\n"
                  + "    \"properties\" : {"
                  + "      \"rawIndexWriterVersion\": 3"
                  + "    }\n"
                  + " }"
      );

      assertEquals(new ForwardIndexConfig.Builder()
          .withCompressionType(null)
          .withDeriveNumDocsPerChunk(false)
          .withRawIndexWriterVersion(3)
          .build());
    }

    @Test
    public void newConfigDisabled2()
        throws IOException {
      addFieldIndexConfig("{\n"
              + "    \"name\": \"dimInt\",\n"
              + "    \"indexes\" : {\n"
              + "      \"forward\": {\n"
              + "          \"disabled\": true\n"
              + "       }\n"
              + "    }\n"
              + "  }"
      );
      assertEquals(ForwardIndexConfig.DISABLED);
    }

    @Test
    public void newConfigDefault()
        throws IOException {
      addFieldIndexConfig(""
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"indexes\" : {}\n"
                  + " }"
      );

      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test(dataProvider = "allChunkCompressionType", dataProviderClass = ForwardIndexTypeTest.class)
    public void newConfigEnabled(String compression)
        throws IOException {
      String valueJson = compression == null ? "null" : "\"" + compression + "\"";
      addFieldIndexConfig(""
                  + " {\n"
                  + "    \"name\": \"dimInt\","
                  + "    \"indexes\" : {"
                  + "      \"forward\": {"
                  + "        \"chunkCompressionType\": " + valueJson + ",\n"
                  + "        \"deriveNumDocsPerChunk\": true,\n"
                  + "        \"rawIndexWriterVersion\": 10\n"
                  + "      }"
                  + "    }\n"
                  + " }"
      );

      assertEquals(
          new ForwardIndexConfig.Builder()
              .withCompressionType(compression == null ? null : ChunkCompressionType.valueOf(compression))
              .withDeriveNumDocsPerChunk(true)
              .withRawIndexWriterVersion(10)
              .build()
      );
    }

    @Test
    public void oldToNewConfConversion()
        throws JsonProcessingException {
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\",\n"
          + "    \"compressionCodec\": \"PASS_THROUGH\",\n"
          + "    \"encodingType\": \"RAW\"\n"
          + " }"
      );
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimInt"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(ForwardIndexType.INDEX_DISPLAY_NAME));
    }
  }

  @Test
  public void testStandardIndex() {
    assertSame(StandardIndexes.forward(), StandardIndexes.forward(), "Standard index should use the same as "
        + "the ForwardIndexType static instance");
  }
}
