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
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.segment.local.segment.index.AbstractSerdeIndexContract;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
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

  @DataProvider(name = "allCompressionCodec")
  public static Object[][] allCompressionCodec() {
    return new Object[][] {
        new Object[] {"PASS_THROUGH", ChunkCompressionType.PASS_THROUGH, null},
        new Object[] {"SNAPPY", ChunkCompressionType.SNAPPY, null},
        new Object[] {"ZSTANDARD", ChunkCompressionType.ZSTANDARD, null},
        new Object[] {"LZ4", ChunkCompressionType.LZ4, null},
        new Object[] {"MV_ENTRY_DICT", null, DictIdCompressionType.MV_ENTRY_DICT},
        new Object[] {null, null, null}
    };
  }

  @DataProvider(name = "allChunkCompression")
  public static Object[][] allChuckCompression() {
    return Arrays.stream(allCompressionCodec())
        .filter(values -> {
          Object compression = values[0];
          FieldConfig.CompressionCodec compressionCodec = compression == null ? null
              : FieldConfig.CompressionCodec.valueOf(compression.toString());
          return compressionCodec == null || compressionCodec.isApplicableToRawIndex();
        })
        .toArray(Object[][]::new);
  }

  @DataProvider(name = "allDictCompression")
  public static Object[][] allDictCompression() {
    return Arrays.stream(allCompressionCodec())
        .filter(values -> {
          Object compression = values[0];
          FieldConfig.CompressionCodec compressionCodec = compression == null ? null
              : FieldConfig.CompressionCodec.valueOf(compression.toString());
          return compressionCodec == null || compressionCodec.isApplicableToDictEncodedIndex();
        })
        .toArray(Object[][]::new);
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
    public void oldConfEnableDictWithMVEntryDictFormat()
        throws IOException {
      addFieldIndexConfig(""
          + "{"
          + "  \"name\": \"dimInt\","
          + "  \"encodingType\": \"DICTIONARY\","
          + "  \"compressionCodec\": \"MV_ENTRY_DICT\""
          + "}"
      );
      assertEquals(
          new ForwardIndexConfig.Builder().withDictIdCompressionType(DictIdCompressionType.MV_ENTRY_DICT).build());
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

      assertEquals(ForwardIndexConfig.DEFAULT);
    }

    @Test(dataProvider = "allCompressionCodec", dataProviderClass = ForwardIndexTypeTest.class)
    public void oldConfEnableRawWithCompression(String compression,
        ChunkCompressionType expectedChunkCompression, DictIdCompressionType expectedDictCompression)
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
                .withCompressionCodec(compression == null ? null : FieldConfig.CompressionCodec.valueOf(compression))
                .withCompressionType(expectedChunkCompression)
                .withDictIdCompressionType(expectedDictCompression)
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
    public void newConfigDisabled()
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

    @Test
    public void newConfigMVEntryDictFormat()
        throws IOException {
      addFieldIndexConfig(""
          + "{"
          + "  \"name\": \"dimInt\","
          + "  \"indexes\" : {"
          + "    \"forward\": {"
          + "      \"dictIdCompressionType\": \"MV_ENTRY_DICT\""
          + "    }"
          + "  }"
          + "}"
      );
      assertEquals(
          new ForwardIndexConfig.Builder().withDictIdCompressionType(DictIdCompressionType.MV_ENTRY_DICT).build());
    }

    @Test(dataProvider = "allChunkCompression", dataProviderClass = ForwardIndexTypeTest.class)
    public void newConfigEnabledWithChunkCompression(String compression,
        ChunkCompressionType expectedChunkCompression, DictIdCompressionType expectedDictCompression)
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
              .withCompressionType(expectedChunkCompression)
              .withDictIdCompressionType(expectedDictCompression)
              .withDeriveNumDocsPerChunk(true)
              .withRawIndexWriterVersion(10)
              .build()
      );
    }

    @Test(dataProvider = "allDictCompression", dataProviderClass = ForwardIndexTypeTest.class)
    public void newConfigEnabledWithDictCompression(String compression,
        ChunkCompressionType expectedChunkCompression, DictIdCompressionType expectedDictCompression)
        throws IOException {
      String valueJson = compression == null ? "null" : "\"" + compression + "\"";
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "      \"forward\": {"
          + "        \"dictIdCompressionType\": " + valueJson + ",\n"
          + "        \"deriveNumDocsPerChunk\": true,\n"
          + "        \"rawIndexWriterVersion\": 10\n"
          + "      }"
          + "    }\n"
          + " }"
      );

      assertEquals(
          new ForwardIndexConfig.Builder()
              .withCompressionCodec(compression == null ? null : FieldConfig.CompressionCodec.valueOf(compression))
              .withCompressionType(expectedChunkCompression)
              .withDictIdCompressionType(expectedDictCompression)
              .withDeriveNumDocsPerChunk(true)
              .withRawIndexWriterVersion(10)
              .build()
      );
    }
    @Test(dataProvider = "allCompressionCodec", dataProviderClass = ForwardIndexTypeTest.class)
    public void newConfigEnabledWithCompressionCodec(String compression,
        ChunkCompressionType expectedChunkCompression, DictIdCompressionType expectedDictCompression)
        throws IOException {
      FieldConfig.CompressionCodec compressionCodec = compression == null ? null
          : FieldConfig.CompressionCodec.valueOf(compression);

      String valueJson = compression == null ? "null" : "\"" + compression + "\"";
      addFieldIndexConfig(""
          + " {\n"
          + "    \"name\": \"dimInt\","
          + "    \"indexes\" : {"
          + "      \"forward\": {"
          + "        \"compressionCodec\": " + valueJson + ",\n"
          + "        \"deriveNumDocsPerChunk\": true,\n"
          + "        \"rawIndexWriterVersion\": 10\n"
          + "      }"
          + "    }\n"
          + " }"
      );

      assertEquals(
          new ForwardIndexConfig.Builder()
              .withCompressionType(expectedChunkCompression)
              .withDictIdCompressionType(expectedDictCompression)
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
