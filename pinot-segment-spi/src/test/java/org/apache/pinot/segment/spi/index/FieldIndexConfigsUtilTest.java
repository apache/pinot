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
package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FieldIndexConfigsUtilTest {

  private static final FieldSpec INT_SPEC = new DimensionFieldSpec("x", FieldSpec.DataType.INT, true);

  private IndexService _originalIndexService;
  private StubIndexType _dictType;
  private StubIndexType _rangeType;
  private StubIndexType _bloomType;

  @BeforeMethod
  public void setUp() {
    _originalIndexService = IndexService.getInstance();
    _dictType = new StubIndexType(StandardIndexes.DICTIONARY_ID, StandardIndexes.DICTIONARY_ID);
    _rangeType = new StubIndexType(StandardIndexes.RANGE_ID, "range");
    _bloomType = new StubIndexType(StandardIndexes.BLOOM_FILTER_ID, "bloom");
    installIndexService(_dictType, _rangeType, _bloomType);
  }

  @AfterMethod
  public void tearDown() {
    IndexService.setInstance(_originalIndexService);
  }

  @Test
  public void testParsesIndexesAndRawEncoding()
      throws Exception {
    JsonNode indexes = JsonUtils.stringToJsonNode("{\"range\": {}, \"bloom\": {}}");
    FieldConfig fc = new FieldConfig.Builder("x")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();

    FieldIndexConfigs configs = FieldIndexConfigsUtil.fromFieldConfig(fc, INT_SPEC);

    // RAW encoding disables the dictionary — verify via the stored DictionaryIndexConfig constant.
    IndexConfig dictConfig = configs.getConfig(_dictType);
    assertTrue(dictConfig.isDisabled(), "RAW => dictionary disabled");

    // range and bloom should be enabled because the JSON contained their pretty-name keys.
    IndexConfig rangeConfig = configs.getConfig(_rangeType);
    assertTrue(rangeConfig.isEnabled(), "range config from JSON should be enabled");

    IndexConfig bloomConfig = configs.getConfig(_bloomType);
    assertTrue(bloomConfig.isEnabled(), "bloom config from JSON should be enabled");
  }

  @Test
  public void testNullFieldConfigUsesDictionaryDefault() {
    FieldIndexConfigs configs = FieldIndexConfigsUtil.fromFieldConfig(null, INT_SPEC);
    // null fieldConfig => dictionary should use DictionaryIndexConfig.DEFAULT (not disabled)
    IndexConfig dictConfig = configs.getConfig(_dictType);
    assertFalse(dictConfig.isDisabled(), "null => dictionary enabled");
  }

  private void installIndexService(StubIndexType... types) {
    Set<IndexPlugin<?>> plugins = new HashSet<>();
    int priority = 0;
    for (StubIndexType type : types) {
      final int p = priority++;
      final StubIndexType t = type;
      plugins.add(new IndexPlugin<StubIndexType>() {
        @Override
        public StubIndexType getIndexType() {
          return t;
        }

        @Override
        public int getPriority() {
          return p;
        }
      });
    }
    IndexService.setInstance(new IndexService(plugins));
  }

  private static final class StubIndexType implements IndexType<IndexConfig, IndexReader, IndexCreator> {
    private final String _id;
    private final String _prettyName;

    StubIndexType(String id, String prettyName) {
      _id = id;
      _prettyName = prettyName;
    }

    @Override
    public String getId() {
      return _id;
    }

    @Override
    public String getPrettyName() {
      return _prettyName;
    }

    @Override
    public Class<IndexConfig> getIndexConfigClass() {
      return IndexConfig.class;
    }

    @Override
    public IndexConfig getDefaultConfig() {
      return IndexConfig.DISABLED;
    }

    @Override
    public Map<String, IndexConfig> getConfig(TableConfig tableConfig, Schema schema) {
      return Map.of();
    }

    @Override
    public IndexCreator createIndexCreator(IndexCreationContext context, IndexConfig indexConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexReaderFactory<IndexReader> getReaderFactory() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
      return List.of();
    }

    @Override
    public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
        Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean requiresDictionary(FieldSpec fieldSpec, IndexConfig indexConfig) {
      return false;
    }

    @Override
    public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, IndexConfig indexConfig) {
      return false;
    }

    @Override
    public void convertToNewFormat(TableConfig tableConfig, Schema schema) {
    }
  }
}
