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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DictionaryIndexConfigTest {

  private IndexService _originalIndexService;

  @BeforeMethod
  public void setUp() {
    _originalIndexService = IndexService.getInstance();
  }

  @AfterMethod
  public void tearDown() {
    IndexService.setInstance(_originalIndexService);
  }

  @Test
  public void withEmptyConf()
      throws JsonProcessingException {
    String confStr = "{}";
    DictionaryIndexConfig config = JsonUtils.stringToObject(confStr, DictionaryIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isOnHeap(), "Unexpected onHeap");
    assertFalse(config.isUseVarLengthDictionary(), "Unexpected useVarLengthDictionary");
  }

  @Test
  public void withDisabledNull()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": null}";
    DictionaryIndexConfig config = JsonUtils.stringToObject(confStr, DictionaryIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isOnHeap(), "Unexpected onHeap");
    assertFalse(config.isUseVarLengthDictionary(), "Unexpected useVarLengthDictionary");
  }

  @Test
  public void withDisabledFalse()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": false}";
    DictionaryIndexConfig config = JsonUtils.stringToObject(confStr, DictionaryIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isOnHeap(), "Unexpected onHeap");
    assertFalse(config.isUseVarLengthDictionary(), "Unexpected useVarLengthDictionary");
  }

  @Test
  public void withDisabledTrue()
      throws JsonProcessingException {
    String confStr = "{\"disabled\": true}";
    DictionaryIndexConfig config = JsonUtils.stringToObject(confStr, DictionaryIndexConfig.class);

    assertTrue(config.isDisabled(), "Unexpected disabled");
    assertFalse(config.isOnHeap(), "Unexpected onHeap");
    assertFalse(config.isUseVarLengthDictionary(), "Unexpected useVarLengthDictionary");
  }

  @Test
  public void withSomeData()
      throws JsonProcessingException {
    String confStr = "{\n"
        + "  \"onHeap\": true,\n"
        + "  \"useVarLengthDictionary\": true\n"
        + "}";
    DictionaryIndexConfig config = JsonUtils.stringToObject(confStr, DictionaryIndexConfig.class);

    assertFalse(config.isDisabled(), "Unexpected disabled");
    assertTrue(config.isOnHeap(), "Unexpected onHeap");
    assertTrue(config.isUseVarLengthDictionary(), "Unexpected useVarLengthDictionary");
  }

  @Test
  public void isDictionaryRequiredReturnsTrueWhenAnyEnabledIndexNeedsDictionary() {
    StubIndexType<?> dictDependent = StubIndexType.dictionaryDependent("needs-dict");
    StubIndexType<?> independent = StubIndexType.independent("no-dict");
    installIndexService(dictDependent, independent);

    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .addUnsafe(dictDependent, IndexConfig.ENABLED)
        .addUnsafe(independent, IndexConfig.ENABLED)
        .build();

    assertTrue(DictionaryIndexConfig.requiresDictionary(fieldSpec, configs));
  }

  @Test
  public void isDictionaryRequiredReturnsFalseWhenDictionaryDependentIndexIsDisabled() {
    StubIndexType<?> dictDependent = StubIndexType.dictionaryDependent("needs-dict");
    StubIndexType<?> independent = StubIndexType.independent("no-dict");
    installIndexService(dictDependent, independent);

    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true);
    // dictDependent left at default (disabled), independent enabled
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .addUnsafe(independent, IndexConfig.ENABLED)
        .build();

    assertFalse(DictionaryIndexConfig.requiresDictionary(fieldSpec, configs));
  }

  @Test
  public void isDictionaryRequiredReturnsFalseWhenAllIndexesAreIndependent() {
    StubIndexType<?> a = StubIndexType.independent("a");
    StubIndexType<?> b = StubIndexType.independent("b");
    installIndexService(a, b);

    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .addUnsafe(a, IndexConfig.ENABLED)
        .addUnsafe(b, IndexConfig.ENABLED)
        .build();

    assertFalse(DictionaryIndexConfig.requiresDictionary(fieldSpec, configs));
  }

  @Test
  public void getIndexTypesToInvalidateReturnsOnlyIndexesThatDeclareInvalidation() {
    StubIndexType<?> rebuilds = StubIndexType.rebuildOnDictionaryChange("rebuilds");
    StubIndexType<?> independent = StubIndexType.independent("independent");
    StubIndexType<?> rebuildsButDisabled = StubIndexType.rebuildOnDictionaryChange("rebuilds-disabled");
    installIndexService(rebuilds, independent, rebuildsButDisabled);

    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .addUnsafe(rebuilds, IndexConfig.ENABLED)
        .addUnsafe(independent, IndexConfig.ENABLED)
        // rebuildsButDisabled left at default (disabled)
        .build();

    Set<IndexType<?, ?, ?>> result =
        DictionaryIndexConfig.getIndexTypesToInvalidateOnDictionaryChange(fieldSpec, configs);

    assertEquals(result, Set.of(rebuilds));
  }

  @Test
  public void getIndexTypesToInvalidateReturnsEmptyWhenNothingDeclaresInvalidation() {
    StubIndexType<?> a = StubIndexType.independent("a");
    StubIndexType<?> b = StubIndexType.independent("b");
    installIndexService(a, b);

    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .addUnsafe(a, IndexConfig.ENABLED)
        .addUnsafe(b, IndexConfig.ENABLED)
        .build();

    assertTrue(DictionaryIndexConfig.getIndexTypesToInvalidateOnDictionaryChange(fieldSpec, configs).isEmpty());
  }

  private static void installIndexService(StubIndexType<?>... types) {
    Set<IndexPlugin<?>> plugins = new HashSet<>();
    int priority = 0;
    for (StubIndexType<?> type : types) {
      plugins.add(new StubIndexPlugin<>(type, priority++));
    }
    IndexService.setInstance(new IndexService(plugins));
  }

  private static final class DimensionFieldSpec extends FieldSpec {
    DimensionFieldSpec(String name, DataType dataType, boolean singleValue) {
      super(name, dataType, singleValue);
    }

    @Override
    public FieldType getFieldType() {
      return FieldType.DIMENSION;
    }
  }

  private static final class StubIndexPlugin<T extends StubIndexType<?>> implements IndexPlugin<T> {
    private final T _type;
    private final int _priority;

    StubIndexPlugin(T type, int priority) {
      _type = type;
      _priority = priority;
    }

    @Override
    public T getIndexType() {
      return _type;
    }

    @Override
    public int getPriority() {
      return _priority;
    }
  }

  /**
   * Minimal {@link IndexType} stub used to drive the {@link DictionaryIndexConfig} static helpers under a controlled
   * {@link IndexService}. All segment-creation/read entry points throw — the helpers only consult the two new
   * dictionary-related methods plus {@link #getId()}.
   */
  private static final class StubIndexType<C extends IndexConfig> implements IndexType<IndexConfig, IndexReader,
      IndexCreator> {
    private final String _id;
    private final boolean _requiresDictionary;
    private final boolean _invalidateOnDictionaryChange;

    static StubIndexType<?> dictionaryDependent(String id) {
      return new StubIndexType<>(id, true, true);
    }

    static StubIndexType<?> independent(String id) {
      return new StubIndexType<>(id, false, false);
    }

    static StubIndexType<?> rebuildOnDictionaryChange(String id) {
      return new StubIndexType<>(id, false, true);
    }

    private StubIndexType(String id, boolean requiresDictionary, boolean invalidateOnDictionaryChange) {
      _id = id;
      _requiresDictionary = requiresDictionary;
      _invalidateOnDictionaryChange = invalidateOnDictionaryChange;
    }

    @Override
    public String getId() {
      return _id;
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
      return _requiresDictionary;
    }

    @Override
    public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, IndexConfig indexConfig) {
      return _invalidateOnDictionaryChange;
    }

    @Override
    public void convertToNewFormat(TableConfig tableConfig, Schema schema) {
    }
  }
}
