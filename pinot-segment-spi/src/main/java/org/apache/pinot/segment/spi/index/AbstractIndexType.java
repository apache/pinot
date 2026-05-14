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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public abstract class AbstractIndexType<C extends IndexConfig, IR extends IndexReader, IC extends IndexCreator>
    implements IndexType<C, IR, IC> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String _id;
  private ColumnConfigDeserializer<C> _deserializer;
  private IndexReaderFactory<IR> _readerFactory;

  protected ColumnConfigDeserializer<C> createDeserializer() {
    ColumnConfigDeserializer<C> fromIndexes =
        IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass());
    ColumnConfigDeserializer<C> fromLegacyConfigs = createDeserializerForLegacyConfigs();
    return fromLegacyConfigs != null ? fromIndexes.withExclusiveAlternative(fromLegacyConfigs) : fromIndexes;
  }

  @Nullable
  protected ColumnConfigDeserializer<C> createDeserializerForLegacyConfigs() {
    return null;
  }

  protected abstract IndexReaderFactory<IR> createReaderFactory();

  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
  }

  public AbstractIndexType(String id) {
    _id = id;
  }

  @Override
  public String getId() {
    return _id;
  }

  @Override
  public Map<String, C> getConfig(TableConfig tableConfig, Schema schema) {
    if (_deserializer == null) {
      _deserializer = createDeserializer();
    }
    try {
      return _deserializer.deserialize(tableConfig, schema);
    } catch (MergedColumnConfigDeserializer.ConfigDeclaredTwiceException ex) {
      throw new MergedColumnConfigDeserializer.ConfigDeclaredTwiceException(ex.getColumn(), this, ex);
    }
  }

  @Override
  public IndexReaderFactory<IR> getReaderFactory() {
    if (_readerFactory == null) {
      _readerFactory = createReaderFactory();
    }
    return _readerFactory;
  }

  /**
   * Migrates legacy {@link org.apache.pinot.spi.config.table.IndexingConfig} index settings into
   * the new {@code FieldConfig.indexes} JsonNode format.
   *
   * <p>The migration is <i>gap-filling</i>: for each column whose typed config is non-default, this
   * method writes the typed config's verbose JsonNode into {@code FieldConfig.indexes} <b>only when
   * the column does not already carry a new-format JsonNode for this index type</b>. Columns
   * already supplied in new format keep their original (possibly slim) JsonNode shape — this
   * preserves user-supplied keys verbatim through the round-trip and avoids fattening pure
   * new-format inputs with the typed-POJO bean-serializer defaults.
   *
   * <p>Both-formats coexistence resolves "new format wins": if the user supplied both a legacy
   * {@code indexingConfig.*} entry and a new-format {@code FieldConfig.indexes[prettyName]}
   * JsonNode for the same column + type, the new-format JsonNode is the source of truth. The
   * legacy entry is dropped by {@link #handleIndexSpecificCleanup} below, matching the documented
   * migration intent.
   */
  public void convertToNewFormat(TableConfig tableConfig, Schema schema) {
    Map<String, C> deserialize = getConfig(tableConfig, schema);
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList() == null
        ? new ArrayList<>()
        : tableConfig.getFieldConfigList();
    Map<String, FieldConfig> fieldConfigMap = fieldConfigList.stream()
        .collect(Collectors.toMap(FieldConfig::getName, Function.identity()));
    for (Map.Entry<String, C> entry : deserialize.entrySet()) {
      C configValue = entry.getValue();
      if (configValue.equals(getDefaultConfig())) {
        continue;
      }
      FieldConfig fieldConfig = fieldConfigMap.get(entry.getKey());
      if (fieldConfig != null) {
        ObjectNode currentIndexes = fieldConfig.getIndexes().isNull()
            ? MAPPER.createObjectNode()
            : MAPPER.valueToTree(fieldConfig.getIndexes());
        JsonNode existing = currentIndexes.get(getPrettyName());
        if (existing != null && !existing.isNull()) {
          // Column already carries a new-format JsonNode for this index type — preserve the
          // user's shape verbatim. Legacy-only inputs fall through to the set() branch below;
          // both-formats coexistence resolves to "new format wins".
          continue;
        }
        currentIndexes.set(getPrettyName(), configValue.toJsonNode());
        FieldConfig.Builder builder = new FieldConfig.Builder(fieldConfig);
        builder.withIndexes(currentIndexes);
        fieldConfigList.remove(fieldConfig);
        fieldConfigList.add(builder.build());
      } else {
        JsonNode indexes = MAPPER.createObjectNode().set(getPrettyName(), configValue.toJsonNode());
        FieldConfig.Builder builder = new FieldConfig.Builder(entry.getKey());
        builder.withIndexes(indexes);
        builder.withEncodingType(FieldConfig.EncodingType.DICTIONARY);
        fieldConfigList.add(builder.build());
      }
    }
    tableConfig.setFieldConfigList(fieldConfigList);
    handleIndexSpecificCleanup(tableConfig);
  }

  @Override
  public String toString() {
    return _id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractIndexType<?, ?, ?> that = (AbstractIndexType<?, ?, ?>) o;
    return _id.equals(that._id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_id);
  }
}
