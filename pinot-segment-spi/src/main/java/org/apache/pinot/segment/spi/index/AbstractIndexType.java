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

  private final String _id;

  /// Lazily created caches of [#getConfig] and [#getReaderFactory].
  ///
  /// `volatile` is required, not just for the null checks in those methods: an index type is a process-wide singleton
  /// held by [IndexService] and is used concurrently by the threads that load, reload and refresh segments. Without
  /// `volatile` these values are published unsafely, so a racing thread can read the non-null reference while the
  /// contents are not yet visible to it.
  private volatile ColumnConfigDeserializer<C> _deserializer;
  private volatile IndexReaderFactory<IR> _readerFactory;

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
    ColumnConfigDeserializer<C> deserializer = _deserializer;
    if (deserializer == null) {
      deserializer = createDeserializer();
      _deserializer = deserializer;
    }
    try {
      return deserializer.deserialize(tableConfig, schema);
    } catch (MergedColumnConfigDeserializer.ConfigDeclaredTwiceException ex) {
      throw new MergedColumnConfigDeserializer.ConfigDeclaredTwiceException(ex.getColumn(), this, ex);
    }
  }

  /// Returns the reader factory, lazily creating and caching it on first access.
  ///
  /// Uses the racy-single-check idiom: two threads may each create a factory, but the factories are equivalent, so
  /// the duplicate work is harmless. Correctness relies on `_readerFactory` being `volatile`; see the field for why.
  @Override
  public IndexReaderFactory<IR> getReaderFactory() {
    IndexReaderFactory<IR> readerFactory = _readerFactory;
    if (readerFactory == null) {
      readerFactory = createReaderFactory();
      _readerFactory = readerFactory;
    }
    return readerFactory;
  }

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
            ? new ObjectMapper().createObjectNode()
            : new ObjectMapper().valueToTree(fieldConfig.getIndexes());
        JsonNode indexes = currentIndexes.set(getPrettyName(), configValue.toJsonNode());
        FieldConfig.Builder builder = new FieldConfig.Builder(fieldConfig);
        builder.withIndexes(indexes);
        fieldConfigList.remove(fieldConfig);
        fieldConfigList.add(builder.build());
      } else {
        JsonNode indexes = new ObjectMapper().createObjectNode().set(getPrettyName(), configValue.toJsonNode());
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
