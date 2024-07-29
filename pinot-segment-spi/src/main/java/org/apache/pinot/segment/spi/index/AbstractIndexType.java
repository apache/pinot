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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public abstract class AbstractIndexType<C extends IndexConfig, IR extends IndexReader, IC extends IndexCreator>
    implements IndexType<C, IR, IC> {

  private final String _id;
  private ColumnConfigDeserializer<C> _deserializer;
  private IndexReaderFactory<IR> _readerFactory;

  protected abstract ColumnConfigDeserializer<C> createDeserializer();

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
