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
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * TODO: implement mutable indexes.
 * @param <C> the class that represents how this object is configured.
 * @param <IR> the {@link IndexReader} subclass that should be used to read indexes of this type.
 * @param <IC> the {@link IndexCreator} subclass that should be used to create indexes of this type.
 */
public interface IndexType<C, IR extends IndexReader, IC extends IndexCreator> {

  /**
   * The unique id that identifies this index type.
   * In case there is more than one implementation for a given index, then all should share the same id in order to be
   * correctly registered in the {@link IndexService}.
   * This is also the value being used as the default toString implementation and the one used as keys when config is
   * specified.
   *
   * <p>Therefore the returned value for each index should be constant across different Pinot versions.</p>
   */
  String getId();

  /**
   * Returns an internal name used in some parts of the code (mainly in format v1 and metadata) that is persisted on
   * disk.
   *
   * <p>Therefore the returned value for each index should be constant across different Pinot versions.</p>
   */
  String getIndexName();

  default Class<C> getIndexConfigClass() {
    throw new UnsupportedOperationException();
  }

  /**
   * The default config when it is not explicitly defined by the user.
   *
   * Can return null if the index should be disabled by default.
   */
  @Nullable
  default C getDefaultConfig() {
    return null;
  }

  /**
   * This method is called to transform from a JSON node to a config object.
   *
   * This is usually used to deserialize {@link FieldConfig#getIndexes() fieldConfigLists.indexes.(indexId)}.
   * @throws IOException
   */
  @Nullable
  default C deserialize(JsonNode node)
      throws IOException {
    return JsonUtils.jsonNodeToObject(node, getIndexConfigClass());
  }

  /**
   * This method can be overridden by indexes that support alternative configuration formats where the configuration is
   * spread on different fields in the TableConfig.
   *
   * Configuration that can be read from the {@link FieldConfig#getIndexes() fieldConfigLists.indexes} shall not be
   * included here.
   */
  default IndexDeclaration<C> deserializeSpreadConf(TableConfig tableConfig, Schema schema, String column) {
    return IndexDeclaration.notDeclared(this);
  }

  /**
   * Transforms a config object into a Jackson {@link JsonNode}.
   */
  default JsonNode serialize(C config) {
    return JsonUtils.objectToJsonNode(config);
  }

  /**
   * Optional method that can be implemented to ignore the index creation.
   *
   * Sometimes it doesn't make sense to create an index, even when the user explicitly asked for it. For example, an
   * inverted index shouldn't be created when the column is sorted.
   *
   * Apache Pinot will call this method once all index configurations have been parsed and it is included in the
   * {@link FieldIndexConfigs} param.
   *
   * This method do not need to return false when the index type itself is not included in the {@link FieldIndexConfigs}
   * param.
   */
  default boolean shouldBeCreated(IndexCreationContext context, FieldIndexConfigs configs) {
    return true;
  }

  /**
   * Returns the {@link IndexCreator} that can should be used to create an index of this type with the given context
   * and configuration.
   *
   * The caller has the ownership of the creator and therefore it has to close it.
   * @param context The object that stores all the contextual information related to the index creation. Like the
   *                cardinality or the total number of documents.
   * @param indexConfig The index specific configuration that should be used.
   */
  default IC createIndexCreator(IndexCreationContext context, C indexConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the {@link IndexReaderFactory} that should be used to return readers for this type.
   */
  default IndexReaderFactory<IR> getReaderFactory() {
    throw new UnsupportedOperationException();
  }

  /**
   * This method is used to extract a compatible reader from a given ColumnIndexContainer.
   *
   * Most implementations just return {@link ColumnIndexContainer#getIndex(IndexType)}, but some may try to reuse other
   * indexes. For example, InvertedIndexType delegates on the ForwardIndexReader when it is sorted.
   */
  @Nullable
  default IR getIndexReader(ColumnIndexContainer indexContainer) {
    throw new UnsupportedOperationException();
  }

  default String getFileExtension(ColumnMetadata columnMetadata) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the index is stored as a buffer or not.
   *
   * Most indexes are stored as a buffer, but for example TextIndexType is stored in a separate lucene file.
   */
  default boolean storedAsBuffer() {
    return true;
  }

  default IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> configsByCol, @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    throw new UnsupportedOperationException();
  }
}
