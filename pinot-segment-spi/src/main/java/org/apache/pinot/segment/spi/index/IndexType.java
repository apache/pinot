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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * @param <C> the class that represents how this object is configured.
 * @param <IR> the {@link IndexReader} subclass that should be used to read indexes of this type.
 * @param <IC> the {@link IndexCreator} subclass that should be used to create indexes of this type.
 */
public interface IndexType<C extends IndexConfig, IR extends IndexReader, IC extends IndexCreator> {
  enum IndexBuildLifecycle {
    DURING_SEGMENT_CREATION,
    POST_SEGMENT_CREATION,
    CUSTOM
  }

  /**
   * The unique id that identifies this index type.
   * <p>The returned value for each index should be constant across different Pinot versions as it is used as:</p>
   *
   * <ul>
   *   <li>The key used when the index is registered in IndexService.</li>
   *   <li>The internal identification in v1 files and metadata persisted on disk.</li>
   *   <li>The default toString implementation.</li>
   *   <li>The key that identifies the index config in the indexes section inside
   *   {@link org.apache.pinot.spi.config.table.FieldConfig}, although specific index types may choose to read other
   *   names (for example, <code>inverted_index</code> may read <code>inverted</code> key.</li>
   * </ul>
   */
  String getId();

  Class<C> getIndexConfigClass();

  /**
   * The default config when it is not explicitly defined by the user.
   */
  C getDefaultConfig();

  Map<String, C> getConfig(TableConfig tableConfig, Schema schema);

  default String getPrettyName() {
    return getId();
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
  IC createIndexCreator(IndexCreationContext context, C indexConfig)
      throws Exception;

  /**
   * Returns the {@link IndexReaderFactory} that should be used to return readers for this type.
   */
  IndexReaderFactory<IR> getReaderFactory();

  /**
   * This method is used to extract a compatible reader from a given ColumnIndexContainer.
   *
   * Most implementations just return {@link ColumnIndexContainer#getIndex(IndexType)}, but some may try to reuse other
   * indexes. For example, InvertedIndexType delegates on the ForwardIndexReader when it is sorted.
   */
  @Nullable
  default IR getIndexReader(ColumnIndexContainer indexContainer) {
    return indexContainer.getIndex(this);
  }

  /**
   * Returns the possible file extensions for this index type.
   *
   * @param columnMetadata an optional filter. In case it is provided, the index type will do its best to try to filter
   *                      which extensions are valid. See ForwardIndexType.
   */
  List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata);

  IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig);

  /**
   * This method is used to perform in place conversion of provided {@link TableConfig} to newer format
   * related to the IndexType that implements it.
   *
   * {@link AbstractIndexType#convertToNewFormat(TableConfig, Schema)} ensures all the index information from old format
   * is made available in the new format while it depends on the individual index types to handle the data cleanup from
   * old format.
   */
  void convertToNewFormat(TableConfig tableConfig, Schema schema);

  /**
   * Creates a mutable index.
   *
   * Implementations return null if the index type doesn't support mutable indexes. Some indexes may support mutable
   * index only in some conditions. For example, they may only be supported if there is a dictionary or if the column is
   * single-valued.
   */
  @Nullable
  default MutableIndex createMutableIndex(MutableIndexContext context, C config) {
    return null;
  }

  default IndexBuildLifecycle getIndexBuildLifecycle() {
    return IndexBuildLifecycle.DURING_SEGMENT_CREATION;
  }
}
