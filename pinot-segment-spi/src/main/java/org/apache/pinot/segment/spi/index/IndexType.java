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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/// @param <C> the class that represents how this object is configured.
/// @param <IR> the [IndexReader] subclass that should be used to read indexes of this type.
/// @param <IC> the [IndexCreator] subclass that should be used to create indexes of this type.
///
/// ## Backward compatibility
///
/// [#requiresDictionary(FieldSpec, IndexConfig)] and
/// [#shouldInvalidateOnDictionaryChange(FieldSpec, IndexConfig)] are declared *abstract* rather than `default` on
/// purpose: every `IndexType` implementation must explicitly declare its dictionary contract. **This is a
/// binary-incompatible change.** External plugins compiled against an older version of this SPI will fail to load
/// (or fail to compile) until they implement both methods. Plugin authors upgrading across this change should
/// consult the Javadoc on each method and pick a value consistent with whether their on-disk index references
/// dictionary IDs.
public interface IndexType<C extends IndexConfig, IR extends IndexReader, IC extends IndexCreator> {
  /**
   * Returns the {@link BuildLifecycle} for this index type. This is used to determine when the index should be built.
   */
  default BuildLifecycle getIndexBuildLifecycle() {
    return BuildLifecycle.DURING_SEGMENT_CREATION;
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

  /// Validates if the index config is valid for the given field spec and other index configs. [IllegalStateException]
  /// is thrown if the validation fails.
  default void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
  }

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

  // TODO: Consider passing in IndexLoadingConfig
  IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig);

  /// Declares whether the on-disk representation of this index *references dictionary IDs* for the given column,
  /// meaning the index cannot be built or read without a dictionary being present.
  ///
  /// This is consulted at segment-creation and segment-reload time to decide whether a shared dictionary must be
  /// materialized for a column whose forward index is RAW-encoded but which carries an enabled secondary index that
  /// needs dictionary IDs (for example `inverted`, `fst`, `ifst`). When any enabled index on the column returns
  /// `true`, the loader auto-creates a shared standalone dictionary for the column even if the user did not
  /// explicitly request one.
  ///
  /// Implementation guidance:
  ///
  ///   - Return `true` only when the on-disk representation of this index references dictionary IDs (e.g. inverted
  ///     posting lists keyed by dict ID, FST/IFST built over the sorted dictionary).
  ///   - Return `false` when the index is computed directly from raw column values (e.g. bloom over hashed values,
  ///     json/text over the raw payload, vector/h3 over the geometry).
  ///   - This must be a pure function of `fieldSpec` and `indexConfig`; it is called during config validation and
  ///     segment creation, not against a live segment. Implementations should NOT short-circuit on
  ///     `indexConfig.isDisabled()` — the caller filters out disabled configs.
  ///
  /// This method is intentionally abstract so that every [IndexType] implementation must explicitly declare its
  /// dictionary dependency. New index types must opt in by considering the contract above.
  ///
  /// @param fieldSpec the column's field spec
  /// @param indexConfig the per-column config for this specific index type
  boolean requiresDictionary(FieldSpec fieldSpec, C indexConfig);

  /// Declares whether the on-disk representation of this index depends on whether a dictionary exists for the
  /// column. If `true`, an existing on-disk index of this type must be deleted and rebuilt when the dictionary for
  /// the column is added or removed across a segment reload.
  ///
  /// Used by segment reload (e.g. `SegmentPreProcessor` / per-index handlers) to decide whether a stale index file
  /// can be reused or must be dropped and rebuilt after a dictionary enable/disable transition. This covers exactly
  /// the presence/absence change of the dictionary:
  ///
  ///   - dictionary previously absent → now enabled (column gained a dictionary), or
  ///   - dictionary previously present → now disabled (column lost its dictionary).
  ///
  /// It does *not* cover changes to dictionary *configuration* while the dictionary remains enabled (heap mode,
  /// capacity, etc.) — those are handled by the per-index [IndexHandler#needUpdateIndices] hook.
  ///
  /// Implementation guidance:
  ///
  ///   - Return `true` when the index's on-disk format differs depending on dictionary presence — i.e.
  ///     enabling/disabling the dictionary would silently leave a wrong-result index on disk if not rebuilt
  ///     (inverted, fst/ifst, range).
  ///   - Return `false` when the index is computed from raw column values and is therefore identical regardless of
  ///     dictionary presence (bloom, json, text, vector, h3, null-value, forward index — encoding is reconciled
  ///     with FieldConfig.encodingType independently of dictionary state).
  ///   - The dictionary index type itself returns `false` — the dictionary *is* what is changing; its own rebuild
  ///     is driven by the dictionary handler, not by this hook.
  ///   - This must be a pure function of `fieldSpec` and `indexConfig`. Implementations should NOT short-circuit
  ///     on `indexConfig.isDisabled()` — the caller filters out disabled configs.
  ///
  /// This method is intentionally abstract so that every [IndexType] implementation must explicitly declare its
  /// rebuild requirement. New index types must opt in by considering the contract above.
  ///
  /// @param fieldSpec the column's field spec
  /// @param indexConfig the per-column config for this specific index type
  boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, C indexConfig);

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

  enum BuildLifecycle {
    /**
     * The index will be built during segment creation, using the {@link IndexCreator#add} call for each of the column
     * values being added.
     */
    DURING_SEGMENT_CREATION,

    /**
     * The index will be build post the segment file has been created, using the {@link IndexHandler#updateIndices} call
     * This is useful for indexes that may need the entire prebuilt segment to be available before they can be built.
     */
    POST_SEGMENT_CREATION,

    /**
     * The index's built lifecycle is managed in a custom manner.
     */
    CUSTOM
  }
}
