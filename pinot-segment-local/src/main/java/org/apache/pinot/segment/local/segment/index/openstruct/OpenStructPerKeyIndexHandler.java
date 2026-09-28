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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Applies an OPEN_STRUCT column's per-key index settings to its materialized children at load time.
///
/// A materialized child (`col$key`) is a real column of the segment but not of the table schema, so
/// [FieldIndexConfigsUtil#createIndexConfigsByColName] -- which walks `schema.getColumnNames()` -- never produces
/// an entry for it. Every standard handler derives its column set from that map, so without an entry a child is
/// invisible to all of them and its `valueFieldConfigs` are silently unused.
///
/// This handler closes that gap the same way the segment-build path does: it derives each child's
/// [FieldIndexConfigs] straight from its `valueFieldConfigs` entry via
/// [FieldIndexConfigsUtil#fromFieldConfig], which exists for exactly this case, and then delegates to the ordinary
/// per-index handlers. Nothing here re-implements index construction -- the children are indexed by the same code
/// that indexes every other column, just told about them.
///
/// **What this makes possible.** Before it, per-key indexes existed only as a side effect of segment creation
/// (`OpenStructColumnSplitter` builds them while writing the child columns), so they were frozen once a segment
/// existed: adding a range index to a key and reloading did nothing. Now a reload applies key index changes the
/// same way it does for any other column.
///
/// The dense/sparse split itself is **not** revisited here. Which keys are materialized is decided when the segment
/// is written; this handler only indexes the children that already exist.
public class OpenStructPerKeyIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpenStructPerKeyIndexHandler.class);

  private final SegmentDirectory _segmentDirectory;
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final Map<String, FieldIndexConfigs> _childConfigs;

  public OpenStructPerKeyIndexHandler(SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
    _segmentDirectory = segmentDirectory;
    _tableConfig = tableConfig;
    _childConfigs = resolveChildConfigs(segmentDirectory, configsByCol, schema);
    _schema = _childConfigs.isEmpty() ? schema : schemaWithChildren(segmentDirectory, schema, _childConfigs.keySet());
  }

  /// The table schema plus one entry per materialized child.
  ///
  /// The delegates are ordinary handlers and some of them look the column up in the schema before doing anything --
  /// [ForwardIndexHandler] skips a column it cannot find, which is precisely how a key silently misses out on the
  /// raw-to-dictionary conversion an inverted index depends on. The children are real columns of the segment, so
  /// their specs come from segment metadata; adding them here makes the delegates treat a key exactly as they treat
  /// any other column. The table's own schema is left untouched -- this copy exists only for the delegates.
  private static Schema schemaWithChildren(SegmentDirectory segmentDirectory, Schema schema,
      Set<String> childColumns) {
    Schema augmented = new Schema();
    augmented.setSchemaName(schema.getSchemaName());
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      augmented.addField(fieldSpec);
    }
    Map<String, ColumnMetadata> columnMetadataMap = segmentDirectory.getSegmentMetadata().getColumnMetadataMap();
    for (String childColumn : childColumns) {
      ColumnMetadata childMetadata = columnMetadataMap.get(childColumn);
      if (childMetadata != null && !augmented.hasColumn(childColumn)) {
        augmented.addField(childMetadata.getFieldSpec());
      }
    }
    return augmented;
  }

  /// One [FieldIndexConfigs] per materialized child, derived from its parent's per-key settings.
  ///
  /// The children come from segment metadata rather than the schema, because the schema does not know they exist.
  /// A child whose parent has no OPEN_STRUCT config, or whose parent column is gone, contributes nothing.
  private static Map<String, FieldIndexConfigs> resolveChildConfigs(SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> configsByCol, Schema schema) {
    Map<String, FieldIndexConfigs> childConfigs = new HashMap<>();
    Map<String, ColumnMetadata> columnMetadataMap = segmentDirectory.getSegmentMetadata().getColumnMetadataMap();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      String childColumn = entry.getKey();
      ColumnMetadata childMetadata = entry.getValue();
      if (!(childMetadata instanceof ColumnMetadataImpl childImpl) || !childImpl.isMaterializedChild()) {
        continue;
      }
      String parent = childImpl.getParentColumn();
      OpenStructIndexConfig parentConfig = openStructConfig(configsByCol.get(parent));
      if (parentConfig == null || !parentConfig.isEnabled()) {
        continue;
      }
      // The sparse blob is one shared column holding every unmaterialized key, so a per-key setting has no
      // meaning for it. Its own index (a JSON index over the blob) is configured on the parent, not per key.
      if (OpenStructNaming.isSparseColumn(childColumn)) {
        continue;
      }
      String key = OpenStructNaming.parseKey(childColumn);
      FieldConfig keyFieldConfig = parentConfig.getValueFieldConfig(key);
      if (keyFieldConfig == null) {
        keyFieldConfig = parentConfig.getDefaultValueFieldConfig();
      }
      FieldSpec childFieldSpec = childMetadata.getFieldSpec();
      childConfigs.put(childColumn, FieldIndexConfigsUtil.fromFieldConfig(keyFieldConfig, childFieldSpec));
    }
    return childConfigs;
  }

  @Nullable
  private static OpenStructIndexConfig openStructConfig(@Nullable FieldIndexConfigs parentConfigs) {
    return parentConfigs == null ? null : parentConfigs.getConfig(StandardIndexes.openStruct());
  }

  /// The handlers that will actually do the work, one per index type that can apply to a child column.
  ///
  /// Delegating rather than re-implementing is the point: a child is indexed by the same handler that indexes
  /// every other column, so the two cannot drift.
  ///
  /// The forward and dictionary handlers are included deliberately. An inverted index needs dictionary ids, so a
  /// key that was written raw and is later given an inverted index has to be converted first -- exactly the
  /// raw-to-dictionary work the forward handler already does for ordinary columns. Excluding them makes the
  /// common case (add an index to a key that had none) silently do nothing.
  ///
  /// Only this handler's own index type is excluded: a child has no OPEN_STRUCT index of its own, and recursing
  /// into it would rediscover the same children forever.
  private List<IndexHandler> delegates() {
    List<IndexHandler> handlers = new ArrayList<>();
    if (_childConfigs.isEmpty()) {
      return handlers;
    }
    for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
      // The forward handler is run separately and first, see updateIndices.
      if (indexType == StandardIndexes.openStruct() || indexType == StandardIndexes.forward()) {
        continue;
      }
      handlers.add(createDelegate(indexType));
    }
    return handlers;
  }

  private IndexHandler createDelegate(IndexType<?, ?, ?> indexType) {
    return indexType.createIndexHandler(_segmentDirectory, _childConfigs, _schema, _tableConfig);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    if (_childConfigs.isEmpty()) {
      return false;
    }
    if (createDelegate(StandardIndexes.forward()).needUpdateIndices(segmentReader)) {
      return true;
    }
    for (IndexHandler handler : delegates()) {
      if (handler.needUpdateIndices(segmentReader)) {
        return true;
      }
    }
    return false;
  }

  /// Mirrors [SegmentPreProcessor]'s own scheduling, which its comment requires anyone changing handler order to
  /// preserve: the forward handler first, then a metadata reload, then everything else. A key that was written raw
  /// and is now given an inverted index needs the raw-to-dictionary conversion to land, and the reload to publish
  /// it, before any dictionary-dependent handler runs -- otherwise those handlers fail on a dictionary that does
  /// not exist yet.
  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    if (_childConfigs.isEmpty()) {
      return;
    }
    IndexHandler forwardHandler = createDelegate(StandardIndexes.forward());
    forwardHandler.updateIndices(segmentWriter);
    _segmentDirectory.reloadMetadata();
    for (IndexHandler handler : delegates()) {
      handler.updateIndices(segmentWriter);
    }
    LOGGER.debug("Applied per-key index settings to {} OPEN_STRUCT child columns", _childConfigs.size());
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    for (IndexHandler handler : delegates()) {
      handler.postUpdateIndicesCleanup(segmentWriter);
    }
  }
}
