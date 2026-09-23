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
package org.apache.pinot.core.operator.blocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.docvalsets.OpenStructDocumentBlockValSet;
import org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;


/// ProjectionBlock holds a column name to Block Map.
/// It provides DocIdSetBlock for a given column.
public class ProjectionBlock implements ValueBlock {
  private final Map<String, DataSource> _dataSourceMap;
  private final DataBlockCache _dataBlockCache;

  public ProjectionBlock(Map<String, DataSource> dataSourceMap, DataBlockCache dataBlockCache) {
    _dataSourceMap = dataSourceMap;
    _dataBlockCache = dataBlockCache;
  }

  @Override
  public int getNumDocs() {
    return _dataBlockCache.getNumDocs();
  }

  @Override
  public int[] getDocIds() {
    return _dataBlockCache.getDocIds();
  }

  @Override
  public BlockValSet getBlockValueSet(ExpressionContext expression) {
    assert expression.getType() == ExpressionContext.Type.IDENTIFIER;
    return getBlockValueSet(expression.getIdentifier());
  }

  @Override
  public BlockValSet getBlockValueSet(String column) {
    DataSource dataSource = _dataSourceMap.get(column);
    // An OPEN_STRUCT parent is only a handle for per-key resolution — it has no forward index, so DataFetcher does
    // not register it and it cannot be read through the block cache. Assemble its document here instead, which is
    // what the storage layer's contract defers to the query layer. Without this `SELECT col` and, worse, `SELECT *`
    // both failed outright on any table carrying one.
    if (dataSource instanceof OpenStructDataSource openStructDataSource) {
      return openStructDocuments(column, openStructDataSource);
    }
    return new ProjectionBlockValSet(_dataBlockCache, column, dataSource);
  }

  /// The column's whole document per row, as JSON text.
  ///
  /// Each key is read through the per-key value set the block already knows how to build, so a key's own type and
  /// null bitmap decide how it is rendered and whether it appears at all — an absent key is omitted rather than
  /// written as null, which is the same distinction `col['key']` makes.
  private BlockValSet openStructDocuments(String column, OpenStructDataSource openStructDataSource) {
    int numDocs = getNumDocs();
    ObjectNode[] documents = new ObjectNode[numDocs];
    for (int docId = 0; docId < numDocs; docId++) {
      documents[docId] = JsonUtils.newObjectNode();
    }
    for (String key : openStructDataSource.getDataSources().keySet()) {
      addKeyToDocuments(documents, numDocs, getBlockValueSet(new String[]{column, key}), key);
    }
    String[] serialized = new String[numDocs];
    for (int docId = 0; docId < numDocs; docId++) {
      serialized[docId] = documents[docId].toString();
    }
    return new OpenStructDocumentBlockValSet(serialized);
  }

  private static void addKeyToDocuments(ObjectNode[] documents, int numDocs, BlockValSet values, String key) {
    RoaringBitmap nulls = values.getNullBitmap();
    if (!values.isSingleValue()) {
      String[][] multiValues = values.getStringValuesMV();
      for (int docId = 0; docId < numDocs; docId++) {
        if (nulls != null && nulls.contains(docId)) {
          continue;
        }
        ArrayNode array = JsonUtils.newArrayNode();
        for (String value : multiValues[docId]) {
          array.add(value);
        }
        documents[docId].set(key, array);
      }
      return;
    }
    switch (values.getValueType().getStoredType()) {
      case INT: {
        int[] ints = values.getIntValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> IntNode.valueOf(ints[docId]));
        break;
      }
      case LONG: {
        long[] longs = values.getLongValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> LongNode.valueOf(longs[docId]));
        break;
      }
      case FLOAT: {
        float[] floats = values.getFloatValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> DoubleNode.valueOf(floats[docId]));
        break;
      }
      case DOUBLE: {
        double[] doubles = values.getDoubleValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> DoubleNode.valueOf(doubles[docId]));
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] decimals = values.getBigDecimalValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> DecimalNode.valueOf(decimals[docId]));
        break;
      }
      case BYTES: {
        byte[][] bytes = values.getBytesValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> TextNode.valueOf(BytesUtils.toHexString(bytes[docId])));
        break;
      }
      default: {
        String[] strings = values.getStringValuesSV();
        putEach(documents, numDocs, nulls, key, docId -> TextNode.valueOf(strings[docId]));
        break;
      }
    }
  }

  private static void putEach(ObjectNode[] documents, int numDocs, @Nullable RoaringBitmap nulls, String key,
      java.util.function.IntFunction<JsonNode> value) {
    for (int docId = 0; docId < numDocs; docId++) {
      // A key absent from this row is absent from its document; writing it as JSON null would say the row carried
      // the key with no value, which is a different fact.
      if (nulls == null || !nulls.contains(docId)) {
        documents[docId].set(key, value.apply(docId));
      }
    }
  }

  @Override
  public BlockValSet getBlockValueSet(String[] paths) {
    // TODO: only support one level of path for now, e.g. `map.key`
    assert paths.length == 2;
    String fullColumnKeyName = ComplexFieldSpec.getFullChildName(paths);
    // Resolve once per ProjectionOperator, not once per block: _dataSourceMap is owned by the operator and
    // shared across every block of the segment. Re-resolving an absent OPEN_STRUCT key rebuilds a null bitmap
    // spanning the whole segment on each block, and DataFetcher keeps the first reader registered under the
    // name anyway, so every later resolution is garbage that also leaves this map disagreeing with the fetcher.
    if (_dataSourceMap.containsKey(fullColumnKeyName)) {
      return getBlockValueSet(fullColumnKeyName);
    }
    DataSource columnDataSource = _dataSourceMap.get(paths[0]);
    DataSource keyDataSource;
    if (columnDataSource instanceof MapDataSource) {
      keyDataSource = ((MapDataSource) columnDataSource).getDataSource(paths[1]);
    } else if (columnDataSource instanceof OpenStructDataSource) {
      keyDataSource = ((OpenStructDataSource) columnDataSource).getDataSource(paths[1]);
    } else {
      throw new IllegalStateException("Path-based access requires MAP or OPEN_STRUCT column: " + paths[0]);
    }
    _dataSourceMap.put(fullColumnKeyName, keyDataSource);
    _dataBlockCache.addDataSource(fullColumnKeyName, keyDataSource);
    return getBlockValueSet(fullColumnKeyName);
  }
}
