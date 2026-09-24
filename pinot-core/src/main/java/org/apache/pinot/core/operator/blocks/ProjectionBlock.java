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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource.MapValueReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.OpenStructKeyFlattener;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;


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
  /// Assembled through [OpenStructDataSource#openMapValueReader()], the reconstruction the storage layer already
  /// owns and the seal path already uses. Going key by key over [OpenStructDataSource#getDataSources()] instead
  /// reads only the materialized keys -- sparse keys share one JSON column and have no DataSource of their own --
  /// so every unmaterialized key would silently vanish from the document.
  private BlockValSet openStructDocuments(String column, OpenStructDataSource openStructDataSource) {
    int numDocs = getNumDocs();
    int[] docIds = getDocIds();
    String[] documents = new String[numDocs];
    try (MapValueReader reader = openStructDataSource.openMapValueReader()) {
      for (int i = 0; i < numDocs; i++) {
        Map<String, Object> document = reader.getMapValue(docIds[i]);
        documents[i] = document == null ? "{}" : JsonUtils.objectToString(renderDocument(document));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read OPEN_STRUCT column: " + column, e);
    }
    return new OpenStructDocumentBlockValSet(documents);
  }

  /// The document as it should read back: nested, and with no key spelled twice.
  ///
  /// A key nested inside an object is materialized under its path -- `configApi.timeTaken` -- while the object it
  /// came from stays in the document whole, so the reconstruction carries the same value both ways. The object is
  /// the shape the source had, so it wins and the paths into it are dropped. `.` is an ordinary key character with
  /// no escape, and that is exactly what makes the container's own entry the thing that disambiguates: a dotted key
  /// whose prefix is not itself a key was never a path, so it stays a key spelled with a dot.
  private static Map<String, Object> renderDocument(Map<String, Object> document) {
    Map<String, Object> rendered = new LinkedHashMap<>(document.size());
    for (Map.Entry<String, Object> entry : document.entrySet()) {
      if (!isPathIntoPresentObject(entry.getKey(), document)) {
        rendered.put(entry.getKey(), renderValue(entry.getValue()));
      }
    }
    return rendered;
  }

  /// Whether `key` is a path into an object that the document also carries whole. `configApi.timeTaken` is, when
  /// `configApi` is a key; a key the document literally spells with a dot is not, because no prefix of it is a key.
  private static boolean isPathIntoPresentObject(String key, Map<String, Object> document) {
    int dot = key.indexOf(OpenStructKeyFlattener.PATH_SEPARATOR);
    while (dot >= 0) {
      if (document.get(key.substring(0, dot)) instanceof Map) {
        return true;
      }
      dot = key.indexOf(OpenStructKeyFlattener.PATH_SEPARATOR, dot + 1);
    }
    return false;
  }

  /// Values as JSON renders them, recursing so a nested object is cleaned up the same way the top level is.
  /// Only BYTES needs a hand: Jackson would base64 it, while every other way of reading this value out of Pinot
  /// -- `col['key']` included -- gives hex.
  @Nullable
  private static Object renderValue(@Nullable Object value) {
    if (value instanceof byte[] bytes) {
      return BytesUtils.toHexString(bytes);
    }
    if (value instanceof Map<?, ?> map) {
      // Keys re-typed, values left raw: renderDocument renders each one as it walks them.
      Map<String, Object> nested = new LinkedHashMap<>(map.size());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        nested.put(String.valueOf(entry.getKey()), entry.getValue());
      }
      return renderDocument(nested);
    }
    if (value instanceof List<?> list) {
      List<Object> rendered = new ArrayList<>(list.size());
      for (Object element : list) {
        rendered.add(renderValue(element));
      }
      return rendered;
    }
    if (value instanceof Object[] array) {
      List<Object> rendered = new ArrayList<>(array.length);
      for (Object element : array) {
        rendered.add(renderValue(element));
      }
      return rendered;
    }
    return value;
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
