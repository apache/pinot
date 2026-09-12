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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.roaringbitmap.RoaringBitmap;


/// Evaluates myMap\['foo'\]
public class ItemTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "item";

  private String[] _keyPath;
  private Dictionary _dictionary;
  private TransformResultMetadata _resultMetadata;
  private boolean _perKeyNullsAvailable;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 2, "Expected exactly 2 arguments, got: %s", arguments.size());

    TransformFunction mapValue = arguments.get(0);
    Preconditions.checkArgument(mapValue instanceof IdentifierTransformFunction,
        "Map Item: Left operand must be an identifier");
    String column = ((IdentifierTransformFunction) mapValue).getColumnName();

    TransformFunction keyValue = arguments.get(1);
    Preconditions.checkArgument(keyValue instanceof LiteralTransformFunction,
        "Map Item: Right operand must be a literal");
    String key = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();
    _keyPath = new String[]{column, key};

    DataSource dataSource = columnContextMap.get(column).getDataSource();
    Preconditions.checkState(dataSource instanceof MapDataSource || dataSource instanceof OpenStructDataSource,
        "Column: %s must be a MAP or OPEN_STRUCT column", column);
    DataSource valueDataSource = dataSource instanceof MapDataSource
        ? ((MapDataSource) dataSource).getDataSource(key) : ((OpenStructDataSource) dataSource).getDataSource(key);
    // Per-key nulls are exact whenever the key's source tracks them: every OPEN_STRUCT key does, a MAP key only when
    // it is absent from the segment
    _perKeyNullsAvailable = valueDataSource.getNullValueVector() != null;
    // Only expose the dictionary when the forward index is dict-encoded. A column can have a dictionary alongside
    // a RAW forward index (e.g. dict + inverted/range), in which case transformToDictIdsSV would fail because
    // BlockValueSet.getDictionaryIdsSV requires a dict-encoded forward index.
    ForwardIndexReader<?> forwardIndex = valueDataSource.getForwardIndex();
    _dictionary = forwardIndex != null && forwardIndex.isDictionaryEncoded() ? valueDataSource.getDictionary() : null;
    DataSourceMetadata valueDataSourceMetadata = valueDataSource.getDataSourceMetadata();
    _resultMetadata =
        new TransformResultMetadata(valueDataSourceMetadata.getDataType(), valueDataSourceMetadata.isSingleValue(),
            _dictionary != null);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  /// Uses the per-key null bitmap when the key's data source tracks nulls, which is exact: every OPEN_STRUCT key does
  /// (the per-key presence bitmap is materialized into a null value vector on both the mutable and sealed paths), and
  /// so does a key absent from a MAP column, whose all-null source marks every document null. A key present in a MAP
  /// column carries no per-key null information, so fall back to [BaseTransformFunction#getNullBitmap] which ORs the
  /// argument bitmaps, yielding the MAP column's own null bitmap: a conservative over-estimate that downstream null
  /// handling narrows further.
  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return _perKeyNullsAvailable ? valueBlock.getBlockValueSet(_keyPath).getNullBitmap()
        : super.getNullBitmap(valueBlock);
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    return valueBlock.getBlockValueSet(_keyPath).getDictionaryIdsSV();
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    return valueBlock.getBlockValueSet(_keyPath).getIntValuesSV();
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    return valueBlock.getBlockValueSet(_keyPath).getLongValuesSV();
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    return valueBlock.getBlockValueSet(_keyPath).getDoubleValuesSV();
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    return valueBlock.getBlockValueSet(_keyPath).getStringValuesSV();
  }
}
