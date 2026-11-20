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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;


/// Evaluates myMap\['foo'\]
public class ItemTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "item";

  private String[] _keyPath;
  private Dictionary _dictionary;
  private TransformResultMetadata _resultMetadata;
  private boolean _perKeyNullsAvailable;
  private ColumnDataType _sourceType;
  // Set only when a multi-stage plan needs a different type from this segment.
  @Nullable
  private ColumnDataType _targetType;
  private Object _defaultValue;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    initWithSchema(arguments, columnContextMap, null);
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      QueryContext queryContext) {
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    initWithSchema(arguments, columnContextMap, queryContext.getSchema());
  }

  private void initWithSchema(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      @Nullable Schema schema) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() == 2 || arguments.size() == 3,
        "Expected column, key and optional result type, got: %s arguments", arguments.size());

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
    DataSourceMetadata metadata = valueDataSource.getDataSourceMetadata();
    if (arguments.size() == 3) {
      Preconditions.checkArgument(dataSource instanceof OpenStructDataSource
          && arguments.get(2) instanceof LiteralTransformFunction, "Expected an OPEN_STRUCT column and a result type");
      String typeName = ((LiteralTransformFunction) arguments.get(2)).getStringLiteral();
      ColumnDataType targetType = ColumnDataType.valueOf(typeName);
      _sourceType = ColumnDataType.fromDataType(metadata.getDataType(), metadata.isSingleValue());
      if (_sourceType != targetType) {
        _targetType = targetType;
        FieldSpec parent =
            schema != null ? schema.getFieldSpecFor(column) : ((OpenStructDataSource) dataSource).getFieldSpec();
        FieldSpec child = parent instanceof ComplexFieldSpec complex ? complex.getChildFieldSpec(key) : null;
        _defaultValue = getDefaultValue(targetType, child);
        _resultMetadata = new TransformResultMetadata(targetType.toDataType(), !targetType.isArray(), false);
        return;
      }
    }
    // Only expose the dictionary when the forward index is dict-encoded. A column can have a dictionary alongside
    // a RAW forward index (e.g. dict + inverted/range), in which case transformToDictIdsSV would fail because
    // BlockValueSet.getDictionaryIdsSV requires a dict-encoded forward index.
    ForwardIndexReader<?> forwardIndex = valueDataSource.getForwardIndex();
    _dictionary = forwardIndex != null && forwardIndex.isDictionaryEncoded() ? valueDataSource.getDictionary() : null;
    _resultMetadata =
        new TransformResultMetadata(metadata.getDataType(), metadata.isSingleValue(), _dictionary != null);
  }

  /// OPEN_STRUCT stores the declared default (or the dimension default for an undeclared key) in absent rows.
  /// A missing multi-value field stores a single element containing that same default.
  private static Object getDefaultValue(ColumnDataType targetType, @Nullable FieldSpec child) {
    FieldSpec.DataType type = targetType.toDataType();
    // The planner maps JSON to VARCHAR. Types sharing storage, such as BYTES and UUID, can have different defaults.
    boolean matches = child != null && (child.getDataType() == type
        || (child.getDataType() == FieldSpec.DataType.JSON && type == FieldSpec.DataType.STRING));
    Object scalar = matches ? child.getDefaultNullValue()
        : FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, type, null);
    scalar = ColumnDataType.OBJECT.toInternal(scalar);
    if (!targetType.isArray()) {
      return scalar;
    }
    Object singleton = Array.newInstance(targetType.getNullPlaceholder().getClass().getComponentType(), 1);
    Array.set(singleton, 0, scalar);
    return singleton;
  }

  /// Reads the key in its native representation before converting it. In particular, asking a STRING dictionary
  /// directly for numbers would parse even the placeholders of null rows. Matching segments never take this path.
  private <T> T convertValues(ValueBlock block, ColumnDataType readType, Class<T> arrayClass) {
    BlockValSet source = block.getBlockValueSet(_keyPath);
    RowBasedBlockValueFetcher fetcher = new RowBasedBlockValueFetcher(new BlockValSet[]{source});
    // An absent key has no value to cast, even when the caller uses placeholders instead of SQL nulls.
    RoaringBitmap nulls = source.getNullBitmap();
    int length = block.getNumDocs();
    Object result = Array.newInstance(arrayClass.getComponentType(), length);
    Object[] row = new Object[1];
    boolean nativeRead = readType == _targetType.getStoredType();
    for (int i = 0; i < length; i++) {
      Object value;
      boolean missing = nulls != null && nulls.contains(i);
      if (missing && _nullHandlingEnabled) {
        value = (nativeRead ? _targetType : readType).getNullPlaceholder();
      } else {
        try {
          if (missing) {
            value = _defaultValue;
          } else {
            fetcher.getRow(i, row, 0);
            value = convertValue(row[0], _sourceType, _targetType);
          }
          if (!nativeRead) {
            // An enclosing transform can request a different representation of the already typed key.
            value = convertValue(value, _targetType, readType);
          }
        } catch (RuntimeException | JsonProcessingException e) {
          throw new IllegalStateException("Cannot convert " + _sourceType + " key '" + _keyPath[1] + "' to "
              + _targetType, e);
        }
      }
      if (value instanceof ByteArray bytes) {
        value = bytes.getBytes();
      } else if (value instanceof ByteArray[] bytes) {
        value = ColumnDataType.BYTES_ARRAY.toExternal(bytes);
      }
      Array.set(result, i, value);
    }
    return arrayClass.cast(result);
  }

  private static Object convertValue(Object value, ColumnDataType sourceType, ColumnDataType targetType)
      throws JsonProcessingException {
    if (targetType == ColumnDataType.STRING) {
      Object formatted = sourceType.convertAndFormat(value);
      return sourceType.isArray() ? JsonUtils.objectToString(formatted) : formatted.toString();
    }
    Preconditions.checkArgument(!sourceType.isArray() || targetType.isArray(), "Cannot cast an array to a scalar");
    Object converted = targetType.toPinotDataType().convert(sourceType.toExternal(value), sourceType.toPinotDataType());
    // Preserve the row fetcher's internal representations, including ByteArray[] for byte[][].
    return ColumnDataType.OBJECT.toInternal(converted);
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
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getIntValuesSV()
        : convertValues(valueBlock, ColumnDataType.INT, int[].class);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getLongValuesSV()
        : convertValues(valueBlock, ColumnDataType.LONG, long[].class);
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getFloatValuesSV()
        : convertValues(valueBlock, ColumnDataType.FLOAT, float[].class);
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getDoubleValuesSV()
        : convertValues(valueBlock, ColumnDataType.DOUBLE, double[].class);
  }

  /// Without this the base class has no way to produce a BIG_DECIMAL: its conversion switch widens INT, LONG, FLOAT,
  /// DOUBLE, STRING and BYTES into one, but a key whose own type is already BIG_DECIMAL matches no case and throws
  /// `Cannot read SV BIG_DECIMAL as BIG_DECIMAL`. Reading it straight off the key's value source is both the fix and
  /// the cheaper path.
  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getBigDecimalValuesSV()
        : convertValues(valueBlock, ColumnDataType.BIG_DECIMAL, BigDecimal[].class);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getStringValuesSV()
        : convertValues(valueBlock, ColumnDataType.STRING, String[].class);
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getBytesValuesSV()
        : convertValues(valueBlock, ColumnDataType.BYTES, byte[][].class);
  }

  // A key can hold a list, in which case its value source is multi-value and the engine asks for the values that
  // way. The result metadata above already reports the key's own shape, so these are the reads that shape implies;
  // without them a multi-value key would be storable and describable but not readable.

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    return valueBlock.getBlockValueSet(_keyPath).getDictionaryIdsMV();
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getIntValuesMV()
        : convertValues(valueBlock, ColumnDataType.INT_ARRAY, int[][].class);
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getLongValuesMV()
        : convertValues(valueBlock, ColumnDataType.LONG_ARRAY, long[][].class);
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getFloatValuesMV()
        : convertValues(valueBlock, ColumnDataType.FLOAT_ARRAY, float[][].class);
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getDoubleValuesMV()
        : convertValues(valueBlock, ColumnDataType.DOUBLE_ARRAY, double[][].class);
  }

  /// The multi-value twin of [#transformToBigDecimalValuesSV]: the base class widens INT, LONG, FLOAT, DOUBLE,
  /// STRING and BYTES into a BIG_DECIMAL, but a key already declared BIG_DECIMAL matches no case and throws
  /// `Cannot read MV BIG_DECIMAL as BIG_DECIMAL`. A sparse key is never dictionary-encoded, so it reaches exactly
  /// that switch.
  @Override
  public BigDecimal[][] transformToBigDecimalValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getBigDecimalValuesMV()
        : convertValues(valueBlock, ColumnDataType.BIG_DECIMAL_ARRAY, BigDecimal[][].class);
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getStringValuesMV()
        : convertValues(valueBlock, ColumnDataType.STRING_ARRAY, String[][].class);
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    return _targetType == null ? valueBlock.getBlockValueSet(_keyPath).getBytesValuesMV()
        : convertValues(valueBlock, ColumnDataType.BYTES_ARRAY, byte[][][].class);
  }
}
