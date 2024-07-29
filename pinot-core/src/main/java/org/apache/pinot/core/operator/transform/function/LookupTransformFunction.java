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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * LOOKUP function takes 4 or more arguments:
 * <ul>
 *   <li><b>TableName:</b> name of the dimension table which will be used</li>
 *   <li><b>ColumnName:</b> column name from the dimension table to look up</li>
 *   <li><b>JoinKey:</b> primary key column name for the dimension table. Note: Only primary key[s] are supported for
 *   JoinKey</li>
 *   <li><b>JoinValue:</b> primary key value</li>
 *   ...<br>
 *   *[If the dimension table has more then one primary keys (composite pk)]
 *     <li><b>JoinKey2</b></li>
 *     <li><b>JoinValue2</b></li>
 *   ...
 * </ul>
 * <br>
 * Example:
 * <pre>{@code SELECT
 *    baseballStats.playerName,
 *    baseballStats.teamID,
 *    LOOKUP('dimBaseballTeams', 'teamName', 'teamID', baseballStats.teamID)
 * FROM
 *    baseballStats
 * LIMIT 10}</pre>
 * <br>
 * Above example joins the dimension table 'baseballTeams' into regular table 'baseballStats' on 'teamID' key.
 * Lookup function returns the value of the column 'teamName'.
 */
public class LookupTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "lookUp";

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final int[] EMPTY_INTS = new int[0];
  private static final long[] EMPTY_LONGS = new long[0];
  private static final float[] EMPTY_FLOATS = new float[0];
  private static final double[] EMPTY_DOUBLES = new double[0];
  private static final String[] EMPTY_STRINGS = new String[0];

  private String _dimColumnName;
  private final List<String> _joinKeys = new ArrayList<>();
  private final List<FieldSpec> _joinValueFieldSpecs = new ArrayList<>();
  private final List<TransformFunction> _joinValueFunctions = new ArrayList<>();

  private DimensionTableDataManager _dataManager;
  private FieldSpec _lookupColumnFieldSpec;

  private int _nullIntValue;
  private long _nullLongValue;
  private float _nullFloatValue;
  private double _nullDoubleValue;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are correct number of arguments
    Preconditions.checkArgument(arguments.size() >= 4,
        "At least 4 arguments are required for LOOKUP transform function: "
            + "LOOKUP(TableName, ColumnName, JoinKey, JoinValue [, JoinKey2, JoinValue2 ...])");
    Preconditions.checkArgument(arguments.size() % 2 == 0,
        "Should have the same number of JoinKey and JoinValue arguments");

    TransformFunction dimTableNameFunction = arguments.get(0);
    Preconditions.checkArgument(dimTableNameFunction instanceof LiteralTransformFunction,
        "First argument must be a literal(string) representing the dimension table name");
    // Lookup parameters
    String dimTableName = TableNameBuilder.OFFLINE.tableNameWithType(
        ((LiteralTransformFunction) dimTableNameFunction).getStringLiteral());

    TransformFunction dimColumnFunction = arguments.get(1);
    Preconditions.checkArgument(dimColumnFunction instanceof LiteralTransformFunction,
        "Second argument must be a literal(string) representing the column name from dimension table to lookup");
    _dimColumnName = ((LiteralTransformFunction) dimColumnFunction).getStringLiteral();

    List<TransformFunction> joinArguments = arguments.subList(2, arguments.size());
    int numJoinArguments = joinArguments.size();
    for (int i = 0; i < numJoinArguments / 2; i++) {
      TransformFunction dimJoinKeyFunction = joinArguments.get((i * 2));
      Preconditions.checkArgument(dimJoinKeyFunction instanceof LiteralTransformFunction,
          "JoinKey argument must be a literal(string) representing the primary key for the dimension table");
      _joinKeys.add(((LiteralTransformFunction) dimJoinKeyFunction).getStringLiteral());

      TransformFunction factJoinValueFunction = joinArguments.get((i * 2) + 1);
      TransformResultMetadata factJoinValueFunctionResultMetadata = factJoinValueFunction.getResultMetadata();
      Preconditions.checkArgument(factJoinValueFunctionResultMetadata.isSingleValue(),
          "JoinValue argument must be a single value expression");
      _joinValueFunctions.add(factJoinValueFunction);
    }

    // Validate lookup table and relevant columns
    _dataManager = DimensionTableDataManager.getInstanceByTableName(dimTableName);
    Preconditions.checkArgument(_dataManager != null, "Dimension table does not exist: %s", dimTableName);

    Preconditions.checkArgument(_dataManager.isPopulated(), "Dimension table is not populated: %s", dimTableName);

    _lookupColumnFieldSpec = _dataManager.getColumnFieldSpec(_dimColumnName);
    Preconditions.checkArgument(_lookupColumnFieldSpec != null, "Column does not exist in dimension table: %s:%s",
        dimTableName, _dimColumnName);

    for (String joinKey : _joinKeys) {
      FieldSpec pkColumnSpec = _dataManager.getColumnFieldSpec(joinKey);
      Preconditions.checkArgument(pkColumnSpec != null, "Primary key column doesn't exist in dimension table: %s:%s",
          dimTableName, joinKey);
      _joinValueFieldSpecs.add(pkColumnSpec);
    }

    List<String> tablePrimaryKeyColumns = _dataManager.getPrimaryKeyColumns();
    Preconditions.checkArgument(_joinKeys.equals(tablePrimaryKeyColumns),
        "Provided join keys (%s) must be the same as table primary keys: %s", _joinKeys, tablePrimaryKeyColumns);

    Object defaultNullValue = _lookupColumnFieldSpec.getDefaultNullValue();
    if (defaultNullValue instanceof Number) {
      _nullIntValue = ((Number) defaultNullValue).intValue();
      _nullLongValue = ((Number) defaultNullValue).longValue();
      _nullFloatValue = ((Number) defaultNullValue).floatValue();
      _nullDoubleValue = ((Number) defaultNullValue).intValue();
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_lookupColumnFieldSpec.getDataType(),
        _lookupColumnFieldSpec.isSingleValueField(), false);
  }

  @FunctionalInterface
  private interface ValueAcceptor {
    void accept(int index, @Nullable Object value);
  }

  private void lookup(ValueBlock valueBlock, ValueAcceptor valueAcceptor) {
    int numPkColumns = _joinKeys.size();
    int numDocuments = valueBlock.getNumDocs();
    Object[] pkColumns = new Object[numPkColumns];
    for (int c = 0; c < numPkColumns; c++) {
      DataType storedType = _joinValueFieldSpecs.get(c).getDataType().getStoredType();
      TransformFunction tf = _joinValueFunctions.get(c);
      switch (storedType) {
        case INT:
          pkColumns[c] = tf.transformToIntValuesSV(valueBlock);
          break;
        case LONG:
          pkColumns[c] = tf.transformToLongValuesSV(valueBlock);
          break;
        case FLOAT:
          pkColumns[c] = tf.transformToFloatValuesSV(valueBlock);
          break;
        case DOUBLE:
          pkColumns[c] = tf.transformToDoubleValuesSV(valueBlock);
          break;
        case STRING:
          pkColumns[c] = tf.transformToStringValuesSV(valueBlock);
          break;
        case BYTES:
          pkColumns[c] = tf.transformToBytesValuesSV(valueBlock);
          break;
        default:
          throw new IllegalStateException("Unknown column type for primary key");
      }
    }

    Object[] pkValues = new Object[numPkColumns];
    PrimaryKey primaryKey = new PrimaryKey(pkValues);
    for (int i = 0; i < numDocuments; i++) {
      // prepare pk
      for (int c = 0; c < numPkColumns; c++) {
        if (pkColumns[c] instanceof int[]) {
          pkValues[c] = ((int[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof long[]) {
          pkValues[c] = ((long[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof String[]) {
          pkValues[c] = ((String[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof float[]) {
          pkValues[c] = ((float[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof double[]) {
          pkValues[c] = ((double[]) pkColumns[c])[i];
        } else if (pkColumns[c] instanceof byte[][]) {
          pkValues[c] = new ByteArray(((byte[][]) pkColumns[c])[i]);
        }
      }
      // lookup
      GenericRow row = _dataManager.lookupRowByPrimaryKey(primaryKey);
      Object value = row == null ? null : row.getValue(_dimColumnName);
      valueAcceptor.accept(i, value);
    }
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    initIntValuesSV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setIntSV);
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    initLongValuesSV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setLongSV);
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    initFloatValuesSV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setFloatSV);
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    initDoubleValuesSV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setDoubleSV);
    return _doubleValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    initStringValuesSV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setStringSV);
    return _stringValuesSV;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    initBytesValuesSV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setBytesSV);
    return _bytesValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    initIntValuesMV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setIntMV);
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    initLongValuesMV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setLongMV);
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    initFloatValuesMV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setFloatMV);
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    initDoubleValuesMV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setDoubleMV);
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_lookupColumnFieldSpec.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    initStringValuesMV(valueBlock.getNumDocs());
    lookup(valueBlock, this::setStringMV);
    return _stringValuesMV;
  }

  private void setIntSV(int index, Object value) {
    if (value instanceof Number) {
      _intValuesSV[index] = ((Number) value).intValue();
    } else {
      _intValuesSV[index] = _nullIntValue;
    }
  }

  private void setLongSV(int index, Object value) {
    if (value instanceof Number) {
      _longValuesSV[index] = ((Number) value).longValue();
    } else {
      _longValuesSV[index] = _nullLongValue;
    }
  }

  private void setFloatSV(int index, Object value) {
    if (value instanceof Number) {
      _floatValuesSV[index] = ((Number) value).floatValue();
    } else {
      _floatValuesSV[index] = _nullFloatValue;
    }
  }

  private void setDoubleSV(int index, Object value) {
    if (value instanceof Number) {
      _doubleValuesSV[index] = ((Number) value).doubleValue();
    } else {
      _doubleValuesSV[index] = _nullDoubleValue;
    }
  }

  private void setStringSV(int index, Object value) {
    if (value != null) {
      _stringValuesSV[index] = String.valueOf(value);
    } else {
      _stringValuesSV[index] = _lookupColumnFieldSpec.getDefaultNullValueString();
    }
  }

  private void setBytesSV(int index, Object value) {
    if (value instanceof byte[]) {
      _bytesValuesSV[index] = (byte[]) value;
    } else {
      _bytesValuesSV[index] = EMPTY_BYTES;
    }
  }

  private void setIntMV(int index, Object value) {
    if (value instanceof int[]) {
      _intValuesMV[index] = (int[]) value;
    } else {
      _intValuesMV[index] = EMPTY_INTS;
    }
  }

  private void setLongMV(int index, Object value) {
    if (value instanceof long[]) {
      _longValuesMV[index] = (long[]) value;
    } else {
      _longValuesMV[index] = EMPTY_LONGS;
    }
  }

  private void setFloatMV(int index, Object value) {
    if (value instanceof float[]) {
      _floatValuesMV[index] = (float[]) value;
    } else {
      _floatValuesMV[index] = EMPTY_FLOATS;
    }
  }

  private void setDoubleMV(int index, Object value) {
    if (value instanceof double[]) {
      _doubleValuesMV[index] = (double[]) value;
    } else {
      _doubleValuesMV[index] = EMPTY_DOUBLES;
    }
  }

  private void setStringMV(int index, Object value) {
    if (value instanceof String[]) {
      _stringValuesMV[index] = (String[]) value;
    } else {
      _stringValuesMV[index] = EMPTY_STRINGS;
    }
  }
}
