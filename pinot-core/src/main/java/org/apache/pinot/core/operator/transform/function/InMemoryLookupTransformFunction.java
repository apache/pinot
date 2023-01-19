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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.manager.offline.InMemoryTable;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;


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
public class InMemoryLookupTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "InMemoryLookUp";

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final int[] EMPTY_INTS = new int[0];
  private static final long[] EMPTY_LONGS = new long[0];
  private static final float[] EMPTY_FLOATS = new float[0];
  private static final double[] EMPTY_DOUBLES = new double[0];
  private static final String[] EMPTY_STRINGS = new String[0];

  private int _inMemoryColIdx;
  private String _inMemoryColName;

  private DataSchema.ColumnDataType _lookupDataType;

  private HashMap<PrimaryKey, Object[]> _keyValuesMap;

  private HashMap<String, Integer> _keyIndexMap;
  private final List<TransformFunction> _joinValueFunctions = new ArrayList<>();

  private int _nullIntValue;
  private long _nullLongValue;
  private float _nullFloatValue;
  private double _nullDoubleValue;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap, QueryContext context) {
    // Check that there are correct number of arguments
    Preconditions.checkArgument(arguments.size() >= 4,
        "At least 4 arguments are required for LOOKUP transform function: "
            + "LOOKUP(TableName, ColumnName, JoinKey, JoinValue [, JoinKey2, JoinValue2 ...])");
    Preconditions.checkArgument(arguments.size() % 2 == 0,
        "Should have the same number of JoinKey and JoinValue arguments");

    TransformFunction inMemoryTableNameFunction = arguments.get(0);
    Preconditions.checkArgument(inMemoryTableNameFunction instanceof LiteralTransformFunction,
        "First argument must be a literal(string) representing the in memory table name");
    String inMemoryTableName = ((LiteralTransformFunction) inMemoryTableNameFunction).getLiteral();
    InMemoryTable inMemoryTable = context.getInMemoryTable(inMemoryTableName);
    Preconditions.checkArgument(inMemoryTable != null, "InMemoryTable cannot be null:" + inMemoryTableName);
    _keyIndexMap = inMemoryTable.getColumnIndex();

    // Lookup parameters
    TransformFunction inMemoryColName = arguments.get(1);
    Preconditions.checkArgument(inMemoryColName instanceof LiteralTransformFunction,
        "Second argument must be a literal(string) representing the column name from in memory table to lookup");
    _inMemoryColName = ((LiteralTransformFunction) inMemoryColName).getLiteral();
    Preconditions.checkArgument(_keyIndexMap.containsKey(_inMemoryColName),
        "Lookup column:" + _inMemoryColName + " doesn't exist in in memory table");
    _inMemoryColIdx = _keyIndexMap.get(_inMemoryColName);
    _lookupDataType = inMemoryTable.getDataType(_inMemoryColIdx);

    List<String> joinKeys = new ArrayList<>();
    List<TransformFunction> joinArguments = arguments.subList(2, arguments.size());
    int numJoinArguments = joinArguments.size();
    for (int i = 0; i < numJoinArguments / 2; i++) {
      TransformFunction inMemoryKeyFunc = joinArguments.get((i * 2));
      Preconditions.checkArgument(inMemoryKeyFunc instanceof LiteralTransformFunction,
          "JoinKey argument must be a literal(string) representing the primary key for the dimension table");
      joinKeys.add(((LiteralTransformFunction) inMemoryKeyFunc).getLiteral());

      TransformFunction factJoinValueFunction = joinArguments.get((i * 2) + 1);
      TransformResultMetadata factJoinValueFunctionResultMetadata = factJoinValueFunction.getResultMetadata();
      Preconditions.checkArgument(factJoinValueFunctionResultMetadata.isSingleValue(),
          "JoinValue argument must be a single value expression");
      _joinValueFunctions.add(factJoinValueFunction);
    }

    for (String joinKey : joinKeys) {
      Preconditions.checkArgument(_keyIndexMap.containsKey(joinKey),
          "joinKey:" + joinKey + " doesn't exist in in memory table");
    }
    _keyValuesMap = inMemoryTable.getHashMap(joinKeys);

    Object defaultNullValue = _lookupDataType.getNullPlaceholder();
    if (defaultNullValue instanceof Number) {
      _nullIntValue = ((Number) defaultNullValue).intValue();
      _nullLongValue = ((Number) defaultNullValue).longValue();
      _nullFloatValue = ((Number) defaultNullValue).floatValue();
      _nullDoubleValue = ((Number) defaultNullValue).intValue();
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_lookupDataType.toDataType(), true, false);
  }

  @FunctionalInterface
  private interface ValueAcceptor {
    void accept(int index, @Nullable Object value);
  }

  private void lookup(ProjectionBlock projectionBlock, ValueAcceptor valueAcceptor) {
    int numPkColumns = _joinValueFunctions.size();
    int numDocuments = projectionBlock.getNumDocs();
    Object[] pkColumns = new Object[numPkColumns];
    for (int c = 0; c < numPkColumns; c++) {
      DataType storedType = _joinValueFunctions.get(c).getResultMetadata().getDataType().getStoredType();
      TransformFunction tf = _joinValueFunctions.get(c);
      switch (storedType) {
        case INT:
          pkColumns[c] = tf.transformToIntValuesSV(projectionBlock);
          break;
        case LONG:
          pkColumns[c] = tf.transformToLongValuesSV(projectionBlock);
          break;
        case FLOAT:
          pkColumns[c] = tf.transformToFloatValuesSV(projectionBlock);
          break;
        case DOUBLE:
          pkColumns[c] = tf.transformToDoubleValuesSV(projectionBlock);
          break;
        case STRING:
          pkColumns[c] = tf.transformToStringValuesSV(projectionBlock);
          break;
        case BYTES:
          pkColumns[c] = tf.transformToBytesValuesSV(projectionBlock);
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
      Object[] row = _keyValuesMap.getOrDefault(primaryKey, null);

      Object value = row == null ? null : row[_inMemoryColIdx];
      valueAcceptor.accept(i, value);
    }
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[numDocs];
    }
    lookup(projectionBlock, this::setIntSV);
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[numDocs];
    }
    lookup(projectionBlock, this::setLongSV);
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[numDocs];
    }
    lookup(projectionBlock, this::setFloatSV);
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[numDocs];
    }
    lookup(projectionBlock, this::setDoubleSV);
    return _doubleValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[numDocs];
    }
    lookup(projectionBlock, this::setStringSV);
    return _stringValuesSV;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_bytesValuesSV == null) {
      _bytesValuesSV = new byte[numDocs][];
    }
    lookup(projectionBlock, this::setBytesSV);
    return _bytesValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_intValuesMV == null) {
      _intValuesMV = new int[numDocs][];
    }
    lookup(projectionBlock, this::setIntMV);
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_longValuesMV == null) {
      _longValuesMV = new long[numDocs][];
    }
    lookup(projectionBlock, this::setLongMV);
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[numDocs][];
    }
    lookup(projectionBlock, this::setFloatMV);
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[numDocs][];
    }
    lookup(projectionBlock, this::setDoubleMV);
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_lookupDataType.toDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(projectionBlock);
    }
    int numDocs = projectionBlock.getNumDocs();
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[numDocs][];
    }
    lookup(projectionBlock, this::setStringMV);
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
      _stringValuesSV[index] = _lookupDataType.getNullPlaceholder().toString();
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
