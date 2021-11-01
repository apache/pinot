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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.core.data.manager.offline.DimensionTableDataManager;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.BigDecimalUtils;
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

  // Lookup parameters
  private String _dimTableName;
  private String _dimColumnName;
  private final List<String> _joinKeys = new ArrayList<>();
  private final List<FieldSpec> _joinValueFieldSpecs = new ArrayList<>();
  private final List<TransformFunction> _joinValueFunctions = new ArrayList<>();

  private DimensionTableDataManager _dataManager;
  private FieldSpec _lookupColumnFieldSpec;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are correct number of arguments
    Preconditions.checkArgument(arguments.size() >= 4,
        "At least 4 arguments are required for LOOKUP transform function: "
            + "LOOKUP(TableName, ColumnName, JoinKey, JoinValue [, JoinKey2, JoinValue2 ...])");
    Preconditions
        .checkArgument(arguments.size() % 2 == 0, "Should have the same number of JoinKey and JoinValue arguments");

    TransformFunction dimTableNameFunction = arguments.get(0);
    Preconditions.checkArgument(dimTableNameFunction instanceof LiteralTransformFunction,
        "First argument must be a literal(string) representing the dimension table name");
    _dimTableName =
        TableNameBuilder.OFFLINE.tableNameWithType(((LiteralTransformFunction) dimTableNameFunction).getLiteral());

    TransformFunction dimColumnFunction = arguments.get(1);
    Preconditions.checkArgument(dimColumnFunction instanceof LiteralTransformFunction,
        "Second argument must be a literal(string) representing the column name from dimension table to lookup");
    _dimColumnName = ((LiteralTransformFunction) dimColumnFunction).getLiteral();

    List<TransformFunction> joinArguments = arguments.subList(2, arguments.size());
    int numJoinArguments = joinArguments.size();
    for (int i = 0; i < numJoinArguments / 2; i++) {
      TransformFunction dimJoinKeyFunction = joinArguments.get((i * 2));
      Preconditions.checkArgument(dimJoinKeyFunction instanceof LiteralTransformFunction,
          "JoinKey argument must be a literal(string) representing the primary key for the dimension table");
      _joinKeys.add(((LiteralTransformFunction) dimJoinKeyFunction).getLiteral());

      TransformFunction factJoinValueFunction = joinArguments.get((i * 2) + 1);
      TransformResultMetadata factJoinValueFunctionResultMetadata = factJoinValueFunction.getResultMetadata();
      Preconditions.checkArgument(factJoinValueFunctionResultMetadata.isSingleValue(),
          "JoinValue argument must be a single value expression");
      _joinValueFunctions.add(factJoinValueFunction);
    }

    // Validate lookup table and relevant columns
    _dataManager = DimensionTableDataManager.getInstanceByTableName(_dimTableName);
    Preconditions.checkArgument(_dataManager != null, "Dimension table does not exist: %s", _dimTableName);

    _lookupColumnFieldSpec = _dataManager.getColumnFieldSpec(_dimColumnName);
    Preconditions
        .checkArgument(_lookupColumnFieldSpec != null, "Column does not exist in dimension table: %s:%s", _dimTableName,
            _dimColumnName);

    for (String joinKey : _joinKeys) {
      FieldSpec pkColumnSpec = _dataManager.getColumnFieldSpec(joinKey);
      Preconditions.checkArgument(pkColumnSpec != null, "Primary key column doesn't exist in dimension table: %s:%s",
          _dimTableName, joinKey);
      _joinValueFieldSpecs.add(pkColumnSpec);
    }

    List<String> tablePrimaryKeyColumns = _dataManager.getPrimaryKeyColumns();
    Preconditions.checkArgument(_joinKeys.equals(tablePrimaryKeyColumns),
        "Provided join keys (%s) must be the same as table primary keys: %s", _joinKeys, tablePrimaryKeyColumns);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_lookupColumnFieldSpec.getDataType(),
        _lookupColumnFieldSpec.isSingleValueField(), false);
  }

  private Object[] lookup(ProjectionBlock projectionBlock) {
    int numPkColumns = _joinKeys.size();
    int numDocuments = projectionBlock.getNumDocs();
    Object[][] pkColumns = new Object[numPkColumns][];
    for (int c = 0; c < numPkColumns; c++) {
      DataType storedType = _joinValueFieldSpecs.get(c).getDataType().getStoredType();
      TransformFunction tf = _joinValueFunctions.get(c);
      switch (storedType) {
        case INT:
          pkColumns[c] = ArrayUtils.toObject(tf.transformToIntValuesSV(projectionBlock));
          break;
        case LONG:
          pkColumns[c] = ArrayUtils.toObject(tf.transformToLongValuesSV(projectionBlock));
          break;
        case FLOAT:
          pkColumns[c] = ArrayUtils.toObject(tf.transformToFloatValuesSV(projectionBlock));
          break;
        case DOUBLE:
          pkColumns[c] = ArrayUtils.toObject(tf.transformToDoubleValuesSV(projectionBlock));
          break;
        case STRING:
          pkColumns[c] = tf.transformToStringValuesSV(projectionBlock);
          break;
        case BYTES:
          byte[][] primitiveValues = tf.transformToBytesValuesSV(projectionBlock);
          pkColumns[c] = new ByteArray[numDocuments];
          for (int i = 0; i < numDocuments; i++) {
            pkColumns[c][i] = new ByteArray(primitiveValues[i]);
          }
          break;
        case BIGDECIMAL:
          pkColumns[c] = tf.transformToBigDecimalValuesSV(projectionBlock);
          break;
        default:
          throw new IllegalStateException("Unknown column type for primary key");
      }
    }

    Object[] resultSet = new Object[numDocuments];
    Object[] pkValues = new Object[numPkColumns];
    for (int i = 0; i < numDocuments; i++) {
      // prepare pk
      for (int c = 0; c < numPkColumns; c++) {
        pkValues[c] = pkColumns[c][i];
      }
      // lookup
      GenericRow row = _dataManager.lookupRowByPrimaryKey(new PrimaryKey(pkValues));
      if (row != null) {
        resultSet[i] = row.getValue(_dimColumnName);
      }
    }
    return resultSet;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    int[] resultSet = new int[lookupObjects.length];
    Arrays.fill(resultSet, ((Number) _lookupColumnFieldSpec.getDefaultNullValue()).intValue());
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = ((Number) lookupObjects[i]).intValue();
      }
    }
    return resultSet;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    long[] resultSet = new long[lookupObjects.length];
    Arrays.fill(resultSet, ((Number) _lookupColumnFieldSpec.getDefaultNullValue()).longValue());
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = ((Number) lookupObjects[i]).longValue();
      }
    }
    return resultSet;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    float[] resultSet = new float[lookupObjects.length];
    Arrays.fill(resultSet, ((Number) _lookupColumnFieldSpec.getDefaultNullValue()).floatValue());
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = ((Number) lookupObjects[i]).floatValue();
      }
    }
    return resultSet;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    double[] resultSet = new double[lookupObjects.length];
    Arrays.fill(resultSet, ((Number) _lookupColumnFieldSpec.getDefaultNullValue()).doubleValue());
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = ((Number) lookupObjects[i]).doubleValue();
      }
    }
    return resultSet;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    String[] resultSet = new String[lookupObjects.length];
    Arrays.fill(resultSet, _lookupColumnFieldSpec.getDefaultNullValueString());
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = lookupObjects[i].toString();
      }
    }
    return resultSet;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    byte[][] resultSet = new byte[lookupObjects.length][0];
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = (byte[]) lookupObjects[i];
      }
    }
    return resultSet;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    BigDecimal[] resultSet = new BigDecimal[lookupObjects.length];
    Arrays.fill(resultSet,
        BigDecimalUtils.valueOf(((Number) _lookupColumnFieldSpec.getDefaultNullValue()).longValue()));
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = BigDecimalUtils.toBigDecimal(lookupObjects[i].toString());
      }
    }
    return resultSet;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    int[][] resultSet = new int[lookupObjects.length][0];
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = (int[]) lookupObjects[i];
      }
    }
    return resultSet;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    long[][] resultSet = new long[lookupObjects.length][0];
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = (long[]) lookupObjects[i];
      }
    }
    return resultSet;
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    float[][] resultSet = new float[lookupObjects.length][0];
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = (float[]) lookupObjects[i];
      }
    }
    return resultSet;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    double[][] resultSet = new double[lookupObjects.length][0];
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = (double[]) lookupObjects[i];
      }
    }
    return resultSet;
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    Object[] lookupObjects = lookup(projectionBlock);
    String[][] resultSet = new String[lookupObjects.length][0];
    for (int i = 0; i < lookupObjects.length; i++) {
      if (lookupObjects[i] != null) {
        resultSet[i] = (String[]) lookupObjects[i];
      }
    }
    return resultSet;
  }
}
