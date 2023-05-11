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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>CaseTransformFunction</code> class implements the CASE-WHEN-THEN-ELSE transformation.
 * <p>
 * The SQL Syntax is: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 WHEN conditionN THEN resultN ELSE
 * result END;
 * <p>
 * Usage: case(${WHEN_STATEMENT_1}, ..., ${WHEN_STATEMENT_N}, ${THEN_EXPRESSION_1}, ..., ${THEN_EXPRESSION_N},
 * ${ELSE_EXPRESSION})
 * <p>
 * There are 2 * N + 1 arguments:
 * <code>WHEN_STATEMENT_$i</code> is a <code>BinaryOperatorTransformFunction</code> represents
 * <code>condition$i</code>
 * <code>THEN_EXPRESSION_$i</code> is a <code>TransformFunction</code> represents <code>result$i</code>
 * <code>ELSE_EXPRESSION</code> is a <code>TransformFunction</code> represents <code>result</code>
 * <p>
 * ELSE_EXPRESSION can be omitted. When none of when statements is evaluated to be true, and there is no else
 * expression, we output null. Note that when statement is considered as false if it is evaluated to be null.
 */
public class CaseTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "case";

  private List<TransformFunction> _whenStatements = new ArrayList<>();
  private List<TransformFunction> _thenStatements = new ArrayList<>();
  private TransformFunction _elseStatement;

  private boolean[] _computeThenStatements;
  private TransformResultMetadata _resultMetadata;
  private int[] _selectedResults;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    // Check that there are more than 2 arguments
    // Else statement can be omitted.
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least two arguments are required for CASE-WHEN function");
    }
    int numWhenStatements = arguments.size() / 2;
    _whenStatements = new ArrayList<>(numWhenStatements);
    _thenStatements = new ArrayList<>(numWhenStatements);
    constructStatementList(arguments);
    _computeThenStatements = new boolean[_thenStatements.size()];
    _resultMetadata = calculateResultMetadata();
  }

  private void constructStatementList(List<TransformFunction> arguments) {
    int numWhenStatements = arguments.size() / 2;
    boolean allBooleanFirstHalf = true;
    boolean notAllBooleanOddHalf = false;
    for (int i = 0; i < numWhenStatements; i++) {
      if (arguments.get(i).getResultMetadata().getDataType() != DataType.BOOLEAN) {
        allBooleanFirstHalf = false;
      }
      if (arguments.get(i * 2).getResultMetadata().getDataType() != DataType.BOOLEAN) {
        notAllBooleanOddHalf = true;
      }
    }
    if (allBooleanFirstHalf && notAllBooleanOddHalf) {
      constructStatementListLegacy(arguments);
    } else {
      constructStatementListCalcite(arguments);
    }
  }

  private void constructStatementListCalcite(List<TransformFunction> arguments) {
    int numWhenStatements = arguments.size() / 2;
    // alternating WHEN and THEN clause, last one ELSE
    for (int i = 0; i < numWhenStatements; i++) {
      _whenStatements.add(arguments.get(i * 2));
      _thenStatements.add(arguments.get(i * 2 + 1));
    }
    if (arguments.size() % 2 != 0) {
      _elseStatement = arguments.get(arguments.size() - 1);
    }
  }

  // TODO: Legacy format, this is here for backward compatibility support, remove after release 0.12
  private void constructStatementListLegacy(List<TransformFunction> arguments) {
    int numWhenStatements = arguments.size() / 2;
    // first half WHEN, second half THEN, last one ELSE
    for (int i = 0; i < numWhenStatements; i++) {
      _whenStatements.add(arguments.get(i));
    }
    for (int i = numWhenStatements; i < numWhenStatements * 2; i++) {
      _thenStatements.add(arguments.get(i));
    }
    if (arguments.size() % 2 != 0) {
      _elseStatement = arguments.get(arguments.size() - 1);
    }
  }

  private TransformResultMetadata calculateResultMetadata() {
    TransformResultMetadata elseStatementResultMetadata = _elseStatement.getResultMetadata();
    DataType dataType = elseStatementResultMetadata.getDataType();
    Preconditions.checkState(elseStatementResultMetadata.isSingleValue(),
        "Unsupported multi-value expression in the ELSE clause");
    int numThenStatements = _thenStatements.size();
    for (int i = 0; i < numThenStatements; i++) {
      TransformFunction thenStatement = _thenStatements.get(i);
      TransformResultMetadata thenStatementResultMetadata = thenStatement.getResultMetadata();
      if (!thenStatementResultMetadata.isSingleValue()) {
        throw new IllegalStateException("Unsupported multi-value expression in the THEN clause of index: " + i);
      }
      DataType thenStatementDataType = thenStatementResultMetadata.getDataType();

      // Upcast the data type to cover all the data types in THEN and ELSE clauses if they don't match
      // For numeric types:
      // - INT & LONG -> LONG
      // - INT & FLOAT/DOUBLE -> DOUBLE
      // - LONG & FLOAT/DOUBLE -> DOUBLE (might lose precision)
      // - FLOAT & DOUBLE -> DOUBLE
      // - Any numeric data type with BIG_DECIMAL -> BIG_DECIMAL
      // Use STRING to handle non-numeric types
      // UNKNOWN data type is ignored unless all data types are unknown, we return unknown types.
      if (thenStatementDataType == dataType) {
        continue;
      }
      switch (dataType) {
        case INT:
          switch (thenStatementDataType) {
            case LONG:
              dataType = DataType.LONG;
              break;
            case FLOAT:
            case DOUBLE:
              dataType = DataType.DOUBLE;
              break;
            case BIG_DECIMAL:
              dataType = DataType.BIG_DECIMAL;
              break;
            case UNKNOWN:
              break;
            default:
              dataType = DataType.STRING;
              break;
          }
          break;
        case LONG:
          switch (thenStatementDataType) {
            case INT: // fall through
            case UNKNOWN:
              break;
            case FLOAT:
            case DOUBLE:
              dataType = DataType.DOUBLE;
              break;
            case BIG_DECIMAL:
              dataType = DataType.BIG_DECIMAL;
              break;
            default:
              dataType = DataType.STRING;
              break;
          }
          break;
        case FLOAT:
          switch (thenStatementDataType) {
            case INT:
            case LONG:
            case DOUBLE:
              dataType = DataType.DOUBLE;
              break;
            case BIG_DECIMAL:
              dataType = DataType.BIG_DECIMAL;
              break;
            case UNKNOWN:
              break;
            default:
              dataType = DataType.STRING;
              break;
          }
          break;
        case DOUBLE:
          switch (thenStatementDataType) {
            case INT:
            case FLOAT:
            case LONG:
            case UNKNOWN:
              break;
            case BIG_DECIMAL:
              dataType = DataType.BIG_DECIMAL;
              break;
            default:
              dataType = DataType.STRING;
              break;
          }
          break;
        case BIG_DECIMAL:
          switch (thenStatementDataType) {
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
            case UNKNOWN:
              break;
            default:
              dataType = DataType.STRING;
              break;
          }
          break;
        case UNKNOWN:
          dataType = thenStatementDataType;
          break;
        default:
          dataType = DataType.STRING;
          break;
      }
    }
    return new TransformResultMetadata(dataType, true, false);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  /**
   * Evaluate the ValueBlock for the WHEN statements, returns an array with the index(1 to N) of matched WHEN clause -1
   * means there is no match.
   */
  private int[] getSelectedArray(ValueBlock valueBlock, boolean nullHandlingEnabled) {
    int numDocs = valueBlock.getNumDocs();
    if (_selectedResults == null || _selectedResults.length < numDocs) {
      _selectedResults = new int[numDocs];
    }
    Arrays.fill(_selectedResults, -1);
    Arrays.fill(_computeThenStatements, false);
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    int numWhenStatements = _whenStatements.size();
    for (int i = 0; i < numWhenStatements; i++) {
      TransformFunction whenStatement = _whenStatements.get(i);
      int[] conditions = getWhenConditions(whenStatement, valueBlock, nullHandlingEnabled);
      for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
        if (conditions[docId] == 1) {
          unselectedDocs.clear(docId);
          _selectedResults[docId] = i;
        }
      }
      if (unselectedDocs.isEmpty()) {
        break;
      }
    }
    // try to prune clauses now
    for (int i = 0; i < numDocs; i++) {
      if (_selectedResults[i] != -1) {
        _computeThenStatements[_selectedResults[i]] = true;
      }
    }
    return _selectedResults;
  }

  // Returns an array of valueBlock length to indicate whether a row is selected or not.
  // When nullHandlingEnabled is set to true, we also check whether the row is null and set to false if null.
  private static int[] getWhenConditions(TransformFunction whenStatement, ValueBlock valueBlock,
      boolean nullHandlingEnabled) {
    if (!nullHandlingEnabled) {
      return whenStatement.transformToIntValuesSV(valueBlock);
    }
    Pair<int[], RoaringBitmap> result = whenStatement.transformToIntValuesSVWithNull(valueBlock);
    RoaringBitmap bitmap = result.getRight();
    int[] intResult = result.getLeft();
    if (bitmap != null) {
      for (int i : bitmap) {
        intResult[i] = 0;
      }
    }
    return intResult;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, int[]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToIntValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _intValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _intValuesSV[docId] = (int) DataSchema.ColumnDataType.INT.getNullPlaceholder();
        }
      } else {
        int[] intValuesSV = _elseStatement.transformToIntValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _intValuesSV[docId] = intValuesSV[docId];
        }
      }
    }
    return _intValuesSV;
  }

  @Override
  public Pair<int[], RoaringBitmap> transformToIntValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<int[], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToIntValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<int[], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _intValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _intValuesSV[docId] = (int) DataSchema.ColumnDataType.INT.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<int[], RoaringBitmap> intValuesNullPair = _elseStatement.transformToIntValuesSVWithNull(valueBlock);
        int[] intValues = intValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = intValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _intValuesSV[docId] = intValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_intValuesSV, bitmap);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, long[]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToLongValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _longValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _longValuesSV[docId] = (long) DataSchema.ColumnDataType.LONG.getNullPlaceholder();
        }
      } else {
        long[] longValuesSV = _elseStatement.transformToLongValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _longValuesSV[docId] = longValuesSV[docId];
        }
      }
    }
    return _longValuesSV;
  }

  @Override
  public Pair<long[], RoaringBitmap> transformToLongValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initLongValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<long[], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToLongValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<long[], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _longValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _longValuesSV[docId] = (long) DataSchema.ColumnDataType.LONG.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<long[], RoaringBitmap> longValuesNullPair = _elseStatement.transformToLongValuesSVWithNull(valueBlock);
        long[] longValues = longValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = longValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _longValuesSV[docId] = longValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_longValuesSV, bitmap);
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, float[]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToFloatValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _floatValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _floatValuesSV[docId] = (float) DataSchema.ColumnDataType.FLOAT.getNullPlaceholder();
        }
      } else {
        float[] floatValuesSV = _elseStatement.transformToFloatValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _floatValuesSV[docId] = floatValuesSV[docId];
        }
      }
    }
    return _floatValuesSV;
  }

  @Override
  public Pair<float[], RoaringBitmap> transformToFloatValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initFloatValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<float[], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToFloatValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<float[], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _floatValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _floatValuesSV[docId] = (float) DataSchema.ColumnDataType.FLOAT.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<float[], RoaringBitmap> floatValuesNullPair = _elseStatement.transformToFloatValuesSVWithNull(valueBlock);
        float[] floatValues = floatValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = floatValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _floatValuesSV[docId] = floatValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_floatValuesSV, bitmap);
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, double[]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToDoubleValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _doubleValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _doubleValuesSV[docId] = (double) DataSchema.ColumnDataType.DOUBLE.getNullPlaceholder();
        }
      } else {
        float[] doubleValuesSV = _elseStatement.transformToFloatValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _doubleValuesSV[docId] = doubleValuesSV[docId];
        }
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public Pair<double[], RoaringBitmap> transformToDoubleValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initDoubleValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<double[], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToDoubleValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<double[], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _doubleValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _doubleValuesSV[docId] = (double) DataSchema.ColumnDataType.DOUBLE.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<double[], RoaringBitmap> doubleValuesNullPair =
            _elseStatement.transformToDoubleValuesSVWithNull(valueBlock);
        double[] doubleValues = doubleValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = doubleValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _doubleValuesSV[docId] = doubleValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_doubleValuesSV, bitmap);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initBigDecimalValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, BigDecimal[]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToBigDecimalValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _bigDecimalValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bigDecimalValuesSV[docId] = (BigDecimal) DataSchema.ColumnDataType.BIG_DECIMAL.getNullPlaceholder();
        }
      } else {
        BigDecimal[] bigDecimalValuesSV = _elseStatement.transformToBigDecimalValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bigDecimalValuesSV[docId] = bigDecimalValuesSV[docId];
        }
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public Pair<BigDecimal[], RoaringBitmap> transformToBigDecimalValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initBigDecimalValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<BigDecimal[], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToBigDecimalValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<BigDecimal[], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _bigDecimalValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bigDecimalValuesSV[docId] = (BigDecimal) DataSchema.ColumnDataType.BIG_DECIMAL.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<BigDecimal[], RoaringBitmap> bigDecimalValuesNullPair =
            _elseStatement.transformToBigDecimalValuesSVWithNull(valueBlock);
        BigDecimal[] bigDecimalValues = bigDecimalValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = bigDecimalValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bigDecimalValuesSV[docId] = bigDecimalValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_bigDecimalValuesSV, bitmap);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, String[]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToStringValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _stringValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _stringValuesSV[docId] = (String) DataSchema.ColumnDataType.STRING.getNullPlaceholder();
        }
      } else {
        String[] stringValuesSV = _elseStatement.transformToStringValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _stringValuesSV[docId] = stringValuesSV[docId];
        }
      }
    }
    return _stringValuesSV;
  }

  @Override
  public Pair<String[], RoaringBitmap> transformToStringValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<String[], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToStringValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<String[], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _stringValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _stringValuesSV[docId] = (String) DataSchema.ColumnDataType.STRING.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<String[], RoaringBitmap> stringValuesNullPair =
            _elseStatement.transformToStringValuesSVWithNull(valueBlock);
        String[] stringValues = stringValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = stringValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _stringValuesSV[docId] = stringValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_stringValuesSV, bitmap);
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    int[] selected = getSelectedArray(valueBlock, false);
    int numDocs = valueBlock.getNumDocs();
    initBytesValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, byte[][]> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToBytesValuesSV(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        _bytesValuesSV[docId] = thenStatementsIndexToValues.get(selected[docId])[docId];
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bytesValuesSV[docId] = (byte[]) DataSchema.ColumnDataType.BYTES.getNullPlaceholder();
        }
      } else {
        byte[][] byteValuesSV = _elseStatement.transformToBytesValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bytesValuesSV[docId] = byteValuesSV[docId];
        }
      }
    }
    return _bytesValuesSV;
  }

  @Override
  public Pair<byte[][], RoaringBitmap> transformToBytesValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSVWithNull(valueBlock);
    }
    final RoaringBitmap bitmap = new RoaringBitmap();
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    initStringValuesSV(numDocs);
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    Map<Integer, Pair<byte[][], RoaringBitmap>> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).transformToBytesValuesSVWithNull(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        Pair<byte[][], RoaringBitmap> nullValuePair = thenStatementsIndexToValues.get(selected[docId]);
        _bytesValuesSV[docId] = nullValuePair.getLeft()[docId];
        RoaringBitmap nullBitmap = nullValuePair.getRight();
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
        if (unselectedDocs.isEmpty()) {
          break;
        }
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bytesValuesSV[docId] = (byte[]) DataSchema.ColumnDataType.BYTES.getNullPlaceholder();
          bitmap.add(docId);
        }
      } else {
        Pair<byte[][], RoaringBitmap> bytesValuesNullPair = _elseStatement.transformToBytesValuesSVWithNull(valueBlock);
        byte[][] byteValues = bytesValuesNullPair.getLeft();
        RoaringBitmap nullBitmap = bytesValuesNullPair.getRight();
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bytesValuesSV[docId] = byteValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return ImmutablePair.of(_bytesValuesSV, bitmap);
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    int[] selected = getSelectedArray(valueBlock, true);
    int numDocs = valueBlock.getNumDocs();
    int numThenStatements = _thenStatements.size();
    BitSet unselectedDocs = new BitSet();
    unselectedDocs.set(0, numDocs);
    final RoaringBitmap bitmap = new RoaringBitmap();
    Map<Integer, RoaringBitmap> thenStatementsIndexToValues = new HashMap<>();
    for (int i = 0; i < numThenStatements; i++) {
      if (_computeThenStatements[i]) {
        thenStatementsIndexToValues.put(i, _thenStatements.get(i).getNullBitmap(valueBlock));
      }
    }
    for (int docId = 0; docId < numDocs; docId++) {
      if (selected[docId] >= 0) {
        RoaringBitmap nullBitmap = thenStatementsIndexToValues.get(selected[docId]);
        if (nullBitmap != null && nullBitmap.contains(docId)) {
          bitmap.add(docId);
        }
        unselectedDocs.clear(docId);
      }
    }
    if (!unselectedDocs.isEmpty()) {
      if (_elseStatement == null) {
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          bitmap.add(docId);
        }
      } else {
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    if (bitmap.isEmpty()) {
      return null;
    }
    return bitmap;
  }
}
