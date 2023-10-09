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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>CaseTransformFunction</code> class implements the CASE-WHEN-THEN-ELSE transformation.
 * <p>
 * The SQL Syntax is: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 WHEN conditionN THEN resultN ELSE
 * result END;
 * <p>
 * Usage: case(${WHEN_STATEMENT_1}, ${THEN_EXPRESSION_1}, ..., ${WHEN_STATEMENT_N}, ${THEN_EXPRESSION_N}, ...,
 * ${ELSE_EXPRESSION})
 * <p>
 * There are 2 * N + 1 arguments:
 * <code>WHEN_STATEMENT_$i</code> is a <code>BinaryOperatorTransformFunction</code> represents <code>condition$i</code>
 * <code>THEN_EXPRESSION_$i</code> is a <code>TransformFunction</code> represents <code>result$i</code>
 * <code>ELSE_EXPRESSION</code> is a <code>TransformFunction</code> represents <code>result</code>
 * <p>
 * ELSE_EXPRESSION can be omitted. When none of when statements is evaluated to be true, and there is no else
 * expression, we output null. Note that when statement is considered as false if it is evaluated to be null.
 * <p>
 * PostgreSQL documentation: <a href="https://www.postgresql.org/docs/current/typeconv-union-case.html">CASE</a>
 */
public class CaseTransformFunction extends ComputeDifferentlyWhenNullHandlingEnabledTransformFunction {
  public static final String FUNCTION_NAME = "case";

  private List<TransformFunction> _whenStatements = new ArrayList<>();
  private List<TransformFunction> _thenStatements = new ArrayList<>();
  private TransformFunction _elseStatement;
  private TransformResultMetadata _resultMetadata;

  private boolean[] _computeThenStatements;
  private int[] _selectedResults;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    // Check that there are more than 2 arguments
    // Else statement can be omitted.
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least two arguments are required for CASE-WHEN function");
    }
    int numWhenStatements = arguments.size() / 2;
    _whenStatements = new ArrayList<>(numWhenStatements);
    _thenStatements = new ArrayList<>(numWhenStatements);
    // Alternating WHEN and THEN clause, last one ELSE
    for (int i = 0; i < numWhenStatements; i++) {
      _whenStatements.add(arguments.get(i * 2));
      _thenStatements.add(arguments.get(i * 2 + 1));
    }
    if (arguments.size() % 2 != 0 && !isNullLiteral(arguments.get(arguments.size() - 1))) {
      _elseStatement = arguments.get(arguments.size() - 1);
    }
    _resultMetadata = new TransformResultMetadata(calculateResultType(), true, false);
    _computeThenStatements = new boolean[numWhenStatements];
  }

  private boolean isNullLiteral(TransformFunction function) {
    return function instanceof LiteralTransformFunction && ((LiteralTransformFunction) function).isNull();
  }

  private DataType calculateResultType() {
    MutablePair<DataType, List<String>> typeAndUnresolvedLiterals =
        new MutablePair<>(DataType.UNKNOWN, new ArrayList<>());
    for (TransformFunction thenStatement : _thenStatements) {
      upcast(typeAndUnresolvedLiterals, thenStatement);
    }
    if (_elseStatement != null) {
      upcast(typeAndUnresolvedLiterals, _elseStatement);
    }
    DataType dataType = typeAndUnresolvedLiterals.getLeft();
    // If all inputs are of type UNKNOWN, resolve as type STRING
    return dataType != DataType.UNKNOWN ? dataType : DataType.STRING;
  }

  private void upcast(MutablePair<DataType, List<String>> currentTypeAndUnresolvedLiterals,
      TransformFunction newFunction) {
    TransformResultMetadata newMetadata = newFunction.getResultMetadata();
    Preconditions.checkArgument(newMetadata.isSingleValue(), "Unsupported multi-value expression in THEN/ELSE clause");
    DataType newType = newMetadata.getDataType();
    if (newType == DataType.UNKNOWN) {
      return;
    }
    DataType currentType = currentTypeAndUnresolvedLiterals.getLeft();
    if (currentType == newType) {
      return;
    }
    List<String> unresolvedLiterals = currentTypeAndUnresolvedLiterals.getRight();
    // Treat string literals as UNKNOWN type. Resolve them when we get a non-UNKNOWN type.
    boolean isNewFunctionStringLiteral = newFunction instanceof LiteralTransformFunction && newType == DataType.STRING;
    if (currentType == DataType.UNKNOWN) {
      if (isNewFunctionStringLiteral) {
        unresolvedLiterals.add(((LiteralTransformFunction) newFunction).getStringLiteral());
      } else {
        currentTypeAndUnresolvedLiterals.setLeft(newType);
        for (String unresolvedLiteral : unresolvedLiterals) {
          checkLiteral(newType, unresolvedLiteral);
        }
        unresolvedLiterals.clear();
      }
    } else {
      assert unresolvedLiterals.isEmpty();
      if (isNewFunctionStringLiteral) {
        checkLiteral(currentType, ((LiteralTransformFunction) newFunction).getStringLiteral());
      } else {
        // Only allow upcast from numeric to numeric: INT -> LONG -> FLOAT -> DOUBLE -> BIG_DECIMAL
        Preconditions.checkArgument(currentType.isNumeric() && newType.isNumeric(), "Cannot upcast from %s to %s",
            currentType, newType);
        if (newType.ordinal() > currentType.ordinal()) {
          currentTypeAndUnresolvedLiterals.setLeft(newType);
        }
      }
    }
  }

  private void checkLiteral(DataType dataType, String literal) {
    switch (dataType) {
      case INT:
        try {
          Integer.parseInt(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for INT");
        }
        break;
      case LONG:
        try {
          Long.parseLong(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for LONG");
        }
        break;
      case FLOAT:
        try {
          Float.parseFloat(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for FLOAT");
        }
        break;
      case DOUBLE:
        try {
          Double.parseDouble(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for DOUBLE");
        }
        break;
      case BIG_DECIMAL:
        try {
          new BigDecimal(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for BIG_DECIMAL");
        }
        break;
      case BOOLEAN:
        Preconditions.checkArgument(
            literal.equalsIgnoreCase("true") || literal.equalsIgnoreCase("false") || literal.equals("1")
                || literal.equals("0"), "Invalid literal: %s for BOOLEAN", literal);
        break;
      case TIMESTAMP:
        try {
          TimestampUtils.toTimestamp(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for TIMESTAMP");
        }
        break;
      case STRING:
      case JSON:
        break;
      case BYTES:
        try {
          BytesUtils.toBytes(literal);
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid literal: " + literal + " for BYTES");
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
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
    int[] intResult = whenStatement.transformToIntValuesSV(valueBlock);
    RoaringBitmap bitmap = whenStatement.getNullBitmap(valueBlock);
    if (bitmap != null) {
      for (int i : bitmap) {
        intResult[i] = 0;
      }
    }
    return intResult;
  }

  @Override
  protected int[] transformToIntValuesSVUsingValue(ValueBlock valueBlock) {
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
          _intValuesSV[docId] = NullValuePlaceHolder.INT;
        }
      } else {
        int[] intValues = _elseStatement.transformToIntValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _intValuesSV[docId] = intValues[docId];
        }
      }
    }
    return _intValuesSV;
  }

  @Override
  protected int[] transformToIntValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i, ImmutablePair.of(_thenStatements.get(i).transformToIntValuesSV(valueBlock),
            _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _intValuesSV[docId] = NullValuePlaceHolder.INT;
          bitmap.add(docId);
        }
      } else {
        int[] intValues = _elseStatement.transformToIntValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _intValuesSV[docId] = intValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _intValuesSV;
  }

  @Override
  protected long[] transformToLongValuesSVUsingValue(ValueBlock valueBlock) {
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
          _longValuesSV[docId] = NullValuePlaceHolder.LONG;
        }
      } else {
        long[] longValues = _elseStatement.transformToLongValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _longValuesSV[docId] = longValues[docId];
        }
      }
    }
    return _longValuesSV;
  }

  @Override
  protected long[] transformToLongValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i, ImmutablePair.of(_thenStatements.get(i).transformToLongValuesSV(valueBlock),
            _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _longValuesSV[docId] = NullValuePlaceHolder.LONG;
          bitmap.add(docId);
        }
      } else {
        long[] longValues = _elseStatement.transformToLongValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _longValuesSV[docId] = longValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _longValuesSV;
  }

  @Override
  protected float[] transformToFloatValuesSVUsingValue(ValueBlock valueBlock) {
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
          _floatValuesSV[docId] = NullValuePlaceHolder.FLOAT;
        }
      } else {
        float[] floatValues = _elseStatement.transformToFloatValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _floatValuesSV[docId] = floatValues[docId];
        }
      }
    }
    return _floatValuesSV;
  }

  @Override
  protected float[] transformToFloatValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i, ImmutablePair.of(_thenStatements.get(i).transformToFloatValuesSV(valueBlock),
            _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _floatValuesSV[docId] = NullValuePlaceHolder.FLOAT;
          bitmap.add(docId);
        }
      } else {
        float[] floatValues = _elseStatement.transformToFloatValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _floatValuesSV[docId] = floatValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _floatValuesSV;
  }

  @Override
  protected double[] transformToDoubleValuesSVUsingValue(ValueBlock valueBlock) {
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
          _doubleValuesSV[docId] = NullValuePlaceHolder.DOUBLE;
        }
      } else {
        double[] doubleValues = _elseStatement.transformToDoubleValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _doubleValuesSV[docId] = doubleValues[docId];
        }
      }
    }
    return _doubleValuesSV;
  }

  @Override
  protected double[] transformToDoubleValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i,
            ImmutablePair.of(_thenStatements.get(i).transformToDoubleValuesSV(valueBlock),
                _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _doubleValuesSV[docId] = NullValuePlaceHolder.DOUBLE;
          bitmap.add(docId);
        }
      } else {
        double[] doubleValues = _elseStatement.transformToDoubleValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _doubleValuesSV[docId] = doubleValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _doubleValuesSV;
  }

  @Override
  protected BigDecimal[] transformToBigDecimalValuesSVUsingValue(ValueBlock valueBlock) {
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
          _bigDecimalValuesSV[docId] = NullValuePlaceHolder.BIG_DECIMAL;
        }
      } else {
        BigDecimal[] bigDecimalValues = _elseStatement.transformToBigDecimalValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bigDecimalValuesSV[docId] = bigDecimalValues[docId];
        }
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  protected BigDecimal[] transformToBigDecimalValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i,
            ImmutablePair.of(_thenStatements.get(i).transformToBigDecimalValuesSV(valueBlock),
                _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _bigDecimalValuesSV[docId] = NullValuePlaceHolder.BIG_DECIMAL;
          bitmap.add(docId);
        }
      } else {
        BigDecimal[] bigDecimalValues = _elseStatement.transformToBigDecimalValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bigDecimalValuesSV[docId] = bigDecimalValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  protected String[] transformToStringValuesSVUsingValue(ValueBlock valueBlock) {
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
          _stringValuesSV[docId] = NullValuePlaceHolder.STRING;
        }
      } else {
        String[] stringValues = _elseStatement.transformToStringValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _stringValuesSV[docId] = stringValues[docId];
        }
      }
    }
    return _stringValuesSV;
  }

  @Override
  protected String[] transformToStringValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i,
            ImmutablePair.of(_thenStatements.get(i).transformToStringValuesSV(valueBlock),
                _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _stringValuesSV[docId] = NullValuePlaceHolder.STRING;
          bitmap.add(docId);
        }
      } else {
        String[] stringValues = _elseStatement.transformToStringValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _stringValuesSV[docId] = stringValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _stringValuesSV;
  }

  @Override
  protected byte[][] transformToBytesValuesSVUsingValue(ValueBlock valueBlock) {
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
          _bytesValuesSV[docId] = NullValuePlaceHolder.BYTES;
        }
      } else {
        byte[][] bytesValues = _elseStatement.transformToBytesValuesSV(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bytesValuesSV[docId] = bytesValues[docId];
        }
      }
    }
    return _bytesValuesSV;
  }

  @Override
  protected byte[][] transformToBytesValuesSVUsingValueAndNull(ValueBlock valueBlock) {
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
        thenStatementsIndexToValues.put(i, ImmutablePair.of(_thenStatements.get(i).transformToBytesValuesSV(valueBlock),
            _thenStatements.get(i).getNullBitmap(valueBlock)));
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
          _bytesValuesSV[docId] = NullValuePlaceHolder.BYTES;
          bitmap.add(docId);
        }
      } else {
        byte[][] bytesValues = _elseStatement.transformToBytesValuesSV(valueBlock);
        RoaringBitmap nullBitmap = _elseStatement.getNullBitmap(valueBlock);
        for (int docId = unselectedDocs.nextSetBit(0); docId >= 0; docId = unselectedDocs.nextSetBit(docId + 1)) {
          _bytesValuesSV[docId] = bytesValues[docId];
          if (nullBitmap != null && nullBitmap.contains(docId)) {
            bitmap.add(docId);
          }
        }
      }
    }
    return _bytesValuesSV;
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
