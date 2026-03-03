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
package org.apache.pinot.core.operator.transform;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.ArrayJoinType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.DocIdOrderedOperator.DocIdOrder;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.ArrayJoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/**
 * ARRAY JOIN operator that expands rows based on array join operands.
 */
public class ArrayJoinOperator extends BaseProjectOperator<ValueBlock> {
  private static final String EXPLAIN_NAME = "ARRAY_JOIN";

  private final QueryContext _queryContext;
  private final List<ArrayJoinContext> _arrayJoinContexts;
  private final BaseProjectOperator<? extends ValueBlock> _inputOperator;
  private final FilterContext _filter;
  private final boolean _applyFilterAfterJoin;
  private final boolean _nullHandlingEnabled;
  private final Map<ExpressionContext, OperandInfo> _operandInfoMap;
  private final Map<String, OperandInfo> _aliasMap;

  public ArrayJoinOperator(QueryContext queryContext, BaseProjectOperator<? extends ValueBlock> inputOperator) {
    _queryContext = queryContext;
    _arrayJoinContexts = queryContext.getArrayJoinContexts();
    _inputOperator = inputOperator;
    _filter = queryContext.getFilter();
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();
    _operandInfoMap = new HashMap<>();
    _aliasMap = new HashMap<>();
    buildOperandInfo();
    _applyFilterAfterJoin = shouldApplyFilterAfterJoin(_filter, _arrayJoinContexts);
  }

  @Override
  protected ValueBlock getNextBlock() {
    ValueBlock inputBlock;
    while ((inputBlock = _inputOperator.nextBlock()) != null) {
      ArrayJoinValueBlock expandedBlock = expandBlock(inputBlock);
      if (_applyFilterAfterJoin && _filter != null && expandedBlock.getNumDocs() > 0) {
        expandedBlock = applyFilter(expandedBlock, _filter);
      }
      if (expandedBlock.getNumDocs() > 0) {
        return expandedBlock;
      }
    }
    return null;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_inputOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _inputOperator.getExecutionStatistics();
  }

  @Override
  public boolean isCompatibleWith(DocIdOrder order) {
    return _inputOperator.isCompatibleWith(order);
  }

  @Override
  public Map<String, ColumnContext> getSourceColumnContextMap() {
    return _inputOperator.getSourceColumnContextMap();
  }

  @Override
  public ColumnContext getResultColumnContext(ExpressionContext expression) {
    OperandInfo operandInfo = resolveOperandInfo(expression);
    if (operandInfo != null) {
      ColumnContext columnContext = _inputOperator.getResultColumnContext(operandInfo.getExpression());
      if (columnContext != null) {
        return ColumnContext.withSingleValue(columnContext, true);
      }
    }
    return _inputOperator.getResultColumnContext(expression);
  }

  @Override
  public BaseProjectOperator<ValueBlock> withOrder(DocIdOrder newOrder) {
    BaseProjectOperator<? extends ValueBlock> reordered = _inputOperator.withOrder(newOrder);
    if (reordered == _inputOperator) {
      return this;
    }
    return new ArrayJoinOperator(_queryContext, reordered);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  public List<ArrayJoinContext> getArrayJoinContexts() {
    return _arrayJoinContexts;
  }

  private void buildOperandInfo() {
    int contextIndex = 0;
    for (ArrayJoinContext context : _arrayJoinContexts) {
      for (ArrayJoinContext.Operand operand : context.getOperands()) {
        OperandInfo operandInfo = new OperandInfo(contextIndex, operand.getExpression(), operand.getAlias());
        _operandInfoMap.put(operand.getExpression(), operandInfo);
        if (operand.getAlias() != null) {
          registerAlias(operand.getAlias(), operandInfo);
        }
      }
      contextIndex++;
    }
  }

  private void registerAlias(String alias, OperandInfo operandInfo) {
    _aliasMap.put(alias, operandInfo);
    String normalized = normalizeAliasKey(alias);
    if (!normalized.equals(alias)) {
      _aliasMap.putIfAbsent(normalized, operandInfo);
    }
  }

  @Nullable
  private OperandInfo resolveOperandInfo(ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
      String identifier = expression.getIdentifier();
      OperandInfo aliasInfo = _aliasMap.get(identifier);
      if (aliasInfo == null) {
        aliasInfo = _aliasMap.get(normalizeAliasKey(identifier));
      }
      if (aliasInfo != null) {
        return aliasInfo;
      }
    }
    return _operandInfoMap.get(expression);
  }

  private ArrayJoinValueBlock expandBlock(ValueBlock inputBlock) {
    int numDocs = inputBlock.getNumDocs();
    if (numDocs == 0) {
      return new ArrayJoinValueBlock(inputBlock, new int[0], new int[_arrayJoinContexts.size()][0], null,
          _operandInfoMap, _aliasMap);
    }

    int numContexts = _arrayJoinContexts.size();
    List<List<OperandData>> operandDataByContext = new ArrayList<>(numContexts);
    for (ArrayJoinContext context : _arrayJoinContexts) {
      List<OperandData> operands = new ArrayList<>(context.getOperands().size());
      for (ArrayJoinContext.Operand operand : context.getOperands()) {
        ExpressionContext expression = operand.getExpression();
        BlockValSet valueSet = inputBlock.getBlockValueSet(expression);
        operands.add(new OperandData(valueSet));
      }
      operandDataByContext.add(operands);
    }

    int[] docIndexMapping;
    int[][] elementIndexMapping;

    int[] docIds = inputBlock.getDocIds();
    List<Integer> docIndexList = new ArrayList<>();
    List<List<Integer>> elementIndexListsByContext = new ArrayList<>(numContexts);
    for (int i = 0; i < numContexts; i++) {
      elementIndexListsByContext.add(new ArrayList<>());
    }

    for (int docIndex = 0; docIndex < numDocs; docIndex++) {
      int[] joinLengths = new int[numContexts];
      boolean skipDoc = false;
      for (int contextIndex = 0; contextIndex < numContexts; contextIndex++) {
        ArrayJoinContext context = _arrayJoinContexts.get(contextIndex);
        int joinLength = computeJoinLength(context, operandDataByContext.get(contextIndex), docIndex);
        if (joinLength == 0) {
          if (context.getType() == ArrayJoinType.INNER) {
            skipDoc = true;
            break;
          } else {
            joinLength = 1;
          }
        }
        joinLengths[contextIndex] = joinLength;
      }
      if (skipDoc) {
        continue;
      }

      int totalRows = 1;
      for (int length : joinLengths) {
        totalRows *= length;
      }

      int[] elementIndices = new int[numContexts];
      for (int row = 0; row < totalRows; row++) {
        docIndexList.add(docIndex);
        for (int contextIndex = 0; contextIndex < numContexts; contextIndex++) {
          elementIndexListsByContext.get(contextIndex).add(elementIndices[contextIndex]);
        }
        for (int contextIndex = numContexts - 1; contextIndex >= 0; contextIndex--) {
          int next = elementIndices[contextIndex] + 1;
          if (next < joinLengths[contextIndex]) {
            elementIndices[contextIndex] = next;
            break;
          }
          elementIndices[contextIndex] = 0;
        }
      }
    }

    int numRows = docIndexList.size();
    docIndexMapping = new int[numRows];
    for (int i = 0; i < numRows; i++) {
      docIndexMapping[i] = docIndexList.get(i);
    }
    elementIndexMapping = new int[numContexts][numRows];
    for (int contextIndex = 0; contextIndex < numContexts; contextIndex++) {
      List<Integer> indices = elementIndexListsByContext.get(contextIndex);
      for (int i = 0; i < numRows; i++) {
        elementIndexMapping[contextIndex][i] = indices.get(i);
      }
    }

    int[] outputDocIds = null;
    if (docIds != null) {
      outputDocIds = new int[numRows];
      for (int i = 0; i < numRows; i++) {
        outputDocIds[i] = docIds[docIndexMapping[i]];
      }
    }

    return new ArrayJoinValueBlock(inputBlock, docIndexMapping, elementIndexMapping, outputDocIds, _operandInfoMap,
        _aliasMap);
  }

  private int computeJoinLength(ArrayJoinContext context, List<OperandData> operands, int docIndex) {
    int minLength = Integer.MAX_VALUE;
    int maxLength = 0;
    boolean hasMultiValue = false;
    for (OperandData operandData : operands) {
      if (operandData.isSingleValue()) {
        continue;
      }
      hasMultiValue = true;
      int length = operandData.getNumEntries(docIndex);
      minLength = Math.min(minLength, length);
      maxLength = Math.max(maxLength, length);
    }
    if (!hasMultiValue) {
      return 1;
    }
    return context.getType() == ArrayJoinType.INNER ? minLength : maxLength;
  }

  private ArrayJoinValueBlock applyFilter(ArrayJoinValueBlock block, FilterContext filter) {
    int numRows = block.getNumDocs();
    if (numRows == 0) {
      return block;
    }
    boolean[] matches = evaluateFilter(filter, block);
    int matchCount = 0;
    for (boolean match : matches) {
      if (match) {
        matchCount++;
      }
    }
    if (matchCount == numRows) {
      return block;
    }
    int numContexts = _arrayJoinContexts.size();
    int[] docIndexMapping = new int[matchCount];
    int[][] elementIndexMapping = new int[numContexts][matchCount];
    int[] outputDocIds = block.getDocIds();
    int[] filteredDocIds = outputDocIds != null ? new int[matchCount] : null;
    int writeIndex = 0;
    for (int i = 0; i < numRows; i++) {
      if (!matches[i]) {
        continue;
      }
      docIndexMapping[writeIndex] = block.getDocIndexMapping()[i];
      for (int contextIndex = 0; contextIndex < numContexts; contextIndex++) {
        elementIndexMapping[contextIndex][writeIndex] = block.getElementIndexMapping()[contextIndex][i];
      }
      if (filteredDocIds != null) {
        filteredDocIds[writeIndex] = outputDocIds[i];
      }
      writeIndex++;
    }
    return new ArrayJoinValueBlock(block.getInputBlock(), docIndexMapping, elementIndexMapping, filteredDocIds,
        _operandInfoMap, _aliasMap);
  }

  private boolean[] evaluateFilter(FilterContext filter, ArrayJoinValueBlock block) {
    int numRows = block.getNumDocs();
    switch (filter.getType()) {
      case CONSTANT:
        boolean[] constants = new boolean[numRows];
        if (filter.isConstantTrue()) {
          for (int i = 0; i < numRows; i++) {
            constants[i] = true;
          }
        }
        return constants;
      case PREDICATE:
        return evaluatePredicate(filter.getPredicate(), block);
      case AND:
        boolean[] andMatches = null;
        for (FilterContext child : filter.getChildren()) {
          boolean[] childMatches = evaluateFilter(child, block);
          if (andMatches == null) {
            andMatches = childMatches;
          } else {
            for (int i = 0; i < numRows; i++) {
              andMatches[i] &= childMatches[i];
            }
          }
        }
        return andMatches != null ? andMatches : new boolean[numRows];
      case OR:
        boolean[] orMatches = new boolean[numRows];
        for (FilterContext child : filter.getChildren()) {
          boolean[] childMatches = evaluateFilter(child, block);
          for (int i = 0; i < numRows; i++) {
            orMatches[i] |= childMatches[i];
          }
        }
        return orMatches;
      case NOT:
        boolean[] childMatches = evaluateFilter(filter.getChildren().get(0), block);
        boolean[] notMatches = new boolean[numRows];
        for (int i = 0; i < numRows; i++) {
          notMatches[i] = !childMatches[i];
        }
        return notMatches;
      default:
        throw new IllegalStateException("Unsupported filter type: " + filter.getType());
    }
  }

  private boolean[] evaluatePredicate(Predicate predicate, ArrayJoinValueBlock block) {
    int numRows = block.getNumDocs();
    boolean[] matches = new boolean[numRows];
    BlockValSet blockValSet = block.getBlockValueSet(predicate.getLhs());
    RoaringBitmap nullBitmap = _nullHandlingEnabled ? blockValSet.getNullBitmap() : null;
    if (predicate.getType() == Predicate.Type.IS_NULL || predicate.getType() == Predicate.Type.IS_NOT_NULL) {
      boolean isNullCheck = predicate.getType() == Predicate.Type.IS_NULL;
      if (nullBitmap == null) {
        if (!isNullCheck) {
          for (int i = 0; i < numRows; i++) {
            matches[i] = true;
          }
        }
        return matches;
      }
      for (int i = 0; i < numRows; i++) {
        boolean isNull = nullBitmap.contains(i);
        matches[i] = isNullCheck == isNull;
      }
      return matches;
    }

    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(predicate, null, blockValSet.getValueType(), _queryContext);
    if (evaluator.isAlwaysFalse()) {
      return matches;
    }

    DataType storedType = blockValSet.getValueType().getStoredType();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT: {
          int[] values = blockValSet.getIntValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case LONG: {
          long[] values = blockValSet.getLongValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case FLOAT: {
          float[] values = blockValSet.getFloatValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case DOUBLE: {
          double[] values = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case BIG_DECIMAL: {
          BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case STRING: {
          String[] values = blockValSet.getStringValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case BYTES: {
          byte[][] values = blockValSet.getBytesValuesSV();
          for (int i = 0; i < numRows; i++) {
            if (nullBitmap != null && nullBitmap.contains(i)) {
              continue;
            }
            matches[i] = evaluator.applySV(values[i]);
          }
          break;
        }
        case UNKNOWN:
          break;
        default:
          throw new UnsupportedOperationException("Unsupported predicate data type: " + storedType);
      }
      return matches;
    }

    switch (storedType) {
      case INT: {
        int[][] values = blockValSet.getIntValuesMV();
        int[] numEntries = blockValSet.getNumMVEntries();
        for (int i = 0; i < numRows; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          matches[i] = evaluator.applyMV(values[i], numEntries[i]);
        }
        break;
      }
      case LONG: {
        long[][] values = blockValSet.getLongValuesMV();
        int[] numEntries = blockValSet.getNumMVEntries();
        for (int i = 0; i < numRows; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          matches[i] = evaluator.applyMV(values[i], numEntries[i]);
        }
        break;
      }
      case FLOAT: {
        float[][] values = blockValSet.getFloatValuesMV();
        int[] numEntries = blockValSet.getNumMVEntries();
        for (int i = 0; i < numRows; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          matches[i] = evaluator.applyMV(values[i], numEntries[i]);
        }
        break;
      }
      case DOUBLE: {
        double[][] values = blockValSet.getDoubleValuesMV();
        int[] numEntries = blockValSet.getNumMVEntries();
        for (int i = 0; i < numRows; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          matches[i] = evaluator.applyMV(values[i], numEntries[i]);
        }
        break;
      }
      case STRING: {
        String[][] values = blockValSet.getStringValuesMV();
        int[] numEntries = blockValSet.getNumMVEntries();
        for (int i = 0; i < numRows; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          matches[i] = evaluator.applyMV(values[i], numEntries[i]);
        }
        break;
      }
      case BYTES: {
        byte[][][] values = blockValSet.getBytesValuesMV();
        int[] numEntries = blockValSet.getNumMVEntries();
        for (int i = 0; i < numRows; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          matches[i] = evaluator.applyMV(values[i], numEntries[i]);
        }
        break;
      }
      default:
        throw new UnsupportedOperationException("Unsupported predicate data type: " + storedType);
    }
    return matches;
  }

  private static boolean shouldApplyFilterAfterJoin(@Nullable FilterContext filter,
      List<ArrayJoinContext> arrayJoinContexts) {
    if (filter == null || arrayJoinContexts == null || arrayJoinContexts.isEmpty()) {
      return false;
    }
    Set<String> arrayJoinColumns = new HashSet<>();
    for (ArrayJoinContext context : arrayJoinContexts) {
      for (ArrayJoinContext.Operand operand : context.getOperands()) {
        operand.getExpression().getColumns(arrayJoinColumns);
        String alias = operand.getAlias();
        if (alias != null) {
          arrayJoinColumns.add(alias);
        }
      }
    }
    if (arrayJoinColumns.isEmpty()) {
      return false;
    }
    Set<String> filterColumns = new HashSet<>();
    filter.getColumns(filterColumns);
    for (String column : filterColumns) {
      if (arrayJoinColumns.contains(column)) {
        return true;
      }
    }
    return false;
  }

  private static final class OperandInfo {
    private final int _contextIndex;
    private final ExpressionContext _expression;
    private final @Nullable String _alias;

    private OperandInfo(int contextIndex, ExpressionContext expression, @Nullable String alias) {
      _contextIndex = contextIndex;
      _expression = expression;
      _alias = alias;
    }

    private int getContextIndex() {
      return _contextIndex;
    }

    private ExpressionContext getExpression() {
      return _expression;
    }

    @SuppressWarnings("unused")
    private @Nullable String getAlias() {
      return _alias;
    }
  }

  private static final class OperandData {
    private final BlockValSet _valueSet;
    private final boolean _isSingleValue;
    private int[] _numEntries;

    private OperandData(BlockValSet valueSet) {
      _valueSet = valueSet;
      _isSingleValue = valueSet.isSingleValue();
    }

    private boolean isSingleValue() {
      return _isSingleValue;
    }

    private int getNumEntries(int docIndex) {
      if (_numEntries == null) {
        _numEntries = _valueSet.getNumMVEntries();
      }
      return _numEntries[docIndex];
    }
  }

  private static String normalizeAliasKey(String alias) {
    String normalized = alias;
    if (normalized.length() >= 2) {
      char first = normalized.charAt(0);
      char last = normalized.charAt(normalized.length() - 1);
      if ((first == '"' && last == '"') || (first == '`' && last == '`')) {
        normalized = normalized.substring(1, normalized.length() - 1);
      }
    }
    return normalized.toLowerCase(Locale.ROOT);
  }

  private static final class ArrayJoinValueBlock implements ValueBlock {
    private final ValueBlock _inputBlock;
    private final int[] _docIndexMapping;
    private final int[][] _elementIndexMapping;
    private final int[] _docIds;
    private final Map<ExpressionContext, BlockValSet> _blockValSetMap = new HashMap<>();
    private final Map<ExpressionContext, OperandInfo> _operandInfoMap;
    private final Map<String, OperandInfo> _aliasMap;

    private ArrayJoinValueBlock(ValueBlock inputBlock, int[] docIndexMapping, int[][] elementIndexMapping,
        int[] docIds, Map<ExpressionContext, OperandInfo> operandInfoMap, Map<String, OperandInfo> aliasMap) {
      _inputBlock = inputBlock;
      _docIndexMapping = docIndexMapping;
      _elementIndexMapping = elementIndexMapping;
      _docIds = docIds;
      _operandInfoMap = operandInfoMap;
      _aliasMap = aliasMap;
    }

    @Override
    public int getNumDocs() {
      return _docIndexMapping.length;
    }

    @Nullable
    @Override
    public int[] getDocIds() {
      return _docIds;
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      BlockValSet cached = _blockValSetMap.get(expression);
      if (cached != null) {
        return cached;
      }
      OperandInfo operandInfo = resolveOperandInfo(expression);
      BlockValSet blockValSet;
      if (operandInfo != null) {
        BlockValSet delegate = _inputBlock.getBlockValueSet(operandInfo.getExpression());
        blockValSet = new ArrayJoinOperandBlockValSet(delegate, _docIndexMapping,
            _elementIndexMapping[operandInfo.getContextIndex()]);
      } else {
        blockValSet = new MappedBlockValSet(_inputBlock.getBlockValueSet(expression), _docIndexMapping);
      }
      _blockValSetMap.put(expression, blockValSet);
      return blockValSet;
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      return getBlockValueSet(ExpressionContext.forIdentifier(column));
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      return new MappedBlockValSet(_inputBlock.getBlockValueSet(paths), _docIndexMapping);
    }

    private OperandInfo resolveOperandInfo(ExpressionContext expression) {
      if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
        String identifier = expression.getIdentifier();
        OperandInfo aliasInfo = _aliasMap.get(identifier);
        if (aliasInfo == null) {
          aliasInfo = _aliasMap.get(normalizeAliasKey(identifier));
        }
        if (aliasInfo != null) {
          return aliasInfo;
        }
      }
      return _operandInfoMap.get(expression);
    }

    private ValueBlock getInputBlock() {
      return _inputBlock;
    }

    private int[] getDocIndexMapping() {
      return _docIndexMapping;
    }

    private int[][] getElementIndexMapping() {
      return _elementIndexMapping;
    }
  }

  private static final class MappedBlockValSet implements BlockValSet {
    private final BlockValSet _delegate;
    private final int[] _docIndexMapping;
    private final int _numDocs;
    private boolean _nullBitmapSet;
    private RoaringBitmap _nullBitmap;

    private MappedBlockValSet(BlockValSet delegate, int[] docIndexMapping) {
      _delegate = delegate;
      _docIndexMapping = docIndexMapping;
      _numDocs = docIndexMapping.length;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      if (_nullBitmapSet) {
        return _nullBitmap;
      }
      RoaringBitmap delegateNulls = _delegate.getNullBitmap();
      if (delegateNulls == null || delegateNulls.isEmpty()) {
        _nullBitmap = null;
      } else {
        RoaringBitmap mappedNulls = new RoaringBitmap();
        for (int i = 0; i < _numDocs; i++) {
          if (delegateNulls.contains(_docIndexMapping[i])) {
            mappedNulls.add(i);
          }
        }
        _nullBitmap = mappedNulls.isEmpty() ? null : mappedNulls;
      }
      _nullBitmapSet = true;
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return _delegate.getValueType();
    }

    @Override
    public boolean isSingleValue() {
      return _delegate.isSingleValue();
    }

    @Nullable
    @Override
    public org.apache.pinot.segment.spi.index.reader.Dictionary getDictionary() {
      return _delegate.getDictionary();
    }

    @Override
    public int[] getDictionaryIdsSV() {
      int[] values = _delegate.getDictionaryIdsSV();
      int[] mapped = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public int[] getIntValuesSV() {
      int[] values = _delegate.getIntValuesSV();
      int[] mapped = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public long[] getLongValuesSV() {
      long[] values = _delegate.getLongValuesSV();
      long[] mapped = new long[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public float[] getFloatValuesSV() {
      float[] values = _delegate.getFloatValuesSV();
      float[] mapped = new float[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public double[] getDoubleValuesSV() {
      double[] values = _delegate.getDoubleValuesSV();
      double[] mapped = new double[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      BigDecimal[] values = _delegate.getBigDecimalValuesSV();
      BigDecimal[] mapped = new BigDecimal[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public String[] getStringValuesSV() {
      String[] values = _delegate.getStringValuesSV();
      String[] mapped = new String[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public byte[][] getBytesValuesSV() {
      byte[][] values = _delegate.getBytesValuesSV();
      byte[][] mapped = new byte[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      int[][] values = _delegate.getDictionaryIdsMV();
      int[][] mapped = new int[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public int[][] getIntValuesMV() {
      int[][] values = _delegate.getIntValuesMV();
      int[][] mapped = new int[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public long[][] getLongValuesMV() {
      long[][] values = _delegate.getLongValuesMV();
      long[][] mapped = new long[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public float[][] getFloatValuesMV() {
      float[][] values = _delegate.getFloatValuesMV();
      float[][] mapped = new float[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public double[][] getDoubleValuesMV() {
      double[][] values = _delegate.getDoubleValuesMV();
      double[][] mapped = new double[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public String[][] getStringValuesMV() {
      String[][] values = _delegate.getStringValuesMV();
      String[][] mapped = new String[_numDocs][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      byte[][][] values = _delegate.getBytesValuesMV();
      byte[][][] mapped = new byte[_numDocs][][];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }

    @Override
    public int[] getNumMVEntries() {
      int[] values = _delegate.getNumMVEntries();
      int[] mapped = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        mapped[i] = values[_docIndexMapping[i]];
      }
      return mapped;
    }
  }

  private static final class ArrayJoinOperandBlockValSet implements BlockValSet {
    private final BlockValSet _delegate;
    private final int[] _docIndexMapping;
    private final int[] _elementIndexMapping;
    private final int _numDocs;
    private final boolean _delegateIsSingleValue;
    private int[] _numEntries;
    private boolean _nullBitmapSet;
    private RoaringBitmap _nullBitmap;

    private ArrayJoinOperandBlockValSet(BlockValSet delegate, int[] docIndexMapping, int[] elementIndexMapping) {
      _delegate = delegate;
      _docIndexMapping = docIndexMapping;
      _elementIndexMapping = elementIndexMapping;
      _numDocs = docIndexMapping.length;
      _delegateIsSingleValue = delegate.isSingleValue();
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      if (_nullBitmapSet) {
        return _nullBitmap;
      }
      RoaringBitmap delegateNulls = _delegate.getNullBitmap();
      if ((delegateNulls == null || delegateNulls.isEmpty()) && !hasMissingElements()) {
        _nullBitmap = null;
      } else {
        RoaringBitmap mappedNulls = new RoaringBitmap();
        for (int i = 0; i < _numDocs; i++) {
          int docIndex = _docIndexMapping[i];
          if (delegateNulls != null && delegateNulls.contains(docIndex)) {
            mappedNulls.add(i);
          } else if (isMissingElement(docIndex, _elementIndexMapping[i])) {
            mappedNulls.add(i);
          }
        }
        _nullBitmap = mappedNulls.isEmpty() ? null : mappedNulls;
      }
      _nullBitmapSet = true;
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return _delegate.getValueType();
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Nullable
    @Override
    public org.apache.pinot.segment.spi.index.reader.Dictionary getDictionary() {
      return _delegate.getDictionary();
    }

    @Override
    public int[] getDictionaryIdsSV() {
      int[] mapped = new int[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        int[] values = _delegate.getDictionaryIdsSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        int[][] values = _delegate.getDictionaryIdsMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public int[] getIntValuesSV() {
      int[] mapped = new int[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        int[] values = _delegate.getIntValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        int[][] values = _delegate.getIntValuesMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public long[] getLongValuesSV() {
      long[] mapped = new long[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        long[] values = _delegate.getLongValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        long[][] values = _delegate.getLongValuesMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public float[] getFloatValuesSV() {
      float[] mapped = new float[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        float[] values = _delegate.getFloatValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        float[][] values = _delegate.getFloatValuesMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public double[] getDoubleValuesSV() {
      double[] mapped = new double[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        double[] values = _delegate.getDoubleValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        double[][] values = _delegate.getDoubleValuesMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      BigDecimal[] mapped = new BigDecimal[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        BigDecimal[] values = _delegate.getBigDecimalValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        throw new UnsupportedOperationException("BIG_DECIMAL array join is not supported for multi-value columns");
      }
      return mapped;
    }

    @Override
    public String[] getStringValuesSV() {
      String[] mapped = new String[_numDocs];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        String[] values = _delegate.getStringValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        String[][] values = _delegate.getStringValuesMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public byte[][] getBytesValuesSV() {
      byte[][] mapped = new byte[_numDocs][];
      RoaringBitmap nullBitmap = getNullBitmap();
      if (_delegateIsSingleValue) {
        byte[][] values = _delegate.getBytesValuesSV();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          mapped[i] = values[_docIndexMapping[i]];
        }
      } else {
        byte[][][] values = _delegate.getBytesValuesMV();
        int[] numEntries = getNumEntries();
        for (int i = 0; i < _numDocs; i++) {
          if (nullBitmap != null && nullBitmap.contains(i)) {
            continue;
          }
          int docIndex = _docIndexMapping[i];
          int elementIndex = _elementIndexMapping[i];
          if (elementIndex < numEntries[docIndex]) {
            mapped[i] = values[docIndex][elementIndex];
          }
        }
      }
      return mapped;
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getIntValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getLongValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[][] getFloatValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[][] getDoubleValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[][] getStringValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getNumMVEntries() {
      throw new UnsupportedOperationException();
    }

    private int[] getNumEntries() {
      if (_numEntries == null) {
        _numEntries = _delegate.getNumMVEntries();
      }
      return _numEntries;
    }

    private boolean hasMissingElements() {
      if (_delegateIsSingleValue || _numDocs == 0) {
        return false;
      }
      int[] numEntries = getNumEntries();
      for (int i = 0; i < _numDocs; i++) {
        int docIndex = _docIndexMapping[i];
        if (_elementIndexMapping[i] >= numEntries[docIndex]) {
          return true;
        }
      }
      return false;
    }

    private boolean isMissingElement(int docIndex, int elementIndex) {
      if (_delegateIsSingleValue) {
        return false;
      }
      int[] numEntries = getNumEntries();
      return elementIndex >= numEntries[docIndex];
    }
  }
}
