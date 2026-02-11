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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.reduce.BaseGapfillProcessor;
import org.apache.pinot.core.query.reduce.GapfillProcessorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies gapfill to an incoming row set in MSQ.
 *
 * <p>This operator expects its input rows to already be projected to match the gapfill query context
 * (i.e. the {@code gapfill()} wrapper has already been evaluated and removed by the upstream project/transform).</p>
 */
public class GapfillOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GapfillOperator.class);
  private static final String EXPLAIN_NAME = "GAPFILL";

  private final MultiStageOperator _input;
  private final DataSchema _schema;
  private final BaseGapfillProcessor _gapfillProcessor;
  private MseBlock.Eos _eosBlock;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public GapfillOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema schema) {
    super(context);
    _input = input;
    _schema = schema;
    _gapfillProcessor = buildGapfillProcessor(context);
  }

  /**
   * Returns {@code true} when the current operator schema contains the columns required to correctly apply gapfill.
   *
   * <p>This is used to avoid applying gapfill at an incorrect stage (e.g. after an aggregation) where required columns
   * such as {@code TIMESERIESON(...)} keys or {@code FILL(...)} target columns are no longer present, which can lead to
   * silently incorrect results.</p>
   */
  public static boolean isApplicable(OpChainExecutionContext context, DataSchema schema) {
    Map<String, String> queryOptions = context.getOpChainMetadata();
    if (!QueryOptionsUtils.isServerSideGapfill(queryOptions)) {
      return false;
    }
    String serializedGapfillQuery = QueryOptionsUtils.getServerSideGapfillQuery(queryOptions);
    if (serializedGapfillQuery == null) {
      return false;
    }
    PinotQuery rootPinotQuery;
    try {
      rootPinotQuery = deserializePinotQuery(serializedGapfillQuery);
    } catch (RuntimeException e) {
      return false;
    }
    PinotQuery gapfillPinotQuery = findGapfillQuery(rootPinotQuery);
    if (gapfillPinotQuery == null) {
      return false;
    }
    QueryContext gapfillQueryContext;
    try {
      gapfillQueryContext = QueryContextConverterUtils.getQueryContext(gapfillPinotQuery);
    } catch (RuntimeException e) {
      return false;
    }
    GapfillUtils.GapfillType gapfillType;
    try {
      gapfillType = GapfillUtils.getGapfillType(gapfillQueryContext);
    } catch (RuntimeException e) {
      return false;
    }
    if (gapfillType == null) {
      return false;
    }
    org.apache.pinot.common.request.context.ExpressionContext gapfillExpression =
        GapfillUtils.getGapfillExpressionContext(gapfillQueryContext, gapfillType);
    if (gapfillExpression == null || gapfillExpression.getFunction() == null) {
      return false;
    }
    // Prefer an order-based check instead of name-based check because MSQ schemas can have compiler-generated names
    // (e.g. "$f0"). If the schema doesn't have enough columns to accommodate the required selection indices, applying
    // gapfill here would cause the processor to mis-map columns and silently produce incorrect results.
    int maxRequiredIndex = GapfillUtils.findTimeBucketColumnIndex(gapfillQueryContext, gapfillType);
    if (maxRequiredIndex < 0) {
      return false;
    }

    // TIMESERIESON columns (partition keys)
    org.apache.pinot.common.request.context.ExpressionContext timeSeriesOn =
        GapfillUtils.getTimeSeriesOnExpressionContext(gapfillExpression);
    if (timeSeriesOn == null || timeSeriesOn.getFunction() == null) {
      return false;
    }
    for (org.apache.pinot.common.request.context.ExpressionContext arg : timeSeriesOn.getFunction().getArguments()) {
      if (arg.getType() == org.apache.pinot.common.request.context.ExpressionContext.Type.IDENTIFIER) {
        Integer idx = findSelectionIndex(gapfillQueryContext, arg.getIdentifier());
        if (idx == null) {
          return false;
        }
        maxRequiredIndex = Math.max(maxRequiredIndex, idx);
      }
    }

    // FILL target columns
    for (String fillColumn : GapfillUtils.getFillExpressions(gapfillExpression).keySet()) {
      Integer idx = findSelectionIndex(gapfillQueryContext, fillColumn);
      if (idx == null) {
        return false;
      }
      maxRequiredIndex = Math.max(maxRequiredIndex, idx);
    }

    return schema.size() > maxRequiredIndex;
  }

  private static Integer findSelectionIndex(QueryContext queryContext, String columnName) {
    List<org.apache.pinot.common.request.context.ExpressionContext> selectExpressions =
        queryContext.getSelectExpressions();
    if (selectExpressions == null) {
      return null;
    }
    List<String> aliasList = queryContext.getAliasList();
    for (int i = 0; i < selectExpressions.size(); i++) {
      if (aliasList != null && aliasList.size() > i && aliasList.get(i) != null
          && aliasList.get(i).equalsIgnoreCase(columnName)) {
        return i;
      }
      org.apache.pinot.common.request.context.ExpressionContext expressionContext = selectExpressions.get(i);
      if (GapfillUtils.isGapfill(expressionContext)) {
        expressionContext = expressionContext.getFunction().getArguments().get(0);
      }
      if (expressionContext.getType() == org.apache.pinot.common.request.context.ExpressionContext.Type.IDENTIFIER
          && expressionContext.getIdentifier().equalsIgnoreCase(columnName)) {
        return i;
      }
      if (expressionContext.toString().equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    return null;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_input);
  }

  @Override
  public Type getOperatorType() {
    return Type.GAPFILL;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG),
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }

    List<Object[]> inputRows = new ArrayList<>();
    while (true) {
      MseBlock block = _input.nextBlock();
      if (block.isEos()) {
        _eosBlock = (MseBlock.Eos) block;
        if (block.isError()) {
          return block;
        }
        break;
      }
      inputRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
    }

    if (inputRows.isEmpty()) {
      if (_eosBlock == null) {
        _eosBlock = SuccessMseBlock.INSTANCE;
      }
      return _eosBlock;
    }

    List<Object[]> coercedRows = coerceObjectValues(_schema, inputRows);
    sortRowsByTimeBucket(coercedRows);
    DataSchema schemaForGapfill = coerceObjectColumns(_schema, coercedRows);
    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    brokerResponseNative.setResultTable(new ResultTable(schemaForGapfill, coercedRows));
    _gapfillProcessor.process(brokerResponseNative);

    ResultTable resultTable = brokerResponseNative.getResultTable();
    if (resultTable == null || resultTable.getRows().isEmpty()) {
      if (_eosBlock == null) {
        _eosBlock = SuccessMseBlock.INSTANCE;
      }
      return _eosBlock;
    }

    return new RowHeapDataBlock(resultTable.getRows(), resultTable.getDataSchema());
  }

  private static void sortRowsByTimeBucket(List<Object[]> rows) {
    // GapfillProcessor expects rows to be ordered by the time-bucket column so it can efficiently short-circuit within
    // each bucket. In MSQ, upstream operators might not preserve any ordering.
    rows.sort(Comparator.comparingLong(GapfillOperator::getTimeBucketMillis));
  }

  private static long getTimeBucketMillis(Object[] row) {
    // For GAPFILL, the time-bucket column is the first projection (index 0) after stripping the wrapper.
    Object value = row[0];
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return Long.parseLong(String.valueOf(value));
  }

  private static DataSchema coerceObjectColumns(DataSchema schema, List<Object[]> rows) {
    ColumnDataType[] types = schema.getColumnDataTypes();
    boolean hasObject = false;
    for (ColumnDataType type : types) {
      if (type == ColumnDataType.OBJECT) {
        hasObject = true;
        break;
      }
    }
    if (!hasObject) {
      return schema;
    }

    String[] columnNames = schema.getColumnNames().clone();
    ColumnDataType[] columnTypes = types.clone();
    for (int col = 0; col < columnTypes.length; col++) {
      if (columnTypes[col] != ColumnDataType.OBJECT) {
        continue;
      }
      ColumnDataType inferred = inferColumnType(rows, col);
      if (inferred != null) {
        columnTypes[col] = inferred;
      }
    }
    return new DataSchema(columnNames, columnTypes);
  }

  private static List<Object[]> coerceObjectValues(DataSchema schema, List<Object[]> rows) {
    ColumnDataType[] types = schema.getColumnDataTypes();
    boolean hasObject = false;
    for (ColumnDataType type : types) {
      if (type == ColumnDataType.OBJECT) {
        hasObject = true;
        break;
      }
    }
    if (!hasObject) {
      return rows;
    }

    List<Object[]> coerced = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      if (row == null) {
        coerced.add(null);
        continue;
      }
      Object[] copy = row.clone();
      int limit = Math.min(copy.length, types.length);
      for (int i = 0; i < limit; i++) {
        if (types[i] != ColumnDataType.OBJECT) {
          continue;
        }
        Object value = copy[i];
        if (value instanceof ValueLongPair) {
          copy[i] = ((ValueLongPair<?>) value).getValue();
        }
      }
      coerced.add(copy);
    }
    return coerced;
  }

  private static ColumnDataType inferColumnType(List<Object[]> rows, int col) {
    for (Object[] row : rows) {
      if (row == null || col >= row.length) {
        continue;
      }
      Object value = row[col];
      if (value == null) {
        continue;
      }
      if (value instanceof Integer) {
        return ColumnDataType.INT;
      }
      if (value instanceof Long) {
        return ColumnDataType.LONG;
      }
      if (value instanceof Float) {
        return ColumnDataType.FLOAT;
      }
      if (value instanceof Double) {
        return ColumnDataType.DOUBLE;
      }
      if (value instanceof String) {
        return ColumnDataType.STRING;
      }
      if (value instanceof byte[]) {
        return ColumnDataType.BYTES;
      }
    }
    return null;
  }

  private static BaseGapfillProcessor buildGapfillProcessor(OpChainExecutionContext context) {
    Map<String, String> queryOptions = context.getOpChainMetadata();
    Preconditions.checkState(QueryOptionsUtils.isServerSideGapfill(queryOptions), "Gapfill query options are missing");
    String serializedGapfillQuery = QueryOptionsUtils.getServerSideGapfillQuery(queryOptions);
    Preconditions.checkState(serializedGapfillQuery != null, "Missing server-side gapfill query");
    PinotQuery rootPinotQuery = deserializePinotQuery(serializedGapfillQuery);
    maybeOverrideGapfillLimit(rootPinotQuery);

    // Build the processor off the specific query level that contains GAPFILL.
    PinotQuery gapfillPinotQuery = findGapfillQuery(rootPinotQuery);
    Preconditions.checkState(gapfillPinotQuery != null, "Gapfill query context missing gapfill expression");

    QueryContext gapfillQueryContext = QueryContextConverterUtils.getQueryContext(gapfillPinotQuery);
    GapfillUtils.GapfillType gapfillType = GapfillUtils.getGapfillType(gapfillQueryContext);
    Preconditions.checkState(gapfillType != null, "Gapfill query context missing gapfill expression");
    return GapfillProcessorFactory.getGapfillProcessorForServer(gapfillQueryContext, gapfillType);
  }

  private static PinotQuery deserializePinotQuery(String serializedPinotQuery) {
    try {
      byte[] bytes = Base64.getDecoder().decode(serializedPinotQuery);
      PinotQuery pinotQuery = new PinotQuery();
      new TDeserializer(new TCompactProtocol.Factory()).deserialize(pinotQuery, bytes);
      return pinotQuery;
    } catch (IllegalArgumentException | TException e) {
      throw new IllegalStateException("Failed to deserialize gapfill query", e);
    }
  }

  private static void maybeOverrideGapfillLimit(PinotQuery pinotQuery) {
    PinotQuery gapfillQuery = findGapfillQuery(pinotQuery);
    if (gapfillQuery == null || gapfillQuery == pinotQuery) {
      return;
    }
    boolean hasExplicitLimit = gapfillQuery.isSetLimit();
    int existingLimit = gapfillQuery.getLimit();
    int defaultQueryLimit = CommonConstants.Broker.DEFAULT_BROKER_QUERY_LIMIT;
    boolean limitIsDefault =
        !hasExplicitLimit || existingLimit <= 0 || existingLimit == defaultQueryLimit;
    QueryContext rootContext;
    GapfillUtils.GapfillType gapfillType;
    try {
      rootContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
      gapfillType = GapfillUtils.getGapfillType(rootContext);
    } catch (RuntimeException e) {
      gapfillQuery.setLimit(CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
      return;
    }
    if (gapfillType == null) {
      if (!hasExplicitLimit) {
        gapfillQuery.setLimit(defaultQueryLimit);
      }
      return;
    }
    switch (gapfillType) {
      case GAP_FILL_AGGREGATE:
      case AGGREGATE_GAP_FILL_AGGREGATE:
        // Avoid truncating the gapfill output before the outer aggregation is applied.
        if (limitIsDefault) {
          gapfillQuery.setLimit(Integer.MAX_VALUE);
        }
        break;
      case GAP_FILL_SELECT:
        // Use the outer limit when available; otherwise fall back to the default.
        int outerLimit = rootContext.getLimit();
        if (outerLimit > 0) {
          gapfillQuery.setLimit(outerLimit);
        } else if (!hasExplicitLimit) {
          gapfillQuery.setLimit(defaultQueryLimit);
        }
        break;
      default:
        if (!hasExplicitLimit) {
          gapfillQuery.setLimit(defaultQueryLimit);
        }
        break;
    }
  }

  private static PinotQuery findGapfillQuery(PinotQuery pinotQuery) {
    PinotQuery current = pinotQuery;
    while (current != null) {
      if (containsGapfillExpression(current.getSelectList())) {
        return current;
      }
      if (current.getDataSource() == null) {
        break;
      }
      current = current.getDataSource().getSubquery();
    }
    return null;
  }

  private static boolean containsGapfillExpression(List<Expression> selectList) {
    if (selectList == null) {
      return false;
    }
    for (Expression expression : selectList) {
      if (containsGapfillExpression(expression)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsGapfillExpression(Expression expression) {
    if (expression == null || expression.getType() != ExpressionType.FUNCTION) {
      return false;
    }
    Function function = expression.getFunctionCall();
    if (function == null) {
      return false;
    }
    if ("gapfill".equalsIgnoreCase(function.getOperator())) {
      return true;
    }
    if ("as".equalsIgnoreCase(function.getOperator()) && function.getOperandsSize() > 0) {
      return containsGapfillExpression(function.getOperands().get(0));
    }
    for (Expression operand : function.getOperands()) {
      if (containsGapfillExpression(operand)) {
        return true;
      }
    }
    return false;
  }
}
