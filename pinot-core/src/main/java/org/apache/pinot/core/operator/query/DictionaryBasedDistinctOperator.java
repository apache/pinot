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
package org.apache.pinot.core.operator.query;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.query.QueryThreadContext;


/**
 * Operator which executes DISTINCT operation based on dictionary
 */
public class DictionaryBasedDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_DICTIONARY";

  private final DataSource _dataSource;
  private final QueryContext _queryContext;

  private int _numDocsScanned;
  private final int _maxRowsInDistinct;
  private boolean _hitMaxRowsLimit;

  public DictionaryBasedDistinctOperator(DataSource dataSource, QueryContext queryContext) {
    _dataSource = dataSource;
    _queryContext = queryContext;
    if (queryContext.getQueryOptions() != null) {
      Integer maxRows = QueryOptionsUtils.getMaxRowsInDistinct(queryContext.getQueryOptions());
      _maxRowsInDistinct = maxRows != null ? maxRows : Integer.MAX_VALUE;
    } else {
      _maxRowsInDistinct = Integer.MAX_VALUE;
    }
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    String column = _queryContext.getSelectExpressions().get(0).getIdentifier();
    Dictionary dictionary = _dataSource.getDictionary();
    assert dictionary != null;
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    DataSchema dataSchema = new DataSchema(new String[]{column},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(dataSourceMetadata.getDataType())});
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    OrderByExpressionContext orderByExpression = orderByExpressions != null ? orderByExpressions.get(0) : null;
    // If ORDER BY is not present, we read the first limit values from the dictionary and return.
    // If ORDER BY is present and the dictionary is sorted, then we read the first/last limit values from the
    // dictionary. If not sorted, then we read the entire dictionary and return it.
    DistinctTable distinctTable;
    switch (dictionary.getValueType()) {
      case INT:
        distinctTable = createIntDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      case LONG:
        distinctTable = createLongDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      case FLOAT:
        distinctTable = createFloatDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      case DOUBLE:
        distinctTable = createDoubleDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      case BIG_DECIMAL:
        distinctTable = createBigDecimalDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      case STRING:
        distinctTable = createStringDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      case BYTES:
        distinctTable = createBytesDistinctTable(dataSchema, dictionary, orderByExpression);
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + dictionary.getValueType());
    }
    DistinctResultsBlock resultsBlock = new DistinctResultsBlock(distinctTable, _queryContext);
    if (_hitMaxRowsLimit) {
      resultsBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_MAX_ROWS);
    }
    return resultsBlock;
  }

  private IntDistinctTable createIntDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    IntDistinctTable distinctTable =
        new IntDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getIntValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getIntValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getIntValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getIntValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  private LongDistinctTable createLongDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    LongDistinctTable distinctTable =
        new LongDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getLongValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getLongValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getLongValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getLongValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  private FloatDistinctTable createFloatDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    FloatDistinctTable distinctTable =
        new FloatDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getFloatValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getFloatValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getFloatValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getFloatValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  private DoubleDistinctTable createDoubleDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    DoubleDistinctTable distinctTable =
        new DoubleDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getDoubleValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getDoubleValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getDoubleValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getDoubleValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  private BigDecimalDistinctTable createBigDecimalDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    BigDecimalDistinctTable distinctTable =
        new BigDecimalDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getBigDecimalValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getBigDecimalValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getBigDecimalValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getBigDecimalValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  private StringDistinctTable createStringDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    StringDistinctTable distinctTable =
        new StringDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getStringValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getStringValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getStringValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getStringValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  private BytesDistinctTable createBytesDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean requiresFullScan = orderByExpression != null && !dictionary.isSorted();
    int rowsConsidered = requiresFullScan ? dictLength : numValuesToKeep;
    int rowsToProcess = clampRows(rowsConsidered);
    BytesDistinctTable distinctTable =
        new BytesDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    if (orderByExpression == null) {
      for (int i = 0; i < rowsToProcess; i++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getByteArrayValue(i));
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getByteArrayValue(i));
          }
        } else {
          for (int i = 0; i < rowsToProcess; i++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getByteArrayValue(dictLength - 1 - i));
          }
        }
      } else {
        for (int i = 0; i < rowsToProcess; i++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getByteArrayValue(i));
        }
      }
    }
    _numDocsScanned = rowsToProcess;
    return distinctTable;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
    return new ExecutionStatistics(_numDocsScanned, 0, _numDocsScanned,
        _dataSource.getDataSourceMetadata().getNumDocs());
  }

  private int clampRows(int rowsConsidered) {
    if (_maxRowsInDistinct == Integer.MAX_VALUE) {
      return rowsConsidered;
    }
    if (rowsConsidered <= _maxRowsInDistinct) {
      return rowsConsidered;
    }
    _hitMaxRowsLimit = true;
    return _maxRowsInDistinct;
  }
}
