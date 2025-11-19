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
import java.util.concurrent.TimeUnit;
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
  private static final int TIME_CHECK_INTERVAL = 1024;
  private static final int TIME_CHECK_MASK = TIME_CHECK_INTERVAL - 1;

  private final DataSource _dataSource;
  private final QueryContext _queryContext;
  private final long _maxExecutionTimeNs;

  private int _numDocsScanned;
  private long _startTimeNs;
  private boolean _hitTimeLimit;

  public DictionaryBasedDistinctOperator(DataSource dataSource, QueryContext queryContext) {
    _dataSource = dataSource;
    _queryContext = queryContext;
    Long maxExecutionTimeMs = QueryOptionsUtils.getMaxExecutionTimeMsInDistinct(queryContext.getQueryOptions());
    _maxExecutionTimeNs =
        maxExecutionTimeMs != null ? TimeUnit.MILLISECONDS.toNanos(maxExecutionTimeMs) : Long.MAX_VALUE;
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    _startTimeNs = System.nanoTime();
    _hitTimeLimit = false;
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
    resultsBlock.setNumDocsScanned(_numDocsScanned);
    if (_hitTimeLimit) {
      resultsBlock.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
    }
    return resultsBlock;
  }

  private IntDistinctTable createIntDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    IntDistinctTable distinctTable =
        new IntDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getIntValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getIntValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getIntValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getIntValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private LongDistinctTable createLongDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    LongDistinctTable distinctTable =
        new LongDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getLongValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getLongValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getLongValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getLongValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private FloatDistinctTable createFloatDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    FloatDistinctTable distinctTable =
        new FloatDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getFloatValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getFloatValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getFloatValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getFloatValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private DoubleDistinctTable createDoubleDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    DoubleDistinctTable distinctTable =
        new DoubleDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getDoubleValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getDoubleValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getDoubleValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getDoubleValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private BigDecimalDistinctTable createBigDecimalDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    BigDecimalDistinctTable distinctTable =
        new BigDecimalDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getBigDecimalValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getBigDecimalValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getBigDecimalValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getBigDecimalValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private StringDistinctTable createStringDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    StringDistinctTable distinctTable =
        new StringDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getStringValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getStringValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getStringValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getStringValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private BytesDistinctTable createBytesDistinctTable(DataSchema dataSchema, Dictionary dictionary,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    BytesDistinctTable distinctTable =
        new BytesDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
    int rowsProcessed = 0;
    if (orderByExpression == null) {
      for (int i = 0; i < numValuesToKeep; i++) {
        if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
          break;
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
        distinctTable.addUnbounded(dictionary.getByteArrayValue(i));
        rowsProcessed++;
      }
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpression.isAsc()) {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getByteArrayValue(i));
            rowsProcessed++;
          }
        } else {
          for (int i = 0; i < numValuesToKeep; i++) {
            if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
              break;
            }
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
            distinctTable.addUnbounded(dictionary.getByteArrayValue(dictLength - 1 - i));
            rowsProcessed++;
          }
        }
      } else {
        for (int i = 0; i < dictLength; i++) {
          if ((i & TIME_CHECK_MASK) == 0 && hasExceededTimeLimit()) {
            break;
          }
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, EXPLAIN_NAME);
          distinctTable.addWithOrderBy(dictionary.getByteArrayValue(i));
          rowsProcessed++;
        }
      }
    }
    _numDocsScanned = rowsProcessed;
    return distinctTable;
  }

  private boolean hasExceededTimeLimit() {
    if (_maxExecutionTimeNs == Long.MAX_VALUE) {
      return false;
    }
    if (System.nanoTime() - _startTimeNs >= _maxExecutionTimeNs) {
      _hitTimeLimit = true;
      return true;
    }
    return false;
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
}
