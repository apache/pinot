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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.trace.Tracing;


/**
 * Operator which executes DISTINCT operation based on dictionary
 */
public class DictionaryBasedDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_DICTIONARY";

  private final DataSource _dataSource;
  private final QueryContext _queryContext;

  private int _numDocsScanned;

  public DictionaryBasedDistinctOperator(DataSource dataSource, QueryContext queryContext) {
    _dataSource = dataSource;
    _queryContext = queryContext;
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    String column = _queryContext.getSelectExpressions().get(0).getIdentifier();
    Dictionary dictionary = _dataSource.getDictionary();
    assert dictionary != null;
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    DataSchema dataSchema = new DataSchema(new String[]{column},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.fromDataTypeSV(dataSourceMetadata.getDataType())});
    int limit = _queryContext.getLimit();
    int dictLength = dictionary.length();
    int numValuesToKeep = Math.min(limit, dictLength);
    boolean nullHandlingEnabled = _queryContext.isNullHandlingEnabled();

    // If ORDER BY is not present, we read the first limit values from the dictionary and return.
    // If ORDER BY is present and the dictionary is sorted, then we read the first/last limit values
    // from the dictionary. If not sorted, then we read the entire dictionary and return it.
    DistinctTable distinctTable;
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    if (orderByExpressions == null) {
      distinctTable =
          new DistinctTable(dataSchema, iterateOnDictionary(dictionary, numValuesToKeep), nullHandlingEnabled);
      _numDocsScanned = numValuesToKeep;
    } else {
      if (dictionary.isSorted()) {
        if (orderByExpressions.get(0).isAsc()) {
          distinctTable =
              new DistinctTable(dataSchema, iterateOnDictionary(dictionary, numValuesToKeep), nullHandlingEnabled);
        } else {
          distinctTable =
              new DistinctTable(dataSchema, iterateOnDictionaryDesc(dictionary, numValuesToKeep), nullHandlingEnabled);
        }
        _numDocsScanned = numValuesToKeep;
      } else {
        distinctTable = new DistinctTable(dataSchema, orderByExpressions, limit, nullHandlingEnabled);
        for (int i = 0; i < dictLength; i++) {
          distinctTable.addWithOrderBy(new Record(new Object[]{dictionary.getInternal(i)}));
        }
        _numDocsScanned = dictLength;
      }
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  private static List<Record> iterateOnDictionary(Dictionary dictionary, int length) {
    List<Record> records = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(i);
      records.add(new Record(new Object[]{dictionary.getInternal(i)}));
    }
    return records;
  }

  private static List<Record> iterateOnDictionaryDesc(Dictionary dictionary, int length) {
    List<Record> records = new ArrayList<>(length);
    int dictLength = dictionary.length();
    for (int i = dictLength - 1, j = 0; i >= (dictLength - length); i--, j++) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(j);
      records.add(new Record(new Object[]{dictionary.getInternal(i)}));
    }
    return records;
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
