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

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.distinct.table.BigDecimalDistinctTable;
import org.apache.pinot.core.query.distinct.table.BytesDistinctTable;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.DoubleDistinctTable;
import org.apache.pinot.core.query.distinct.table.FloatDistinctTable;
import org.apache.pinot.core.query.distinct.table.IntDistinctTable;
import org.apache.pinot.core.query.distinct.table.LongDistinctTable;
import org.apache.pinot.core.query.distinct.table.StringDistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Distinct operator that uses inverted index dictId→docIds map directly instead of scanning docs.
 * Supports filter-aware SELECT DISTINCT on columns with inverted index, avoiding projection pipeline.
 */
public class InvertedIndexDistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT_INVERTED_INDEX";

  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;
  private final DataSource _dataSource;
  private final Dictionary _dictionary;
  private final InvertedIndexReader<?> _invertedIndexReader;

  private int _numValuesProcessed = 0;

  public InvertedIndexDistinctOperator(IndexSegment indexSegment, SegmentContext segmentContext,
      QueryContext queryContext, BaseFilterOperator filterOperator, DataSource dataSource) {
    _indexSegment = indexSegment;
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _filterOperator = filterOperator;
    _dataSource = dataSource;
    _dictionary = dataSource.getDictionary();
    _invertedIndexReader = dataSource.getInvertedIndex();
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    if (expressions.size() != 1) {
      throw new IllegalStateException("InvertedIndexDistinctOperator supports single expression only");
    }

    ExpressionContext expr = expressions.get(0);
    String column = parseColumnIdentifier(expr);
    if (column == null) {
      throw new IllegalStateException("InvertedIndexDistinctOperator expects simple column identifier");
    }

    RoaringBitmap filteredDocIds = buildFilteredDocIds();
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    DataSchema dataSchema = new DataSchema(new String[]{column},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(dataSourceMetadata.getDataType())});
    OrderByExpressionContext orderByExpression =
        _queryContext.getOrderByExpressions() != null ? _queryContext.getOrderByExpressions().get(0) : null;

    DistinctTable distinctTable = createDistinctTable(dataSchema, orderByExpression);
    int dictLength = _dictionary.length();
    int limit = _queryContext.getLimit();

    for (int dictId = 0; dictId < dictLength; dictId++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numValuesProcessed, EXPLAIN_NAME);

      Object docIdsObj = _invertedIndexReader.getDocIds(dictId);
      if (!(docIdsObj instanceof ImmutableRoaringBitmap)) {
        continue;
      }
      ImmutableRoaringBitmap docIds = (ImmutableRoaringBitmap) docIdsObj;
      if (docIds.isEmpty()) {
        continue;
      }

      boolean includeValue;
      if (filteredDocIds == null) {
        includeValue = true;
      } else {
        RoaringBitmap docIdsRoaring = docIds.toMutableRoaringBitmap().toRoaringBitmap();
        RoaringBitmap intersection = RoaringBitmap.and(docIdsRoaring, filteredDocIds);
        includeValue = !intersection.isEmpty();
      }

      if (includeValue) {
        boolean done = addValueToDistinctTable(distinctTable, dictId, orderByExpression);
        _numValuesProcessed++;
        if (done) {
          break;
        }
      }

      if (distinctTable.hasLimit() && distinctTable.size() >= limit) {
        break;
      }
    }

    return new DistinctResultsBlock(distinctTable, _queryContext);
  }

  private DistinctTable createDistinctTable(DataSchema dataSchema,
      @Nullable OrderByExpressionContext orderByExpression) {
    int limit = _queryContext.getLimit();
    switch (_dictionary.getValueType()) {
      case INT:
        return new IntDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      case LONG:
        return new LongDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      case FLOAT:
        return new FloatDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      case DOUBLE:
        return new DoubleDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      case BIG_DECIMAL:
        return new BigDecimalDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      case STRING:
        return new StringDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      case BYTES:
        return new BytesDistinctTable(dataSchema, limit, _queryContext.isNullHandlingEnabled(), orderByExpression);
      default:
        throw new IllegalStateException("Unsupported data type: " + _dictionary.getValueType());
    }
  }

  private boolean addValueToDistinctTable(DistinctTable distinctTable, int dictId,
      @Nullable OrderByExpressionContext orderByExpression) {
    switch (_dictionary.getValueType()) {
      case INT:
        return addToTable((IntDistinctTable) distinctTable, _dictionary.getIntValue(dictId), orderByExpression);
      case LONG:
        return addToTable((LongDistinctTable) distinctTable, _dictionary.getLongValue(dictId), orderByExpression);
      case FLOAT:
        return addToTable((FloatDistinctTable) distinctTable, _dictionary.getFloatValue(dictId), orderByExpression);
      case DOUBLE:
        return addToTable((DoubleDistinctTable) distinctTable, _dictionary.getDoubleValue(dictId), orderByExpression);
      case BIG_DECIMAL:
        return addToTable((BigDecimalDistinctTable) distinctTable, _dictionary.getBigDecimalValue(dictId),
            orderByExpression);
      case STRING:
        return addToTable((StringDistinctTable) distinctTable, _dictionary.getStringValue(dictId), orderByExpression);
      case BYTES:
        return addToTable((BytesDistinctTable) distinctTable, _dictionary.getByteArrayValue(dictId), orderByExpression);
      default:
        throw new IllegalStateException("Unsupported data type: " + _dictionary.getValueType());
    }
  }

  private static boolean addToTable(IntDistinctTable table, int value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(LongDistinctTable table, long value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(FloatDistinctTable table, float value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(DoubleDistinctTable table, double value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(BigDecimalDistinctTable table, java.math.BigDecimal value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(StringDistinctTable table, String value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  private static boolean addToTable(BytesDistinctTable table, ByteArray value,
      @Nullable OrderByExpressionContext orderByExpression) {
    if (table.hasLimit()) {
      if (orderByExpression != null) {
        table.addWithOrderBy(value);
        return false;
      } else {
        return table.addWithoutOrderBy(value);
      }
    } else {
      table.addUnbounded(value);
      return false;
    }
  }

  @Nullable
  private RoaringBitmap buildFilteredDocIds() {
    if (_filterOperator.isResultMatchingAll()) {
      return null;
    }

    if (_filterOperator.canProduceBitmaps()) {
      return _filterOperator.getBitmaps().reduce().toRoaringBitmap();
    }

    if (_filterOperator.isResultEmpty()) {
      return new RoaringBitmap();
    }

    RoaringBitmap bitmap = new RoaringBitmap();
    DocIdSetPlanNode docIdSetPlanNode =
        new DocIdSetPlanNode(_segmentContext, _queryContext, DocIdSetPlanNode.MAX_DOC_PER_CALL, _filterOperator);
    var docIdSetOperator = docIdSetPlanNode.run();
    DocIdSetBlock block;
    while ((block = docIdSetOperator.nextBlock()) != null) {
      int[] docIds = block.getDocIds();
      int length = block.getLength();
      bitmap.addN(docIds, 0, length);
    }
    return bitmap;
  }

  @Nullable
  private static String parseColumnIdentifier(ExpressionContext expr) {
    if (expr.getType() != ExpressionContext.Type.IDENTIFIER) {
      return null;
    }
    return expr.getIdentifier();
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numValuesProcessed, 0, 0, numTotalDocs);
  }

  @Override
  public String toExplainString() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    return EXPLAIN_NAME + "(keyColumns:" + (expressions.isEmpty() ? "" : expressions.get(0).toString()) + ")";
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    List<ExpressionContext> selectExpressions = _queryContext.getSelectExpressions();
    if (!selectExpressions.isEmpty()) {
      attributeBuilder.putStringList("keyColumns", List.of(selectExpressions.get(0).toString()));
    }
  }

  /**
   * Returns true if the expression is a simple column identifier with inverted index and dictionary.
   */
  public static boolean canUseInvertedIndexDistinct(IndexSegment indexSegment, ExpressionContext expr,
      QueryContext queryContext) {
    String column = parseColumnIdentifier(expr);
    if (column == null) {
      return false;
    }
    DataSource dataSource = indexSegment.getDataSourceNullable(column);
    if (dataSource == null) {
      return false;
    }
    if (dataSource.getInvertedIndex() == null || dataSource.getDictionary() == null) {
      return false;
    }
    // When nullHandlingEnabled and column has nulls, fall back to default DistinctOperator
    // (same as DictionaryBasedDistinctOperator)
    if (queryContext.isNullHandlingEnabled()) {
      NullValueVectorReader nullValueReader = dataSource.getNullValueVector();
      if (nullValueReader != null && !nullValueReader.getNullBitmap().isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
