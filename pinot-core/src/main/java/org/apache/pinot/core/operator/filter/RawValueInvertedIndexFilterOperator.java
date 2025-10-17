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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.CaseFormat;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.index.readers.RawValueBitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Filter operator that uses raw value bitmap inverted index to handle predicates on raw encoded columns.
 */
public class RawValueInvertedIndexFilterOperator extends BaseColumnFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_RAW_INVERTED_INDEX";

  private final PredicateEvaluator _predicateEvaluator;
  private final RawValueBitmapInvertedIndexReader _invertedIndexReader;
  private final DataType _dataType;
  private final boolean _exclusive;

  public RawValueInvertedIndexFilterOperator(QueryContext queryContext, PredicateEvaluator predicateEvaluator,
      DataSource dataSource, int numDocs) {
    super(queryContext, dataSource, numDocs);
    _predicateEvaluator = predicateEvaluator;
    _invertedIndexReader = (RawValueBitmapInvertedIndexReader) dataSource.getInvertedIndex();
    _dataType = dataSource.getDataSourceMetadata().getDataType();
    _exclusive = predicateEvaluator.isExclusive();
  }

  @Override
  protected BlockDocIdSet getTrues() {
    // Handle null predicate
    if (_predicateEvaluator.isAlwaysFalse()) {
      return EmptyDocIdSet.getInstance();
    }
    if (_predicateEvaluator.isAlwaysTrue()) {
      return new BitmapDocIdSet(new MutableRoaringBitmap(), _numDocs);
    }

    // Get bitmap for each matching value and OR them together
    MutableRoaringBitmap result = computeMatchingBitmap();


    return new BitmapDocIdSet(result, _numDocs);
  }

  private MutableRoaringBitmap computeMatchingBitmap() {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    Predicate predicate = _predicateEvaluator.getPredicate();
    Predicate.Type predicateType = predicate.getType();
    switch (predicateType) {
      case EQ:
        addMatchingValueBitmap(bitmap, ((EqPredicate) predicate).getValue());
        break;
      case NOT_EQ:
        addMatchingValueBitmap(bitmap, ((NotEqPredicate) predicate).getValue());
        break;
      case IN:
        for (String value : ((InPredicate) predicate).getValues()) {
          addMatchingValueBitmap(bitmap, value);
        }
        break;
      case NOT_IN:
        for (String value : ((NotInPredicate) predicate).getValues()) {
          addMatchingValueBitmap(bitmap, value);
        }
        break;
      case RANGE:
        // For range queries, we need to scan through all values and apply the predicate
        // This is not efficient, but it's the only way to handle range queries with raw encoding
        // TODO: Add support for range index with raw encoding
        throw new UnsupportedOperationException("Range predicates not supported for raw encoded columns");
      default:
        throw new IllegalStateException("Unsupported predicate type: " + predicateType);
    }
    MutableRoaringBitmap result = bitmap;
    if (_exclusive) {
      // For exclusive predicates (e.g. NOT_IN, NOT_EQ), invert the bitmap
      result = new MutableRoaringBitmap();
      result.add(0L, _numDocs);
      result.andNot(bitmap);
    }
    return result;
  }

  @Override
  public int getNumMatchingDocs() {
    return computeMatchingBitmap().getCardinality();
  }

  private void addMatchingValueBitmap(MutableRoaringBitmap bitmap, String value) {
    ImmutableRoaringBitmap valueBitmap;
    switch (_dataType) {
      case INT:
        valueBitmap = _invertedIndexReader.getDocIdsForInt(Integer.parseInt(value));
        break;
      case LONG:
        valueBitmap = _invertedIndexReader.getDocIdsForLong(Long.parseLong(value));
        break;
      case FLOAT:
        valueBitmap = _invertedIndexReader.getDocIdsForFloat(Float.parseFloat(value));
        break;
      case DOUBLE:
        valueBitmap = _invertedIndexReader.getDocIdsForDouble(Double.parseDouble(value));
        break;
      case STRING:
        valueBitmap = _invertedIndexReader.getDocIdsForString(value);
        break;
      default:
        throw new IllegalStateException("Unsupported data type for raw inverted index: " + _dataType);
    }
    bitmap.or(valueBitmap);
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, _exclusive, computeMatchingBitmap());
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  protected BlockDocIdSet getNextBlockWithoutNullHandling() {
    return getTrues();
  }

  @Override
  public List<? extends Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:raw_inverted_index");
    Predicate predicate = _predicateEvaluator.getPredicate();
    stringBuilder.append(",operator:").append(predicate.getType());
    stringBuilder.append(",predicate:").append(predicate);
    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("indexLookUp", "raw_inverted_index");
    attributeBuilder.putString("operator", _predicateEvaluator.getPredicate().getType().name());
    attributeBuilder.putString("predicate", _predicateEvaluator.getPredicate().toString());
  }
}
