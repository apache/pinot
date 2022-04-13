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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Filter operator for JSON_MATCH. E.g. SELECT ... WHERE JSON_MATCH(column_name, filter_string)
 */
public class JsonMatchFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_JSON_INDEX";

  private final JsonIndexReader _jsonIndex;
  private final int _numDocs;
  private final JsonMatchPredicate _predicate;

  public JsonMatchFilterOperator(JsonIndexReader jsonIndex, JsonMatchPredicate predicate,
      int numDocs) {
    _jsonIndex = jsonIndex;
    _predicate = predicate;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    ImmutableRoaringBitmap bitmap = _jsonIndex.getMatchingDocIds(_predicate.getValue());
    record(bitmap);
    return new FilterBlock(new BitmapDocIdSet(bitmap, _numDocs));
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    return _jsonIndex.getMatchingDocIds(_predicate.getValue()).getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, false, _jsonIndex.getMatchingDocIds(_predicate.getValue()));
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:json_index");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }

  private void record(ImmutableRoaringBitmap bitmap) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setColumnName(_predicate.getLhs().getIdentifier());
      recording.setFilter(FilterType.INDEX, _predicate.getType().name());
      recording.setNumDocsMatchingAfterFilter(bitmap.getCardinality());
    }
  }
}
