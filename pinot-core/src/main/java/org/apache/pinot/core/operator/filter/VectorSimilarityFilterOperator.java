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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Operator for Vector Search query.
 * <p>Currently, we only support vector similarity search on float array column.
 * Example:
 * {
 *  "type": "vectorSimilarity",
 *  "leftValue": "embedding",
 *  "rightValue": [1.0, 2.0, 3.0],
 *  "topK": 10
 *  }
 *
 */
public class VectorSimilarityFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_INDEX";

  private final VectorIndexReader _vectorIndexReader;
  private final VectorSimilarityPredicate _predicate;
  private ImmutableRoaringBitmap _matches;

  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs) {
    super(numDocs, false);
    _vectorIndexReader = vectorIndexReader;
    _predicate = predicate;
    _matches = null;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_matches == null) {
      _matches = _vectorIndexReader.getDocIds(_predicate.getValue(), _predicate.getTopK());
    }
    return new BitmapDocIdSet(_matches, _numDocs);
  }

  @Override
  public int getNumMatchingDocs() {
    if (_matches == null) {
      _matches = _vectorIndexReader.getDocIds(_predicate.getValue(), _predicate.getTopK());
    }
    return _matches.getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    if (_matches == null) {
      _matches = _vectorIndexReader.getDocIds(_predicate.getValue(), _predicate.getTopK());
    }
    record(_matches);
    return new BitmapCollection(_numDocs, false, _matches);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME + "(indexLookUp:vector_index"
        + ", operator:" + _predicate.getType()
        + ", vector identifier:" + _predicate.getLhs().getIdentifier()
        + ", vector literal:" + Arrays.toString(_predicate.getValue())
        + ", topK to search:" + _predicate.getTopK()
        + ')';
  }

  private void record(ImmutableRoaringBitmap matches) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
      recording.setColumnName(_predicate.getLhs().getIdentifier());
      recording.setFilter(FilterType.INDEX, "VECTOR_SIMILARITY");
      recording.setInputDataType(FieldSpec.DataType.FLOAT, false);
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
    }
  }
}
