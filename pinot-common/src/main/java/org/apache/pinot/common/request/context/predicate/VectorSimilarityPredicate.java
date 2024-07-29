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
package org.apache.pinot.common.request.context.predicate;

import java.util.Arrays;
import java.util.Objects;
import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Predicate for vector similarity search.
 * NOTE: Currently, we only support vector similarity search on float array column.
 * Example:
 *   {
 *   "type": "vectorSimilarity",
 *   "leftValue": "embedding",
 *   "rightValue": [1.0, 2.0, 3.0],
 *   "topK": 10
 *   }
 */
public class VectorSimilarityPredicate extends BasePredicate {
  public static final int DEFAULT_TOP_K = 10;
  private final float[] _value;
  private final int _topK;

  public VectorSimilarityPredicate(ExpressionContext lhs, float[] value, int topK) {
    super(lhs);
    _value = value;
    _topK = topK;
  }

  @Override
  public Type getType() {
    return Type.VECTOR_SIMILARITY;
  }

  public float[] getValue() {
    return _value;
  }

  public int getTopK() {
    return _topK;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorSimilarityPredicate)) {
      return false;
    }
    VectorSimilarityPredicate that = (VectorSimilarityPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Arrays.equals(_value, that._value) && _topK == that._topK;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, Arrays.hashCode(_value), _topK);
  }

  @Override
  public String toString() {
    return "vector_similarity(" + _lhs + ",'" + Arrays.toString(_value) + "'," + _topK + ")";
  }
}
