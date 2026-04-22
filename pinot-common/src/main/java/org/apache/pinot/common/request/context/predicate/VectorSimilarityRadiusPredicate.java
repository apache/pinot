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
 * Predicate for vector similarity radius/threshold search.
 * Returns all documents whose vector distance from the query vector is within the specified threshold.
 *
 * <p>Unlike {@link VectorSimilarityPredicate} which returns a fixed number of top-K results,
 * this predicate returns all documents that satisfy the distance threshold constraint.</p>
 *
 * <p>An internal safety limit caps the maximum number of candidates retrieved from the ANN index
 * before exact distance filtering is applied.</p>
 *
 * <p>Example SQL:</p>
 * <pre>
 *   WHERE VECTOR_SIMILARITY_RADIUS(embedding, ARRAY[1.0, 2.0, 3.0], 0.5)
 * </pre>
 *
 * <p>NOTE: Currently, we only support vector similarity search on float array columns.</p>
 */
public class VectorSimilarityRadiusPredicate extends BasePredicate {
  public static final float DEFAULT_THRESHOLD = 0.5f;
  /**
   * Default internal candidate limit for index-assisted radius search. This is used as a cap
   * when retrieving ANN candidates before exact distance filtering. The effective limit is
   * {@code Math.min(DEFAULT_INTERNAL_LIMIT, numDocs)} so it adapts to small segments.
   * For large segments, consider increasing this via a query option if results appear truncated.
   */
  public static final int DEFAULT_INTERNAL_LIMIT = 100_000;

  private final float[] _value;
  private final float _threshold;

  public VectorSimilarityRadiusPredicate(ExpressionContext lhs, float[] value, float threshold) {
    super(lhs);
    _value = value;
    _threshold = threshold;
  }

  @Override
  public Type getType() {
    return Type.VECTOR_SIMILARITY_RADIUS;
  }

  public float[] getValue() {
    return _value;
  }

  public float getThreshold() {
    return _threshold;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorSimilarityRadiusPredicate)) {
      return false;
    }
    VectorSimilarityRadiusPredicate that = (VectorSimilarityRadiusPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Arrays.equals(_value, that._value)
        && Float.compare(_threshold, that._threshold) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, Arrays.hashCode(_value), _threshold);
  }

  @Override
  public String toString() {
    return "vector_similarity_radius(" + _lhs + ",'" + Arrays.toString(_value) + "'," + _threshold + ")";
  }
}
