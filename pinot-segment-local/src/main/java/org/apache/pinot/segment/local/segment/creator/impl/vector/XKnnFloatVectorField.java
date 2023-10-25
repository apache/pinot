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
package org.apache.pinot.segment.local.segment.creator.impl.vector;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;


public class XKnnFloatVectorField extends KnnFloatVectorField {
  public static final int MAX_DIMS_COUNT = 2048;

  private static FieldType createType(float[] v, VectorSimilarityFunction similarityFunction) {
    if (v == null) {
      throw new IllegalArgumentException("vector value must not be null");
    }
    int dimension = v.length;
    if (dimension == 0) {
      throw new IllegalArgumentException("cannot index an empty vector");
    }
    if (dimension > MAX_DIMS_COUNT) {
      throw new IllegalArgumentException("cannot index vectors with dimension greater than " + MAX_DIMS_COUNT);
    }
    if (similarityFunction == null) {
      throw new IllegalArgumentException("similarity function must not be null");
    }
    FieldType type = new FieldType() {
      @Override
      public int vectorDimension() {
        return dimension;
      }

      @Override
      public VectorEncoding vectorEncoding() {
        return VectorEncoding.FLOAT32;
      }

      @Override
      public VectorSimilarityFunction vectorSimilarityFunction() {
        return similarityFunction;
      }
    };
    type.freeze();

    return type;
  }

  public XKnnFloatVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    super(name, vector, createType(vector, similarityFunction));
  }

  public XKnnFloatVectorField(String name, float[] vector) {
    this(name, vector, VectorSimilarityFunction.EUCLIDEAN);
  }
}
