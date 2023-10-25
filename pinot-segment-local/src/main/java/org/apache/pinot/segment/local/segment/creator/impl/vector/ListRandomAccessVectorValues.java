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

import java.util.List;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;


public class ListRandomAccessVectorValues implements RandomAccessVectorValues<float[]> {

  private final List<float[]> _vectors;
  private final int _dimension;

  public ListRandomAccessVectorValues(List<float[]> vectors, int dimension) {
    _vectors = vectors;
    _dimension = dimension;
  }

  @Override
  public int size() {
    return _vectors.size();
  }

  @Override
  public int dimension() {
    return _dimension;
  }

  @Override
  public float[] vectorValue(int targetOrd) {
    return _vectors.get(targetOrd);
  }

  @Override
  public ListRandomAccessVectorValues copy() {
    return new ListRandomAccessVectorValues(_vectors, _dimension);
  }
}
