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
package org.apache.pinot.query.planner.partitioning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.query.planner.serde.ProtoProperties;


/**
 * The {@code FieldSelectionKeySelector} simply extract a column value out from a row array {@link Object[]}.
 */
public class FieldSelectionKeySelector implements KeySelector<Object[], Object[]> {
  private static final String HASH_ALGORITHM = "absHashCode";

  @ProtoProperties
  private List<Integer> _columnIndices;

  public FieldSelectionKeySelector() {
  }

  public FieldSelectionKeySelector(int columnIndex) {
    _columnIndices = Collections.singletonList(columnIndex);
  }

  public FieldSelectionKeySelector(List<Integer> columnIndices) {
    _columnIndices = new ArrayList<>();
    _columnIndices.addAll(columnIndices);
  }

  public FieldSelectionKeySelector(int... columnIndices) {
    _columnIndices = new ArrayList<>();
    for (int columnIndex : columnIndices) {
      _columnIndices.add(columnIndex);
    }
  }

  public List<Integer> getColumnIndices() {
    return _columnIndices;
  }

  @Override
  public Object[] getKey(Object[] input) {
    Object[] key = new Object[_columnIndices.size()];
    for (int i = 0; i < _columnIndices.size(); i++) {
      key[i] = input[_columnIndices.get(i)];
    }
    return key;
  }

  @Override
  public int computeHash(Object[] input) {
    // use a hashing algorithm that is agnostic to the ordering of the columns.
    // For example, computeHash(input) will be identical for both of the following
    // values of _columnIndices:
    // - [1, 2, 4]
    // - [4, 1, 2]
    // this is necessary because calcite always sorts _columnIndices for a hash
    // broadcast, which may reorder the columns differently for different sides
    // of a join
    //
    // the result of hashcode sums will follow the Irwin-Hall distribution, which
    // is notably not a normal distribution although likely acceptable in the case
    // when there are either many inputs or not many partitions
    // also see: https://en.wikipedia.org/wiki/Irwin–Hall_distribution
    // also see: https://github.com/apache/pinot/issues/9998
    //
    // TODO: consider better hashing algorithms than hashCode sum, such as XOR'ing
    int hashCode = 0;
    for (int columnIndex : _columnIndices) {
      Object value = input[columnIndex];
      if (value != null) {
        hashCode += value.hashCode();
      }
    }

    // return a positive number because this is used directly to modulo-index
    return hashCode & Integer.MAX_VALUE;
  }

  @Override
  public String hashAlgorithm() {
    return HASH_ALGORITHM;
  }
}
