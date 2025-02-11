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
package org.apache.pinot.query.planner.physical.colocated;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rex.RexInputRef;


/**
 * ColocationKey describes how data is distributed in a given stage. It consists of a list of columns which are stored
 * as a list of {@link RexInputRef#getIndex()}, the number of partitions and the hash-algorithm used. A given stage may
 * have more than 1 ColocationKey, in which case one may use a {@link java.util.Set< ColocationKey >} to represent this
 * behavior.
 *
 * <p>
 *  In other words, when a PlanNode has the schema: (user_uuid, col1, col2, ...), and the ColocationKey is
 *  ([0], 8, murmur), then that means that the data for the PlanNode is partitioned using the user_uuid column, into
 *  8 partitions where the partitionId is computed using murmur(user_uuid) % 8.
 *
 *  For a join stage the data is partitioned by the senders using their respective join-keys. In that case, we may
 *  have more than 1 ColocationKey applicable for the JoinNode, and it can be represented by a set as:
 *  {([0], 8, murmur), ([leftSchemaSize + 0], 8, murmur)}, assuming both senders partition using Murmur into 8
 *  partitions. Note that a set of ColocationKey means that the partition keys are independent and they don't have any
 *  ordering, i.e. the data is partitioned by both the join-key of the left child and the join-key of the right child.
 * </p>
 */
class ColocationKey {
  private List<Integer> _indices;
  private int _numPartitions;
  private String _hashAlgorithm;

  public List<Integer> getIndices() {
    return _indices;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public String getHashAlgorithm() {
    return _hashAlgorithm;
  }

  public ColocationKey(int numPartitions, String algorithm) {
    _numPartitions = numPartitions;
    _hashAlgorithm = algorithm;
    _indices = new ArrayList<>();
  }

  public ColocationKey(int index, int numPartitions, String algorithm) {
    _indices = new ArrayList<>();
    _indices.add(index);
    _numPartitions = numPartitions;
    _hashAlgorithm = algorithm;
  }

  public void addIndex(int index) {
    _indices.add(index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColocationKey that = (ColocationKey) o;
    return _indices.equals(that._indices) && _numPartitions == that._numPartitions && _hashAlgorithm
        .equals(that._hashAlgorithm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_indices, _numPartitions, _hashAlgorithm);
  }
}
