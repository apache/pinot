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
package org.apache.pinot.segment.spi;

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// [ColumnShape] for an empty column (zero rows).
public class EmptyColumnShape implements ColumnShape {
  private final FieldSpec _fieldSpec;
  private final PartitionFunction _partitionFunction;
  private final Set<Integer> _partitions;

  // TODO: Revisit if we need to maintain partition info for empty columns
  public EmptyColumnShape(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
      @Nullable Set<Integer> partitions) {
    _fieldSpec = fieldSpec;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
  }

  @Override
  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  public int getTotalDocs() {
    return 0;
  }

  @Override
  public int getCardinality() {
    return 0;
  }

  @Override
  public boolean isSorted() {
    return isSingleValue();
  }

  @Nullable
  @Override
  public Comparable<?> getMinValue() {
    return null;
  }

  @Nullable
  @Override
  public Comparable<?> getMaxValue() {
    return null;
  }

  @Override
  public int getLengthOfShortestElement() {
    DataType storedType = getStoredType();
    return storedType.isFixedWidth() ? storedType.size() : 0;
  }

  @Override
  public int getLengthOfLongestElement() {
    DataType storedType = getStoredType();
    return storedType.isFixedWidth() ? storedType.size() : 0;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return 0;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return 0;
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return 0;
  }

  @Nullable
  @Override
  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    return _partitions;
  }
}
