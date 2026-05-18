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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.EmptyColumnShape;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/// Column statistics for an empty column (zero rows).
public class EmptyColumnStatistics extends EmptyColumnShape implements ColumnStatistics {

  public EmptyColumnStatistics(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
      @Nullable Set<Integer> partitions) {
    super(fieldSpec, partitionFunction, partitions);
  }

  @Nullable
  @Override
  public Object getUniqueValuesSet() {
    return null;
  }
}
