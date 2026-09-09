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
package org.apache.pinot.query.runtime.operator.exchange;

import java.util.Arrays;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.query.QueryThreadContext;


/** Stateless, thread-safe numeric implementation of the legacy exchange hash, not the native join hash. */
final class ArrowHashPartitioner {
  private static final String ROUTE_SCOPE = "HashExchange";

  private ArrowHashPartitioner() {
  }

  static boolean supports(ArrowDataBlock block, int[] keyIds) {
    DataSchema schema = block.getDataSchema();
    for (int keyId : keyIds) {
      if (keyId < 0 || keyId >= schema.size()) {
        return false;
      }
      FieldVector vector = block.getRoot().getVector(keyId);
      if (vector.getField().getDictionary() != null) {
        return false;
      }
      boolean supported = switch (schema.getColumnDataType(keyId)) {
        case INT, BOOLEAN -> vector instanceof IntVector || vector instanceof BitVector;
        case LONG, TIMESTAMP -> vector instanceof BigIntVector;
        case FLOAT -> vector instanceof Float4Vector;
        case DOUBLE -> vector instanceof Float8Vector;
        default -> false;
      };
      if (!supported) {
        return false;
      }
    }
    return true;
  }

  /** Returns stable, disjoint row selections; empty destinations have a null selection. */
  static int[][] partitionRows(ArrowDataBlock block, int[] keyIds, int numPartitions) {
    int numRows = block.getNumberOfRows();
    int[] hashes = new int[numRows];
    for (int keyId : keyIds) {
      addHashes(block.getRoot().getVector(keyId), hashes);
    }
    int[] counts = new int[numPartitions];
    for (int row = 0; row < numRows; row++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
      int partition = (hashes[row] & Integer.MAX_VALUE) % numPartitions;
      hashes[row] = partition;
      counts[partition]++;
    }
    int[][] partitions = new int[numPartitions][];
    for (int partition = 0; partition < numPartitions; partition++) {
      if (counts[partition] != 0) {
        partitions[partition] = new int[counts[partition]];
      }
    }
    Arrays.fill(counts, 0);
    for (int row = 0; row < numRows; row++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
      int partition = hashes[row];
      partitions[partition][counts[partition]++] = row;
    }
    return partitions;
  }

  private static void addHashes(FieldVector vector, int[] hashes) {
    if (vector instanceof IntVector values) {
      for (int row = 0; row < hashes.length; row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!values.isNull(row)) {
          hashes[row] += values.get(row);
        }
      }
    } else if (vector instanceof BitVector values) {
      // BOOLEAN is an Integer in the row-heap representation, not a java.lang.Boolean.
      for (int row = 0; row < hashes.length; row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!values.isNull(row)) {
          hashes[row] += values.get(row);
        }
      }
    } else if (vector instanceof BigIntVector values) {
      for (int row = 0; row < hashes.length; row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!values.isNull(row)) {
          hashes[row] += Long.hashCode(values.get(row));
        }
      }
    } else if (vector instanceof Float4Vector values) {
      for (int row = 0; row < hashes.length; row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!values.isNull(row)) {
          hashes[row] += Float.floatToIntBits(values.get(row));
        }
      }
    } else if (vector instanceof Float8Vector values) {
      for (int row = 0; row < hashes.length; row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!values.isNull(row)) {
          hashes[row] += Long.hashCode(Double.doubleToLongBits(values.get(row)));
        }
      }
    } else {
      throw new IllegalArgumentException("Unsupported Arrow hash key vector: " + vector.getClass().getSimpleName());
    }
  }
}
