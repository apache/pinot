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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.utils.DataSchema;


/// How a [MailboxSendNode] sends its output to one of its receiver stages.
///
/// A send node with one receiver, or a spool whose receivers all read the same data in the same way, needs no spec:
/// every receiver uses the distribution, the keys and the data schema of the node. A spec lets one receiver of a
/// spool use a different distribution or read a subset of the columns.
///
/// - `distributionType` and `keys` route each row. The keys are ordinals of the sender's data schema, and the sender
///   computes them on the full row, before it applies the column mask.
/// - `columnMask`, when not null, lists the ordinals of the sender's data schema that the receiver reads, in the order
///   that the receiver expects. Null means every column, in order.
///
/// Instances are immutable.
public record ReceiverSpec(int receiverStageId, RelDistribution.Type distributionType, List<Integer> keys,
                           @Nullable List<Integer> columnMask) {
  public ReceiverSpec {
    keys = List.copyOf(keys);
    columnMask = columnMask != null ? List.copyOf(columnMask) : null;
  }

  /// Returns true for the distributions that an explicit receiver spec can use. The default spec of a node uses the
  /// distribution of the node, whatever it is.
  ///
  /// A spec changes how the sender routes rows, not how the planner connects the workers. The planner connects every
  /// sender worker to every receiver worker for these distributions, so a spec can switch between them freely.
  /// `SINGLETON` (a local exchange) and pre-partitioned exchanges wire the workers 1-to-1, so they cannot vary per
  /// receiver.
  public static boolean isSupported(RelDistribution.Type distributionType) {
    return distributionType == RelDistribution.Type.HASH_DISTRIBUTED
        || distributionType == RelDistribution.Type.BROADCAST_DISTRIBUTED
        || distributionType == RelDistribution.Type.RANDOM_DISTRIBUTED;
  }

  /// Returns the data schema that the receiver reads when the sender sends `senderSchema`.
  public DataSchema receiverSchema(DataSchema senderSchema) {
    if (columnMask == null) {
      return senderSchema;
    }
    String[] names = new String[columnMask.size()];
    DataSchema.ColumnDataType[] types = new DataSchema.ColumnDataType[columnMask.size()];
    for (int i = 0; i < names.length; i++) {
      int column = columnMask.get(i);
      names[i] = senderSchema.getColumnName(column);
      types[i] = senderSchema.getColumnDataType(column);
    }
    return new DataSchema(names, types);
  }

  /// Returns the column mask as an array, or null when the receiver reads every column.
  @Nullable
  public int[] columnMaskArray() {
    if (columnMask == null) {
      return null;
    }
    int[] result = new int[columnMask.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = columnMask.get(i);
    }
    return result;
  }

  @Override
  public String toString() {
    return receiverStageId + ":" + distributionType + (keys.isEmpty() ? "" : keys)
        + (columnMask == null ? "" : "cols" + columnMask);
  }
}
