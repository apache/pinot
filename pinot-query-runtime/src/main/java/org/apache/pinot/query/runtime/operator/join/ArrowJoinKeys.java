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
package org.apache.pinot.query.runtime.operator.join;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Reads unboxed join keys in bounded batches; callers separately consult validity bits.
 */
public final class ArrowJoinKeys {
  public static final int BATCH_SIZE = 1024;

  private ArrowJoinKeys() {
  }

  public static void read(FieldVector vector, ColumnDataType type, int start, int count, long[] keys) {
    if (vector.getField().getDictionary() != null) {
      throw new IllegalArgumentException("Dictionary-encoded numeric join keys are not supported");
    }
    ArrowBuf data = vector.getDataBuffer();
    switch (type) {
      case INT:
      case FLOAT:
        // Match Fastutil primitive lookups: signed zeros and different NaN payload bits remain distinct.
        for (int i = 0; i < count; i++) {
          keys[i] = data.getInt(((long) start + i) * Integer.BYTES);
        }
        break;
      case LONG:
      case TIMESTAMP:
      case DOUBLE:
        for (int i = 0; i < count; i++) {
          keys[i] = data.getLong(((long) start + i) * Long.BYTES);
        }
        break;
      case BOOLEAN:
        if (vector instanceof BitVector) {
          for (int i = 0; i < count; i++) {
            int row = start + i;
            keys[i] = (data.getByte(row >>> 3) >>> (row & 7)) & 1;
          }
        } else {
          for (int i = 0; i < count; i++) {
            keys[i] = data.getInt(((long) start + i) * Integer.BYTES);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported Arrow join key type: " + type);
    }
  }
}
