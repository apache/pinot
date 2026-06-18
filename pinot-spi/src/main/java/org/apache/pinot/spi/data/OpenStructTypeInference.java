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
package org.apache.pinot.spi.data;

import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.PinotDataType;


/// Infers the {@link FieldSpec.DataType} for an OPEN_STRUCT key from raw ingested values when the key has
/// no declared child {@link FieldSpec}. This is OPEN_STRUCT-specific policy (it keeps TIMESTAMP, folds
/// DATE/TIME/UUID to STRING, widens BYTE/CHARACTER/SHORT to INT, and returns {@code null} for values that
/// cannot be represented as a stored column type), distinct from the JSON-node-based inference in
/// {@code JsonUtils.valueOf}.
public final class OpenStructTypeInference {
  private OpenStructTypeInference() {
  }

  /// Infers the {@link FieldSpec.DataType} from a raw ingested value. Returns {@code null} when the value
  /// cannot be represented as a stored column type; callers decide whether to drop the entry or fall back
  /// to a default (e.g. STRING).
  @Nullable
  public static FieldSpec.DataType inferDataType(Object rawValue) {
    switch (PinotDataType.getSingleValueType(rawValue)) {
      case INTEGER:
      case BYTE:
      case CHARACTER:
      case SHORT:
        return FieldSpec.DataType.INT;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case BIG_DECIMAL:
        return FieldSpec.DataType.BIG_DECIMAL;
      case BOOLEAN:
        return FieldSpec.DataType.BOOLEAN;
      case TIMESTAMP:
        return FieldSpec.DataType.TIMESTAMP;
      case STRING:
      case DATE:
      case TIME:
      case UUID:
        return FieldSpec.DataType.STRING;
      case BYTES:
        return FieldSpec.DataType.BYTES;
      default:
        return null;
    }
  }
}
