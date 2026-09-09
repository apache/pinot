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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.JoinNode;


/**
 * Allocation-free capability checks for the initial Arrow join subset. No Arrow types enter the planner.
 */
public final class ArrowJoinSupport {
  private ArrowJoinSupport() {
  }

  public static boolean supports(JoinNode node, DataSchema left, DataSchema right) {
    JoinRelType type = node.getJoinType();
    if ((type != JoinRelType.INNER && type != JoinRelType.LEFT && type != JoinRelType.SEMI && type != JoinRelType.ANTI)
        || node.getLeftKeys().size() != 1 || node.getRightKeys().size() != 1
        || !node.getNonEquiConditions().isEmpty() || !supportsSchema(left) || !supportsSchema(right)) {
      return false;
    }
    int leftKey = node.getLeftKeys().get(0);
    int rightKey = node.getRightKeys().get(0);
    if (leftKey < 0 || leftKey >= left.size() || rightKey < 0 || rightKey >= right.size()) {
      return false;
    }
    ColumnDataType keyType = left.getColumnDataType(leftKey);
    if (keyType != right.getColumnDataType(rightKey) || !supportsKey(keyType)) {
      return false;
    }
    DataSchema result = node.getDataSchema();
    boolean leftOnly = type == JoinRelType.SEMI || type == JoinRelType.ANTI;
    if (result.size() != left.size() + (leftOnly ? 0 : right.size())) {
      return false;
    }
    for (int col = 0; col < result.size(); col++) {
      ColumnDataType expected = col < left.size() ? left.getColumnDataType(col)
          : right.getColumnDataType(col - left.size());
      if (result.getColumnDataType(col) != expected) {
        return false;
      }
    }
    return true;
  }

  public static boolean supportsSchema(DataSchema schema) {
    for (ColumnDataType type : schema.getColumnDataTypes()) {
      switch (type) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case TIMESTAMP:
        case STRING:
        case JSON:
        case BYTES:
          break;
        default:
          return false;
      }
    }
    return true;
  }

  private static boolean supportsKey(ColumnDataType type) {
    return type == ColumnDataType.INT || type == ColumnDataType.LONG || type == ColumnDataType.FLOAT
        || type == ColumnDataType.DOUBLE || type == ColumnDataType.BOOLEAN || type == ColumnDataType.TIMESTAMP;
  }

  static Schema arrowSchema(DataSchema schema) {
    List<Field> fields = new ArrayList<>(schema.size());
    for (int col = 0; col < schema.size(); col++) {
      ArrowType type = switch (schema.getColumnDataType(col)) {
        case INT -> new ArrowType.Int(32, true);
        case LONG, TIMESTAMP -> new ArrowType.Int(64, true);
        case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        case BOOLEAN -> new ArrowType.Bool();
        case STRING, JSON -> new ArrowType.Utf8();
        case BYTES -> new ArrowType.Binary();
        default -> throw new IllegalArgumentException("Unsupported Arrow join column: " + schema.getColumnName(col));
      };
      fields.add(Field.nullable(schema.getColumnName(col), type));
    }
    return new Schema(fields);
  }
}
