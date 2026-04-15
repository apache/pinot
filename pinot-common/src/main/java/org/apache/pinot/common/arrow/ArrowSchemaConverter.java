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
package org.apache.pinot.common.arrow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Converts between Pinot {@link DataSchema} and Apache Arrow {@link Schema}.
 *
 * <p>String and JSON columns use dictionary encoding (Arrow {@code Int32} index into a {@code VarChar} dictionary)
 * to reduce memory usage and improve hash-join performance. All other types map directly to their Arrow equivalents.
 */
public class ArrowSchemaConverter {
  /** Name used for list element child vectors. */
  public static final String ELEMENTS = "elements";

  private ArrowSchemaConverter() {
  }

  /**
   * Converts an Arrow {@link Schema} to a Pinot {@link DataSchema}.
   */
  public static DataSchema toDataSchema(Schema schema) {
    List<Field> fields = schema.getFields();
    String[] columnNames = new String[fields.size()];
    ColumnDataType[] columnDataTypes = new ColumnDataType[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      columnNames[i] = field.getName();
      columnDataTypes[i] = arrowTypeToPinot(field.getType());
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  /**
   * Converts a Pinot {@link DataSchema} to an Arrow {@link Schema}.
   *
   * <p>String-typed columns are dictionary-encoded (index stored as {@code Int32}).
   */
  public static Schema toSchema(DataSchema dataSchema) {
    List<Field> fields = new ArrayList<>(dataSchema.size());
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    for (int i = 0; i < columnNames.length; i++) {
      fields.add(pinotTypeToArrowField(columnNames[i], columnDataTypes[i]));
    }
    return new Schema(fields);
  }

  // ----- type conversion helpers -----

  private static ColumnDataType arrowTypeToPinot(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Bool) {
      return ColumnDataType.BOOLEAN;
    }
    if (arrowType instanceof ArrowType.Int) {
      return ((ArrowType.Int) arrowType).getBitWidth() == 32 ? ColumnDataType.INT : ColumnDataType.LONG;
    }
    if (arrowType instanceof ArrowType.FloatingPoint) {
      return ((ArrowType.FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.SINGLE
          ? ColumnDataType.FLOAT : ColumnDataType.DOUBLE;
    }
    if (arrowType instanceof ArrowType.Utf8) {
      return ColumnDataType.STRING;
    }
    if (arrowType instanceof ArrowType.Binary) {
      return ColumnDataType.BYTES;
    }
    if (arrowType instanceof ArrowType.List) {
      return ColumnDataType.STRING_ARRAY;
    }
    if (arrowType instanceof ArrowType.Null) {
      return ColumnDataType.UNKNOWN;
    }
    return ColumnDataType.STRING;
  }

  /**
   * Converts a Pinot {@link ColumnDataType} to an Arrow {@link Field}.
   */
  public static Field pinotTypeToArrowField(String columnName, ColumnDataType columnDataType) {
    switch (columnDataType) {
      case BOOLEAN:
        return nullable(columnName, new ArrowType.Bool());
      case INT:
        return nullable(columnName, new ArrowType.Int(32, true));
      case LONG:
      case TIMESTAMP:
        return nullable(columnName, new ArrowType.Int(64, true));
      case FLOAT:
        return nullable(columnName, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
      case DOUBLE:
        return nullable(columnName, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
      case STRING:
      case JSON:
        // Dictionary-encoded: index vector (Int32) → dictionary (VarChar)
        return dictionaryEncoded(columnName);
      case BYTES:
      case MAP:
      case OBJECT:
        return nullable(columnName, new ArrowType.Binary());
      case BOOLEAN_ARRAY:
        return listOf(columnName, new ArrowType.Bool());
      case INT_ARRAY:
        return listOf(columnName, new ArrowType.Int(32, true));
      case LONG_ARRAY:
      case TIMESTAMP_ARRAY:
        return listOf(columnName, new ArrowType.Int(64, true));
      case FLOAT_ARRAY:
        return listOf(columnName, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
      case DOUBLE_ARRAY:
        return listOf(columnName, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
      case STRING_ARRAY:
        return listOfDictionaryEncoded(columnName);
      case BYTES_ARRAY:
        return listOf(columnName, new ArrowType.Binary());
      case UNKNOWN:
        return nullable(columnName, new ArrowType.Null());
      default:
        throw new IllegalArgumentException("Unsupported column type: " + columnDataType);
    }
  }

  private static Field nullable(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  private static Field dictionaryEncoded(String name) {
    DictionaryEncoding encoding = new DictionaryEncoding(0, true, null);
    FieldType indexFieldType = new FieldType(true, new ArrowType.Int(32, true), encoding, null);
    return new Field(name, indexFieldType, null);
  }

  private static Field listOf(String name, ArrowType elementType) {
    List<Field> children = Collections.singletonList(new Field(ELEMENTS, FieldType.nullable(elementType), null));
    return new Field(name, FieldType.nullable(new ArrowType.List()), children);
  }

  private static Field listOfDictionaryEncoded(String name) {
    DictionaryEncoding encoding = new DictionaryEncoding(0, true, null);
    FieldType indexFieldType = new FieldType(true, new ArrowType.Int(32, true), encoding, null);
    List<Field> children = Collections.singletonList(new Field(ELEMENTS, indexFieldType, null));
    return new Field(name, FieldType.nullable(new ArrowType.List()), children);
  }
}
