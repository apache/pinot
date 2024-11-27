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
package org.apache.pinot.plugin.inputformat.parquet;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.joda.time.DateTimeConstants;

import static java.lang.Math.pow;


/**
 * ParquetNativeRecordExtractor extract values from Parquet {@link Group}.
 */
public class ParquetNativeRecordExtractor extends BaseRecordExtractor<Group> {

  /**
   * Number of days between Julian day epoch (January 1, 4713 BC) and Unix day epoch (January 1, 1970).
   * The value of this constant is {@value}.
   */
  public static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;

  public static final long NANOS_PER_MILLISECOND = 1000000;

  private Set<String> _fields;
  private boolean _extractAll = false;

  public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
    /*
     * Precision <= 18 checks for the max number of digits for an unscaled long,
     * else treat with big integer conversion
     */
    if (precision <= 18) {
      ByteBuffer buffer = value.toByteBuffer();
      byte[] bytes = buffer.array();
      int start = buffer.arrayOffset() + buffer.position();
      int end = buffer.arrayOffset() + buffer.limit();
      long unscaled = 0L;
      int i = start;
      while (i < end) {
        unscaled = (unscaled << 8 | bytes[i] & 0xff);
        i++;
      }
      int bits = 8 * (end - start);
      long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
      if (unscaledNew <= -pow(10, 18) || unscaledNew >= pow(10, 18)) {
        return new BigDecimal(unscaledNew);
      } else {
        return BigDecimal.valueOf(unscaledNew / pow(10, scale));
      }
    } else {
      return new BigDecimal(new BigInteger(value.getBytes()), scale);
    }
  }

  @Override
  public void init(@Nullable Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Set.of();
    } else {
      _fields = Set.copyOf(fields);
    }
  }

  @Override
  public GenericRow extract(Group from, GenericRow to) {
    GroupType fromType = from.getType();
    if (_extractAll) {
      List<Type> fields = fromType.getFields();
      for (Type field : fields) {
        String fieldName = field.getName();
        Object value = extractValue(from, fromType.getFieldIndex(fieldName));
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }
    } else {
      for (String fieldName : _fields) {
        Object value = fromType.containsField(fieldName) ? extractValue(from, fromType.getFieldIndex(fieldName)) : null;
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }
    }
    return to;
  }

  @Nullable
  private Object extractValue(Group from, int fieldIndex) {
    int numValues = from.getFieldRepetitionCount(fieldIndex);
    Type fieldType = from.getType().getType(fieldIndex);
    if (numValues == 0) {
      return null;
    }
    if (numValues == 1) {
      return extractValue(from, fieldIndex, fieldType, 0);
    }
    // For multi-value (repeated field)
    Object[] results = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      results[i] = extractValue(from, fieldIndex, fieldType, i);
    }
    return results;
  }

  @Nullable
  private Object extractValue(Group from, int fieldIndex, Type fieldType, int index) {
    LogicalTypeAnnotation logicalTypeAnnotation = fieldType.getLogicalTypeAnnotation();
    if (fieldType.isPrimitive()) {
      PrimitiveType.PrimitiveTypeName primitiveTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
      switch (primitiveTypeName) {
        case INT32:
          int intValue = from.getInteger(fieldIndex, index);
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
            return BigDecimal.valueOf(intValue, decimalLogicalTypeAnnotation.getScale());
          }
          return intValue;
        case INT64:
          long longValue = from.getLong(fieldIndex, index);
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
            return BigDecimal.valueOf(longValue, decimalLogicalTypeAnnotation.getScale());
          }
          return longValue;
        case FLOAT:
          return from.getFloat(fieldIndex, index);
        case DOUBLE:
          return from.getDouble(fieldIndex, index);
        case BOOLEAN:
          return from.getValueToString(fieldIndex, index);
        case INT96:
          Binary int96 = from.getInt96(fieldIndex, index);
          return convertInt96ToLong(int96.getBytes());
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
            return binaryToDecimal(from.getBinary(fieldIndex, index), decimalLogicalTypeAnnotation.getPrecision(),
                decimalLogicalTypeAnnotation.getScale());
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return from.getValueToString(fieldIndex, index);
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            return from.getValueToString(fieldIndex, index);
          }
          return from.getBinary(fieldIndex, index).getBytes();
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported field type: %s, primitive type: %s, logical type: %s", fieldType,
                  primitiveTypeName, logicalTypeAnnotation));
      }
    } else if ((fieldType.isRepetition(Type.Repetition.OPTIONAL)) || (fieldType.isRepetition(Type.Repetition.REQUIRED))
        || (fieldType.isRepetition(Type.Repetition.REPEATED))) {
      Group group = from.getGroup(fieldIndex, index);
      if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
        return extractList(group);
      }
      return extractMap(group);
    }
    return null;
  }

  public static long convertInt96ToLong(byte[] int96Bytes) {
    ByteBuffer buf = ByteBuffer.wrap(int96Bytes).order(ByteOrder.LITTLE_ENDIAN);
    return (buf.getInt(8) - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * DateTimeConstants.MILLIS_PER_DAY
        + buf.getLong(0) / NANOS_PER_MILLISECOND;
  }

  public Object[] extractList(Group group) {
    int numValues = group.getType().getFieldCount();
    Object[] array = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      array[i] = extractValue(group, i);
    }
    if (numValues == 1 && array[0] == null) {
      return new Object[0];
    }
    if (numValues == 1 && array[0] instanceof Object[]) {
      return (Object[]) array[0];
    }
    return array;
  }

  public Map<String, Object> extractMap(Group group) {
    int numValues = group.getType().getFieldCount();
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(numValues);
    for (int i = 0; i < numValues; i++) {
      map.put(group.getType().getType(i).getName(), extractValue(group, i));
    }
    return map;
  }
}
