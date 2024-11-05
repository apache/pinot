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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.JsonParseException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 *  The <code>PinotDataType</code> enum represents the data type of a value in a row from recordReader and provides
 *  utility methods to convert value across types if applicable.
 *  <p>We don't use <code>PinotDataType</code> to maintain type information, but use it to help organize the data and
 *  use {@link DataType} to maintain type information separately across various readers.
 *  <p>NOTE:
 *  <ul>
 *    <li>We will silently lose information if a conversion causes us to do so (e.g. DOUBLE to INT)</li>
 *    <li>We will throw exception if a conversion is not possible (e.g. BOOLEAN to INT).</li>
 *    <li>We will throw exception if the conversion throws exception (e.g. "foo" -> INT)</li>
 *  </ul>
 */
public enum PinotDataType {

  /**
   * When converting from BOOLEAN to other types:
   * - Numeric types:
   *   - true -> 1
   *   - false -> 0
   * - String:
   *   - true -> "true"
   *   - false -> "false"
   *
   * When converting to BOOLEAN from other types:
   * - Numeric types:
   *   - 0 -> false
   *   - Others -> true
   * - String:
   *   - "true" (case-insensitive) -> true
   *   - "1" -> true (for backward-compatibility where we used to use integer 1 to represent true)
   *   - Others ->  false
   */
  BOOLEAN {
    @Override
    public int toInt(Object value) {
      return ((Boolean) value) ? 1 : 0;
    }

    @Override
    public long toLong(Object value) {
      return ((Boolean) value) ? 1L : 0L;
    }

    @Override
    public float toFloat(Object value) {
      return ((Boolean) value) ? 1f : 0f;
    }

    @Override
    public double toDouble(Object value) {
      return ((Boolean) value) ? 1d : 0d;
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return ((Boolean) value) ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Boolean) value;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to BYTES");
    }

    @Override
    public Boolean convert(Object value, PinotDataType sourceType) {
      return sourceType.toBoolean(value);
    }

    @Override
    public Integer toInternal(Object value) {
      return ((Boolean) value) ? 1 : 0;
    }
  },

  BYTE {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toInt(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Byte) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTE to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTE to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTE to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTE to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTE to BYTES");
    }
  },

  CHARACTER {
    @Override
    public int toInt(Object value) {
      return (int) ((Character) value);
    }

    @Override
    public long toLong(Object value) {
      return (long) ((Character) value);
    }

    @Override
    public float toFloat(Object value) {
      return (float) ((Character) value);
    }

    @Override
    public double toDouble(Object value) {
      return (double) ((Character) value);
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toInt(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Character) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to BYTES");
    }
  },

  SHORT {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toInt(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Short) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to BYTES");
    }
  },

  INTEGER {
    @Override
    public int toInt(Object value) {
      return (Integer) value;
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf((Integer) value);
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Integer) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from INTEGER to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from INTEGER to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return LocalDate.ofEpochDay(toLong(value));
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return LocalTime.ofNanoOfDay(toLong(value) * 1000000);
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from INTEGER to BYTES");
    }

    @Override
    public Integer convert(Object value, PinotDataType sourceType) {
      return sourceType.toInt(value);
    }
  },

  LONG {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return (Long) value;
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      // BigDecimal.valueOf(long) translates a long value into a BigDecimal value with a scale of zero.
      // This "static factory method" is provided in preference to a (long) constructor because it allows for reuse of
      // frequently used BigDecimal values.
      return BigDecimal.valueOf((Long) value);
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Long) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp((Long) value);
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), ZoneId.of("UTC"));
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return LocalDate.ofEpochDay((Long) value);
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return LocalTime.ofNanoOfDay((Long) value * 1000000);
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from LONG to BYTES");
    }

    @Override
    public Long convert(Object value, PinotDataType sourceType) {
      return sourceType.toLong(value);
    }
  },

  FLOAT {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return (Float) value;
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      // Use string representation of the value to create BigDecimal to avoid getting the exact floating-point value.
      // new BigDecimal(123.45f) -> 123.4499969482421875
      return new BigDecimal(value.toString());
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Float) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to BYTES");
    }

    @Override
    public Float convert(Object value, PinotDataType sourceType) {
      return sourceType.toFloat(value);
    }
  },

  DOUBLE {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return (Double) value;
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      // Use string representation of the value to create BigDecimal to avoid getting the exact floating-point value.
      // new BigDecimal(123.45) -> 123.4500000000000028421709430404007434844970703125
      return new BigDecimal(value.toString());
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Double) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp(((Double) value).longValue());
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Double) value).longValue()), ZoneId.of("UTC"));
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return LocalDate.ofEpochDay(((Double) value).longValue());
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return LocalTime.ofNanoOfDay(((Double) value).longValue() * 1000000);
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DOUBLE to BYTES");
    }

    @Override
    public Double convert(Object value, PinotDataType sourceType) {
      return sourceType.toDouble(value);
    }
  },

  BIG_DECIMAL {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return (BigDecimal) value;
    }

    @Override
    public boolean toBoolean(Object value) {
      return !value.equals(BigDecimal.ZERO);
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp(((Number) value).longValue());
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), ZoneId.of("UTC"));
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return LocalDate.ofEpochDay(((Number) value).longValue());
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1000000);
    }

    @Override
    public String toString(Object value) {
      return ((BigDecimal) value).toPlainString();
    }

    @Override
    public byte[] toBytes(Object value) {
      return BigDecimalUtils.serialize((BigDecimal) value);
    }

    @Override
    public BigDecimal convert(Object value, PinotDataType sourceType) {
      return sourceType.toBigDecimal(value);
    }
  },

  /**
   * When converting from TIMESTAMP to other types:
   * - LONG/DOUBLE: millis since epoch value
   * - String: SQL timestamp format (e.g. "2021-01-01 01:01:01.001")
   *
   * When converting to TIMESTAMP from other types:
   * - LONG/DOUBLE: read long value as millis since epoch
   * - String:
   *   - SQL timestamp format (e.g. "2021-01-01 01:01:01.001")
   *   - Millis since epoch value (e.g. "1609491661001")
   */
  TIMESTAMP {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      return ((Timestamp) value).getTime();
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      return ((Timestamp) value).getTime();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toLong(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return (Timestamp) value;
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to BYTES");
    }

    @Override
    public Timestamp convert(Object value, PinotDataType sourceType) {
      return sourceType.toTimestamp(value);
    }

    @Override
    public Long toInternal(Object value) {
      return ((Timestamp) value).getTime();
    }
  },

  /**
   * When converting from TIMESTAMP_NTZ to other types:
   * - LONG/DOUBLE: millis since epoch value
   * - String: SQL timestamp format (e.g. "2021-01-01 01:01:01.001")
   *
   * When converting to TIMESTAMP_NTZ from other types:
   * - LONG/DOUBLE: read long value as millis since epoch
   * - String:
   *   - SQL timestamp format (e.g. "2021-01-01 01:01:01.001")
   *   - Millis since epoch value (e.g. "1609491661001")
   */
  TIMESTAMP_NTZ {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      return ((LocalDateTime) value).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      return ((LocalDateTime) value).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toLong(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return (LocalDateTime) value;
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP_NTZ to BYTES");
    }

    @Override
    public LocalDateTime convert(Object value, PinotDataType sourceType) {
      return sourceType.toLocalDateTime(value);
    }

    @Override
    public Long toInternal(Object value) {
      return toLong(value);
    }
  },

  DATE {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      return ((LocalDate) value).toEpochDay();
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      return ((LocalDate) value).toEpochDay();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toLong(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return (LocalDate) value;
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to TIME");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DATE to BYTES");
    }

    @Override
    public LocalDate convert(Object value, PinotDataType sourceType) {
      return sourceType.toLocalDate(value);
    }

    @Override
    public Long toInternal(Object value) {
      return toLong(value);
    }
  },

  TIME {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      return ((LocalTime) value).toNanoOfDay() / 1000000;
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      return ((LocalTime) value).toNanoOfDay() / 1000000;
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(toLong(value));
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return (LocalTime) value;
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIME to BYTES");
    }

    @Override
    public LocalTime convert(Object value, PinotDataType sourceType) {
      return sourceType.toLocalTime(value);
    }

    @Override
    public Long toInternal(Object value) {
      return toLong(value);
    }
  },

  STRING {
    @Override
    public int toInt(Object value) {
      return Integer.parseInt(value.toString().trim());
    }

    @Override
    public long toLong(Object value) {
      return Long.parseLong(value.toString().trim());
    }

    @Override
    public float toFloat(Object value) {
      // NOTE: No need to trim here because Float.valueOf() will trim the string
      return Float.parseFloat(value.toString());
    }

    @Override
    public double toDouble(Object value) {
      // NOTE: No need to trim here because Double.valueOf() will trim the string
      return Double.parseDouble(value.toString());
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return new BigDecimal(value.toString().trim());
    }

    @Override
    public boolean toBoolean(Object value) {
      return BooleanUtils.toBoolean(value.toString().trim());
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return TimestampUtils.toTimestamp(value.toString().trim());
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return TimestampUtils.toLocalDateTime(value.toString().trim());
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return TimestampUtils.toLocalDate(value.toString().trim());
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return TimestampUtils.toLocalTime(value.toString().trim());
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      return BytesUtils.toBytes(value.toString().trim());
    }

    @Override
    public String convert(Object value, PinotDataType sourceType) {
      return sourceType.toString(value);
    }
  },

  JSON {
    @Override
    public int toInt(Object value) {
      return Integer.parseInt(value.toString().trim());
    }

    @Override
    public long toLong(Object value) {
      return Long.parseLong(value.toString().trim());
    }

    @Override
    public float toFloat(Object value) {
      return Float.parseFloat(value.toString());
    }

    @Override
    public double toDouble(Object value) {
      return Double.parseDouble(value.toString());
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return new BigDecimal(value.toString().trim());
    }

    @Override
    public boolean toBoolean(Object value) {
      return Boolean.parseBoolean(value.toString().trim());
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return TimestampUtils.toTimestamp(value.toString().trim());
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return TimestampUtils.toLocalDateTime(value.toString().trim());
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return TimestampUtils.toLocalDate(value.toString().trim());
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return TimestampUtils.toLocalTime(value.toString().trim());
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      // Base64 encoding is the commonly used mechanism for encoding binary data in JSON documents. Note that
      // toJson function converts byte[] into a Base64 encoded json string value.
      try {
        return Base64.getDecoder().decode(value.toString());
      } catch (Exception e) {
        throw new RuntimeException("Unable to convert JSON base64 encoded string value to BYTES. Input value: " + value,
            e);
      }
    }

    @Override
    public String convert(Object value, PinotDataType sourceType) {
      return sourceType.toJson(value);
    }
  },

  BYTES {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to LONG");
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to DOUBLE");
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimalUtils.deserialize((byte[]) value);
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to TIMESTAMP");
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to TIMESTAMP_NTZ");
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to DATE");
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to TIME");
    }

    @Override
    public String toString(Object value) {
      return BytesUtils.toHexString((byte[]) value);
    }

    @Override
    public byte[] toBytes(Object value) {
      return (byte[]) value;
    }

    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return sourceType.toBytes(value);
    }
  },

  OBJECT {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Object value) {
      return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    @Override
    public boolean toBoolean(Object value) {
      return ((Number) value).doubleValue() != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp(((Number) value).longValue());
    }

    @Override
    public LocalDateTime toLocalDateTime(Object value) {
      return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), ZoneId.of("UTC"));
    }

    @Override
    public LocalDate toLocalDate(Object value) {
      return LocalDate.ofEpochDay(((Number) value).longValue());
    }

    @Override
    public LocalTime toLocalTime(Object value) {
      return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1000000);
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from OBJECT to BYTES");
    }

    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return value;
    }
  },

  MAP {
    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      switch (sourceType) {
        case OBJECT:
        case MAP:
          if (value instanceof Map) {
            return value;
          } else {
            throw new UnsupportedOperationException(String.format("Cannot convert '%s' (Class of value: '%s') to MAP",
                sourceType, value.getClass()));
          }
        default:
          throw new UnsupportedOperationException(String.format("Cannot convert '%s' (Class of value: '%s') to MAP",
              sourceType, value.getClass()));
      }
    }
  },

  BYTE_ARRAY {
    @Override
    public byte[] toBytes(Object value) {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      byte[] bytes = new byte[length];
      for (int i = 0; i < length; i++) {
        bytes[i] = (Byte) valueArray[i];
      }
      return bytes;
    }
  },

  CHARACTER_ARRAY,

  SHORT_ARRAY,

  /*
    NOTE:
      Primitive array is used in query execution, query response, scalar function arguments and return values.
      Object array is used in GenericRow for data ingestion.
      We need to keep them separately because they cannot automatically cast to the other type.
   */

  PRIMITIVE_INT_ARRAY {
    @Override
    public int[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveIntArray(value);
    }
  },

  INTEGER_ARRAY {
    @Override
    public Integer[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toIntegerArray(value);
    }
  },

  PRIMITIVE_LONG_ARRAY {
    @Override
    public long[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveLongArray(value);
    }
  },

  LONG_ARRAY {
    @Override
    public Long[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toLongArray(value);
    }
  },

  PRIMITIVE_FLOAT_ARRAY {
    @Override
    public float[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveFloatArray(value);
    }
  },

  FLOAT_ARRAY {
    @Override
    public Float[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toFloatArray(value);
    }
  },

  PRIMITIVE_DOUBLE_ARRAY {
    @Override
    public double[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveDoubleArray(value);
    }
  },

  DOUBLE_ARRAY {
    @Override
    public Double[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toDoubleArray(value);
    }
  },

  BOOLEAN_ARRAY {
    @Override
    public boolean[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toBooleanArray(value);
    }

    @Override
    public Integer[] toInternal(Object value) {
      boolean[] booleanArray = (boolean[]) value;
      int length = booleanArray.length;
      Integer[] intArray = new Integer[length];
      for (int i = 0; i < length; i++) {
        intArray[i] = booleanArray[i] ? 1 : 0;
      }
      return intArray;
    }
  },

  TIMESTAMP_ARRAY {
    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return sourceType.toTimestampArray(value);
    }

    @Override
    public Long[] toInternal(Object value) {
      Timestamp[] timestampArray = (Timestamp[]) value;
      int length = timestampArray.length;
      Long[] longArray = new Long[length];
      for (int i = 0; i < length; i++) {
        longArray[i] = timestampArray[i].getTime();
      }
      return longArray;
    }
  },

  TIMESTAMP_NTZ_ARRAY {
    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return sourceType.toLocalDateTimeArray(value);
    }

    @Override
    public Long[] toInternal(Object value) {
      LocalDateTime[] localDateTimeArray = (LocalDateTime[]) value;
      int length = localDateTimeArray.length;
      Long[] longArray = new Long[length];
      for (int i = 0; i < length; i++) {
        longArray[i] = localDateTimeArray[i].atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
      }
      return longArray;
    }
  },

  DATE_ARRAY {
    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return sourceType.toLocalDateArray(value);
    }

    @Override
    public Long[] toInternal(Object value) {
      LocalDate[] localDateArray = (LocalDate[]) value;
      int length = localDateArray.length;
      Long[] longArray = new Long[length];
      for (int i = 0; i < length; i++) {
        longArray[i] = localDateArray[i].toEpochDay();
      }
      return longArray;
    }
  },

  TIME_ARRAY {
    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return sourceType.toLocalTimeArray(value);
    }

    @Override
    public Long[] toInternal(Object value) {
      LocalTime[] localTimeArray = (LocalTime[]) value;
      int length = localTimeArray.length;
      Long[] longArray = new Long[length];
      for (int i = 0; i < length; i++) {
        longArray[i] = localTimeArray[i].toNanoOfDay() / 1000000;
      }
      return longArray;
    }
  },

  STRING_ARRAY {
    @Override
    public String[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toStringArray(value);
    }
  },

  BYTES_ARRAY {
    @Override
    public byte[][] convert(Object value, PinotDataType sourceType) {
      return sourceType.toBytesArray(value);
    }
  },

  COLLECTION,

  OBJECT_ARRAY;

  /**
   * NOTE: override toInt(), toLong(), toFloat(), toDouble(), toBoolean(), toTimestamp(), toString(), and
   * toBytes() for single-value types.
   */

  public int toInt(Object value) {
    return getSingleValueType().toInt(toObjectArray(value)[0]);
  }

  public long toLong(Object value) {
    return getSingleValueType().toLong(toObjectArray(value)[0]);
  }

  public float toFloat(Object value) {
    return getSingleValueType().toFloat(toObjectArray(value)[0]);
  }

  public double toDouble(Object value) {
    return getSingleValueType().toDouble(toObjectArray(value)[0]);
  }

  public BigDecimal toBigDecimal(Object value) {
    return getSingleValueType().toBigDecimal(toObjectArray(value)[0]);
  }

  public boolean toBoolean(Object value) {
    return getSingleValueType().toBoolean(((Object[]) value)[0]);
  }

  public Timestamp toTimestamp(Object value) {
    return getSingleValueType().toTimestamp(((Object[]) value)[0]);
  }

  public LocalDateTime toLocalDateTime(Object value) {
    return getSingleValueType().toLocalDateTime(value);
  }

  public LocalDate toLocalDate(Object value) {
    return getSingleValueType().toLocalDate(value);
  }

  public LocalTime toLocalTime(Object value) {
    return getSingleValueType().toLocalTime(value);
  }

  public String toString(Object value) {
    return getSingleValueType().toString(toObjectArray(value)[0]);
  }

  public String toJson(Object value) {
    if (value instanceof String) {
      try {
        // Try to parse the string as JSON first
        return JsonUtils.stringToJsonNodeWithBigDecimal((String) value).toString();
      } catch (JsonParseException jpe) {
        // String does not represent a well-formed JSON. Ignore this exception because we are going to try to convert
        // Java String object to JSON string.
      } catch (Exception e) {
        throw new RuntimeException("Unable to convert String into JSON. Input value: " + value, e);
      }
    }

    // Try converting Java object into JSON.
    try {
      return JsonUtils.objectToString(value);
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to convert " + value.getClass().getCanonicalName() + " to JSON. Input value: " + value, e);
    }
  }

  public byte[] toBytes(Object value) {
    return getSingleValueType().toBytes(toObjectArray(value)[0]);
  }

  public int[] toPrimitiveIntArray(Object value) {
    if (value instanceof int[]) {
      return (int[]) value;
    }
    if (isSingleValue()) {
      return new int[]{toInt(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      int[] intArray = new int[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          intArray[i] = singleValueType.toInt(valueArray[i]);
        } catch (ClassCastException e) {
          intArray[i] = anyToInt(valueArray[i]);
        }
      }
      return intArray;
    }
  }

  public Integer[] toIntegerArray(Object value) {
    if (value instanceof Integer[]) {
      return (Integer[]) value;
    }
    if (isSingleValue()) {
      return new Integer[]{toInt(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Integer[] integerArray = new Integer[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          integerArray[i] = singleValueType.toInt(valueArray[i]);
        } catch (ClassCastException e) {
          integerArray[i] = anyToInt(valueArray[i]);
        }
      }
      return integerArray;
    }
  }

  public long[] toPrimitiveLongArray(Object value) {
    if (value instanceof long[]) {
      return (long[]) value;
    }
    if (isSingleValue()) {
      return new long[]{toLong(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      long[] longArray = new long[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          longArray[i] = singleValueType.toLong(valueArray[i]);
        } catch (ClassCastException e) {
          longArray[i] = anyToLong(valueArray[i]);
        }
      }
      return longArray;
    }
  }

  public Long[] toLongArray(Object value) {
    if (value instanceof Long[]) {
      return (Long[]) value;
    }
    if (isSingleValue()) {
      return new Long[]{toLong(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Long[] longArray = new Long[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          longArray[i] = singleValueType.toLong(valueArray[i]);
        } catch (ClassCastException e) {
          longArray[i] = anyToLong(valueArray[i]);
        }
      }
      return longArray;
    }
  }

  public float[] toPrimitiveFloatArray(Object value) {
    if (value instanceof float[]) {
      return (float[]) value;
    }
    if (isSingleValue()) {
      return new float[]{toFloat(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      float[] floatArray = new float[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          floatArray[i] = singleValueType.toFloat(valueArray[i]);
        } catch (ClassCastException e) {
          floatArray[i] = anyToFloat(valueArray[i]);
        }
      }
      return floatArray;
    }
  }

  public Float[] toFloatArray(Object value) {
    if (value instanceof Float[]) {
      return (Float[]) value;
    }
    if (isSingleValue()) {
      return new Float[]{toFloat(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Float[] floatArray = new Float[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          floatArray[i] = singleValueType.toFloat(valueArray[i]);
        } catch (ClassCastException e) {
          floatArray[i] = anyToFloat(valueArray[i]);
        }
      }
      return floatArray;
    }
  }

  public double[] toPrimitiveDoubleArray(Object value) {
    if (value instanceof double[]) {
      return (double[]) value;
    }
    if (isSingleValue()) {
      return new double[]{toDouble(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      double[] doubleArray = new double[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          doubleArray[i] = singleValueType.toDouble(valueArray[i]);
        } catch (ClassCastException e) {
          doubleArray[i] = anyToDouble(valueArray[i]);
        }
      }
      return doubleArray;
    }
  }

  public Double[] toDoubleArray(Object value) {
    if (value instanceof Double[]) {
      return (Double[]) value;
    }
    if (isSingleValue()) {
      return new Double[]{toDouble(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Double[] doubleArray = new Double[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        try {
          doubleArray[i] = singleValueType.toDouble(valueArray[i]);
        } catch (ClassCastException e) {
          doubleArray[i] = anyToDouble(valueArray[i]);
        }
      }
      return doubleArray;
    }
  }

  public String[] toStringArray(Object value) {
    if (value instanceof String[]) {
      return (String[]) value;
    }
    if (isSingleValue()) {
      return new String[]{toString(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      String[] stringArray = new String[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        stringArray[i] = singleValueType.toString(valueArray[i]);
      }
      return stringArray;
    }
  }

  public byte[][] toBytesArray(Object value) {
    if (value instanceof byte[][]) {
      return (byte[][]) value;
    }
    if (isSingleValue()) {
      return new byte[][]{toBytes(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      byte[][] bytesArray = new byte[length][];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        bytesArray[i] = singleValueType.toBytes(valueArray[i]);
      }
      return bytesArray;
    }
  }

  private static Object[] toObjectArray(Object array) {
    if (array instanceof Collection) {
      return ((Collection<?>) array).toArray();
    }
    Class<?> componentType = array.getClass().getComponentType();
    if (componentType.isPrimitive()) {
      if (componentType == int.class) {
        return ArrayUtils.toObject((int[]) array);
      }
      if (componentType == long.class) {
        return ArrayUtils.toObject((long[]) array);
      }
      if (componentType == float.class) {
        return ArrayUtils.toObject((float[]) array);
      }
      if (componentType == double.class) {
        return ArrayUtils.toObject((double[]) array);
      }
      throw new UnsupportedOperationException("Unsupported primitive array type: " + componentType);
    } else {
      return (Object[]) array;
    }
  }

  public boolean[] toBooleanArray(Object value) {
    if (value instanceof boolean[]) {
      return (boolean[]) value;
    }
    if (isSingleValue()) {
      return new boolean[]{toBoolean(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      boolean[] booleanArray = new boolean[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        booleanArray[i] = singleValueType.toBoolean(valueArray[i]);
      }
      return booleanArray;
    }
  }

  public Timestamp[] toTimestampArray(Object value) {
    if (value instanceof Timestamp[]) {
      return (Timestamp[]) value;
    }
    if (isSingleValue()) {
      return new Timestamp[]{toTimestamp(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Timestamp[] timestampArray = new Timestamp[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        timestampArray[i] = singleValueType.toTimestamp(valueArray[i]);
      }
      return timestampArray;
    }
  }

  public LocalDateTime[] toLocalDateTimeArray(Object value) {
    if (value instanceof LocalDateTime[]) {
      return (LocalDateTime[]) value;
    }
    if (isSingleValue()) {
      return new LocalDateTime[]{toLocalDateTime(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      LocalDateTime[] localDateTimeArray = new LocalDateTime[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        localDateTimeArray[i] = singleValueType.toLocalDateTime(valueArray[i]);
      }
      return localDateTimeArray;
    }
  }

  public LocalDate[] toLocalDateArray(Object value) {
    if (value instanceof LocalDate[]) {
      return (LocalDate[]) value;
    }
    if (isSingleValue()) {
      return new LocalDate[]{toLocalDate(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      LocalDate[] localDateArray = new LocalDate[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        localDateArray[i] = singleValueType.toLocalDate(valueArray[i]);
      }
      return localDateArray;
    }
  }

  public LocalTime[] toLocalTimeArray(Object value) {
    if (value instanceof LocalTime[]) {
      return (LocalTime[]) value;
    }
    if (isSingleValue()) {
      return new LocalTime[]{toLocalTime(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      LocalTime[] localTimeArray = new LocalTime[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        localTimeArray[i] = singleValueType.toLocalTime(valueArray[i]);
      }
      return localTimeArray;
    }
  }

  public Object convert(Object value, PinotDataType sourceType) {
    throw new UnsupportedOperationException("Cannot convert value from " + sourceType + " to " + this);
  }

  /**
   * Converts to the internal representation of the value.
   * <ul>
   *   <li>BOOLEAN -> int</li>
   *   <li>TIMESTAMP -> long</li>
   *   <li>BOOLEAN_ARRAY -> int[]</li>
   *   <li>TIMESTAMP_ARRAY -> long[]</li>
   * </ul>
   */
  public Object toInternal(Object value) {
    return value;
  }

  public boolean isSingleValue() {
    return this.ordinal() <= OBJECT.ordinal();
  }

  public PinotDataType getSingleValueType() {
    switch (this) {
      case BYTE_ARRAY:
        return BYTE;
      case CHARACTER_ARRAY:
        return CHARACTER;
      case SHORT_ARRAY:
        return SHORT;
      case PRIMITIVE_INT_ARRAY:
      case INTEGER_ARRAY:
        return INTEGER;
      case PRIMITIVE_LONG_ARRAY:
      case LONG_ARRAY:
        return LONG;
      case PRIMITIVE_FLOAT_ARRAY:
      case FLOAT_ARRAY:
        return FLOAT;
      case PRIMITIVE_DOUBLE_ARRAY:
      case DOUBLE_ARRAY:
        return DOUBLE;
      case STRING_ARRAY:
        return STRING;
      case BYTES_ARRAY:
        return BYTES;
      case OBJECT_ARRAY:
      case COLLECTION:
        return OBJECT;
      case BOOLEAN_ARRAY:
        return BOOLEAN;
      case TIMESTAMP_ARRAY:
        return TIMESTAMP;
      case TIMESTAMP_NTZ_ARRAY:
        return TIMESTAMP_NTZ;
      case DATE_ARRAY:
        return DATE;
      case TIME_ARRAY:
        return TIME;
      default:
        throw new IllegalStateException("There is no single-value type for " + this);
    }
  }

  public static PinotDataType getSingleValueType(Class<?> cls) {
    if (cls == Integer.class) {
      return INTEGER;
    }
    if (cls == Long.class) {
      return LONG;
    }
    if (cls == Float.class) {
      return FLOAT;
    }
    if (cls == Double.class) {
      return DOUBLE;
    }
    if (cls == BigDecimal.class) {
      return BIG_DECIMAL;
    }
    if (cls == String.class) {
      return STRING;
    }
    if (cls == byte[].class) {
      return BYTES;
    }
    if (cls == Boolean.class) {
      return BOOLEAN;
    }
    if (cls == Timestamp.class) {
      return TIMESTAMP;
    }
    if (cls == LocalDateTime.class) {
      return TIMESTAMP_NTZ;
    }
    if (cls == LocalDate.class) {
      return DATE;
    }
    if (cls == LocalTime.class) {
      return TIME;
    }
    if (cls == Byte.class) {
      return BYTE;
    }
    if (cls == Character.class) {
      return CHARACTER;
    }
    if (cls == Short.class) {
      return SHORT;
    }
    if (cls != null && Map.class.isAssignableFrom(cls)) {
      return MAP;
    }
    return OBJECT;
  }

  public static PinotDataType getMultiValueType(Class<?> cls) {
    if (cls == Integer.class) {
      return INTEGER_ARRAY;
    }
    if (cls == Long.class) {
      return LONG_ARRAY;
    }
    if (cls == Float.class) {
      return FLOAT_ARRAY;
    }
    if (cls == Double.class) {
      return DOUBLE_ARRAY;
    }
    if (cls == String.class) {
      return STRING_ARRAY;
    }
    if (cls == Byte.class) {
      return BYTE_ARRAY;
    }
    if (cls == Character.class) {
      return CHARACTER_ARRAY;
    }
    if (cls == Short.class) {
      return SHORT_ARRAY;
    }
    if (cls == byte[].class) {
      return BYTES_ARRAY;
    }
    if (cls == Boolean.class) {
      return BOOLEAN_ARRAY;
    }
    if (cls == Timestamp.class) {
      return TIMESTAMP_ARRAY;
    }
    return OBJECT_ARRAY;
  }

  private static int anyToInt(Object val) {
    return getSingleValueType(val.getClass()).toInt(val);
  }

  private static long anyToLong(Object val) {
    return getSingleValueType(val.getClass()).toLong(val);
  }

  private static float anyToFloat(Object val) {
    return getSingleValueType(val.getClass()).toFloat(val);
  }

  private static double anyToDouble(Object val) {
    return getSingleValueType(val.getClass()).toDouble(val);
  }

  /**
   * Returns the {@link PinotDataType} for the given {@link FieldSpec} for data ingestion purpose. Returns object array
   * type for multi-valued types.
   */
  public static PinotDataType getPinotDataTypeForIngestion(FieldSpec fieldSpec) {
    DataType dataType = fieldSpec.getDataType();
    switch (dataType) {
      case INT:
        return fieldSpec.isSingleValueField() ? INTEGER : INTEGER_ARRAY;
      case LONG:
        return fieldSpec.isSingleValueField() ? LONG : LONG_ARRAY;
      case FLOAT:
        return fieldSpec.isSingleValueField() ? FLOAT : FLOAT_ARRAY;
      case DOUBLE:
        return fieldSpec.isSingleValueField() ? DOUBLE : DOUBLE_ARRAY;
      case BIG_DECIMAL:
        if (fieldSpec.isSingleValueField()) {
          return BIG_DECIMAL;
        }
        throw new IllegalStateException("There is no multi-value type for BigDecimal");
      case BOOLEAN:
        return fieldSpec.isSingleValueField() ? BOOLEAN : BOOLEAN_ARRAY;
      case TIMESTAMP:
        return fieldSpec.isSingleValueField() ? TIMESTAMP : TIMESTAMP_ARRAY;
      case TIMESTAMP_NTZ:
        return fieldSpec.isSingleValueField() ? TIMESTAMP_NTZ : TIMESTAMP_NTZ_ARRAY;
      case DATE:
        return fieldSpec.isSingleValueField() ? DATE : DATE_ARRAY;
      case TIME:
        return fieldSpec.isSingleValueField() ? TIME : TIME_ARRAY;
      case JSON:
        if (fieldSpec.isSingleValueField()) {
          return JSON;
        }
        throw new IllegalStateException("There is no multi-value type for JSON");
      case STRING:
        return fieldSpec.isSingleValueField() ? STRING : STRING_ARRAY;
      case BYTES:
        return fieldSpec.isSingleValueField() ? BYTES : BYTES_ARRAY;
      case MAP:
        if (fieldSpec.isSingleValueField()) {
          return MAP;
        }
        throw new IllegalStateException("There is no multi-value type for MAP");
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type: " + dataType + " in field: " + fieldSpec.getName());
    }
  }

  /**
   * Returns the {@link PinotDataType} for the given {@link ColumnDataType} for query execution purpose. Returns
   * primitive array type for multi-valued types.
   */
  public static PinotDataType getPinotDataTypeForExecution(ColumnDataType columnDataType) {
    switch (columnDataType) {
      case INT:
        return INTEGER;
      case LONG:
        return LONG;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case BIG_DECIMAL:
        return BIG_DECIMAL;
      case BOOLEAN:
        return BOOLEAN;
      case TIMESTAMP:
        return TIMESTAMP;
      case TIMESTAMP_NTZ:
        return TIMESTAMP_NTZ;
      case DATE:
        return DATE;
      case TIME:
        return TIME;
      case STRING:
        return STRING;
      case JSON:
        return JSON;
      case BYTES:
        return BYTES;
      case OBJECT:
        return OBJECT;
      case INT_ARRAY:
        return PRIMITIVE_INT_ARRAY;
      case LONG_ARRAY:
        return PRIMITIVE_LONG_ARRAY;
      case FLOAT_ARRAY:
        return PRIMITIVE_FLOAT_ARRAY;
      case DOUBLE_ARRAY:
        return PRIMITIVE_DOUBLE_ARRAY;
      case STRING_ARRAY:
        return STRING_ARRAY;
      default:
        throw new IllegalStateException("Cannot convert ColumnDataType: " + columnDataType + " to PinotDataType");
    }
  }
}
