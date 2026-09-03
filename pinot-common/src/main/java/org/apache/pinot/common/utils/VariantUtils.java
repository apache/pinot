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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;


/**
 * Query-side operations for Pinot {@code VARIANT} values.
 *
 * <p>The utility navigates the Parquet Variant binary representation directly. It never materializes a JSON tree.
 * Instances are not required, and stateless convenience methods are thread-safe. Overloads that accept a
 * caller-provided {@link ReusableResult} require that result to be thread-confined and not shared by concurrent calls.
 * An empty byte array is Pinot's SQL-null placeholder and is never decoded as an envelope.
 */
public final class VariantUtils {
  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final BigDecimal MIN_INT_DECIMAL = BigDecimal.valueOf(Integer.MIN_VALUE);
  private static final BigDecimal MAX_INT_DECIMAL = BigDecimal.valueOf(Integer.MAX_VALUE);
  private static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);
  private static final int MAX_JSON_NESTING_DEPTH = 100;
  private static final int MAX_VARIANT_DECIMAL_PRECISION = 38;
  private static final int MAX_VARIANT_DECIMAL_SCALE = 38;
  private static final int MAX_VARIANT_DECIMAL_BYTES = 16;
  private static final long MICROS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
  private static final long NANOS_PER_MICRO = TimeUnit.MICROSECONDS.toNanos(1);
  private static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);
  private static final int VARIANT_BASIC_TYPE_MASK = 0x03;
  private static final int VARIANT_PRIMITIVE_TYPE_MASK = 0x3F;
  private static final int VARIANT_PRIMITIVE = 0;
  private static final int VARIANT_SHORT_STRING = 1;
  private static final int VARIANT_OBJECT = 2;
  private static final int VARIANT_ARRAY = 3;
  private static final int VARIANT_NULL = 0;
  private static final int VARIANT_TRUE = 1;
  private static final int VARIANT_FALSE = 2;
  private static final int VARIANT_INT8 = 3;
  private static final int VARIANT_INT16 = 4;
  private static final int VARIANT_INT32 = 5;
  private static final int VARIANT_INT64 = 6;
  private static final int VARIANT_DOUBLE = 7;
  private static final int VARIANT_DECIMAL4 = 8;
  private static final int VARIANT_DECIMAL8 = 9;
  private static final int VARIANT_DECIMAL16 = 10;
  private static final int VARIANT_DATE = 11;
  private static final int VARIANT_TIMESTAMP_TZ = 12;
  private static final int VARIANT_TIMESTAMP_NTZ = 13;
  private static final int VARIANT_FLOAT = 14;
  private static final int VARIANT_BINARY = 15;
  private static final int VARIANT_LONG_STRING = 16;
  private static final int VARIANT_TIME = 17;
  private static final int VARIANT_TIMESTAMP_NANOS_TZ = 18;
  private static final int VARIANT_TIMESTAMP_NANOS_NTZ = 19;
  private static final int VARIANT_UUID = 20;
  private static final int VARIANT_METADATA_VERSION_MASK = 0x0F;
  private static final int VARIANT_METADATA_VERSION = 1;
  private static final VariantPath ROOT_PATH = new VariantPath(new PathElement[0]);

  private VariantUtils() {
  }

  /**
   * Statically supported result types for {@code variantGet} and {@code tryVariantGet}.
   */
  public enum ResultType {
    BOOLEAN(DataType.BOOLEAN, SqlTypeName.BOOLEAN),
    INT(DataType.INT, SqlTypeName.INTEGER),
    LONG(DataType.LONG, SqlTypeName.BIGINT),
    FLOAT(DataType.FLOAT, SqlTypeName.REAL),
    DOUBLE(DataType.DOUBLE, SqlTypeName.DOUBLE),
    BIG_DECIMAL(DataType.BIG_DECIMAL, SqlTypeName.DECIMAL),
    STRING(DataType.STRING, SqlTypeName.VARCHAR),
    BYTES(DataType.BYTES, SqlTypeName.VARBINARY),
    UUID(DataType.UUID, SqlTypeName.UUID),
    TIMESTAMP(DataType.TIMESTAMP, SqlTypeName.TIMESTAMP),
    VARIANT(DataType.VARIANT, SqlTypeName.VARIANT),
    JSON(DataType.JSON, SqlTypeName.VARCHAR);

    private final DataType _dataType;
    private final SqlTypeName _sqlTypeName;

    ResultType(DataType dataType, SqlTypeName sqlTypeName) {
      _dataType = dataType;
      _sqlTypeName = sqlTypeName;
    }

    public DataType getDataType() {
      return _dataType;
    }

    public SqlTypeName getSqlTypeName() {
      return _sqlTypeName;
    }
  }

  /**
   * An immutable, pre-parsed Variant path. The v1 grammar supports {@code $}, dot-separated object fields, and
   * non-negative array subscripts.
   */
  public static final class VariantPath {
    private final PathElement[] _elements;

    private VariantPath(PathElement[] elements) {
      _elements = elements;
    }
  }

  /**
   * Reusable, unboxed destination for vectorized Variant extraction.
   *
   * <p>Only the getter corresponding to the requested {@link ResultType} is defined after a successful extraction.
   * The instance is mutable and not thread-safe; callers should retain one per transform-function instance.
   */
  public static final class ReusableResult {
    private final Cursor _cursor = new Cursor();
    private int _intValue;
    private long _longValue;
    private float _floatValue;
    private double _doubleValue;
    private BigDecimal _bigDecimalValue;
    private String _stringValue;
    private byte[] _bytesValue;

    public int getIntValue() {
      return _intValue;
    }

    public long getLongValue() {
      return _longValue;
    }

    public float getFloatValue() {
      return _floatValue;
    }

    public double getDoubleValue() {
      return _doubleValue;
    }

    public BigDecimal getBigDecimalValue() {
      return _bigDecimalValue;
    }

    public String getStringValue() {
      return _stringValue;
    }

    /**
     * Returns the extracted BYTES, VARIANT, or direct 16-byte UUID representation.
     */
    public byte[] getBytesValue() {
      return _bytesValue;
    }

    public UUID getUuidValue() {
      return UuidUtils.toUUID(_bytesValue);
    }

    /**
     * Materializes the extracted value in the external representation used by scalar functions and ingestion.
     */
    public Object getExternalValue(ResultType resultType) {
      switch (resultType) {
        case BOOLEAN:
          return _intValue != 0;
        case INT:
          return _intValue;
        case LONG:
          return _longValue;
        case FLOAT:
          return _floatValue;
        case DOUBLE:
          return _doubleValue;
        case BIG_DECIMAL:
          return _bigDecimalValue;
        case STRING:
        case JSON:
          return _stringValue;
        case BYTES:
        case VARIANT:
          return _bytesValue;
        case UUID:
          return UuidUtils.toUUID(_bytesValue);
        case TIMESTAMP:
          return new Timestamp(_longValue);
        default:
          throw new IllegalStateException("Unhandled Variant target type: " + resultType);
      }
    }

    /**
     * Materializes the extracted value in {@link DataSchema}'s internal representation.
     *
     * <p>TIMESTAMP remains epoch milliseconds and UUID wraps the directly copied 16-byte value, avoiding an
     * external-object round trip in the multi-stage engine.
     */
    public Object getInternalValue(ResultType resultType) {
      switch (resultType) {
        case BOOLEAN:
          return _intValue;
        case INT:
          return _intValue;
        case LONG:
        case TIMESTAMP:
          return _longValue;
        case FLOAT:
          return _floatValue;
        case DOUBLE:
          return _doubleValue;
        case BIG_DECIMAL:
          return _bigDecimalValue;
        case STRING:
        case JSON:
          return _stringValue;
        case BYTES:
        case UUID:
        case VARIANT:
          return new ByteArray(_bytesValue);
        default:
          throw new IllegalStateException("Unhandled Variant target type: " + resultType);
      }
    }
  }

  /**
   * Parses a target type literal once for reuse by a transform function.
   */
  public static ResultType parseResultType(String targetType) {
    if (targetType == null) {
      throw new IllegalArgumentException("Variant target type must not be null");
    }
    try {
      return ResultType.valueOf(targetType.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported Variant target type: " + targetType, e);
    }
  }

  /**
   * Compiles a v1 Variant path.
   */
  public static VariantPath compilePath(String path) {
    if (path == null || path.isEmpty() || path.charAt(0) != '$') {
      throw new IllegalArgumentException("Variant path must start with '$': " + path);
    }
    List<PathElement> elements = new ArrayList<>();
    int index = 1;
    while (index < path.length()) {
      char current = path.charAt(index);
      if (current == '.') {
        int fieldStart = ++index;
        while (index < path.length()) {
          char next = path.charAt(index);
          if (next == '.' || next == '[') {
            break;
          }
          index++;
        }
        if (fieldStart == index) {
          throw new IllegalArgumentException("Variant path contains an empty field: " + path);
        }
        elements.add(PathElement.forField(path.substring(fieldStart, index)));
      } else if (current == '[') {
        int subscriptStart = ++index;
        while (index < path.length() && Character.isDigit(path.charAt(index))) {
          index++;
        }
        if (subscriptStart == index || index >= path.length() || path.charAt(index) != ']') {
          throw new IllegalArgumentException("Invalid Variant array subscript in path: " + path);
        }
        try {
          elements.add(PathElement.forIndex(Integer.parseInt(path.substring(subscriptStart, index))));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Variant array subscript is too large in path: " + path, e);
        }
        index++;
      } else {
        throw new IllegalArgumentException("Unexpected character at offset " + index + " in Variant path: " + path);
      }
    }
    return new VariantPath(elements.toArray(new PathElement[0]));
  }

  /**
   * Extracts a Variant value. A missing path or SQL null returns Java null; a Variant null remains an encoded Variant
   * value.
   */
  @Nullable
  public static byte[] variantGet(@Nullable byte[] envelope, String path) {
    return (byte[]) variantGet(envelope, compilePath(path), ResultType.VARIANT);
  }

  /**
   * Strictly extracts and converts a value. A missing path or SQL null returns Java null. A Variant null remains
   * encoded when the target type is {@link ResultType#VARIANT}, and returns Java null for other target types. An
   * incompatible non-null value throws.
   */
  @Nullable
  public static Object variantGet(@Nullable byte[] envelope, String path, String targetType) {
    return variantGet(envelope, compilePath(path), parseResultType(targetType));
  }

  /**
   * Strictly extracts using pre-parsed path and type values.
   */
  @Nullable
  public static Object variantGet(@Nullable byte[] envelope, VariantPath path, ResultType targetType) {
    ReusableResult result = new ReusableResult();
    return extractInto(envelope, path, targetType, result) ? result.getExternalValue(targetType) : null;
  }

  /**
   * Strictly extracts into a reusable, unboxed result.
   *
   * @return {@code false} for SQL null, a missing path, or Variant null converted to a non-Variant target
   */
  public static boolean extractInto(@Nullable byte[] envelope, VariantPath path, ResultType targetType,
      ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return false;
    }
    Objects.requireNonNull(path, "path must not be null");
    Objects.requireNonNull(targetType, "targetType must not be null");
    Cursor cursor = result._cursor;
    if (!cursor.navigate(envelope, path)) {
      return false;
    }
    if (cursor.getType() == Variant.Type.NULL && targetType != ResultType.VARIANT) {
      return false;
    }
    convert(cursor, targetType, result);
    return true;
  }

  /**
   * Tolerant Variant extraction. Malformed input returns Java null.
   */
  @Nullable
  public static byte[] tryVariantGet(@Nullable byte[] envelope, String path) {
    return (byte[]) tryVariantGet(envelope, compilePath(path), ResultType.VARIANT);
  }

  /**
   * Tolerant typed extraction. Malformed input and incompatible types return Java null.
   */
  @Nullable
  public static Object tryVariantGet(@Nullable byte[] envelope, String path, String targetType) {
    try {
      return tryVariantGet(envelope, compilePath(path), parseResultType(targetType));
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Tolerant extraction using pre-parsed path and type values.
   */
  @Nullable
  public static Object tryVariantGet(@Nullable byte[] envelope, VariantPath path, ResultType targetType) {
    try {
      ReusableResult result = new ReusableResult();
      return tryExtractInto(envelope, path, targetType, result) ? result.getExternalValue(targetType) : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Tolerantly extracts into a reusable, unboxed result.
   *
   * @return {@code false} for SQL null, missing paths, Variant null converted to a non-Variant target, malformed input,
   *     or an incompatible conversion
   */
  public static boolean tryExtractInto(@Nullable byte[] envelope, VariantPath path, ResultType targetType,
      ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return false;
    }
    Objects.requireNonNull(path, "path must not be null");
    Objects.requireNonNull(targetType, "targetType must not be null");
    Cursor cursor = result._cursor;
    try {
      if (!cursor.navigate(envelope, path)) {
        return false;
      }
      if (cursor.getType() == Variant.Type.NULL && targetType != ResultType.VARIANT) {
        return false;
      }
      return tryConvert(cursor, targetType, result);
    } catch (IllegalArgumentException | IllegalStateException | UnsupportedOperationException
        | IndexOutOfBoundsException e) {
      // Cursor operations use these exceptions only for malformed or unsupported Variant encodings.
      return false;
    }
  }

  /**
   * Returns whether the path is present. A present Variant null counts as present.
   */
  @Nullable
  public static Boolean variantExists(@Nullable byte[] envelope, String path) {
    return variantExists(envelope, compilePath(path));
  }

  /**
   * Returns whether a compiled path is present. A present Variant null counts as present.
   */
  @Nullable
  public static Boolean variantExists(@Nullable byte[] envelope, VariantPath path) {
    return variantExists(envelope, path, new ReusableResult());
  }

  /**
   * Allocation-free compiled-path form of {@link #variantExists(byte[], VariantPath)} when the caller retains the
   * supplied result between rows.
   */
  @Nullable
  public static Boolean variantExists(@Nullable byte[] envelope, VariantPath path, ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return null;
    }
    return result._cursor.navigate(envelope, Objects.requireNonNull(path, "path must not be null"));
  }

  /**
   * Returns whether the root value is a Variant null. SQL null is not a Variant null.
   */
  public static boolean isVariantNull(@Nullable byte[] envelope) {
    return isVariantNull(envelope, ROOT_PATH, new ReusableResult());
  }

  /**
   * Returns whether a present value at the path is a Variant null. SQL null and missing paths return false.
   */
  public static boolean isVariantNull(@Nullable byte[] envelope, String path) {
    return isVariantNull(envelope, compilePath(path));
  }

  /**
   * Returns whether a present value at a compiled path is a Variant null. SQL null and missing paths return false.
   */
  public static boolean isVariantNull(@Nullable byte[] envelope, VariantPath path) {
    return isVariantNull(envelope, path, new ReusableResult());
  }

  /**
   * Allocation-free compiled-path form of {@link #isVariantNull(byte[], VariantPath)} when the caller retains the
   * supplied result between rows.
   */
  public static boolean isVariantNull(@Nullable byte[] envelope, VariantPath path, ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return false;
    }
    Cursor cursor = result._cursor;
    return cursor.navigate(envelope, Objects.requireNonNull(path, "path must not be null"))
        && cursor.getType() == Variant.Type.NULL;
  }

  /**
   * Returns the Variant type name at the root, or Java null for SQL null.
   */
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope) {
    return variantTypeOf(envelope, ROOT_PATH, new ReusableResult());
  }

  /**
   * Returns the Variant type name at a path, or Java null for SQL null or a missing path.
   */
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope, String path) {
    return variantTypeOf(envelope, compilePath(path));
  }

  /**
   * Returns the Variant type name at a compiled path, or Java null for SQL null or a missing path.
   */
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope, VariantPath path) {
    return variantTypeOf(envelope, path, new ReusableResult());
  }

  /**
   * Allocation-free compiled-path form of {@link #variantTypeOf(byte[], VariantPath)} when the caller retains the
   * supplied result between rows.
   */
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope, VariantPath path, ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return null;
    }
    Cursor cursor = result._cursor;
    return cursor.navigate(envelope, Objects.requireNonNull(path, "path must not be null"))
        ? typeName(cursor.getType()) : null;
  }

  /**
   * Renders the Variant value as canonical JSON text without constructing a JSON tree.
   */
  @Nullable
  public static String variantToJson(@Nullable byte[] envelope) {
    if (isSqlNull(envelope)) {
      return null;
    }
    ReusableResult result = new ReusableResult();
    Cursor cursor = result._cursor;
    cursor.navigate(envelope, ROOT_PATH);
    return variantToJson(cursor.asVariant());
  }

  /**
   * Parses JSON text into a Pinot Variant envelope without constructing a JSON tree.
   */
  @Nullable
  public static byte[] parseJsonToVariant(@Nullable String json) {
    if (json == null) {
      return null;
    }
    try (JsonParser parser = JSON_FACTORY.createParser(json)) {
      JsonToken token = parser.nextToken();
      if (token == null) {
        throw new IllegalArgumentException("Cannot parse empty text as Variant");
      }
      VariantBuilder builder = new VariantBuilder();
      appendJsonValue(parser, token, builder, 0);
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("Unexpected trailing token after Variant JSON value");
      }
      Variant variant = builder.build();
      return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
    } catch (IOException | RuntimeException e) {
      throw new IllegalArgumentException("Cannot parse JSON as Variant", e);
    }
  }

  /**
   * Tolerant JSON parser. Malformed or unsupported input returns Java null.
   */
  @Nullable
  public static byte[] tryParseJsonToVariant(@Nullable String json) {
    try {
      return parseJsonToVariant(json);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static boolean isSqlNull(@Nullable byte[] envelope) {
    return envelope == null || envelope.length == 0;
  }

  private static void convert(Cursor value, ResultType targetType, ReusableResult result) {
    switch (targetType) {
      case BOOLEAN:
        requireType(value, Variant.Type.BOOLEAN, targetType);
        result._intValue = value.getBoolean() ? 1 : 0;
        break;
      case INT:
        result._intValue = toInt(value, targetType);
        break;
      case LONG:
        result._longValue = toLong(value, targetType);
        break;
      case FLOAT:
        result._floatValue = toFloat(value, targetType);
        break;
      case DOUBLE:
        result._doubleValue = toDouble(value, targetType);
        break;
      case BIG_DECIMAL:
        result._bigDecimalValue = toBigDecimal(value, targetType);
        break;
      case STRING:
        requireType(value, Variant.Type.STRING, targetType);
        result._stringValue = value.getString();
        break;
      case BYTES:
        requireType(value, Variant.Type.BINARY, targetType);
        result._bytesValue = value.getBinary();
        break;
      case UUID:
        requireType(value, Variant.Type.UUID, targetType);
        result._bytesValue = value.getUuidBytes();
        break;
      case TIMESTAMP:
        result._longValue = toTimestampMillis(value, targetType);
        break;
      case VARIANT:
        result._bytesValue = value.copyEnvelope();
        break;
      case JSON:
        result._stringValue = variantToJson(value.asVariant());
        break;
      default:
        throw new IllegalStateException("Unhandled Variant target type: " + targetType);
    }
  }

  private static boolean tryConvert(Cursor value, ResultType targetType, ReusableResult result) {
    Variant.Type valueType = value.getType();
    switch (targetType) {
      case BOOLEAN:
        if (valueType != Variant.Type.BOOLEAN) {
          return false;
        }
        result._intValue = value.getBoolean() ? 1 : 0;
        return true;
      case INT:
        return tryConvertToInt(value, valueType, result);
      case LONG:
        return tryConvertToLong(value, valueType, result);
      case FLOAT:
        switch (valueType) {
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            result._floatValue = value.getInteger();
            return true;
          case FLOAT:
            result._floatValue = value.getFloat();
            return true;
          case DOUBLE:
            result._floatValue = (float) value.getDouble();
            return true;
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            result._floatValue = value.getDecimal().floatValue();
            return true;
          default:
            return false;
        }
      case DOUBLE:
        switch (valueType) {
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            result._doubleValue = value.getInteger();
            return true;
          case FLOAT:
            result._doubleValue = value.getFloat();
            return true;
          case DOUBLE:
            result._doubleValue = value.getDouble();
            return true;
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            result._doubleValue = value.getDecimal().doubleValue();
            return true;
          default:
            return false;
        }
      case BIG_DECIMAL:
        switch (valueType) {
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            result._bigDecimalValue = BigDecimal.valueOf(value.getInteger());
            return true;
          case FLOAT:
            float floatValue = value.getFloat();
            if (!Float.isFinite(floatValue)) {
              return false;
            }
            result._bigDecimalValue = BigDecimal.valueOf(floatValue);
            return true;
          case DOUBLE:
            double doubleValue = value.getDouble();
            if (!Double.isFinite(doubleValue)) {
              return false;
            }
            result._bigDecimalValue = BigDecimal.valueOf(doubleValue);
            return true;
          case DECIMAL4:
          case DECIMAL8:
          case DECIMAL16:
            result._bigDecimalValue = value.getDecimal();
            return true;
          default:
            return false;
        }
      case STRING:
        if (valueType != Variant.Type.STRING) {
          return false;
        }
        result._stringValue = value.getString();
        return true;
      case BYTES:
        if (valueType != Variant.Type.BINARY) {
          return false;
        }
        result._bytesValue = value.getBinary();
        return true;
      case UUID:
        if (valueType != Variant.Type.UUID) {
          return false;
        }
        result._bytesValue = value.getUuidBytes();
        return true;
      case TIMESTAMP:
        switch (valueType) {
          case DATE:
            result._longValue = value.getInteger() * TimeUnit.DAYS.toMillis(1);
            return true;
          case TIMESTAMP_TZ:
          case TIMESTAMP_NTZ:
            result._longValue = Math.floorDiv(value.getInteger(), TimeUnit.MILLISECONDS.toMicros(1));
            return true;
          case TIMESTAMP_NANOS_TZ:
          case TIMESTAMP_NANOS_NTZ:
            result._longValue = Math.floorDiv(value.getInteger(), TimeUnit.MILLISECONDS.toNanos(1));
            return true;
          default:
            return false;
        }
      case VARIANT:
        result._bytesValue = value.copyEnvelope();
        return true;
      case JSON:
        result._stringValue = variantToJson(value.asVariant());
        return true;
      default:
        throw new AssertionError("Unhandled Variant target type: " + targetType);
    }
  }

  private static boolean tryConvertToInt(Cursor value, Variant.Type valueType, ReusableResult result) {
    switch (valueType) {
      case BYTE:
      case SHORT:
      case INT:
        result._intValue = (int) value.getInteger();
        return true;
      case LONG:
        long longValue = value.getInteger();
        if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
          return false;
        }
        result._intValue = (int) longValue;
        return true;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        BigDecimal decimalValue = value.getDecimal();
        if (!isIntegralInRange(decimalValue, MIN_INT_DECIMAL, MAX_INT_DECIMAL)) {
          return false;
        }
        result._intValue = decimalValue.intValue();
        return true;
      default:
        return false;
    }
  }

  private static boolean tryConvertToLong(Cursor value, Variant.Type valueType, ReusableResult result) {
    switch (valueType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        result._longValue = value.getInteger();
        return true;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        BigDecimal decimalValue = value.getDecimal();
        if (!isIntegralInRange(decimalValue, MIN_LONG_DECIMAL, MAX_LONG_DECIMAL)) {
          return false;
        }
        result._longValue = decimalValue.longValue();
        return true;
      default:
        return false;
    }
  }

  private static boolean isIntegralInRange(BigDecimal value, BigDecimal minimum, BigDecimal maximum) {
    return value.compareTo(minimum) >= 0 && value.compareTo(maximum) <= 0
        && (value.scale() <= 0 || value.stripTrailingZeros().scale() <= 0);
  }

  private static int toInt(Cursor value, ResultType targetType) {
    switch (value.getType()) {
      case BYTE:
      case SHORT:
      case INT:
        return (int) value.getInteger();
      case LONG:
        return Math.toIntExact(value.getInteger());
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return value.getDecimal().intValueExact();
      default:
        throw typeMismatch(value, targetType);
    }
  }

  private static long toLong(Cursor value, ResultType targetType) {
    switch (value.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return value.getInteger();
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return value.getDecimal().longValueExact();
      default:
        throw typeMismatch(value, targetType);
    }
  }

  private static float toFloat(Cursor value, ResultType targetType) {
    switch (value.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return value.getInteger();
      case FLOAT:
        return value.getFloat();
      case DOUBLE:
        return (float) value.getDouble();
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return value.getDecimal().floatValue();
      default:
        throw typeMismatch(value, targetType);
    }
  }

  private static double toDouble(Cursor value, ResultType targetType) {
    switch (value.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return value.getInteger();
      case FLOAT:
        return value.getFloat();
      case DOUBLE:
        return value.getDouble();
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return value.getDecimal().doubleValue();
      default:
        throw typeMismatch(value, targetType);
    }
  }

  private static BigDecimal toBigDecimal(Cursor value, ResultType targetType) {
    switch (value.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return BigDecimal.valueOf(value.getInteger());
      case FLOAT:
        return BigDecimal.valueOf(value.getFloat());
      case DOUBLE:
        return BigDecimal.valueOf(value.getDouble());
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return value.getDecimal();
      default:
        throw typeMismatch(value, targetType);
    }
  }

  private static long toTimestampMillis(Cursor value, ResultType targetType) {
    switch (value.getType()) {
      case DATE:
        return Math.multiplyExact(value.getInteger(), TimeUnit.DAYS.toMillis(1));
      case TIMESTAMP_TZ:
      case TIMESTAMP_NTZ:
        return Math.floorDiv(value.getInteger(), TimeUnit.MILLISECONDS.toMicros(1));
      case TIMESTAMP_NANOS_TZ:
      case TIMESTAMP_NANOS_NTZ:
        return Math.floorDiv(value.getInteger(), TimeUnit.MILLISECONDS.toNanos(1));
      default:
        throw typeMismatch(value, targetType);
    }
  }

  private static void requireType(Cursor value, Variant.Type expected, ResultType targetType) {
    if (value.getType() != expected) {
      throw typeMismatch(value, targetType);
    }
  }

  private static IllegalArgumentException typeMismatch(Cursor value, ResultType targetType) {
    return new IllegalArgumentException(
        "Cannot convert Variant " + typeName(value.getType()) + " to " + targetType.name());
  }

  private static String typeName(Variant.Type type) {
    switch (type) {
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return "DECIMAL";
      default:
        return type.name();
    }
  }

  private static String variantToJson(Variant variant) {
    try {
      StringWriter writer = new StringWriter();
      try (JsonGenerator generator = JSON_FACTORY.createGenerator(writer)) {
        writeJsonValue(generator, variant);
      }
      return writer.toString();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot render Variant as JSON", e);
    }
  }

  private static void writeJsonValue(JsonGenerator generator, Variant variant)
      throws IOException {
    switch (variant.getType()) {
      case OBJECT:
        generator.writeStartObject();
        for (int i = 0; i < variant.numObjectElements(); i++) {
          Variant.ObjectField field = variant.getFieldAtIndex(i);
          generator.writeFieldName(field.key);
          writeJsonValue(generator, field.value);
        }
        generator.writeEndObject();
        break;
      case ARRAY:
        generator.writeStartArray();
        for (int i = 0; i < variant.numArrayElements(); i++) {
          writeJsonValue(generator, variant.getElementAtIndex(i));
        }
        generator.writeEndArray();
        break;
      case NULL:
        generator.writeNull();
        break;
      case BOOLEAN:
        generator.writeBoolean(variant.getBoolean());
        break;
      case BYTE:
        generator.writeNumber(variant.getByte());
        break;
      case SHORT:
        generator.writeNumber(variant.getShort());
        break;
      case INT:
        generator.writeNumber(variant.getInt());
        break;
      case LONG:
        generator.writeNumber(variant.getLong());
        break;
      case FLOAT:
        generator.writeNumber(variant.getFloat());
        break;
      case DOUBLE:
        generator.writeNumber(variant.getDouble());
        break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        generator.writeNumber(variant.getDecimal());
        break;
      case STRING:
        generator.writeString(variant.getString());
        break;
      case BINARY:
        generator.writeBinary(toBytes(variant.getBinary()));
        break;
      case UUID:
        generator.writeString(variant.getUUID().toString());
        break;
      case DATE:
        generator.writeString(LocalDate.ofEpochDay(variant.getInt()).toString());
        break;
      case TIMESTAMP_TZ:
        generator.writeString(instantFromMicros(variant.getLong()).toString());
        break;
      case TIMESTAMP_NTZ:
        generator.writeString(LocalDateTime.ofInstant(instantFromMicros(variant.getLong()), ZoneOffset.UTC).toString());
        break;
      case TIMESTAMP_NANOS_TZ:
        generator.writeString(instantFromNanos(variant.getLong()).toString());
        break;
      case TIMESTAMP_NANOS_NTZ:
        generator.writeString(LocalDateTime.ofInstant(instantFromNanos(variant.getLong()), ZoneOffset.UTC).toString());
        break;
      case TIME:
        generator.writeString(LocalTime.ofNanoOfDay(Math.floorMod(variant.getLong() * NANOS_PER_MICRO, NANOS_PER_DAY))
            .toString());
        break;
      default:
        throw new IllegalStateException("Unsupported Variant type: " + variant.getType());
    }
  }

  private static Instant instantFromMicros(long micros) {
    long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
    long nanos = Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICRO;
    return Instant.ofEpochSecond(seconds, nanos);
  }

  private static Instant instantFromNanos(long nanos) {
    return Instant.ofEpochSecond(Math.floorDiv(nanos, TimeUnit.SECONDS.toNanos(1)),
        Math.floorMod(nanos, TimeUnit.SECONDS.toNanos(1)));
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    ByteBuffer view = buffer.slice();
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return bytes;
  }

  private static void appendJsonValue(JsonParser parser, JsonToken token, VariantBuilder builder, int depth)
      throws IOException {
    if (depth > MAX_JSON_NESTING_DEPTH) {
      throw new IllegalArgumentException("Variant JSON exceeds maximum nesting depth " + MAX_JSON_NESTING_DEPTH);
    }
    switch (token) {
      case START_OBJECT:
        VariantObjectBuilder objectBuilder = builder.startObject();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          if (parser.currentToken() != JsonToken.FIELD_NAME) {
            throw new IllegalArgumentException("Expected a JSON object field name");
          }
          objectBuilder.appendKey(parser.currentName());
          JsonToken fieldValue = parser.nextToken();
          if (fieldValue == null) {
            throw new IllegalArgumentException("Unexpected end of JSON object");
          }
          appendJsonValue(parser, fieldValue, objectBuilder, depth + 1);
        }
        builder.endObject();
        break;
      case START_ARRAY:
        VariantArrayBuilder arrayBuilder = builder.startArray();
        while (true) {
          JsonToken element = parser.nextToken();
          if (element == JsonToken.END_ARRAY) {
            break;
          }
          if (element == null) {
            throw new IllegalArgumentException("Unexpected end of JSON array");
          }
          appendJsonValue(parser, element, arrayBuilder, depth + 1);
        }
        builder.endArray();
        break;
      case VALUE_NULL:
        builder.appendNull();
        break;
      case VALUE_TRUE:
        builder.appendBoolean(true);
        break;
      case VALUE_FALSE:
        builder.appendBoolean(false);
        break;
      case VALUE_STRING:
        builder.appendString(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        appendInteger(parser, builder);
        break;
      case VALUE_NUMBER_FLOAT:
        appendDecimal(parser.getDecimalValue(), builder);
        break;
      default:
        throw new IllegalArgumentException("Unsupported JSON token for Variant: " + token);
    }
  }

  private static void appendInteger(JsonParser parser, VariantBuilder builder)
      throws IOException {
    switch (parser.getNumberType()) {
      case INT:
        builder.appendInt(parser.getIntValue());
        break;
      case LONG:
        builder.appendLong(parser.getLongValue());
        break;
      case BIG_INTEGER:
        appendBigInteger(parser.getBigIntegerValue(), builder);
        break;
      default:
        throw new IllegalArgumentException("Unsupported JSON integer representation: " + parser.getNumberType());
    }
  }

  private static void appendBigInteger(BigInteger value, VariantBuilder builder) {
    if (value.bitLength() < Integer.SIZE) {
      builder.appendInt(value.intValue());
    } else if (value.bitLength() < Long.SIZE) {
      builder.appendLong(value.longValue());
    } else {
      appendDecimal(new BigDecimal(value), builder);
    }
  }

  private static void appendDecimal(BigDecimal value, VariantBuilder builder) {
    BigDecimal normalized = value;
    if (normalized.scale() < 0) {
      // Parquet Variant stores scale as an unsigned byte. Expand exponent notation exactly instead of allowing a
      // negative scale to wrap during encoding.
      long expandedPrecision = (long) normalized.precision() - normalized.scale();
      if (normalized.signum() != 0 && expandedPrecision > MAX_VARIANT_DECIMAL_PRECISION) {
        throw unsupportedVariantDecimal(value);
      }
      normalized = normalized.signum() == 0 ? BigDecimal.ZERO : normalized.setScale(0);
    } else if (normalized.scale() > MAX_VARIANT_DECIMAL_SCALE) {
      // Accept values whose excessive lexical scale consists only of insignificant trailing zeros.
      normalized = normalized.stripTrailingZeros();
      if (normalized.scale() < 0) {
        normalized = normalized.setScale(0);
      }
    }
    byte[] unscaledBytes = normalized.unscaledValue().toByteArray();
    if (normalized.scale() > MAX_VARIANT_DECIMAL_SCALE
        || normalized.precision() > MAX_VARIANT_DECIMAL_PRECISION
        || unscaledBytes.length > MAX_VARIANT_DECIMAL_BYTES) {
      throw unsupportedVariantDecimal(value);
    }
    builder.appendDecimal(normalized);
  }

  private static IllegalArgumentException unsupportedVariantDecimal(BigDecimal value) {
    return new IllegalArgumentException(
        "JSON decimal exceeds Parquet Variant decimal(38) encoding: precision=" + value.precision()
            + ", scale=" + value.scale());
  }

  /**
   * Mutable zero-copy view over one selected value in a Pinot envelope.
   *
   * <p>The constants and layouts used here mirror Parquet Variant encoding version 1. Keeping this cursor on
   * {@link ReusableResult} avoids allocating envelope views, Variant wrappers, and navigation wrappers for every row.
   */
  private static final class Cursor {
    private byte[] _envelope;
    private int _metadataOffset;
    private int _metadataLength;
    private boolean _metadataParsed;
    private int _metadataOffsetSize;
    private int _metadataDictSize;
    private int _metadataOffsetListOffset;
    private int _metadataDataOffset;
    private int _metadataDataLength;
    private int _selectedOffset;
    private int _selectedLength;

    private boolean navigate(byte[] envelope, VariantPath path) {
      reset(envelope);
      for (PathElement element : path._elements) {
        if (element._field != null) {
          if (getType() != Variant.Type.OBJECT || !selectObjectField(element._fieldUtf8)) {
            return false;
          }
        } else if (getType() != Variant.Type.ARRAY || !selectArrayElement(element._index)) {
          return false;
        }
      }
      return true;
    }

    private void reset(byte[] envelope) {
      int metadataLength = VariantEnvelope.validateAndGetMetadataLength(envelope);
      int valueLength = envelope.length - VariantEnvelope.HEADER_SIZE - metadataLength;

      _envelope = envelope;
      _metadataOffset = VariantEnvelope.HEADER_SIZE;
      _metadataLength = metadataLength;
      requireRange(_metadataOffset, 1, _metadataOffset + _metadataLength, "Variant metadata");
      int metadataVersion = _envelope[_metadataOffset] & VARIANT_METADATA_VERSION_MASK;
      if (metadataVersion != VARIANT_METADATA_VERSION) {
        throw new UnsupportedOperationException("Unsupported variant metadata version: " + metadataVersion);
      }
      _metadataParsed = false;
      _selectedOffset = VariantEnvelope.HEADER_SIZE + metadataLength;
      _selectedLength = valueLength;
    }

    private Variant.Type getType() {
      int header = getHeader();
      int basicType = header & VARIANT_BASIC_TYPE_MASK;
      int typeInfo = (header >>> 2) & VARIANT_PRIMITIVE_TYPE_MASK;
      switch (basicType) {
        case VARIANT_SHORT_STRING:
          return Variant.Type.STRING;
        case VARIANT_OBJECT:
          return Variant.Type.OBJECT;
        case VARIANT_ARRAY:
          return Variant.Type.ARRAY;
        case VARIANT_PRIMITIVE:
          switch (typeInfo) {
            case VARIANT_NULL:
              return Variant.Type.NULL;
            case VARIANT_TRUE:
            case VARIANT_FALSE:
              return Variant.Type.BOOLEAN;
            case VARIANT_INT8:
              return Variant.Type.BYTE;
            case VARIANT_INT16:
              return Variant.Type.SHORT;
            case VARIANT_INT32:
              return Variant.Type.INT;
            case VARIANT_INT64:
              return Variant.Type.LONG;
            case VARIANT_DOUBLE:
              return Variant.Type.DOUBLE;
            case VARIANT_DECIMAL4:
              return Variant.Type.DECIMAL4;
            case VARIANT_DECIMAL8:
              return Variant.Type.DECIMAL8;
            case VARIANT_DECIMAL16:
              return Variant.Type.DECIMAL16;
            case VARIANT_DATE:
              return Variant.Type.DATE;
            case VARIANT_TIMESTAMP_TZ:
              return Variant.Type.TIMESTAMP_TZ;
            case VARIANT_TIMESTAMP_NTZ:
              return Variant.Type.TIMESTAMP_NTZ;
            case VARIANT_FLOAT:
              return Variant.Type.FLOAT;
            case VARIANT_BINARY:
              return Variant.Type.BINARY;
            case VARIANT_LONG_STRING:
              return Variant.Type.STRING;
            case VARIANT_TIME:
              return Variant.Type.TIME;
            case VARIANT_TIMESTAMP_NANOS_TZ:
              return Variant.Type.TIMESTAMP_NANOS_TZ;
            case VARIANT_TIMESTAMP_NANOS_NTZ:
              return Variant.Type.TIMESTAMP_NANOS_NTZ;
            case VARIANT_UUID:
              return Variant.Type.UUID;
            default:
              throw new UnsupportedOperationException("Unknown type in Variant. primitive type: " + typeInfo);
          }
        default:
          throw new IllegalStateException("Unhandled Variant basic type: " + basicType);
      }
    }

    private boolean getBoolean() {
      int typeInfo = getPrimitiveTypeInfo();
      if (typeInfo != VARIANT_TRUE && typeInfo != VARIANT_FALSE) {
        throw new IllegalArgumentException("Cannot read non-boolean Variant value as BOOLEAN");
      }
      return typeInfo == VARIANT_TRUE;
    }

    private long getInteger() {
      int typeInfo = getPrimitiveTypeInfo();
      switch (typeInfo) {
        case VARIANT_INT8:
          return readSignedLittleEndian(_selectedOffset + 1, 1);
        case VARIANT_INT16:
          return readSignedLittleEndian(_selectedOffset + 1, 2);
        case VARIANT_INT32:
        case VARIANT_DATE:
          return readSignedLittleEndian(_selectedOffset + 1, Integer.BYTES);
        case VARIANT_INT64:
        case VARIANT_TIMESTAMP_TZ:
        case VARIANT_TIMESTAMP_NTZ:
        case VARIANT_TIME:
        case VARIANT_TIMESTAMP_NANOS_TZ:
        case VARIANT_TIMESTAMP_NANOS_NTZ:
          return readSignedLittleEndian(_selectedOffset + 1, Long.BYTES);
        default:
          throw new IllegalArgumentException("Cannot read non-integer Variant value as an integer");
      }
    }

    private float getFloat() {
      if (getPrimitiveTypeInfo() != VARIANT_FLOAT) {
        throw new IllegalArgumentException("Cannot read non-float Variant value as FLOAT");
      }
      return Float.intBitsToFloat((int) readSignedLittleEndian(_selectedOffset + 1, Float.BYTES));
    }

    private double getDouble() {
      if (getPrimitiveTypeInfo() != VARIANT_DOUBLE) {
        throw new IllegalArgumentException("Cannot read non-double Variant value as DOUBLE");
      }
      return Double.longBitsToDouble(readSignedLittleEndian(_selectedOffset + 1, Double.BYTES));
    }

    private BigDecimal getDecimal() {
      int typeInfo = getPrimitiveTypeInfo();
      requireSelectedRange(1, 1);
      int scale = Byte.toUnsignedInt(_envelope[_selectedOffset + 1]);
      switch (typeInfo) {
        case VARIANT_DECIMAL4:
          return BigDecimal.valueOf(readSignedLittleEndian(_selectedOffset + 2, Integer.BYTES), scale);
        case VARIANT_DECIMAL8:
          return BigDecimal.valueOf(readSignedLittleEndian(_selectedOffset + 2, Long.BYTES), scale);
        case VARIANT_DECIMAL16:
          requireSelectedRange(2, 16);
          byte[] unscaled = new byte[16];
          for (int i = 0; i < unscaled.length; i++) {
            unscaled[i] = _envelope[_selectedOffset + 17 - i];
          }
          return new BigDecimal(new BigInteger(unscaled), scale);
        default:
          throw new IllegalArgumentException("Cannot read non-decimal Variant value as DECIMAL");
      }
    }

    private String getString() {
      int header = getHeader();
      int basicType = header & VARIANT_BASIC_TYPE_MASK;
      int typeInfo = (header >>> 2) & VARIANT_PRIMITIVE_TYPE_MASK;
      int contentOffset;
      int length;
      if (basicType == VARIANT_SHORT_STRING) {
        contentOffset = 1;
        length = typeInfo;
      } else if (basicType == VARIANT_PRIMITIVE && typeInfo == VARIANT_LONG_STRING) {
        contentOffset = 1 + Integer.BYTES;
        length = readUnsignedLittleEndian(_selectedOffset + 1, Integer.BYTES, selectedLimit());
      } else {
        throw new IllegalArgumentException("Cannot read non-string Variant value as STRING");
      }
      requireSelectedRange(contentOffset, length);
      return new String(_envelope, _selectedOffset + contentOffset, length, StandardCharsets.UTF_8);
    }

    private byte[] getBinary() {
      if (getPrimitiveTypeInfo() != VARIANT_BINARY) {
        throw new IllegalArgumentException("Cannot read non-binary Variant value as BINARY");
      }
      int length = readUnsignedLittleEndian(_selectedOffset + 1, Integer.BYTES, selectedLimit());
      int contentOffset = 1 + Integer.BYTES;
      requireSelectedRange(contentOffset, length);
      byte[] bytes = new byte[length];
      System.arraycopy(_envelope, _selectedOffset + contentOffset, bytes, 0, length);
      return bytes;
    }

    private byte[] getUuidBytes() {
      if (getPrimitiveTypeInfo() != VARIANT_UUID) {
        throw new IllegalArgumentException("Cannot read non-UUID Variant value as UUID");
      }
      requireSelectedRange(1, UuidUtils.UUID_NUM_BYTES);
      byte[] bytes = new byte[UuidUtils.UUID_NUM_BYTES];
      System.arraycopy(_envelope, _selectedOffset + 1, bytes, 0, UuidUtils.UUID_NUM_BYTES);
      return bytes;
    }

    private byte[] copyEnvelope() {
      return VariantEnvelope.encode(_envelope, _metadataOffset, _metadataLength, _envelope, _selectedOffset,
          _selectedLength);
    }

    private Variant asVariant() {
      return new Variant(_envelope, _selectedOffset, _selectedLength, _envelope, _metadataOffset, _metadataLength);
    }

    private boolean selectObjectField(byte[] fieldUtf8) {
      int header = getHeader();
      int typeInfo = (header >>> 2) & VARIANT_PRIMITIVE_TYPE_MASK;
      int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
      int numElements = readUnsignedLittleEndian(_selectedOffset + 1, sizeBytes, selectedLimit());
      int idSize = ((typeInfo >>> 2) & 3) + 1;
      int offsetSize = (typeInfo & 3) + 1;
      int idStart = checkedPosition((long) _selectedOffset + 1 + sizeBytes, selectedLimit(), "Variant object ids");
      int offsetStart = checkedPosition((long) idStart + (long) numElements * idSize, selectedLimit(),
          "Variant object offsets");
      int dataStart = checkedPosition((long) offsetStart + ((long) numElements + 1) * offsetSize, selectedLimit(),
          "Variant object data");
      int finalOffsetPosition = checkedPosition((long) offsetStart + (long) numElements * offsetSize,
          selectedLimit(), "Variant object final offset");
      int totalDataLength = readUnsignedLittleEndian(finalOffsetPosition, offsetSize, selectedLimit());
      requireRange(dataStart, totalDataLength, selectedLimit(), "Variant object data");

      for (int i = 0; i < numElements; i++) {
        int id = readUnsignedLittleEndian(idStart + i * idSize, idSize, offsetStart);
        if (metadataKeyEquals(id, fieldUtf8)) {
          int offset = readUnsignedLittleEndian(offsetStart + i * offsetSize, offsetSize, dataStart);
          int nextOffset = readUnsignedLittleEndian(offsetStart + (i + 1) * offsetSize, offsetSize, dataStart);
          if (offset > nextOffset || nextOffset > totalDataLength) {
            throw new IllegalStateException(
                "Invalid Variant object offsets: " + offset + ", " + nextOffset + ", total=" + totalDataLength);
          }
          _selectedOffset = dataStart + offset;
          _selectedLength = nextOffset - offset;
          return true;
        }
      }
      return false;
    }

    private boolean selectArrayElement(int index) {
      int header = getHeader();
      int typeInfo = (header >>> 2) & VARIANT_PRIMITIVE_TYPE_MASK;
      int sizeBytes = ((typeInfo >>> 2) & 1) == 0 ? 1 : Integer.BYTES;
      int numElements = readUnsignedLittleEndian(_selectedOffset + 1, sizeBytes, selectedLimit());
      int offsetSize = (typeInfo & 3) + 1;
      int offsetStart = checkedPosition((long) _selectedOffset + 1 + sizeBytes, selectedLimit(),
          "Variant array offsets");
      int dataStart = checkedPosition((long) offsetStart + ((long) numElements + 1) * offsetSize, selectedLimit(),
          "Variant array data");
      int finalOffsetPosition = checkedPosition((long) offsetStart + (long) numElements * offsetSize,
          selectedLimit(), "Variant array final offset");
      int totalDataLength = readUnsignedLittleEndian(finalOffsetPosition, offsetSize, selectedLimit());
      requireRange(dataStart, totalDataLength, selectedLimit(), "Variant array data");
      if (index >= numElements) {
        return false;
      }
      int offset = readUnsignedLittleEndian(offsetStart + index * offsetSize, offsetSize, dataStart);
      int nextOffset = readUnsignedLittleEndian(offsetStart + (index + 1) * offsetSize, offsetSize, dataStart);
      if (offset > nextOffset || nextOffset > totalDataLength) {
        throw new IllegalStateException(
            "Invalid Variant array offsets: " + offset + ", " + nextOffset + ", total=" + totalDataLength);
      }
      _selectedOffset = dataStart + offset;
      _selectedLength = nextOffset - offset;
      return true;
    }

    private boolean metadataKeyEquals(int id, byte[] expected) {
      ensureMetadataParsed();
      if (id < 0 || id >= _metadataDictSize) {
        throw new IllegalArgumentException(
            "Invalid dictionary id: " + id + ". dictionary size: " + _metadataDictSize);
      }
      int offset = readUnsignedLittleEndian(_metadataOffsetListOffset + id * _metadataOffsetSize,
          _metadataOffsetSize, _metadataDataOffset);
      int nextOffset = readUnsignedLittleEndian(_metadataOffsetListOffset + (id + 1) * _metadataOffsetSize,
          _metadataOffsetSize, _metadataDataOffset);
      if (offset > nextOffset || nextOffset > _metadataDataLength) {
        throw new IllegalStateException(
            "Invalid Variant metadata offsets: " + offset + ", " + nextOffset + ", total=" + _metadataDataLength);
      }
      int length = nextOffset - offset;
      if (length != expected.length) {
        return false;
      }
      int start = _metadataDataOffset + offset;
      for (int i = 0; i < length; i++) {
        if (_envelope[start + i] != expected[i]) {
          return false;
        }
      }
      return true;
    }

    private void ensureMetadataParsed() {
      if (_metadataParsed) {
        return;
      }
      int metadataLimit = _metadataOffset + _metadataLength;
      int header = Byte.toUnsignedInt(_envelope[_metadataOffset]);
      int offsetSize = ((header >>> 6) & 3) + 1;
      int dictSize = readUnsignedLittleEndian(_metadataOffset + 1, offsetSize, metadataLimit);
      int offsetListOffset = checkedPosition((long) _metadataOffset + 1 + offsetSize, metadataLimit,
          "Variant metadata offsets");
      int dataOffset = checkedPosition((long) offsetListOffset + ((long) dictSize + 1) * offsetSize, metadataLimit,
          "Variant metadata data");
      int finalOffsetPosition = checkedPosition((long) offsetListOffset + (long) dictSize * offsetSize, metadataLimit,
          "Variant metadata final offset");
      int dataLength = readUnsignedLittleEndian(finalOffsetPosition, offsetSize, metadataLimit);
      requireRange(dataOffset, dataLength, metadataLimit, "Variant metadata data");
      _metadataOffsetSize = offsetSize;
      _metadataDictSize = dictSize;
      _metadataOffsetListOffset = offsetListOffset;
      _metadataDataOffset = dataOffset;
      _metadataDataLength = dataLength;
      _metadataParsed = true;
    }

    private int getHeader() {
      requireSelectedRange(0, 1);
      return Byte.toUnsignedInt(_envelope[_selectedOffset]);
    }

    private int getPrimitiveTypeInfo() {
      int header = getHeader();
      if ((header & VARIANT_BASIC_TYPE_MASK) != VARIANT_PRIMITIVE) {
        throw new IllegalArgumentException("Variant value is not a primitive");
      }
      return (header >>> 2) & VARIANT_PRIMITIVE_TYPE_MASK;
    }

    private long readSignedLittleEndian(int offset, int numBytes) {
      requireRange(offset, numBytes, selectedLimit(), "Variant value");
      long value = 0;
      for (int i = 0; i < numBytes - 1; i++) {
        value |= (long) Byte.toUnsignedInt(_envelope[offset + i]) << (Byte.SIZE * i);
      }
      return value | (long) _envelope[offset + numBytes - 1] << (Byte.SIZE * (numBytes - 1));
    }

    private int readUnsignedLittleEndian(int offset, int numBytes, int limit) {
      return VariantUtils.readUnsignedLittleEndian(_envelope, offset, numBytes, limit);
    }

    private void requireSelectedRange(int relativeOffset, int length) {
      if (relativeOffset < 0 || (long) relativeOffset + length > _selectedLength) {
        throw new IllegalArgumentException(
            "Invalid Variant value range: offset=" + relativeOffset + ", length=" + length + ", valueLength="
                + _selectedLength);
      }
    }

    private int selectedLimit() {
      return _selectedOffset + _selectedLength;
    }
  }

  private static int readUnsignedLittleEndian(byte[] bytes, int offset, int numBytes, int limit) {
    if (numBytes < 1 || numBytes > Integer.BYTES) {
      throw new IllegalArgumentException("Invalid unsigned integer width: " + numBytes);
    }
    requireRange(offset, numBytes, limit, "Variant unsigned integer");
    long value = 0;
    for (int i = 0; i < numBytes; i++) {
      value |= (long) Byte.toUnsignedInt(bytes[offset + i]) << (Byte.SIZE * i);
    }
    if (value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Variant unsigned integer exceeds supported Java range: " + value);
    }
    return (int) value;
  }

  private static int checkedPosition(long position, int limit, String description) {
    if (position < 0 || position > limit) {
      throw new IllegalArgumentException(description + " exceeds encoded value bounds");
    }
    return (int) position;
  }

  private static void requireRange(int offset, int length, int limit, String description) {
    if (offset < 0 || length < 0 || (long) offset + length > limit) {
      throw new IllegalArgumentException(
          description + " exceeds encoded bounds: offset=" + offset + ", length=" + length + ", limit=" + limit);
    }
  }

  private static final class PathElement {
    private final String _field;
    private final byte[] _fieldUtf8;
    private final int _index;

    private PathElement(String field, byte[] fieldUtf8, int index) {
      _field = field;
      _fieldUtf8 = fieldUtf8;
      _index = index;
    }

    private static PathElement forField(String field) {
      return new PathElement(field, field.getBytes(StandardCharsets.UTF_8), -1);
    }

    private static PathElement forIndex(int index) {
      return new PathElement(null, null, index);
    }
  }
}
