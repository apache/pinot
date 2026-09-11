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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;


/// Query-side operations for Pinot {@code VARIANT} values.
///
/// <p>The utility navigates the Parquet Variant binary representation directly. It never materializes a JSON tree.
/// Instances are not required, and stateless convenience methods are thread-safe. Overloads that accept a
/// caller-provided {@link ReusableResult} require that result to be thread-confined and not shared by concurrent calls.
/// An empty byte array is Pinot's SQL-null placeholder and is never decoded as an envelope.
///
/// <p>The cursor's encoding constants deliberately mirror parquet-variant's package-private {@code VariantUtil}
/// constants. {@code VariantUtilsTest} checks those constants and every {@code Variant.Type} whenever the pinned
/// parquet dependency changes. Metadata versions and primitive tags unknown to this cursor fail closed; support for a
/// new parquet-variant encoding must be added here in the same change that upgrades the codec.
public final class VariantUtils {
  public static final String RAW_VARIANT_REQUIRES_NULL_HANDLING_ERROR =
      "Raw VARIANT projection requires query null handling to be enabled; set enableNullHandling=true";

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final int MAX_VARIANT_DECIMAL_SCALE_BYTE = (1 << Byte.SIZE) - 1;
  private static final int DECIMAL_POWER_LIMBS = 10;
  private static final int DECIMAL_CONVERSION_LIMBS = 14;
  private static final long[][] DECIMAL_POWERS_OF_FIVE = decimalPowersOfFive();
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
  private static final int OBJECT_BINARY_SEARCH_THRESHOLD = 32;
  private static final int INVALID_UTF8_COMPARISON = Integer.MIN_VALUE;
  private static final VariantPath ROOT_PATH = new VariantPath(new PathElement[0]);

  private VariantUtils() {
  }

  private static long[][] decimalPowersOfFive() {
    long[][] powers = new long[MAX_VARIANT_DECIMAL_SCALE_BYTE + 1][DECIMAL_POWER_LIMBS];
    powers[0][0] = 1;
    for (int scale = 1; scale < powers.length; scale++) {
      long carry = 0;
      for (int limb = 0; limb < DECIMAL_POWER_LIMBS; limb++) {
        long value = powers[scale - 1][limb];
        long lowProduct = (value & 0xFFFF_FFFFL) * 5 + carry;
        long highProduct = (value >>> Integer.SIZE) * 5 + (lowProduct >>> Integer.SIZE);
        powers[scale][limb] = highProduct << Integer.SIZE | lowProduct & 0xFFFF_FFFFL;
        carry = highProduct >>> Integer.SIZE;
      }
    }
    return powers;
  }

  /// Returns whether a final result schema contains raw VARIANT values. Callers that serialize raw VARIANT results
  /// must also require query null handling because Pinot's reserved empty-byte SQL-null placeholder cannot otherwise
  /// be distinguished from a logical Variant value.
  public static boolean containsRawVariantResult(DataSchema resultSchema) {
    for (ColumnDataType dataType : resultSchema.getColumnDataTypes()) {
      if (dataType == ColumnDataType.VARIANT) {
        return true;
      }
    }
    return false;
  }

  /// Statically supported result types for {@code variantGet} and {@code tryVariantGet}.
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

  /// An immutable, pre-parsed Variant path. The v1 grammar supports {@code $}, dot-separated object fields, and
  /// non-negative array subscripts.
  public static final class VariantPath {
    private final PathElement[] _elements;

    private VariantPath(PathElement[] elements) {
      _elements = elements;
    }
  }

  /// Reusable, unboxed destination for vectorized Variant extraction.
  ///
  /// <p>Only the getter corresponding to the requested {@link ResultType} is defined after a successful extraction.
  /// The instance is mutable and not thread-safe; callers should retain one per transform-function instance. Every
  /// extraction may replace its state. Each successful byte-valued extraction installs a newly materialized array.
  /// Values returned as {@code byte[]} or as a {@link ByteArray} may be retained after this result is reused, but they
  /// are read-only by contract and must be copied before mutation.
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

    /// Returns the extracted BYTES, VARIANT, or direct 16-byte UUID representation.
    ///
    /// <p>The returned array is replaced, but not mutated, by the next byte-valued extraction. It may be retained after
    /// this result is reused, but must be treated as immutable and copied before mutation.
    public byte[] getBytesValue() {
      return _bytesValue;
    }

    public UUID getUuidValue() {
      return UuidUtils.toUUID(_bytesValue);
    }

    /// Materializes the extracted value in the external representation used by scalar functions and ingestion.
    ///
    /// <p>For BYTES and VARIANT, the returned {@code byte[]} may be retained after this result is reused. It must be
    /// treated as immutable and copied before mutation.
    public Object toExternalValue(ResultType resultType) {
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

    /// Materializes the extracted value in {@link DataSchema}'s internal representation.
    ///
    /// <p>TIMESTAMP remains epoch milliseconds and UUID wraps the directly copied 16-byte value, avoiding an
    /// external-object round trip in the multi-stage engine. For BYTES, UUID, and VARIANT, the returned
    /// {@link ByteArray} wraps a newly materialized array that may be retained after this result is reused. Neither the
    /// wrapper nor its array may be mutated; callers must copy the array before mutation.
    public Object toInternalValue(ResultType resultType) {
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

  /// Parses a target type literal once for reuse by a transform function.
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

  /// Compiles a v1 Variant path.
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

  /// Extracts a Variant value. A missing path or SQL null returns Java null; a Variant null remains an encoded Variant
  /// value.
  @Nullable
  public static byte[] variantGet(@Nullable byte[] envelope, String path) {
    return (byte[]) variantGet(envelope, compilePath(path), ResultType.VARIANT);
  }

  /// Strictly extracts and converts a value. A missing path or SQL null returns Java null. A Variant null remains
  /// encoded when the target type is {@link ResultType#VARIANT}, and returns Java null for other target types. An
  /// incompatible non-null value throws.
  @Nullable
  public static Object variantGet(@Nullable byte[] envelope, String path, String targetType) {
    return variantGet(envelope, compilePath(path), parseResultType(targetType));
  }

  /// Strictly extracts using pre-parsed path and type values.
  @Nullable
  public static Object variantGet(@Nullable byte[] envelope, VariantPath path, ResultType targetType) {
    ReusableResult result = new ReusableResult();
    return extractInto(envelope, path, targetType, result) ? result.toExternalValue(targetType) : null;
  }

  /// Strictly extracts into a reusable, unboxed result.
  ///
  /// @return {@code false} for SQL null, a missing path, or Variant null converted to a non-Variant target
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

  /// Tolerant Variant extraction. Malformed input returns Java null.
  @Nullable
  public static byte[] tryVariantGet(@Nullable byte[] envelope, String path) {
    return (byte[]) tryVariantGet(envelope, compilePath(path), ResultType.VARIANT);
  }

  /// Tolerant typed extraction. Malformed input and incompatible types return Java null.
  @Nullable
  public static Object tryVariantGet(@Nullable byte[] envelope, String path, String targetType) {
    try {
      return tryVariantGet(envelope, compilePath(path), parseResultType(targetType));
    } catch (RuntimeException e) {
      return null;
    }
  }

  /// Tolerant extraction using pre-parsed path and type values.
  @Nullable
  public static Object tryVariantGet(@Nullable byte[] envelope, VariantPath path, ResultType targetType) {
    try {
      ReusableResult result = new ReusableResult();
      return tryExtractInto(envelope, path, targetType, result) ? result.toExternalValue(targetType) : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /// Tolerantly extracts into a reusable, unboxed result.
  ///
  /// @return {@code false} for SQL null, missing paths, Variant null converted to a non-Variant target,
  ///     malformed input, or an incompatible conversion
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

  /// Returns whether the path is present. A present Variant null counts as present.
  @Nullable
  public static Boolean variantExists(@Nullable byte[] envelope, String path) {
    return variantExists(envelope, compilePath(path));
  }

  /// Returns whether a compiled path is present. A present Variant null counts as present.
  @Nullable
  public static Boolean variantExists(@Nullable byte[] envelope, VariantPath path) {
    return variantExists(envelope, path, new ReusableResult());
  }

  /// Allocation-free compiled-path form of {@link #variantExists(byte[], VariantPath)} when the caller retains the
  /// supplied result between rows.
  @Nullable
  public static Boolean variantExists(@Nullable byte[] envelope, VariantPath path, ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return null;
    }
    return result._cursor.navigate(envelope, Objects.requireNonNull(path, "path must not be null"));
  }

  /// Returns whether the root value is a Variant null. SQL null is not a Variant null.
  public static boolean isVariantNull(@Nullable byte[] envelope) {
    return isVariantNull(envelope, ROOT_PATH, new ReusableResult());
  }

  /// Returns whether a present value at the path is a Variant null. SQL null and missing paths return false.
  public static boolean isVariantNull(@Nullable byte[] envelope, String path) {
    return isVariantNull(envelope, compilePath(path));
  }

  /// Returns whether a present value at a compiled path is a Variant null. SQL null and missing paths return false.
  public static boolean isVariantNull(@Nullable byte[] envelope, VariantPath path) {
    return isVariantNull(envelope, path, new ReusableResult());
  }

  /// Allocation-free compiled-path form of {@link #isVariantNull(byte[], VariantPath)} when the caller retains the
  /// supplied result between rows.
  public static boolean isVariantNull(@Nullable byte[] envelope, VariantPath path, ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return false;
    }
    Cursor cursor = result._cursor;
    return cursor.navigate(envelope, Objects.requireNonNull(path, "path must not be null"))
        && cursor.getType() == Variant.Type.NULL;
  }

  /// Returns the Variant type name at the root, or Java null for SQL null.
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope) {
    return variantTypeOf(envelope, ROOT_PATH, new ReusableResult());
  }

  /// Returns the Variant type name at a path, or Java null for SQL null or a missing path.
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope, String path) {
    return variantTypeOf(envelope, compilePath(path));
  }

  /// Returns the Variant type name at a compiled path, or Java null for SQL null or a missing path.
  @Nullable
  public static String variantTypeOf(@Nullable byte[] envelope, VariantPath path) {
    return variantTypeOf(envelope, path, new ReusableResult());
  }

  /// Allocation-free compiled-path form of {@link #variantTypeOf(byte[], VariantPath)} when the caller retains the
  /// supplied result between rows.
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

  /// Renders the Variant value as canonical JSON text without constructing a JSON tree.
  @Nullable
  public static String variantToJson(@Nullable byte[] envelope) {
    return variantToJson(envelope, new ReusableResult());
  }

  /// Allocation-reduced form of [#variantToJson(byte[])] when the caller retains the supplied result between rows.
  @Nullable
  public static String variantToJson(@Nullable byte[] envelope, ReusableResult result) {
    Objects.requireNonNull(result, "result must not be null");
    if (isSqlNull(envelope)) {
      return null;
    }
    Cursor cursor = result._cursor;
    cursor.navigate(envelope, ROOT_PATH);
    return variantToJson(cursor.asVariant());
  }

  /// Parses JSON text into a Pinot Variant envelope without constructing a JSON tree.
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

  /// Tolerant JSON parser. Malformed or unsupported input returns Java null.
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
            result._floatValue = value.getDecimalAsFloat();
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
            result._doubleValue = value.getDecimalAsDouble();
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
            // DATE is int32 in Variant v1, but keep this path exact and aligned with strict conversion if that
            // representation is ever widened.
            try {
              result._longValue = Math.multiplyExact(value.getInteger(), TimeUnit.DAYS.toMillis(1));
              return true;
            } catch (ArithmeticException e) {
              return false;
            }
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
        if (!value.tryGetDecimalAsLongExact()) {
          return false;
        }
        long decimalIntValue = value.getConvertedDecimalLong();
        if (decimalIntValue < Integer.MIN_VALUE || decimalIntValue > Integer.MAX_VALUE) {
          return false;
        }
        result._intValue = (int) decimalIntValue;
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
        if (!value.tryGetDecimalAsLongExact()) {
          return false;
        }
        result._longValue = value.getConvertedDecimalLong();
        return true;
      default:
        return false;
    }
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
        return Math.toIntExact(value.getDecimalAsLongExact());
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
        return value.getDecimalAsLongExact();
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
        return value.getDecimalAsFloat();
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
        return value.getDecimalAsDouble();
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
      // StringBuilder-backed writer: StringWriter wraps a synchronized StringBuffer and would pay a monitor
      // acquisition per append on this per-row rendering path.
      StringBuilderWriter writer = new StringBuilderWriter();
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

  /// Mutable zero-copy view over one selected value in a Pinot envelope.
  ///
  /// <p>The constants and layouts used here mirror Parquet Variant encoding version 1. Keeping this cursor on
  /// {@link ReusableResult} avoids allocating envelope views, Variant wrappers, and navigation wrappers for every row.
  private static final class Cursor {
    /// Memo slot marker: the path key has not been resolved against any metadata dictionary yet.
    private static final int MEMO_UNKNOWN = -1;
    /// Lazily computed per-row comparison of this row's metadata region against the previous navigated row's.
    private static final int METADATA_CMP_UNKNOWN = 0;
    private static final int METADATA_CMP_MATCH = 1;
    private static final int METADATA_CMP_MISMATCH = 2;

    /// Memo slot marker: no object-entry index hint has been recorded for the path element.
    private static final int HINT_UNKNOWN = -1;
    /// Memo slot marker: the path key was absent from the previous row's entire metadata dictionary.
    private static final int MEMO_ABSENT_FROM_DICTIONARY = -2;
    /// Objects at or below this entry count resolve by direct linear scan; the navigation memo cannot beat it.
    private static final int SMALL_OBJECT_LINEAR_THRESHOLD = 4;
    /// Comparing an anchored metadata dictionary costs one pass over every metadata byte. When the dictionary is much
    /// larger than the object being searched, repeating the direct object scan is cheaper than validating a cached
    /// absent-from-dictionary verdict. This conservative budget retains the memo for the wide-object benchmark shapes
    /// while bypassing it for small objects backed by large shared dictionaries.
    private static final int ABSENT_MEMO_METADATA_BYTES_PER_OBJECT_ENTRY_BUDGET = 16;


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
    private long _convertedDecimalLong;
    private final long[] _decimalDivisor = new long[DECIMAL_CONVERSION_LIMBS];
    private final long[] _decimalRemainder = new long[DECIMAL_CONVERSION_LIMBS];
    private boolean _decimalNegative;
    private long _decimalMagnitudeHigh;
    private long _decimalMagnitudeLow;

    // Cross-row navigation memo. Rows in a segment overwhelmingly share one metadata dictionary and one object
    // layout, so the dictionary id and object-entry index resolved for each path element on one row almost always
    // resolve the next row in O(1). The memo is self-validating per row: a hint is only trusted after re-checking,
    // on the current row's bytes, that the hinted entry's id still spells the path key
    // ([#selectObjectField(PathElement, int)]), so stale hints degrade to the ordinary search instead of
    // corrupting results. Only the absent-from-dictionary marker relies on cross-row state; it is anchored to the
    // exact envelope it was proven under and reused only when the current row's metadata region is byte-identical
    // to that anchored region ([#absentFromDictionary(int)]).
    private VariantPath _memoPath;
    private int[] _memoIds;
    private int[] _memoHints;
    // Per-element anchor for MEMO_ABSENT_FROM_DICTIONARY: the envelope (and its metadata region) under which the
    // absence was proven. Only a row whose metadata is byte-identical to the anchored region may reuse the verdict.
    private byte[][] _memoAbsentEnvelopes;
    private int[] _memoAbsentMetadataOffsets;
    private int[] _memoAbsentMetadataLengths;
    // Previous navigated row, used ONLY as a heuristic gate: the dictionary-classification investment on a miss
    // (a full dictionary scan) is made only when the metadata repeated across consecutive rows, so heterogeneous
    // per-row dictionaries keep the pre-memo miss cost. Never used to validate a verdict.
    private byte[] _previousRowEnvelope;
    private int _previousRowMetadataOffset;
    private int _previousRowMetadataLength;
    private int _metadataCmpState;

    private boolean navigate(byte[] envelope, VariantPath path) {
      reset(envelope);
      PathElement[] elements = path._elements;
      if (path != _memoPath) {
        _memoPath = path;
        _memoIds = new int[elements.length];
        _memoHints = new int[elements.length];
        _memoAbsentEnvelopes = new byte[elements.length][];
        _memoAbsentMetadataOffsets = new int[elements.length];
        _memoAbsentMetadataLengths = new int[elements.length];
        Arrays.fill(_memoIds, MEMO_UNKNOWN);
      }
      for (int i = 0; i < elements.length; i++) {
        PathElement element = elements[i];
        if (element._field != null) {
          if (getType() != Variant.Type.OBJECT || !selectObjectField(element, i)) {
            return false;
          }
        } else if (getType() != Variant.Type.ARRAY || !selectArrayElement(element._index)) {
          return false;
        }
      }
      return true;
    }

    /// Heuristic investment gate: returns whether this row's metadata region is byte-identical to the previous
    /// navigated row's, computed at most once per row. A stale or missed comparison only skips an optimization; it
    /// can never validate a verdict.
    private boolean metadataRepeatedAcrossRows() {
      if (_metadataCmpState == METADATA_CMP_UNKNOWN) {
        byte[] previous = _previousRowEnvelope;
        _metadataCmpState = previous != null && _previousRowMetadataLength == _metadataLength
            && (previous == _envelope || Arrays.equals(previous, _previousRowMetadataOffset,
                _previousRowMetadataOffset + _previousRowMetadataLength,
                _envelope, _metadataOffset, _metadataOffset + _metadataLength))
            ? METADATA_CMP_MATCH : METADATA_CMP_MISMATCH;
      }
      return _metadataCmpState == METADATA_CMP_MATCH;
    }

    /// Returns whether the path element's absent-from-dictionary verdict applies to this row: the current metadata
    /// region must be byte-identical to the region the absence was proven under. Rows that bypass the memo (small
    /// objects, non-object levels, failed navigations) can never invalidate this anchor because it is compared
    /// directly, not against the previous navigated row.
    private boolean absentFromDictionary(int elementIndex) {
      byte[] anchor = _memoAbsentEnvelopes[elementIndex];
      if (anchor == null) {
        return false;
      }
      int anchorOffset = _memoAbsentMetadataOffsets[elementIndex];
      int anchorLength = _memoAbsentMetadataLengths[elementIndex];
      return anchorLength == _metadataLength && (anchor == _envelope
          || Arrays.equals(anchor, anchorOffset, anchorOffset + anchorLength,
              _envelope, _metadataOffset, _metadataOffset + _metadataLength));
    }

    private void reset(byte[] envelope) {
      // Capture the outgoing row for the metadata-stability gate before its fields are overwritten. Envelopes are
      // read-only by contract, so retaining the reference is safe and copy-free.
      _previousRowEnvelope = _envelope;
      _previousRowMetadataOffset = _metadataOffset;
      _previousRowMetadataLength = _metadataLength;
      _metadataCmpState = METADATA_CMP_UNKNOWN;

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
              throw unsupportedPrimitiveType(typeInfo);
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

    /// Converts a decimal directly from its encoded unscaled integer without materializing {@link BigDecimal}.
    /// The fixed-limb division rounds the exact decimal rational directly to binary32, avoiding the double-rounding
    /// error that would result from casting a rounded binary64 value to float.
    private float getDecimalAsFloat() {
      loadDecimalMagnitude();
      if ((_decimalMagnitudeHigh | _decimalMagnitudeLow) == 0) {
        return 0F;
      }
      int scale = getDecimalScale();
      int binaryExponent = decimalBinaryExponent(scale);
      if (binaryExponent < -150) {
        return _decimalNegative ? -0F : 0F;
      }
      int quantumExponent = Math.max(binaryExponent - 23, -149);
      long significand = roundedDecimalSignificand(scale, quantumExponent);
      float value = Math.scalb((float) significand, quantumExponent);
      return _decimalNegative ? -value : value;
    }

    /// Converts a decimal directly from its encoded unscaled integer without materializing {@link BigDecimal}.
    /// The exact quotient/remainder calculation supplies IEEE-754 round-to-nearest, ties-to-even semantics without
    /// the double-rounding error from separately approximating the unscaled integer and decimal divisor.
    private double getDecimalAsDouble() {
      loadDecimalMagnitude();
      if ((_decimalMagnitudeHigh | _decimalMagnitudeLow) == 0) {
        return 0D;
      }
      int scale = getDecimalScale();
      int binaryExponent = decimalBinaryExponent(scale);
      int quantumExponent = binaryExponent - 52;
      long significand = roundedDecimalSignificand(scale, quantumExponent);
      double value = Math.scalb((double) significand, quantumExponent);
      return _decimalNegative ? -value : value;
    }

    private void loadDecimalMagnitude() {
      int typeInfo = getPrimitiveTypeInfo();
      long high;
      long low;
      switch (typeInfo) {
        case VARIANT_DECIMAL4:
          high = 0;
          low = readSignedLittleEndian(_selectedOffset + 2, Integer.BYTES);
          break;
        case VARIANT_DECIMAL8:
          high = 0;
          low = readSignedLittleEndian(_selectedOffset + 2, Long.BYTES);
          break;
        case VARIANT_DECIMAL16:
          requireSelectedRange(2, 16);
          low = readSignedLittleEndian(_selectedOffset + 2, Long.BYTES);
          high = readSignedLittleEndian(_selectedOffset + 2 + Long.BYTES, Long.BYTES);
          break;
        default:
          throw new IllegalArgumentException("Cannot read non-decimal Variant value as DECIMAL");
      }
      _decimalNegative = typeInfo == VARIANT_DECIMAL16 ? high < 0 : low < 0;
      if (_decimalNegative) {
        if (typeInfo == VARIANT_DECIMAL16) {
          low = -low;
          high = ~high + (low == 0 ? 1 : 0);
        } else {
          low = -low;
          high = 0;
        }
      }
      _decimalMagnitudeHigh = high;
      _decimalMagnitudeLow = low;
    }

    private int decimalBinaryExponent(int scale) {
      int exponent = unsigned128BitLength(_decimalMagnitudeHigh, _decimalMagnitudeLow)
          - (decimalPowerOfFiveBitLength(scale) + scale);
      loadDecimalDivisor(scale, scale + Math.max(exponent, 0));
      loadDecimalMagnitudeShifted(Math.max(-exponent, 0));
      int comparison = compareDecimalScratch(_decimalRemainder, _decimalDivisor);
      return comparison >= 0 ? exponent : exponent - 1;
    }

    /// Returns round((unscaled / 10^scale) / 2^quantumExponent) using exact fixed-limb arithmetic.
    private long roundedDecimalSignificand(int scale, int quantumExponent) {
      int binaryShift = -quantumExponent - scale;
      int numeratorShift = Math.max(binaryShift, 0);
      loadDecimalDivisor(scale, Math.max(-binaryShift, 0));
      Arrays.fill(_decimalRemainder, 0);

      int numeratorBits = unsigned128BitLength(_decimalMagnitudeHigh, _decimalMagnitudeLow) + numeratorShift;
      long quotient = 0;
      for (int bitIndex = numeratorBits - 1; bitIndex >= 0; bitIndex--) {
        int sourceBit = bitIndex - numeratorShift;
        long incomingBit;
        if (sourceBit < 0) {
          incomingBit = 0;
        } else if (sourceBit < Long.SIZE) {
          incomingBit = _decimalMagnitudeLow >>> sourceBit & 1L;
        } else {
          incomingBit = _decimalMagnitudeHigh >>> (sourceBit - Long.SIZE) & 1L;
        }
        shiftDecimalRemainderLeft(incomingBit);
        quotient <<= 1;
        if (compareDecimalScratch(_decimalRemainder, _decimalDivisor) >= 0) {
          subtractDecimalDivisor();
          quotient |= 1;
        }
      }

      shiftDecimalRemainderLeft(0);
      int halfwayComparison = compareDecimalScratch(_decimalRemainder, _decimalDivisor);
      if (halfwayComparison > 0 || halfwayComparison == 0 && (quotient & 1L) != 0) {
        quotient++;
      }
      return quotient;
    }

    private void loadDecimalDivisor(int scale, int binaryShift) {
      Arrays.fill(_decimalDivisor, 0);
      int wordShift = binaryShift / Long.SIZE;
      int bitShift = binaryShift % Long.SIZE;
      long[] powerOfFive = DECIMAL_POWERS_OF_FIVE[scale];
      for (int source = 0; source < DECIMAL_POWER_LIMBS; source++) {
        long value = powerOfFive[source];
        int target = source + wordShift;
        if (target >= _decimalDivisor.length) {
          if (value != 0) {
            throw new ArithmeticException("Decimal divisor exceeds fixed-limb conversion capacity");
          }
          continue;
        }
        _decimalDivisor[target] |= value << bitShift;
        if (bitShift != 0) {
          long upper = value >>> (Long.SIZE - bitShift);
          if (target + 1 < _decimalDivisor.length) {
            _decimalDivisor[target + 1] |= upper;
          } else if (upper != 0) {
            throw new ArithmeticException("Decimal divisor exceeds fixed-limb conversion capacity");
          }
        }
      }
    }

    private void loadDecimalMagnitudeShifted(int binaryShift) {
      Arrays.fill(_decimalRemainder, 0);
      int wordShift = binaryShift / Long.SIZE;
      int bitShift = binaryShift % Long.SIZE;
      loadDecimalMagnitudeLimb(_decimalMagnitudeLow, wordShift, bitShift);
      loadDecimalMagnitudeLimb(_decimalMagnitudeHigh, wordShift + 1, bitShift);
    }

    private void loadDecimalMagnitudeLimb(long value, int target, int bitShift) {
      if (target >= _decimalRemainder.length) {
        if (value != 0) {
          throw new ArithmeticException("Decimal magnitude exceeds fixed-limb conversion capacity");
        }
        return;
      }
      _decimalRemainder[target] |= value << bitShift;
      if (bitShift != 0) {
        long upper = value >>> (Long.SIZE - bitShift);
        if (target + 1 < _decimalRemainder.length) {
          _decimalRemainder[target + 1] |= upper;
        } else if (upper != 0) {
          throw new ArithmeticException("Decimal magnitude exceeds fixed-limb conversion capacity");
        }
      }
    }

    private static int decimalPowerOfFiveBitLength(int scale) {
      long[] power = DECIMAL_POWERS_OF_FIVE[scale];
      for (int limb = power.length - 1; limb >= 0; limb--) {
        if (power[limb] != 0) {
          return limb * Long.SIZE + Long.SIZE - Long.numberOfLeadingZeros(power[limb]);
        }
      }
      throw new IllegalStateException("Power of five cannot be zero");
    }

    private void shiftDecimalRemainderLeft(long incomingBit) {
      long carry = incomingBit;
      for (int i = 0; i < _decimalRemainder.length; i++) {
        long value = _decimalRemainder[i];
        _decimalRemainder[i] = value << 1 | carry;
        carry = value >>> (Long.SIZE - 1);
      }
    }

    private static int compareDecimalScratch(long[] left, long[] right) {
      for (int i = left.length - 1; i >= 0; i--) {
        int comparison = Long.compareUnsigned(left[i], right[i]);
        if (comparison != 0) {
          return comparison;
        }
      }
      return 0;
    }

    private void subtractDecimalDivisor() {
      long borrow = 0;
      for (int i = 0; i < _decimalRemainder.length; i++) {
        long left = _decimalRemainder[i];
        long difference = left - _decimalDivisor[i];
        long divisorBorrow = Long.compareUnsigned(left, _decimalDivisor[i]) < 0 ? 1 : 0;
        long result = difference - borrow;
        long incomingBorrow = borrow != 0 && difference == 0 ? 1 : 0;
        _decimalRemainder[i] = result;
        borrow = divisorBorrow | incomingBorrow;
      }
    }

    private static int unsigned128BitLength(long high, long low) {
      return high != 0 ? Long.SIZE * 2 - Long.numberOfLeadingZeros(high)
          : low != 0 ? Long.SIZE - Long.numberOfLeadingZeros(low) : 0;
    }

    /// Returns an exact long conversion for a decimal or throws with the same contract as
    /// {@link BigDecimal#longValueExact()}.
    private long getDecimalAsLongExact() {
      if (!tryGetDecimalAsLongExact()) {
        throw new ArithmeticException("Rounding necessary or decimal overflow");
      }
      return _convertedDecimalLong;
    }

    private long getConvertedDecimalLong() {
      return _convertedDecimalLong;
    }

    /// Attempts an exact long conversion directly on the encoded decimal. DECIMAL16 division uses four unsigned
    /// 32-bit limbs so it remains allocation-free even when the unscaled value does not fit in a Java long.
    private boolean tryGetDecimalAsLongExact() {
      int typeInfo = getPrimitiveTypeInfo();
      int scale = getDecimalScale();
      switch (typeInfo) {
        case VARIANT_DECIMAL4:
          return tryConvertScaledLong(readSignedLittleEndian(_selectedOffset + 2, Integer.BYTES), scale);
        case VARIANT_DECIMAL8:
          return tryConvertScaledLong(readSignedLittleEndian(_selectedOffset + 2, Long.BYTES), scale);
        case VARIANT_DECIMAL16:
          requireSelectedRange(2, 16);
          return tryConvertScaledInt128(
              readSignedLittleEndian(_selectedOffset + 2 + Long.BYTES, Long.BYTES),
              readSignedLittleEndian(_selectedOffset + 2, Long.BYTES), scale);
        default:
          throw new IllegalArgumentException("Cannot read non-decimal Variant value as DECIMAL");
      }
    }

    private int getDecimalScale() {
      requireSelectedRange(1, 1);
      return Byte.toUnsignedInt(_envelope[_selectedOffset + 1]);
    }

    private boolean tryConvertScaledLong(long unscaled, int scale) {
      for (int i = 0; i < scale && unscaled != 0; i++) {
        if (unscaled % 10 != 0) {
          return false;
        }
        unscaled /= 10;
      }
      _convertedDecimalLong = unscaled;
      return true;
    }

    private boolean tryConvertScaledInt128(long high, long low, int scale) {
      boolean negative = high < 0;
      if (negative) {
        low = -low;
        high = ~high + (low == 0 ? 1 : 0);
      }

      long limb3 = high >>> Integer.SIZE;
      long limb2 = high & Integer.toUnsignedLong(-1);
      long limb1 = low >>> Integer.SIZE;
      long limb0 = low & Integer.toUnsignedLong(-1);
      for (int i = 0; i < scale && (limb3 | limb2 | limb1 | limb0) != 0; i++) {
        long remainder = 0;
        long dividend = remainder << Integer.SIZE | limb3;
        limb3 = dividend / 10;
        remainder = dividend % 10;
        dividend = remainder << Integer.SIZE | limb2;
        limb2 = dividend / 10;
        remainder = dividend % 10;
        dividend = remainder << Integer.SIZE | limb1;
        limb1 = dividend / 10;
        remainder = dividend % 10;
        dividend = remainder << Integer.SIZE | limb0;
        limb0 = dividend / 10;
        if (dividend % 10 != 0) {
          return false;
        }
      }
      if (limb3 != 0 || limb2 != 0) {
        return false;
      }
      long magnitude = limb1 << Integer.SIZE | limb0;
      if (negative) {
        if (Long.compareUnsigned(magnitude, Long.MIN_VALUE) > 0) {
          return false;
        }
        _convertedDecimalLong = magnitude == Long.MIN_VALUE ? Long.MIN_VALUE : -magnitude;
      } else {
        if (magnitude < 0) {
          return false;
        }
        _convertedDecimalLong = magnitude;
      }
      return true;
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

    private boolean selectObjectField(PathElement field, int elementIndex) {
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

      if (numElements <= SMALL_OBJECT_LINEAR_THRESHOLD) {
        // For a handful of entries a direct scan costs at most a few key comparisons — no more than validating the
        // memo would — so the memo machinery is skipped entirely.
        int index = findObjectFieldIndexLinear(field._fieldUtf8, 0, numElements, idStart, idSize, offsetStart);
        if (index < 0) {
          return false;
        }
        selectObjectFieldAtIndex(index, offsetStart, offsetSize, dataStart, totalDataLength);
        return true;
      }

      int memoId = _memoIds[elementIndex];
      if (memoId >= 0) {
        ensureMetadataParsed();
        if (memoId < _metadataDictSize && metadataKeyEquals(memoId, field._fieldUtf8)) {
          // The memoized dictionary id still spells the path key on this row's bytes, so the id alone identifies
          // the field: dictionary strings are distinct per the Variant specification, and object entries reference
          // dictionary ids. This check is local to the current row, so a stale memo can never select a wrong field.
          int hint = _memoHints[elementIndex];
          if (hint >= 0 && hint < numElements
              && readUnsignedLittleEndian(idStart + hint * idSize, idSize, offsetStart) == memoId) {
            selectObjectFieldAtIndex(hint, offsetStart, offsetSize, dataStart, totalDataLength);
            return true;
          }
          return selectObjectFieldById(memoId, elementIndex, numElements, idStart, idSize, offsetStart, offsetSize,
              dataStart, totalDataLength);
        }
        // The dictionary changed underneath the memo; fall through to a fresh resolution.
      } else if (memoId == MEMO_ABSENT_FROM_DICTIONARY) {
        if (isAbsentDictionaryMemoCostEffective(numElements) && absentFromDictionary(elementIndex)) {
          // No object under an identical dictionary can contain a key the dictionary does not define.
          return false;
        }
        // The current object is cheaper to scan than its metadata is to compare, or the metadata changed.
        clearMemo(elementIndex);
      }
      return selectObjectFieldSlow(field, elementIndex, numElements, idStart, idSize, offsetStart, offsetSize,
          dataStart, totalDataLength);
    }

    /// Selects the entry whose id equals the already-validated dictionary id, scanning ids without key comparisons.
    ///
    /// A metadata dictionary with duplicate strings (a hard specification violation, unlike the ordering deviations
    /// the key-based fallbacks tolerate) could make an id-scan miss where a key-based scan would match a duplicate;
    /// that shape is deliberately outside the tolerance contract.
    private boolean selectObjectFieldById(int memoId, int elementIndex, int numElements, int idStart, int idSize,
        int offsetStart, int offsetSize, int dataStart, int totalDataLength) {
      for (int i = 0; i < numElements; i++) {
        if (readUnsignedLittleEndian(idStart + i * idSize, idSize, offsetStart) == memoId) {
          _memoHints[elementIndex] = i;
          selectObjectFieldAtIndex(i, offsetStart, offsetSize, dataStart, totalDataLength);
          return true;
        }
      }
      // The id is absent from this object's id list, so the field is absent from this object.
      return false;
    }

    /// Full key-based resolution, run when the memo cannot answer; re-memoizes for the following rows.
    private boolean selectObjectFieldSlow(PathElement field, int elementIndex, int numElements, int idStart,
        int idSize, int offsetStart, int offsetSize, int dataStart, int totalDataLength) {
      int index = findObjectFieldIndex(field, numElements, idStart, idSize, offsetStart);
      if (index >= 0) {
        _memoIds[elementIndex] = readUnsignedLittleEndian(idStart + index * idSize, idSize, offsetStart);
        _memoHints[elementIndex] = index;
        _memoAbsentEnvelopes[elementIndex] = null;
        selectObjectFieldAtIndex(index, offsetStart, offsetSize, dataStart, totalDataLength);
        return true;
      }
      if (!isAbsentDictionaryMemoCostEffective(numElements)) {
        clearMemo(elementIndex);
        return false;
      }
      if (!metadataRepeatedAcrossRows()) {
        // The dictionary changes row over row, so classifying the miss against the whole dictionary would cost a
        // full dictionary scan per row with nothing reusable. Keep the pre-memo miss cost instead.
        clearMemo(elementIndex);
        return false;
      }
      int dictionaryId = findDictionaryId(field._fieldUtf8);
      _memoIds[elementIndex] = dictionaryId >= 0 ? dictionaryId : MEMO_ABSENT_FROM_DICTIONARY;
      _memoHints[elementIndex] = HINT_UNKNOWN;
      if (dictionaryId >= 0) {
        _memoAbsentEnvelopes[elementIndex] = null;
      } else {
        // Anchor the absence verdict to the exact metadata it was proven under; envelopes are read-only by contract.
        _memoAbsentEnvelopes[elementIndex] = _envelope;
        _memoAbsentMetadataOffsets[elementIndex] = _metadataOffset;
        _memoAbsentMetadataLengths[elementIndex] = _metadataLength;
      }
      return false;
    }

    private boolean isAbsentDictionaryMemoCostEffective(int numElements) {
      return _metadataLength <= (long) numElements * ABSENT_MEMO_METADATA_BYTES_PER_OBJECT_ENTRY_BUDGET;
    }

    private void clearMemo(int elementIndex) {
      _memoIds[elementIndex] = MEMO_UNKNOWN;
      _memoHints[elementIndex] = HINT_UNKNOWN;
      _memoAbsentEnvelopes[elementIndex] = null;
    }

    /// Returns the object-entry index whose key equals the path field, or {@code -1} when absent.
    private int findObjectFieldIndex(PathElement field, int numElements, int idStart, int idSize, int offsetStart) {
      if (numElements < OBJECT_BINARY_SEARCH_THRESHOLD || !field._binarySearchSafe) {
        return findObjectFieldIndexLinear(field._fieldUtf8, 0, numElements, idStart, idSize, offsetStart);
      }

      // Preserve the linear lookup's constant-time first-field case before paying the binary-search cost for the rest
      // of a wide object.
      int firstId = readUnsignedLittleEndian(idStart, idSize, offsetStart);
      if (metadataKeyEquals(firstId, field._fieldUtf8)) {
        return 0;
      }

      int low = 1;
      int high = numElements - 1;
      while (low <= high) {
        int index = (low + high) >>> 1;
        int id = readUnsignedLittleEndian(idStart + index * idSize, idSize, offsetStart);
        int comparison = metadataKeyCompare(id, field._field);
        if (comparison == INVALID_UTF8_COMPARISON) {
          // Malformed UTF-8 is not a valid Variant key, but retain the old byte-equality behavior for tolerant
          // callers instead of relying on an ordering that is no longer defined.
          return findObjectFieldIndexLinear(field._fieldUtf8, 1, numElements, idStart, idSize, offsetStart);
        }
        if (comparison < 0) {
          low = index + 1;
        } else if (comparison > 0) {
          high = index - 1;
        } else {
          return index;
        }
      }
      // parquet-java orders object keys with String.compareTo (UTF-16 code units), while other conforming producers
      // such as Arrow Rust use Unicode scalar/UTF-8 order. Those orders differ for supplementary characters relative
      // to U+E000..U+FFFF, so a miss under the parquet-java ordering is not authoritative for an external envelope.
      // Preserve the allocation-free binary fast path for hits, then use byte equality as the interoperability-safe
      // fallback for misses and unknown producer orderings.
      return findObjectFieldIndexLinear(field._fieldUtf8, 1, numElements, idStart, idSize, offsetStart);
    }

    private int findObjectFieldIndexLinear(byte[] fieldUtf8, int startIndex, int numElements, int idStart,
        int idSize, int offsetStart) {
      for (int i = startIndex; i < numElements; i++) {
        int id = readUnsignedLittleEndian(idStart + i * idSize, idSize, offsetStart);
        if (metadataKeyEquals(id, fieldUtf8)) {
          return i;
        }
      }
      return -1;
    }

    /// Returns the dictionary id whose string equals the key bytes, or {@code -1} when the dictionary does not
    /// define the key. Linear on purpose: this runs once per metadata change, and it makes no assumption about the
    /// producer's dictionary ordering.
    private int findDictionaryId(byte[] keyUtf8) {
      ensureMetadataParsed();
      for (int id = 0; id < _metadataDictSize; id++) {
        if (metadataKeyEquals(id, keyUtf8)) {
          return id;
        }
      }
      return -1;
    }

    private void selectObjectFieldAtIndex(int index, int offsetStart, int offsetSize, int dataStart,
        int totalDataLength) {
      int offset = readUnsignedLittleEndian(offsetStart + index * offsetSize, offsetSize, dataStart);
      int valueOffset = checkedPosition((long) dataStart + offset, dataStart + totalDataLength,
          "Variant object field");
      int valueLength = encodedValueLength(valueOffset, dataStart + totalDataLength);
      _selectedOffset = valueOffset;
      _selectedLength = valueLength;
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

    /// Returns the exact byte length of the self-delimiting Variant value at {@code valueOffset}.
    ///
    /// <p>Object field offsets are sorted by key, while their encoded values may appear in any physical order. The
    /// adjacent field offset is therefore not necessarily the end of the selected value. Reading the selected value's
    /// own header preserves exact subtree envelopes without scanning every object offset.
    private int encodedValueLength(int valueOffset, int limit) {
      requireRange(valueOffset, 1, limit, "Variant value");
      int header = Byte.toUnsignedInt(_envelope[valueOffset]);
      int basicType = header & VARIANT_BASIC_TYPE_MASK;
      int typeInfo = (header >>> 2) & VARIANT_PRIMITIVE_TYPE_MASK;
      int length;
      switch (basicType) {
        case VARIANT_SHORT_STRING:
          length = 1 + typeInfo;
          break;
        case VARIANT_OBJECT: {
          int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
          int numElements = readUnsignedLittleEndian(valueOffset + 1, sizeBytes, limit);
          int idSize = ((typeInfo >>> 2) & 3) + 1;
          int offsetSize = (typeInfo & 3) + 1;
          int offsetStart = checkedPosition((long) valueOffset + 1 + sizeBytes + (long) numElements * idSize,
              limit, "Variant object offsets");
          int dataStart = checkedPosition((long) offsetStart + ((long) numElements + 1) * offsetSize, limit,
              "Variant object data");
          int finalOffsetPosition = checkedPosition((long) offsetStart + (long) numElements * offsetSize, limit,
              "Variant object final offset");
          int dataLength = readUnsignedLittleEndian(finalOffsetPosition, offsetSize, limit);
          int valueEnd = checkedPosition((long) dataStart + dataLength, limit, "Variant object value");
          return valueEnd - valueOffset;
        }
        case VARIANT_ARRAY: {
          int sizeBytes = ((typeInfo >>> 2) & 1) == 0 ? 1 : Integer.BYTES;
          int numElements = readUnsignedLittleEndian(valueOffset + 1, sizeBytes, limit);
          int offsetSize = (typeInfo & 3) + 1;
          int offsetStart = checkedPosition((long) valueOffset + 1 + sizeBytes, limit, "Variant array offsets");
          int dataStart = checkedPosition((long) offsetStart + ((long) numElements + 1) * offsetSize, limit,
              "Variant array data");
          int finalOffsetPosition = checkedPosition((long) offsetStart + (long) numElements * offsetSize, limit,
              "Variant array final offset");
          int dataLength = readUnsignedLittleEndian(finalOffsetPosition, offsetSize, limit);
          int valueEnd = checkedPosition((long) dataStart + dataLength, limit, "Variant array value");
          return valueEnd - valueOffset;
        }
        case VARIANT_PRIMITIVE:
          switch (typeInfo) {
            case VARIANT_NULL:
            case VARIANT_TRUE:
            case VARIANT_FALSE:
              length = 1;
              break;
            case VARIANT_INT8:
              length = 2;
              break;
            case VARIANT_INT16:
              length = 3;
              break;
            case VARIANT_INT32:
            case VARIANT_DATE:
            case VARIANT_FLOAT:
              length = 5;
              break;
            case VARIANT_INT64:
            case VARIANT_DOUBLE:
            case VARIANT_TIMESTAMP_TZ:
            case VARIANT_TIMESTAMP_NTZ:
            case VARIANT_TIME:
            case VARIANT_TIMESTAMP_NANOS_TZ:
            case VARIANT_TIMESTAMP_NANOS_NTZ:
              length = 9;
              break;
            case VARIANT_DECIMAL4:
              length = 6;
              break;
            case VARIANT_DECIMAL8:
              length = 10;
              break;
            case VARIANT_DECIMAL16:
              length = 18;
              break;
            case VARIANT_BINARY:
            case VARIANT_LONG_STRING:
              length = checkedPosition((long) valueOffset + 1 + Integer.BYTES
                  + readUnsignedLittleEndian(valueOffset + 1, Integer.BYTES, limit), limit, "Variant binary value")
                  - valueOffset;
              break;
            case VARIANT_UUID:
              length = 1 + UuidUtils.UUID_NUM_BYTES;
              break;
            default:
              throw unsupportedPrimitiveType(typeInfo);
          }
          break;
        default:
          throw new IllegalStateException("Unhandled Variant basic type: " + basicType);
      }
      requireRange(valueOffset, length, limit, "Variant value");
      return length;
    }

    private static UnsupportedOperationException unsupportedPrimitiveType(int typeInfo) {
      return new UnsupportedOperationException(
          "Unsupported Parquet Variant primitive type: " + typeInfo + "; update VariantUtils with the codec");
    }

    /// Compares one encoded metadata key with {@code expected} using Java String's UTF-16 code-unit order without
    /// materializing the encoded key. Returns {@link #INVALID_UTF8_COMPARISON} when the metadata key is not valid
    /// UTF-8, in which case callers must not rely on object-key ordering.
    private int metadataKeyCompare(int id, String expected) {
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

      int byteIndex = _metadataDataOffset + offset;
      int byteLimit = _metadataDataOffset + nextOffset;
      int charIndex = 0;
      while (byteIndex < byteLimit) {
        int decoded = decodeUtf8CodePoint(byteIndex, byteLimit);
        if (decoded < 0) {
          return INVALID_UTF8_COMPARISON;
        }
        byteIndex += decoded >>> 24;
        int codePoint = decoded & 0x1F_FFFF;
        if (codePoint < Character.MIN_SUPPLEMENTARY_CODE_POINT) {
          if (charIndex == expected.length()) {
            return 1;
          }
          int comparison = Character.compare((char) codePoint, expected.charAt(charIndex++));
          if (comparison != 0) {
            return comparison;
          }
        } else {
          if (charIndex == expected.length()) {
            return 1;
          }
          int comparison = Character.compare(Character.highSurrogate(codePoint), expected.charAt(charIndex++));
          if (comparison != 0) {
            return comparison;
          }
          if (charIndex == expected.length()) {
            return 1;
          }
          comparison = Character.compare(Character.lowSurrogate(codePoint), expected.charAt(charIndex++));
          if (comparison != 0) {
            return comparison;
          }
        }
      }
      return charIndex == expected.length() ? 0 : -1;
    }

    /// Returns the decoded code point with its encoded width in the top byte, or {@code -1} for malformed UTF-8.
    private int decodeUtf8CodePoint(int index, int limit) {
      int first = Byte.toUnsignedInt(_envelope[index]);
      if (first <= 0x7F) {
        return 1 << 24 | first;
      }
      if (first >= 0xC2 && first <= 0xDF) {
        if (index + 1 >= limit) {
          return -1;
        }
        int second = Byte.toUnsignedInt(_envelope[index + 1]);
        if ((second & 0xC0) != 0x80) {
          return -1;
        }
        return 2 << 24 | (first & 0x1F) << 6 | second & 0x3F;
      }
      if (first >= 0xE0 && first <= 0xEF) {
        if (index + 2 >= limit) {
          return -1;
        }
        int second = Byte.toUnsignedInt(_envelope[index + 1]);
        int third = Byte.toUnsignedInt(_envelope[index + 2]);
        if ((third & 0xC0) != 0x80
            || (first == 0xE0 ? second < 0xA0 || second > 0xBF
            : first == 0xED ? second < 0x80 || second > 0x9F : (second & 0xC0) != 0x80)) {
          return -1;
        }
        return 3 << 24 | (first & 0x0F) << 12 | (second & 0x3F) << 6 | third & 0x3F;
      }
      if (first >= 0xF0 && first <= 0xF4) {
        if (index + 3 >= limit) {
          return -1;
        }
        int second = Byte.toUnsignedInt(_envelope[index + 1]);
        int third = Byte.toUnsignedInt(_envelope[index + 2]);
        int fourth = Byte.toUnsignedInt(_envelope[index + 3]);
        if ((third & 0xC0) != 0x80 || (fourth & 0xC0) != 0x80
            || (first == 0xF0 ? second < 0x90 || second > 0xBF
            : first == 0xF4 ? second < 0x80 || second > 0x8F : (second & 0xC0) != 0x80)) {
          return -1;
        }
        return 4 << 24 | (first & 0x07) << 18 | (second & 0x3F) << 12 | (third & 0x3F) << 6
            | fourth & 0x3F;
      }
      return -1;
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
      if (length < 16) {
        // Short keys dominate real paths; keep them on a simple scalar loop and reserve the vectorized
        // comparison for long keys.
        for (int i = 0; i < length; i++) {
          if (_envelope[start + i] != expected[i]) {
            return false;
          }
        }
        return true;
      }
      return Arrays.equals(_envelope, start, start + length, expected, 0, length);
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
    private final boolean _binarySearchSafe;

    private PathElement(String field, byte[] fieldUtf8, int index, boolean binarySearchSafe) {
      _field = field;
      _fieldUtf8 = fieldUtf8;
      _index = index;
      _binarySearchSafe = binarySearchSafe;
    }

    private static PathElement forField(String field) {
      return new PathElement(field, field.getBytes(StandardCharsets.UTF_8), -1, hasWellFormedUtf16(field));
    }

    private static PathElement forIndex(int index) {
      return new PathElement(null, null, index, false);
    }

    private static boolean hasWellFormedUtf16(String value) {
      int i = 0;
      while (i < value.length()) {
        char current = value.charAt(i);
        if (Character.isHighSurrogate(current)) {
          if (i + 1 == value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) {
            return false;
          }
          i += 2;
        } else if (Character.isLowSurrogate(current)) {
          return false;
        } else {
          i++;
        }
      }
      return true;
    }
  }
}
