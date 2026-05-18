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
package org.apache.pinot.plugin.inputformat.arrow;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.utils.TimestampUtils;


/// Extracts a single Arrow row into a [GenericRow]. Reader-scoped state ([VectorSchemaRoot] +
/// dictionary map) is bound once via [#setReader]; per-row [#extract] calls take a [Record] holding
/// only the row index. Dispatch is schema-driven — each column is walked using its [Field], so the
/// logical type drives the conversion rather than the runtime Java type of the value.
///
/// **Scalars** (Arrow type → Java output):
/// - `Bool` → `Boolean`
/// - `Int(8/16)` → `Integer` (widened from `Byte` / `Short`)
/// - `Int(32)` → `Integer`
/// - `Int(64)` → `Long`
/// - `FloatingPoint(SINGLE)` → `Float`
/// - `FloatingPoint(DOUBLE)` → `Double`
/// - `Decimal` → `BigDecimal`
/// - `Utf8` / `LargeUtf8` → `String` (via `Text.toString()`)
/// - `Binary` / `LargeBinary` / `FixedSizeBinary` → `byte[]`
/// - `Null` → `null` (every row is null by definition)
///
/// **Temporal** (per the schema's `DateUnit` / `TimeUnit`):
/// - `Timestamp` no-TZ → [Timestamp] (Arrow surfaces all four units as `LocalDateTime`; interpreted
///   as a UTC instant)
/// - `Timestamp` with-TZ → [Timestamp] (Arrow surfaces all four units as `Long` epoch; constructed
///   per the schema's `TimeUnit`, sub-millisecond precision preserved via [TimestampUtils])
/// - `Date` → [LocalDate] (`DateDayVector` surfaces as `Integer` raw days; `DateMilliVector` as
///   `LocalDateTime` at UTC midnight — both reduce to a calendar date)
/// - `Time` → [LocalTime] (`TimeSecVector` as `Integer`, `TimeMilliVector` as `LocalDateTime`,
///   `TimeMicroVector` / `TimeNanoVector` as `Long` — all collapse onto nanoseconds-since-midnight)
/// - `Interval` / `Duration` → ISO-8601 `String` via `value.toString()` — `java.time.Period` /
///   `java.time.Duration` / `PeriodDuration` all have meaningful toString (e.g. `"P1Y2M"`,
///   `"PT5H30M"`, `"P1Y2M3D PT4H5M6S"`)
///
/// With `extractRawTimeValues = true` ([ArrowRecordExtractorConfig]) the `Date` / `Time` /
/// `Timestamp` cases bypass the contract conversion: `Date` → `int` days-since-epoch (regardless of
/// `DateUnit` — `DateMilli` is always UTC midnight, so reducing to days is lossless); `Time` /
/// `Timestamp` → raw `int` / `long` in the schema's `TimeUnit`. `Interval` / `Duration` are
/// unaffected. Temporal values that surface inside a `Union` branch don't see the bypass either —
/// the chosen branch's [Field] isn't visible from the value alone, so we can't pick a unit; they
/// always coerce to `Timestamp` UTC.
///
/// **Complex** (recurse with the [Field]'s child fields):
/// - `List` / `LargeList` / `FixedSizeList` → `Object[]`
/// - `Struct` → `Map<String, Object>`
/// - `Map` → `Map<String, Object>` (Arrow's `List<Map<KEY, VALUE>>` entry list is flattened;
///   keys are stringified per [BaseRecordExtractor#stringifyMapKey])
/// - `Union` → recursively dispatched by the value's runtime Java type (the chosen branch isn't
///   visible from the value alone — nested complex sub-branches fall back to `value.toString()`)
///
/// **Other**:
/// - dictionary-encoded vector → decoded against the bound dictionary, then dispatched on the
///   decoded vector's [Field] (so the logical type — e.g. `Utf8` — drives conversion, not the
///   dictionary's index type)
///
/// Unrecognized types (`NONE` / future Arrow additions) throw [IllegalStateException].
///
/// **Quirks worth knowing:**
/// - `UInt2Vector` (unsigned 16-bit) returns `Character`, not a `Number` — Arrow's Java bindings
///   use `char` as the only natively unsigned primitive. We widen to `int` per the contract.
/// - `DateDayVector` / `DateMilliVector` return *different* Java types (`Integer` vs
///   `LocalDateTime`) for the same logical `DATE` type — historical asymmetry in Arrow's API.
public class ArrowRecordExtractor extends BaseRecordExtractor<ArrowRecordExtractor.Record> {

  /// Per-batch context used by [#extract]. Holds the row index plus the active vectors for the
  /// current batch — decoded copies for dictionary-encoded columns, the raw [FieldVector] otherwise
  /// (parallel to the extractor's `_fieldVectors`). Decoded vectors are owned by this Record and
  /// closed via [#close]; closing is also implicit on the next [#prepareBatch] call.
  ///
  /// Lifecycle: caller invokes [#prepareBatch] once at the start of a batch, then [#setRowId] +
  /// [#extract] per row. Reusable across batches/files; close once at end of iteration.
  public static final class Record implements AutoCloseable {
    private int _rowId;
    private ValueVector[] _activeVectors;
    private boolean[] _ownsVector;

    public void setRowId(int rowId) {
      _rowId = rowId;
    }

    @Override
    public void close() {
      if (_activeVectors != null) {
        for (int i = 0; i < _activeVectors.length; i++) {
          if (_ownsVector[i]) {
            _activeVectors[i].close();
          }
        }
        _activeVectors = null;
        _ownsVector = null;
      }
    }
  }

  private boolean _extractRawTimeValues;

  // Reader-scoped state — initialized in [#setReader], read by per-row [#extract]. The dictionary map
  // is held so per-row lookups don't re-traverse the reader; the field vectors are pre-resolved against
  // the include list so the per-row loop is a flat array walk (names are read inline from each field
  // vector — `Field#getName` is a plain getter).
  private Map<Long, Dictionary> _dictionaries;
  private FieldVector[] _fieldVectors;

  @Override
  protected void initConfig(@Nullable RecordExtractorConfig config) {
    if (config instanceof ArrowRecordExtractorConfig) {
      _extractRawTimeValues = ((ArrowRecordExtractorConfig) config).isExtractRawTimeValues();
    }
  }

  /// Binds the extractor to `reader` for the upcoming run of [#extract] calls. Must be called before
  /// [#prepareBatch] — once per file (`ArrowRecordReader`) or per `decode()` call (`ArrowMessageDecoder`).
  /// Resolves the include list against the reader's [VectorSchemaRoot] and stashes the dictionary map.
  public void setReader(ArrowReader reader)
      throws IOException {
    _dictionaries = reader.getDictionaryVectors();
    VectorSchemaRoot root = reader.getVectorSchemaRoot();
    List<FieldVector> fieldVectors = root.getFieldVectors();
    if (_extractAll) {
      _fieldVectors = fieldVectors.toArray(new FieldVector[0]);
    } else {
      List<FieldVector> matched = new ArrayList<>(_fields.size());
      for (FieldVector fieldVector : fieldVectors) {
        if (_fields.contains(fieldVector.getField().getName())) {
          matched.add(fieldVector);
        }
      }
      _fieldVectors = matched.toArray(new FieldVector[0]);
    }
  }

  /// Prepares `record` for the current batch: closes any prior decoded vectors and decodes each
  /// dictionary-encoded column from `_fieldVectors` once into `record._activeVectors[i]`.
  /// Non-dictionary columns share the raw [FieldVector] reference (no copy). Must be called after
  /// each `loadNextBatch` and before the per-row [#extract] loop.
  public void prepareBatch(Record record) {
    record.close();
    int numFields = _fieldVectors.length;
    ValueVector[] activeVectors = new ValueVector[numFields];
    boolean[] ownsVector = new boolean[numFields];
    for (int i = 0; i < numFields; i++) {
      FieldVector fieldVector = _fieldVectors[i];
      DictionaryEncoding encoding = fieldVector.getField().getDictionary();
      if (encoding != null) {
        activeVectors[i] = DictionaryEncoder.decode(fieldVector, _dictionaries.get(encoding.getId()));
        ownsVector[i] = true;
      } else {
        activeVectors[i] = fieldVector;
      }
    }
    record._activeVectors = activeVectors;
    record._ownsVector = ownsVector;
  }

  /// Reads each included column at `from._rowId` and dispatches by the active vector's [Field]'s
  /// logical type. The active vector is the dictionary-decoded vector for dictionary-encoded
  /// columns (so dispatch sees the value type — e.g. `Utf8` — not the dictionary's index type), or
  /// the raw [FieldVector] otherwise.
  @Override
  public GenericRow extract(Record from, GenericRow to) {
    FieldVector[] fieldVectors = _fieldVectors;
    ValueVector[] activeVectors = from._activeVectors;
    for (int i = 0; i < fieldVectors.length; i++) {
      ValueVector activeVector = activeVectors[i];
      Object rawValue = activeVector.getObject(from._rowId);
      to.putValue(fieldVectors[i].getField().getName(),
          rawValue != null ? convert(activeVector.getField(), rawValue) : null);
    }
    return to;
  }

  /// Schema-driven dispatch — one branch per [ArrowType.ArrowTypeID]; complex types recurse with
  /// their child [Field]s, scalars normalize per the contract.
  @Nullable
  private Object convert(Field field, Object value) {
    ArrowType type = field.getType();
    switch (type.getTypeID()) {
      // Pass-through — Arrow boxes these directly into the contract output type.
      case Bool:            // Boolean
      case FloatingPoint:   // Float / Double
      case Decimal:         // BigDecimal
      case Binary:          // byte[]
      case LargeBinary:     // byte[]
      case FixedSizeBinary: // byte[]
        return value;
      // toString — `Utf8` / `LargeUtf8` produce `String`; `Interval` / `Duration` produce ISO-8601
      // (`java.time.Period` / `java.time.Duration` / `PeriodDuration` all have meaningful toString).
      case Utf8:
      case LargeUtf8:
      case Interval:
      case Duration:
        return value.toString();
      // Integer — `Byte` widens to `Integer` per contract (sign-extended for signed `TinyIntVector`,
      // zero-extended via `& 0xFF` for unsigned `UInt1Vector`); `Short` (signed `SmallIntVector`)
      // sign-extends; `Character` (unsigned 16, from `UInt2Vector`) widens to its `int` code point;
      // `Integer` / `Long` pass through.
      case Int:
        if (value instanceof Byte) {
          int v = (Byte) value;
          return ((ArrowType.Int) type).getIsSigned() ? v : v & 0xFF;
        }
        if (value instanceof Short) {
          return ((Short) value).intValue();
        }
        if (value instanceof Character) {
          return (int) (Character) value;
        }
        return value;
      // Null — NullVector.getObject always returns null; extractValue short-circuits on null, so
      // this branch is unreachable in practice. Defensive return.
      case Null:
        return null;
      // Logical temporal — schema's `TimeUnit` drives the conversion.
      case Timestamp:
        return convertTimestamp((ArrowType.Timestamp) type, value);
      case Date:
        return convertDate((ArrowType.Date) type, value);
      case Time:
        return convertTime((ArrowType.Time) type, value);
      // Multi-value — `List` (and primitive-array lists) → `Object[]`.
      case List:
      case LargeList:
      case FixedSizeList:
        return convertList(field.getChildren().get(0), (List<?>) value);
      // Map / nested complex types.
      case Map:
        // The Map field's children are [entriesStruct]; the entries struct's children are
        // [keyField, valueField] (named per MapVector.KEY_NAME / VALUE_NAME).
        Field entriesField = field.getChildren().get(0);
        return convertMap(entriesField.getChildren().get(0), entriesField.getChildren().get(1), (List<?>) value);
      case Struct:
        return convertStruct(field.getChildren(), (Map<?, ?>) value);
      case Union:
        // The chosen branch isn't visible from the resolved value alone — dispatch by the value's
        // runtime Java type. Nested complex sub-branches fall back to `value.toString()`.
        return convertByRuntimeType(value);
      default:
        // `NONE` is a placeholder; any other ID is a future Arrow addition.
        throw new IllegalStateException("Unsupported Arrow type: " + type + " for field: " + field.getName());
    }
  }

  /// Constructs a [Timestamp] from an Arrow `Timestamp` value. No-TZ vectors surface as
  /// `LocalDateTime` (interpreted as UTC); with-TZ vectors surface as `Long` epoch counted in the
  /// schema's `TimeUnit`. Sub-millisecond precision is preserved via [TimestampUtils].
  /// With [#_extractRawTimeValues] the raw `long` epoch in the schema's `TimeUnit` is returned.
  private Object convertTimestamp(ArrowType.Timestamp type, Object value) {
    if (_extractRawTimeValues) {
      if (value instanceof LocalDateTime) {
        // No-TZ vector — convert the LocalDateTime back to an epoch `long` in the declared unit.
        Instant instant = ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
        return toEpochInUnit(instant, type.getUnit());
      }
      // With-TZ vector — already raw `long` in the declared unit.
      return value;
    }
    if (value instanceof LocalDateTime) {
      return Timestamp.from(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
    }
    long raw = ((Number) value).longValue();
    switch (type.getUnit()) {
      case SECOND:
        return new Timestamp(raw * 1000L);
      case MILLISECOND:
        return new Timestamp(raw);
      case MICROSECOND:
        return TimestampUtils.fromMicrosSinceEpoch(raw);
      case NANOSECOND:
        return TimestampUtils.fromNanosSinceEpoch(raw);
      default:
        throw new IllegalStateException("Unsupported Timestamp unit: " + type.getUnit());
    }
  }

  private static long toEpochInUnit(Instant instant, org.apache.arrow.vector.types.TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return instant.getEpochSecond();
      case MILLISECOND:
        return instant.toEpochMilli();
      case MICROSECOND:
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
      case NANOSECOND:
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
      default:
        throw new IllegalStateException("Unsupported Timestamp unit: " + unit);
    }
  }

  /// Reduces an Arrow `Date` value to its contract Java type ([LocalDate]), or to `int`
  /// days-since-epoch when [#_extractRawTimeValues] is set. `DateDayVector` surfaces as `Integer`
  /// raw days; `DateMilliVector` surfaces as `LocalDateTime` at UTC midnight.
  private Object convertDate(ArrowType.Date type, Object value) {
    int days;
    switch (type.getUnit()) {
      case DAY:
        days = (Integer) value;
        break;
      case MILLISECOND:
        days = (int) ((LocalDateTime) value).toLocalDate().toEpochDay();
        break;
      default:
        throw new IllegalStateException("Unsupported Date unit: " + type.getUnit());
    }
    return _extractRawTimeValues ? days : LocalDate.ofEpochDay(days);
  }

  /// Constructs a [LocalTime] from an Arrow `Time` value, dispatched by the schema's `TimeUnit`:
  /// `TimeMilliVector` surfaces as `LocalDateTime`; `TimeSecVector` as `Integer`;
  /// `TimeMicroVector` / `TimeNanoVector` as `Long`. All collapse onto nanoseconds-since-midnight.
  /// With [#_extractRawTimeValues] the raw count in the schema's `TimeUnit` is returned instead.
  private Object convertTime(ArrowType.Time type, Object value) {
    if (_extractRawTimeValues) {
      if (value instanceof LocalDateTime) {
        // `TimeMilliVector` surfaces as `LocalDateTime`; raw is `int` ms since midnight.
        return (int) (((LocalDateTime) value).toLocalTime().toNanoOfDay() / 1_000_000L);
      }
      // `TimeSecVector` (Integer) / `TimeMicroVector` / `TimeNanoVector` (Long) — already raw.
      return value;
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toLocalTime();
    }
    long raw = ((Number) value).longValue();
    switch (type.getUnit()) {
      case SECOND:
        return LocalTime.ofSecondOfDay(raw);
      case MILLISECOND:
        return LocalTime.ofNanoOfDay(raw * 1_000_000L);
      case MICROSECOND:
        return LocalTime.ofNanoOfDay(raw * 1_000L);
      case NANOSECOND:
        return LocalTime.ofNanoOfDay(raw);
      default:
        throw new IllegalStateException("Unsupported Time unit: " + type.getUnit());
    }
  }

  private Object[] convertList(Field elementField, List<?> list) {
    int size = list.size();
    Object[] result = new Object[size];
    int i = 0;
    for (Object element : list) {
      result[i++] = element != null ? convert(elementField, element) : null;
    }
    return result;
  }

  /// Flattens an Arrow `Map` column's entry list (`List<Map<KEY_NAME, VALUE_NAME>>`) into a
  /// `Map<String, Object>`, recursing into each value via [#convert] and stringifying each key via
  /// [BaseRecordExtractor#stringifyMapKey] per the contract. Entries with a `null` key (input or
  /// post-conversion) are dropped.
  private Map<String, Object> convertMap(Field keyField, Field valueField, List<?> entries) {
    Map<String, Object> result = Maps.newLinkedHashMapWithExpectedSize(entries.size());
    for (Object entry : entries) {
      if (entry == null) {
        continue;
      }
      Map<?, ?> entryMap = (Map<?, ?>) entry;
      Object rawKey = entryMap.get(MapVector.KEY_NAME);
      if (rawKey == null) {
        continue;
      }
      Object convertedKey = convert(keyField, rawKey);
      if (convertedKey == null) {
        continue;
      }
      Object rawValue = entryMap.get(MapVector.VALUE_NAME);
      result.put(stringifyMapKey(convertedKey), rawValue != null ? convert(valueField, rawValue) : null);
    }
    return result;
  }

  private Map<String, Object> convertStruct(List<Field> childFields, Map<?, ?> value) {
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(childFields.size());
    for (Field childField : childFields) {
      String name = childField.getName();
      Object rawValue = value.get(name);
      result.put(name, rawValue != null ? convert(childField, rawValue) : null);
    }
    return result;
  }

  /// Runtime-type dispatch used by the `Union` case (where the chosen branch isn't accessible
  /// from the resolved value). Mirrors the scalar handling of [#convert] for the common Arrow
  /// boxed types; nested complex types fall back to `value.toString()` because their child
  /// [Field]s aren't reachable from here.
  private static Object convertByRuntimeType(Object value) {
    if (value instanceof Number) {
      if (value instanceof Byte || value instanceof Short) {
        return ((Number) value).intValue();
      }
      return value;
    }
    if (value instanceof Boolean || value instanceof byte[]) {
      return value;
    }
    if (value instanceof Character) {
      // `UInt2Vector` surfaces as `Character`; widen to `int` per the Int(16) contract.
      return (int) (Character) value;
    }
    if (value instanceof LocalDateTime) {
      // Ambiguous between Timestamp / Date / Time — best-effort: treat as Timestamp UTC.
      return Timestamp.from(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
    }
    // `Text` (Utf8 / LargeUtf8), `Period` / `Duration` / `PeriodDuration` (Interval / Duration), and
    // anything unrecognized fall through to `toString()`.
    return value.toString();
  }
}
