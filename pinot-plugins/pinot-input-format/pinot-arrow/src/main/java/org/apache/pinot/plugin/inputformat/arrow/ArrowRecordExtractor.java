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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


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
          rawValue != null
              ? ArrowToPinotTypeConverter.toPinotValue(activeVector.getField(), rawValue, _extractRawTimeValues)
              : null);
    }
    return to;
  }
}
