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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.MapUtils;


/// Materializes supported native Parquet schemas directly into Pinot values, without the intermediate Group graph.
/// Schema decisions and converter callbacks are bound once. Each record owns its collections and binary values;
/// only immutable strings are shared. Column readers can explicitly request canonical JSON for required String maps;
/// ordinary row readers retain native maps. Not thread-safe: each file reader owns its materializer.
///
/// Supports plain primitives, STRING/ENUM, integer annotations, INT64 TIMESTAMP, and standard MAP/three-level LIST
/// containers over those values. The caller must check [#supports(MessageType)] before construction; other schemas
/// continue through [ParquetNativeRecordExtractor], preserving its broader logical-type and legacy encoding support.
final class ParquetNativeRecordMaterializer extends RecordMaterializer<GenericRow> {
  private final GroupConverter _root;
  private final boolean _extractRawTimeValues;
  private final List<CanonicalJsonMapConverter> _canonicalJsonConverters = new ArrayList<>();
  private GenericRow _record;

  ParquetNativeRecordMaterializer(MessageType schema, Set<String> fieldsToRead, boolean extractRawTimeValues) {
    this(schema, fieldsToRead, extractRawTimeValues, Set.of());
  }

  ParquetNativeRecordMaterializer(MessageType schema, Set<String> fieldsToRead, boolean extractRawTimeValues,
      Set<String> canonicalJsonColumns) {
    _extractRawTimeValues = extractRawTimeValues;
    boolean extractAll = fieldsToRead == null || fieldsToRead.isEmpty();
    String[] fields = new String[schema.getFieldCount()];
    Converter[] converters = new Converter[fields.length];
    for (int i = 0; i < fields.length; i++) {
      Type field = schema.getType(i);
      String name = field.getName();
      if (extractAll || fieldsToRead.contains(name)) {
        fields[i] = name;
        if (canonicalJsonColumns.contains(name)) {
          CanonicalJsonMapConverter converter =
              new CanonicalJsonMapConverter(field.asGroupType(), value -> _record.putValue(name, value));
          _canonicalJsonConverters.add(converter);
          converters[i] = converter;
        } else {
          converters[i] = createConverter(field, value -> _record.putValue(name, value));
        }
      } else {
        converters[i] = createConverter(field, value -> { });
      }
    }
    _root = new GroupConverter() {
      @Override
      public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
      }

      @Override
      public void start() {
        // Optional fields whose converters are not called must still appear as null, as in the native extractor.
        for (String field : fields) {
          if (field != null) {
            _record.putValue(field, null);
          }
        }
      }

      @Override
      public void end() {
      }
    };
  }

  static boolean supports(MessageType schema) {
    for (Type field : schema.getFields()) {
      if (!supports(field)) {
        return false;
      }
    }
    return true;
  }

  private static boolean supports(Type type) {
    if (type.isRepetition(Type.Repetition.REPEATED)) {
      return false;
    }
    LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
    if (type.isPrimitive()) {
      PrimitiveType.PrimitiveTypeName physicalType = type.asPrimitiveType().getPrimitiveTypeName();
      switch (physicalType) {
        case BOOLEAN:
        case FLOAT:
        case DOUBLE:
          return logicalType == null;
        case INT32:
          return logicalType == null || logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation;
        case INT64:
          return logicalType == null || logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
              || logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return logicalType == null || logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
              || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
        default:
          return false;
      }
    }
    GroupType group = type.asGroupType();
    if (group.getFieldCount() != 1 || !group.getType(0).isRepetition(Type.Repetition.REPEATED)
        || group.getType(0).isPrimitive()) {
      return false;
    }
    GroupType repeated = group.getType(0).asGroupType();
    if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      // Only the standard three-level wrapper, not legacy forms whose single field is a real struct.
      return repeated.getFieldCount() == 1 && !"array".equals(repeated.getName())
          && !(group.getName() + "_tuple").equals(repeated.getName()) && supports(repeated.getType(0));
    }
    if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
      return repeated.getFieldCount() == 2 && repeated.containsField("key") && repeated.containsField("value")
          && repeated.getType("key").isPrimitive()
          && repeated.getType("key").isRepetition(Type.Repetition.REQUIRED) && supports(repeated.getType("key"))
          && supports(repeated.getType("value"));
    }
    return false;
  }

  void setRecord(GenericRow record) {
    _record = record;
  }

  void resetCanonicalJsonCaches() {
    for (CanonicalJsonMapConverter converter : _canonicalJsonConverters) {
      converter.resetRowGroup();
    }
  }

  @Override
  public GenericRow getCurrentRecord() {
    return _record;
  }

  @Override
  public GroupConverter getRootConverter() {
    return _root;
  }

  private Converter createConverter(Type type, Consumer<Object> sink) {
    if (type.isPrimitive()) {
      return new ValueConverter(type.asPrimitiveType(), sink);
    }
    GroupType group = type.asGroupType();
    if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
      return new MapConverter(group, sink);
    }
    return new ListConverter(group, sink);
  }

  private final class ValueConverter extends PrimitiveConverter {
    private final Consumer<Object> _sink;
    private final boolean _string;
    private final LogicalTypeAnnotation.TimeUnit _timestampUnit;
    private Dictionary _dictionary;
    private String[] _dictionaryStrings;

    ValueConverter(PrimitiveType type, Consumer<Object> sink) {
      _sink = sink;
      LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
      _string = logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
          || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
      _timestampUnit = logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
          ? ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType).getUnit() : null;
    }

    @Override
    public boolean hasDictionarySupport() {
      return _string;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictionaryStrings = new String[dictionary.getMaxId() + 1];
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      String value = _dictionaryStrings[dictionaryId];
      if (value == null) {
        value = _dictionary.decodeToBinary(dictionaryId).toStringUsingUTF8();
        _dictionaryStrings[dictionaryId] = value;
      }
      _sink.accept(value);
    }

    @Override
    public void addBinary(Binary value) {
      _sink.accept(_string ? value.toStringUsingUTF8() : value.getBytes());
    }

    @Override
    public void addBoolean(boolean value) {
      _sink.accept(value);
    }

    @Override
    public void addInt(int value) {
      _sink.accept(value);
    }

    @Override
    public void addLong(long value) {
      if (_timestampUnit != null && !_extractRawTimeValues) {
        _sink.accept(ParquetUtils.convertLongToTimestamp(value, _timestampUnit));
      } else {
        _sink.accept(value);
      }
    }

    @Override
    public void addFloat(float value) {
      _sink.accept(value);
    }

    @Override
    public void addDouble(double value) {
      _sink.accept(value);
    }
  }

  private final class MapConverter extends GroupConverter {
    private final Consumer<Object> _sink;
    private final GroupConverter _entryConverter;
    private Map<String, Object> _values;
    private Object _key;
    private Object _value;

    MapConverter(GroupType type, Consumer<Object> sink) {
      _sink = sink;
      GroupType entryType = type.getType(0).asGroupType();
      int keyIndex = entryType.getFieldIndex("key");
      Converter[] converters = new Converter[2];
      converters[keyIndex] = createConverter(entryType.getType(keyIndex), value -> _key = value);
      converters[1 - keyIndex] = createConverter(entryType.getType(1 - keyIndex), value -> _value = value);
      _entryConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return converters[fieldIndex];
        }

        @Override
        public void start() {
          _key = null;
          _value = null;
        }

        @Override
        public void end() {
          if (_values == null) {
            _values = new HashMap<>();
          }
          _values.put(BaseRecordExtractor.stringifyMapKey(_key), _value);
        }
      };
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return _entryConverter;
    }

    @Override
    public void start() {
      _values = null;
    }

    @Override
    public void end() {
      _sink.accept(_values == null ? Map.of() : _values);
    }
  }

  /// Owns token vectors, never decoder scratch. Dictionary IDs are meaningful only within the converter's epoch.
  private static final class JsonMapSequence {
    private final int[] _ids;
    private final String[] _literals;
    private int _length;
    private int _hash;

    JsonMapSequence(int[] ids, String[] literals, int length, int hash) {
      _ids = ids;
      _literals = literals;
      _length = length;
      _hash = hash;
    }

    @Override
    public int hashCode() {
      return _hash;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof JsonMapSequence sequence) || _length != sequence._length || _hash != sequence._hash) {
        return false;
      }
      for (int i = 0; i < _length; i++) {
        if (_ids[i] != sequence._ids[i]
            || _ids[i] < 0 && !_literals[i].equals(sequence._literals[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /// Converts required String MAPs once per repeated token sequence, including mixed dictionary and PLAIN pages.
  /// Oversized sequences retain the bounded logical-map cache path; no row's mutable map or scratch is retained.
  private static final class CanonicalJsonMapConverter extends GroupConverter {
    private static final int MAX_TOKENS = 512;
    private static final int CAPACITY = 4096;
    private static final long MAX_RETAINED_WEIGHT = 4L * 1024 * 1024;
    private static final long MAX_ENTRY_WEIGHT = 256L * 1024;
    private final Consumer<Object> _sink;
    private final JsonStringConverter _key = new JsonStringConverter();
    private final JsonStringConverter _value = new JsonStringConverter();
    private final int[] _ids = new int[MAX_TOKENS];
    private final String[] _literals = new String[MAX_TOKENS];
    private final JsonMapSequence _lookup = new JsonMapSequence(_ids, _literals, 0, 1);
    private final LinkedHashMap<Object, CachedJson> _entries = new LinkedHashMap<>(CAPACITY, 0.75f, true);
    private final GroupConverter _entryConverter;
    @Nullable
    private Map<String, Object> _fallback;
    private int _length;
    private int _hash;
    private long _retainedWeight;

    CanonicalJsonMapConverter(GroupType type, Consumer<Object> sink) {
      _sink = sink;
      int keyIndex = type.getType(0).asGroupType().getFieldIndex("key");
      _entryConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return fieldIndex == keyIndex ? _key : _value;
        }

        @Override
        public void start() {
          _key.reset();
          _value.reset();
        }

        @Override
        public void end() {
          if (_fallback == null && _length == MAX_TOKENS) {
            _fallback = materialize();
            Arrays.fill(_literals, 0, _length, null);
            _length = 0;
          }
          if (_fallback != null) {
            _fallback.put(_key.resolve(_key._id, _key._literal), _value.resolve(_value._id, _value._literal));
          } else {
            append(_key);
            append(_value);
          }
        }
      };
    }

    private void append(JsonStringConverter converter) {
      _ids[_length] = converter._id;
      _literals[_length++] = converter._literal;
      _hash = 31 * _hash + (converter._id >= 0 ? converter._id : Objects.requireNonNull(converter._literal).hashCode());
    }

    private Map<String, Object> materialize() {
      Map<String, Object> map = new HashMap<>();
      for (int i = 0; i < _length; i += 2) {
        map.put(_key.resolve(_ids[i], _literals[i]), _value.resolve(_ids[i + 1], _literals[i + 1]));
      }
      return map;
    }

    void resetRowGroup() {
      clearEntries();
      _key.clearDictionary();
      _value.clearDictionary();
      Arrays.fill(_literals, null);
      _fallback = null;
      _length = 0;
    }

    private void clearEntries() {
      _entries.clear();
      _retainedWeight = 0;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return _entryConverter;
    }

    @Override
    public void start() {
      _fallback = null;
      _length = 0;
      _hash = 1;
    }

    @Override
    public void end() {
      try {
        if (_fallback == null && _length == 0) {
          _sink.accept("{}");
          return;
        }
        _lookup._length = _length;
        _lookup._hash = _hash;
        CachedJson cached = _entries.get(_fallback != null ? _fallback : _lookup);
        if (cached != null) {
          _sink.accept(cached.canonical());
          return;
        }
        Map<String, Object> map = _fallback != null ? _fallback : materialize();
        String canonical = MapUtils.toString(map);
        long weight = 256L + 2L * canonical.length();
        if (_fallback != null) {
          weight += 256L * map.size();
          for (Map.Entry<String, Object> entry : map.entrySet()) {
            weight += 2L * entry.getKey().length() + 2L * ((String) entry.getValue()).length();
          }
        } else {
          weight += 16L * _length;
          for (int i = 0; i < _length; i++) {
            if (_ids[i] < 0) {
              weight += 64L + 2L * _literals[i].length();
            }
          }
        }
        if (weight <= MAX_ENTRY_WEIGHT) {
          Object key = _fallback != null ? Map.copyOf(map)
              : new JsonMapSequence(Arrays.copyOf(_ids, _length), Arrays.copyOf(_literals, _length), _length, _hash);
          _entries.put(key, new CachedJson(canonical, weight));
          _retainedWeight += weight;
          while (_entries.size() > CAPACITY || _retainedWeight > MAX_RETAINED_WEIGHT) {
            var iterator = _entries.entrySet().iterator();
            _retainedWeight -= iterator.next().getValue().weight();
            iterator.remove();
          }
        }
        _sink.accept(canonical);
      } finally {
        Arrays.fill(_literals, 0, _length, null);
        _fallback = null;
        _length = 0;
        _key.reset();
        _value.reset();
      }
    }

    private record CachedJson(String canonical, long weight) {
    }

    /// Dictionary IDs avoid decoding/hash work on hits; PLAIN strings participate in the same sequence cache.
    private final class JsonStringConverter extends PrimitiveConverter {
      @Nullable
      private Dictionary _dictionary;
      @Nullable
      private String[] _strings;
      @Nullable
      private String _literal;
      private int _id = -1;

      @Override
      public boolean hasDictionarySupport() {
        return true;
      }

      @Override
      public void setDictionary(Dictionary dictionary) {
        clearEntries();
        _dictionary = dictionary;
        _strings = new String[dictionary.getMaxId() + 1];
      }

      @Override
      public void addValueFromDictionary(int dictionaryId) {
        _id = dictionaryId;
        _literal = null;
      }

      @Override
      public void addBinary(Binary value) {
        _id = -1;
        _literal = value.toStringUsingUTF8();
      }

      void reset() {
        _id = -1;
        _literal = null;
      }

      void clearDictionary() {
        _dictionary = null;
        _strings = null;
        reset();
      }

      String resolve(int id, @Nullable String literal) {
        if (id < 0) {
          return Objects.requireNonNull(literal, "Missing required MAP string");
        }
        String[] strings = Objects.requireNonNull(_strings, "Missing MAP dictionary");
        String value = strings[id];
        if (value == null) {
          value = Objects.requireNonNull(_dictionary).decodeToBinary(id).toStringUsingUTF8();
          strings[id] = value;
        }
        return value;
      }
    }
  }

  private final class ListConverter extends GroupConverter {
    private final Consumer<Object> _sink;
    private final GroupConverter _elementConverter;
    private List<Object> _values;
    private Object _element;

    ListConverter(GroupType type, Consumer<Object> sink) {
      _sink = sink;
      Converter converter = createConverter(type.getType(0).asGroupType().getType(0), value -> _element = value);
      _elementConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return converter;
        }

        @Override
        public void start() {
          _element = null;
        }

        @Override
        public void end() {
          _values.add(_element);
        }
      };
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return _elementConverter;
    }

    @Override
    public void start() {
      _values = new ArrayList<>();
    }

    @Override
    public void end() {
      _sink.accept(_values.toArray());
    }
  }
}
