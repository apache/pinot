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
package org.apache.pinot.common.utils.helix;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A [ZkSerializer] that reads gzip-compressed ideal state and external view znodes with less allocation than
/// [ZNRecordSerializer], and returns an equal [ZNRecord].
///
/// Only gzip-compressed znodes use the compact parser. A plain (uncompressed) znode goes to [ZNRecordSerializer]
/// unchanged. Pinot compresses large table ideal states, so plain znodes are the small ones, and there the compact
/// parser does not pay off: in a size sweep it allocated about 1x to 3.5x as much and used 1x to 1.5x the CPU of
/// [ZNRecordSerializer] up to about 200 segments. On gzip-compressed znodes of any size it allocated about 0.06x to
/// 0.3x as much and used about 0.25x to 0.75x the CPU.
///
/// Compared with [ZNRecordSerializer#deserialize], for a gzip-compressed znode:
/// - It streams a gzip-compressed znode through a [GZIPInputStream] straight into the JSON parser. It does not
///   inflate the whole payload into a growing byte array first.
/// - Its [JsonFactory] does not canonicalize or intern field names, so the parser does not build a symbol table with
///   one entry per segment.
/// - Instance names (the keys of the instance-state maps and the values of the list fields) are interned once per
///   parse. Routing keys its server maps by interned instance ids, so lookups keep their identity fast path.
/// - State values come from a fixed table of the known segment states. An unknown state is interned.
/// - Segment names (the keys of the map fields) are interned, so they keep their identity with the names that the
///   routing components already hold.
/// - Equal instance-state maps within one znode share one [ImmutableSortedArrayMap]. The outer segment map is an
///   [ImmutableSortedArrayMap] too.
///
/// The map fields of a record from the compact parser are immutable: any mutation throws
/// [UnsupportedOperationException]. Simple fields and list fields are mutable [LinkedHashMap]s, as with
/// [ZNRecordSerializer]. Scalar setters of the record (version, creation time, etc.) work as usual.
///
/// When the compact parser fails, the serializer logs a warning and falls back to [ZNRecordSerializer]. [#serialize]
/// always delegates to [ZNRecordSerializer].
///
/// [HelixHelper#toIdealState] and [HelixHelper#toExternalView] wrap a record from the compact parser without the copy
/// of the outer segment map that `new IdealState(record)` and `new ExternalView(record)` make. Those constructors
/// share the inner instance-state maps, so an [org.apache.helix.model.IdealState] or
/// [org.apache.helix.model.ExternalView] built from a compact record has immutable instance-state maps either way.
/// A record from [ZNRecordSerializer] (a plain znode or the fallback) still goes through those constructors.
///
/// Thread-safety: the serializer is stateless apart from the shared [JsonFactory], which is thread-safe. Each call
/// uses its own parser and dictionaries, so concurrent calls are safe.
public class CompactZNRecordSerializer implements ZkSerializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactZNRecordSerializer.class);

  private static final JsonFactory JSON_FACTORY = JsonFactory.builder()
      .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
      .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
      .build();

  /// Jackson has its own input buffer, so the inflater only needs a small one.
  private static final int GZIP_BUFFER_SIZE = 2 * 1024;
  /// Small, so that the fixed cost per parse stays low; the arrays grow geometrically.
  private static final int INITIAL_SEGMENT_CAPACITY = 16;
  private static final int INITIAL_STATE_MAP_TABLE_CAPACITY = 16;
  private static final int INITIAL_INSTANCE_CAPACITY = 8;

  /// The states a segment replica can have. The constants are string literals, so they are already interned.
  private static final String[] KNOWN_STATES = {
      SegmentStateModel.ONLINE, SegmentStateModel.CONSUMING, SegmentStateModel.OFFLINE, SegmentStateModel.ERROR,
      "DROPPED"
  };

  private final ZNRecordSerializer _delegate;

  public CompactZNRecordSerializer() {
    this(new ZNRecordSerializer());
  }

  public CompactZNRecordSerializer(ZNRecordSerializer delegate) {
    _delegate = delegate;
  }

  @Override
  public byte[] serialize(Object data) {
    return _delegate.serialize(data);
  }

  @Nullable
  @Override
  public Object deserialize(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      // Reading a parent or empty node, same as ZNRecordSerializer
      return null;
    }
    if (!GZipCompressionUtil.isCompressed(bytes)) {
      // Plain znodes are small; the default deserializer is cheaper there (see the class documentation)
      return _delegate.deserialize(bytes);
    }
    try {
      return deserializeCompact(bytes);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while deserializing ZNRecord with the compact reader, falling back to the "
          + "default deserializer", e);
      return _delegate.deserialize(bytes);
    }
  }

  /// Parses the given znode content, gzip-compressed or plain, with the compact parser. Throws on malformed input;
  /// [#deserialize] then falls back. [#deserialize] only passes gzip-compressed content; tests and benchmarks also
  /// call it with plain content.
  static ZNRecord deserializeCompact(byte[] bytes)
      throws IOException {
    if (!GZipCompressionUtil.isCompressed(bytes)) {
      try (JsonParser parser = JSON_FACTORY.createParser(bytes)) {
        return new Reader(parser).readRecord();
      }
    }
    GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes), GZIP_BUFFER_SIZE);
    try (JsonParser parser = JSON_FACTORY.createParser(gzipInputStream)) {
      ZNRecord record = new Reader(parser).readRecord();
      // The parser stops at the end of the record. GZIPInputStream verifies the CRC-32 and the size in the gzip
      // trailer only when it reaches the end of the stream, so read the rest of the stream (as
      // GZipCompressionUtil.uncompress does). A bad trailer throws, and deserialize falls back.
      gzipInputStream.transferTo(OutputStream.nullOutputStream());
      return record;
    }
  }

  /// The state of one parse: the parser, the dictionaries, and scratch buffers. Not thread-safe; one per call.
  private static final class Reader {
    private final JsonParser _parser;
    private final HashMap<String, String> _instanceNames = new HashMap<>();
    private final StateMapTable _stateMaps = new StateMapTable();
    private String[] _instanceKeys = new String[INITIAL_INSTANCE_CAPACITY];
    private Object[] _instanceStates = new Object[INITIAL_INSTANCE_CAPACITY];

    Reader(JsonParser parser) {
      _parser = parser;
    }

    ZNRecord readRecord()
        throws IOException {
      expect(_parser.nextToken(), JsonToken.START_OBJECT);
      String id = null;
      Map<String, String> simpleFields = null;
      Map<String, List<String>> listFields = null;
      Map<String, Map<String, String>> mapFields = null;
      byte[] rawPayload = null;
      boolean hasSimpleFields = false;
      boolean hasListFields = false;
      boolean hasMapFields = false;
      JsonToken token;
      while ((token = _parser.nextToken()) == JsonToken.FIELD_NAME) {
        String name = _parser.currentName();
        token = _parser.nextToken();
        switch (name) {
          case "id":
            id = token == JsonToken.VALUE_NULL ? null : scalarText(token);
            break;
          case "simpleFields":
            hasSimpleFields = true;
            simpleFields = token == JsonToken.VALUE_NULL ? null : readSimpleFields(token);
            break;
          case "listFields":
            hasListFields = true;
            listFields = token == JsonToken.VALUE_NULL ? null : readListFields(token);
            break;
          case "mapFields":
            hasMapFields = true;
            mapFields = token == JsonToken.VALUE_NULL ? null : readMapFields(token);
            break;
          case "rawPayload":
            rawPayload = token == JsonToken.VALUE_NULL ? null : _parser.getBinaryValue();
            break;
          default:
            // ZNRecord ignores unknown properties
            _parser.skipChildren();
            break;
        }
      }
      // Like ObjectMapper.readValue (FAIL_ON_TRAILING_TOKENS is off), content after the record is ignored
      expect(token, JsonToken.END_OBJECT);

      ZNRecord record = new ZNRecord(id);
      if (hasSimpleFields) {
        record.setSimpleFields(simpleFields);
      }
      if (hasListFields) {
        record.setListFields(listFields);
      }
      if (hasMapFields) {
        record.setMapFields(mapFields);
      }
      if (rawPayload != null) {
        record.setRawPayload(rawPayload);
      }
      return record;
    }

    private Map<String, String> readSimpleFields(JsonToken token)
        throws IOException {
      expect(token, JsonToken.START_OBJECT);
      Map<String, String> simpleFields = new LinkedHashMap<>();
      while ((token = _parser.nextToken()) == JsonToken.FIELD_NAME) {
        // ZNRecordSerializer interns all field names
        String key = _parser.currentName().intern();
        token = _parser.nextToken();
        simpleFields.put(key, token == JsonToken.VALUE_NULL ? null : scalarText(token));
      }
      expect(token, JsonToken.END_OBJECT);
      return simpleFields;
    }

    private Map<String, List<String>> readListFields(JsonToken token)
        throws IOException {
      expect(token, JsonToken.START_OBJECT);
      Map<String, List<String>> listFields = new LinkedHashMap<>();
      while ((token = _parser.nextToken()) == JsonToken.FIELD_NAME) {
        String key = _parser.currentName().intern();
        token = _parser.nextToken();
        if (token == JsonToken.VALUE_NULL) {
          listFields.put(key, null);
          continue;
        }
        expect(token, JsonToken.START_ARRAY);
        List<String> list = new ArrayList<>();
        while ((token = _parser.nextToken()) != JsonToken.END_ARRAY) {
          // List fields hold instance names (e.g. the preference lists of SEMI_AUTO resources)
          list.add(token == JsonToken.VALUE_NULL ? null : instanceName(scalarText(token)));
        }
        listFields.put(key, list);
      }
      expect(token, JsonToken.END_OBJECT);
      return listFields;
    }

    private Map<String, Map<String, String>> readMapFields(JsonToken token)
        throws IOException {
      expect(token, JsonToken.START_OBJECT);
      String[] segments = new String[INITIAL_SEGMENT_CAPACITY];
      Object[] stateMaps = new Object[INITIAL_SEGMENT_CAPACITY];
      int numSegments = 0;
      while ((token = _parser.nextToken()) == JsonToken.FIELD_NAME) {
        String segment = _parser.currentName().intern();
        token = _parser.nextToken();
        Map<String, String> stateMap = token == JsonToken.VALUE_NULL ? null : readStateMap(token);
        if (numSegments == segments.length) {
          int newCapacity = segments.length * 2;
          segments = Arrays.copyOf(segments, newCapacity);
          stateMaps = Arrays.copyOf(stateMaps, newCapacity);
        }
        segments[numSegments] = segment;
        stateMaps[numSegments] = stateMap;
        numSegments++;
      }
      expect(token, JsonToken.END_OBJECT);
      return ImmutableSortedArrayMap.sortAndWrap(segments, stateMaps, numSegments);
    }

    private Map<String, String> readStateMap(JsonToken token)
        throws IOException {
      expect(token, JsonToken.START_OBJECT);
      int size = 0;
      while ((token = _parser.nextToken()) == JsonToken.FIELD_NAME) {
        String instance = instanceName(_parser.currentName());
        token = _parser.nextToken();
        String state = token == JsonToken.VALUE_NULL ? null : stateValue(token);
        if (size == _instanceKeys.length) {
          _instanceKeys = Arrays.copyOf(_instanceKeys, size * 2);
          _instanceStates = Arrays.copyOf(_instanceStates, size * 2);
        }
        _instanceKeys[size] = instance;
        _instanceStates[size] = state;
        size++;
      }
      expect(token, JsonToken.END_OBJECT);
      size = sortInPlace(_instanceKeys, _instanceStates, size);
      return _stateMaps.intern(_instanceKeys, _instanceStates, size);
    }

    /// Returns the canonical (interned) instance name equal to the given name.
    private String instanceName(String name) {
      String canonical = _instanceNames.get(name);
      if (canonical == null) {
        canonical = name.intern();
        _instanceNames.put(canonical, canonical);
      }
      return canonical;
    }

    /// Returns the canonical state for the current value token without allocating for the known states.
    private String stateValue(JsonToken token)
        throws IOException {
      if (token != JsonToken.VALUE_STRING) {
        return scalarText(token).intern();
      }
      char[] chars = _parser.getTextCharacters();
      int offset = _parser.getTextOffset();
      int length = _parser.getTextLength();
      for (String knownState : KNOWN_STATES) {
        if (matches(knownState, chars, offset, length)) {
          return knownState;
        }
      }
      return new String(chars, offset, length).intern();
    }

    /// Returns the text of a scalar token. Like Jackson's `Map<String, String>` binding, numbers and booleans become
    /// their text, and objects or arrays fail.
    private String scalarText(JsonToken token)
        throws IOException {
      if (!token.isScalarValue()) {
        throw new JsonParseException(_parser, "Expected a scalar value but got: " + token);
      }
      return _parser.getText();
    }

    private void expect(@Nullable JsonToken actual, JsonToken expected)
        throws JsonParseException {
      if (actual != expected) {
        throw new JsonParseException(_parser, "Expected " + expected + " but got: " + actual);
      }
    }
  }

  private static boolean matches(String s, char[] chars, int offset, int length) {
    if (s.length() != length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (s.charAt(i) != chars[offset + i]) {
        return false;
      }
    }
    return true;
  }

  /// Sorts the first `size` entries of the parallel arrays by key with an insertion sort (instance-state maps hold a
  /// handful of replicas). When a key appears more than once, the last value wins. Returns the number of distinct
  /// keys, which are compacted at the start of the arrays.
  static int sortInPlace(String[] keys, Object[] values, int size) {
    int numUnique = 0;
    for (int i = 0; i < size; i++) {
      String key = keys[i];
      Object value = values[i];
      int low = 0;
      int high = numUnique - 1;
      int found = -1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int cmp = keys[mid].compareTo(key);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          found = mid;
          break;
        }
      }
      if (found >= 0) {
        values[found] = value;
        continue;
      }
      System.arraycopy(keys, low, keys, low + 1, numUnique - low);
      System.arraycopy(values, low, values, low + 1, numUnique - low);
      keys[low] = key;
      values[low] = value;
      numUnique++;
    }
    return numUnique;
  }

  /// Deduplicates instance-state maps by content within one parse. Keys and values passed in are canonical (interned
  /// or from the known state table), so content equality is reference equality element by element.
  private static final class StateMapTable {
    private ImmutableSortedArrayMap<?>[] _maps = new ImmutableSortedArrayMap<?>[INITIAL_STATE_MAP_TABLE_CAPACITY];
    private int[] _hashes = new int[INITIAL_STATE_MAP_TABLE_CAPACITY];
    private int _size;

    @SuppressWarnings("unchecked")
    Map<String, String> intern(String[] keys, Object[] values, int size) {
      int hash = 1;
      for (int i = 0; i < size; i++) {
        hash = 31 * hash + keys[i].hashCode();
        hash = 31 * hash + (values[i] == null ? 0 : values[i].hashCode());
      }
      // Avoid 0, which marks an empty slot
      hash = hash == 0 ? 1 : hash;
      int mask = _maps.length - 1;
      int slot = mix(hash) & mask;
      while (_maps[slot] != null) {
        if (_hashes[slot] == hash && sameContent(_maps[slot], keys, values, size)) {
          return (Map<String, String>) _maps[slot];
        }
        slot = (slot + 1) & mask;
      }
      ImmutableSortedArrayMap<String> map = size == 0 ? ImmutableSortedArrayMap.empty()
          : new ImmutableSortedArrayMap<>(Arrays.copyOf(keys, size), Arrays.copyOf(values, size));
      _maps[slot] = map;
      _hashes[slot] = hash;
      if (++_size * 2 > _maps.length) {
        resize();
      }
      return map;
    }

    private static boolean sameContent(ImmutableSortedArrayMap<?> map, String[] keys, Object[] values, int size) {
      if (map.size() != size) {
        return false;
      }
      for (int i = 0; i < size; i++) {
        if (map.keyAt(i) != keys[i] || map.valueAt(i) != values[i]) {
          return false;
        }
      }
      return true;
    }

    private void resize() {
      ImmutableSortedArrayMap<?>[] oldMaps = _maps;
      int[] oldHashes = _hashes;
      _maps = new ImmutableSortedArrayMap<?>[oldMaps.length * 2];
      _hashes = new int[oldMaps.length * 2];
      int mask = _maps.length - 1;
      for (int i = 0; i < oldMaps.length; i++) {
        if (oldMaps[i] != null) {
          int slot = mix(oldHashes[i]) & mask;
          while (_maps[slot] != null) {
            slot = (slot + 1) & mask;
          }
          _maps[slot] = oldMaps[i];
          _hashes[slot] = oldHashes[i];
        }
      }
    }

    private static int mix(int hash) {
      return hash ^ (hash >>> 16);
    }
  }
}
