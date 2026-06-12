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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;


/// Extracts Pinot [GenericRow] from a ProtoBuf [Message].
///
/// **Proto source type → Java input → Java output type:**
/// - `bool` → `Boolean` → `Boolean`
/// - `int32` / `sint32` / `sfixed32` / `uint32` / `fixed32` → `Integer` → `Integer`
/// - `int64` / `sint64` / `sfixed64` / `uint64` / `fixed64` → `Long` → `Long`
/// - `float` → `Float` → `Float`
/// - `double` → `Double` → `Double`
/// - `string` → `String` → `String`
/// - `bytes` → [ByteString] → `byte[]` (via [ByteString#toByteArray])
/// - `enum` → [Descriptors.EnumValueDescriptor] → enum constant name `String`
/// - `message` → [Message] → `Map<String, Object>` (recursive over the message's set fields)
/// - `repeated X` → `List<X>` → `Object[]` (each element recursively converted)
/// - `map<K, V>` → `Map<K, V>` → `Map<String, Object>` (keys stringified via
///   [BaseRecordExtractor#stringifyMapKey], values recursively converted)
/// - proto3 `optional` field that is unset / cleared → `null`
public class ProtoBufRecordExtractor extends BaseRecordExtractor<Message> {

  // Cached field descriptors initialized lazily on first message extraction to avoid repeated lookups
  // via findFieldByName. The cache is invalidated when the descriptor's full name changes (handles schema
  // evolution within a single reader).
  private String _descriptorFullName;
  private Descriptors.FieldDescriptor[] _fieldDescriptors;
  private String[] _fieldNames;

  /// Initializes the field descriptor cache from the message descriptor. Called on the first message and when
  /// schema changes are detected (compared via the descriptor's full name). For the include-list path, fields
  /// not present in the descriptor are skipped — `DataTypeTransformer` back-fills nulls downstream — so the
  /// cache only contains live descriptors and the per-row loop never has to null-check.
  private void initFieldDescriptors(Descriptors.Descriptor descriptor) {
    if (_extractAll) {
      List<Descriptors.FieldDescriptor> fieldDescriptors = descriptor.getFields();
      int numFields = fieldDescriptors.size();
      _fieldDescriptors = new Descriptors.FieldDescriptor[numFields];
      _fieldNames = new String[numFields];
      for (int i = 0; i < numFields; i++) {
        Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
        _fieldDescriptors[i] = fieldDescriptor;
        _fieldNames[i] = fieldDescriptor.getName();
      }
    } else {
      int numFields = _fields.size();
      Descriptors.FieldDescriptor[] fieldDescriptors = new Descriptors.FieldDescriptor[numFields];
      String[] fieldNames = new String[numFields];
      int i = 0;
      for (String fieldName : _fields) {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
        if (fieldDescriptor != null) {
          fieldDescriptors[i] = fieldDescriptor;
          fieldNames[i] = fieldName;
          i++;
        }
      }
      _fieldDescriptors = i < numFields ? Arrays.copyOf(fieldDescriptors, i) : fieldDescriptors;
      _fieldNames = i < numFields ? Arrays.copyOf(fieldNames, i) : fieldNames;
    }
    _descriptorFullName = descriptor.getFullName();
  }

  @Override
  public GenericRow extract(Message from, GenericRow to) {
    Descriptors.Descriptor descriptor = from.getDescriptorForType();
    // Initialize or reinitialize cache if descriptor changed (handles schema evolution).
    if (_descriptorFullName == null || !_descriptorFullName.equals(descriptor.getFullName())) {
      initFieldDescriptors(descriptor);
    }
    // The cache only contains live descriptors — fields requested but not in the source schema were filtered
    // out at cache-build time.
    int numFields = _fieldDescriptors.length;
    for (int i = 0; i < numFields; i++) {
      Descriptors.FieldDescriptor fd = _fieldDescriptors[i];
      Object value = getFieldValue(fd, from);
      to.putValue(_fieldNames[i], value != null ? extractValue(fd, value) : null);
    }
    return to;
  }

  /// Reads the field value from `message`, returning `null` for unset / cleared optional fields (so they
  /// surface as `null` instead of the proto default). See the
  /// [field-presence docs](https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md).
  @Nullable
  private static Object getFieldValue(Descriptors.FieldDescriptor fd, Message message) {
    return !fd.hasPresence() || message.hasField(fd) ? message.getField(fd) : null;
  }

  /// Dispatches a non-null protobuf field value off `fd`'s shape:
  /// - `map<K, V>` → [#extractMap] → `Map<String, Object>`
  /// - `repeated X` → [#extractList] → `Object[]`
  /// - scalar / message → [#extractSingleValue]
  private static Object extractValue(Descriptors.FieldDescriptor fd, Object value) {
    if (fd.isMapField()) {
      //noinspection unchecked
      return extractMap(fd, (Collection<Message>) value);
    }
    if (fd.isRepeated()) {
      return extractList(fd, (Collection<?>) value);
    }
    return extractSingleValue(fd, value);
  }

  /// Converts one non-null scalar / message value to its Pinot type per the matrix on the class Javadoc.
  private static Object extractSingleValue(Descriptors.FieldDescriptor fd, Object value) {
    switch (fd.getJavaType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return value;
      case BYTE_STRING:
        return ((ByteString) value).toByteArray();
      case ENUM:
        return value.toString();
      case MESSAGE:
        return extractMessage((Message) value);
      default:
        throw new IllegalStateException("Unsupported ProtoBuf type: " + fd.getJavaType());
    }
  }

  /// Converts a `repeated X` field's [List] of values to `Object[]`, recursively converting each element.
  private static Object[] extractList(Descriptors.FieldDescriptor fd, Collection<?> values) {
    Object[] result = new Object[values.size()];
    int i = 0;
    for (Object value : values) {
      result[i++] = value != null ? extractSingleValue(fd, value) : null;
    }
    return result;
  }

  /// Converts a `map<K, V>` field's [List] of entry messages to `Map<String, Object>`. Each entry message
  /// has exactly two fields (key at index 0, value at index 1) per the protobuf encoding. By the protobuf
  /// map spec, both keys and values must always be present, so no null-guard is needed on either. Keys are
  /// stringified via [BaseRecordExtractor#stringifyMapKey] per the `Map<String, Object>` contract — protobuf
  /// allows numeric / bool / string key types, all of which have a stable `toString()`.
  private static Map<String, Object> extractMap(Descriptors.FieldDescriptor fd, Collection<Message> entries) {
    List<Descriptors.FieldDescriptor> entryFields = fd.getMessageType().getFields();
    Descriptors.FieldDescriptor keyFd = entryFields.get(0);
    Descriptors.FieldDescriptor valueFd = entryFields.get(1);
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(entries.size());
    for (Message entry : entries) {
      Object key = extractSingleValue(keyFd, entry.getField(keyFd));
      Object value = extractSingleValue(valueFd, entry.getField(valueFd));
      map.put(BaseRecordExtractor.stringifyMapKey(key), value);
    }
    return map;
  }

  /// Converts a nested [Message] to `Map<String, Object>` keyed by field name. Iterates only the message's
  /// set fields ([Message#getAllFields]) so unset optional / default fields don't pollute the map. Values
  /// from `getAllFields()` are always non-null per the protobuf API.
  private static Map<String, Object> extractMessage(Message message) {
    Map<Descriptors.FieldDescriptor, Object> setFields = message.getAllFields();
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(setFields.size());
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : setFields.entrySet()) {
      Descriptors.FieldDescriptor fd = entry.getKey();
      map.put(fd.getName(), extractValue(fd, entry.getValue()));
    }
    return map;
  }
}
