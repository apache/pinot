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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for ProtoBuf records
 *
 * <p>Performance optimizations:
 * <ul>
 *   <li>Field descriptors are cached during initialization to avoid repeated lookups</li>
 *   <li>A reusable ProtoBufFieldInfo instance is used to reduce object allocation</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class ProtoBufRecordExtractor extends BaseRecordExtractor<Message> {

  private Set<String> _fields;
  private boolean _extractAll = false;

  // Cached field descriptors to avoid repeated lookups via findFieldByName
  private Descriptors.FieldDescriptor[] _cachedFieldDescriptors;
  private String[] _cachedFieldNames;
  // Store the descriptor's full name to detect schema changes
  private String _cachedDescriptorFullName;

  // Reusable ProtoBufFieldInfo to reduce object allocation
  private final ProtoBufFieldInfo _reusableFieldInfo = new ProtoBufFieldInfo(null, null);

  @Override
  public void init(@Nullable Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Set.of();
    } else {
      _extractAll = false;
      _fields = Set.copyOf(fields);
    }
    // Reset cache state for re-initialization
    _cachedDescriptorFullName = null;
    _cachedFieldDescriptors = null;
    _cachedFieldNames = null;
  }

  /**
   * Initializes the field descriptor cache from the message descriptor.
   * This is called on the first message and when schema changes are detected.
   *
   * <p>Schema changes are detected by comparing the descriptor's full name.
   * This handles:
   * <ul>
   *   <li>Different message types (should not happen in normal use)</li>
   *   <li>Schema evolution scenarios where descriptor might change</li>
   * </ul>
   */
  private void initFieldDescriptorCache(Descriptors.Descriptor descriptor) {
    if (_extractAll) {
      List<Descriptors.FieldDescriptor> fieldsList = descriptor.getFields();
      int numFields = fieldsList.size();
      _cachedFieldDescriptors = new Descriptors.FieldDescriptor[numFields];
      _cachedFieldNames = new String[numFields];
      for (int i = 0; i < numFields; i++) {
        Descriptors.FieldDescriptor fd = fieldsList.get(i);
        _cachedFieldDescriptors[i] = fd;
        _cachedFieldNames[i] = fd.getName();
      }
    } else {
      int numFields = _fields.size();
      _cachedFieldDescriptors = new Descriptors.FieldDescriptor[numFields];
      _cachedFieldNames = new String[numFields];
      int i = 0;
      for (String fieldName : _fields) {
        _cachedFieldDescriptors[i] = descriptor.findFieldByName(fieldName);
        _cachedFieldNames[i] = fieldName;
        i++;
      }
    }
    _cachedDescriptorFullName = descriptor.getFullName();
  }

  /**
   * For fields that are not set, we want to populate a null, instead of proto default.
   */
  private Object getFieldValue(Descriptors.FieldDescriptor fieldDescriptor, Message message) {
    // In order to support null, the field needs to support _field presence_
    // See https://github.com/protocolbuffers/protobuf/blob/main/docs/field_presence.md
    // or FieldDescriptor#hasPresence()
    if (!fieldDescriptor.hasPresence() || message.hasField(fieldDescriptor)) {
      return message.getField(fieldDescriptor);
    } else {
      return null;
    }
  }

  @Override
  public GenericRow extract(Message from, GenericRow to) {
    Descriptors.Descriptor descriptor = from.getDescriptorForType();

    // Initialize or reinitialize cache if descriptor changed (handles schema evolution)
    if (_cachedDescriptorFullName == null || !_cachedDescriptorFullName.equals(descriptor.getFullName())) {
      initFieldDescriptorCache(descriptor);
    }

    // Use cached field descriptors to avoid repeated lookups
    int numFields = _cachedFieldDescriptors.length;
    for (int i = 0; i < numFields; i++) {
      Descriptors.FieldDescriptor fieldDescriptor = _cachedFieldDescriptors[i];
      String fieldName = _cachedFieldNames[i];

      Object fieldValue;
      if (fieldDescriptor == null) {
        // Field not found in descriptor (only possible in non-extractAll mode)
        fieldValue = null;
      } else {
        fieldValue = getFieldValue(fieldDescriptor, from);
        if (fieldValue != null) {
          // Reuse ProtoBufFieldInfo to avoid object allocation
          _reusableFieldInfo.setFieldValue(fieldValue);
          _reusableFieldInfo.setFieldDescriptor(fieldDescriptor);
          fieldValue = convert(_reusableFieldInfo);
        }
      }
      to.putValue(fieldName, fieldValue);
    }
    return to;
  }

  /**
   * Returns whether the object is a ProtoBuf Message.
   */
  @Override
  protected boolean isRecord(Object value) {
    return ((ProtoBufFieldInfo) value).getFieldValue() instanceof Message;
  }

  /**
   * Returns whether the field is a multi-value type.
   */
  @Override
  protected boolean isMultiValue(Object value) {
    ProtoBufFieldInfo protoBufFieldInfo = (ProtoBufFieldInfo) value;
    return protoBufFieldInfo.getFieldValue() instanceof Collection && !protoBufFieldInfo.getFieldDescriptor()
        .isMapField();
  }

  /**
   * Returns whether the field is a map type.
   */
  @Override
  protected boolean isMap(Object value) {
    ProtoBufFieldInfo protoBufFieldInfo = (ProtoBufFieldInfo) value;
    return protoBufFieldInfo.getFieldValue() instanceof Collection && protoBufFieldInfo.getFieldDescriptor()
        .isMapField();
  }

  /**
   * Handles the conversion of every value in the ProtoBuf map.
   *
   * @param value should be verified to contain a ProtoBuf map prior to calling this method as it will be handled
   *              as a map field without checking
   */
  @Override
  protected Map<Object, Object> convertMap(Object value) {
    ProtoBufFieldInfo protoBufFieldInfo = (ProtoBufFieldInfo) value;
    List<Descriptors.FieldDescriptor> fieldDescriptors =
        protoBufFieldInfo.getFieldDescriptor().getMessageType().getFields();
    Descriptors.FieldDescriptor keyDescriptor = fieldDescriptors.get(0);
    Descriptors.FieldDescriptor valueDescriptor = fieldDescriptors.get(1);
    Collection<Message> messages = (Collection<Message>) protoBufFieldInfo.getFieldValue();
    Map<Object, Object> convertedMap = Maps.newHashMapWithExpectedSize(messages.size());
    for (Message message : messages) {
      Object fieldKey = message.getField(keyDescriptor);
      if (fieldKey != null) {
        Object fieldValue = message.getField(valueDescriptor);
        Object convertedValue = fieldValue != null ? convert(new ProtoBufFieldInfo(fieldValue, valueDescriptor)) : null;
        convertedMap.put(convertSingleValue(new ProtoBufFieldInfo(fieldKey, keyDescriptor)), convertedValue);
      }
    }
    return convertedMap;
  }

  /**
   * Handles the conversion of each value of the Protobuf collection. Converts the Collection to an Object array.
   *
   * @param value should be verified to contain a ProtoBuf collection prior to calling this method as it will
   *              be handled as a collection field without checking
   */
  @Override
  protected Object[] convertMultiValue(Object value) {
    ProtoBufFieldInfo protoBufFieldInfo = (ProtoBufFieldInfo) value;
    Descriptors.FieldDescriptor fieldDescriptor = protoBufFieldInfo.getFieldDescriptor();
    Collection<Object> values = (Collection<Object>) protoBufFieldInfo.getFieldValue();
    int numValues = values.size();
    Object[] convertedValues = new Object[numValues];
    int index = 0;
    for (Object fieldValue : values) {
      Object convertedValue = fieldValue != null ? convert(new ProtoBufFieldInfo(fieldValue, fieldDescriptor)) : null;
      convertedValues[index++] = convertedValue;
    }
    return convertedValues;
  }

  /**
   * Handles conversion of ProtoBuf single values.
   */
  @Override
  protected Object convertSingleValue(Object value) {
    Object fieldValue = ((ProtoBufFieldInfo) value).getFieldValue();
    if (fieldValue instanceof ByteString) {
      return ((ByteString) fieldValue).toByteArray();
    } else if (fieldValue instanceof Number) {
      return fieldValue;
    }
    return fieldValue.toString();
  }

  /**
   * Handles conversion of ProtoBuf {@link Message} types
   *
   * @param value should be verified to contain a ProtoBuf Message prior to calling this method as it will be
   *              handled as a Message without checking
   */
  @Override
  protected Map<Object, Object> convertRecord(Object value) {
    ProtoBufFieldInfo record = (ProtoBufFieldInfo) value;
    Map<Descriptors.FieldDescriptor, Object> fields = ((Message) record.getFieldValue()).getAllFields();
    Map<Object, Object> convertedMap = Maps.newHashMapWithExpectedSize(fields.size());
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fields.entrySet()) {
      Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
      Object fieldValue = entry.getValue();
      Object convertedValue = fieldValue != null ? convert(new ProtoBufFieldInfo(fieldValue, fieldDescriptor)) : null;
      convertedMap.put(fieldDescriptor.getName(), convertedValue);
    }
    return convertedMap;
  }
}
