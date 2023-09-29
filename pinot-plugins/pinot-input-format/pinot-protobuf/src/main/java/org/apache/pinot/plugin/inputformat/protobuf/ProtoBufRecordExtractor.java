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

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for ProtoBuf records
 */
public class ProtoBufRecordExtractor extends BaseRecordExtractor<Message> {

  private Set<String> _fields;
  private boolean _extractAll = false;

  @Override
  public void init(@Nullable Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Collections.emptySet();
    } else {
      _fields = ImmutableSet.copyOf(fields);
    }
  }

  /**
   * For fields that are not set, we want to populate a null, instead of proto default.
   * @param fieldDescriptor
   * @param message
   */
  private Object getFieldValue(Descriptors.FieldDescriptor fieldDescriptor, Message message) {
    // Note w.r.t proto3 - If a field is not declared with optional keyword, there's no way to distinguish
    // if its explicitly set to a proto default or not been set at all i.e hasField() returns false
    // and we would use null.
    if (fieldDescriptor.isRepeated() || !fieldDescriptor.hasOptionalKeyword() || message.hasField(fieldDescriptor)) {
      return message.getField(fieldDescriptor);
    } else {
      return null;
    }
  }

  @Override
  public GenericRow extract(Message from, GenericRow to) {
    Descriptors.Descriptor descriptor = from.getDescriptorForType();
    if (_extractAll) {
      for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
        Object fieldValue = getFieldValue(fieldDescriptor, from);
        if (fieldValue != null) {
          fieldValue = convert(new ProtoBufFieldInfo(fieldValue, fieldDescriptor));
        }
        to.putValue(fieldDescriptor.getName(), fieldValue);
      }
    } else {
      for (String fieldName : _fields) {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
        Object fieldValue = fieldDescriptor == null ? null : getFieldValue(fieldDescriptor, from);
        if (fieldValue != null) {
          fieldValue = convert(new ProtoBufFieldInfo(fieldValue, descriptor.findFieldByName(fieldName)));
        }
        to.putValue(fieldName, fieldValue);
      }
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
  @Nullable
  protected Object convertMap(Object value) {
    ProtoBufFieldInfo protoBufFieldInfo = (ProtoBufFieldInfo) value;
    Collection<Message> messages = (Collection<Message>) protoBufFieldInfo.getFieldValue();
    if (messages.isEmpty()) {
      return null;
    }

    List<Descriptors.FieldDescriptor> fieldDescriptors =
        protoBufFieldInfo.getFieldDescriptor().getMessageType().getFields();
    Descriptors.FieldDescriptor keyFieldDescriptor = fieldDescriptors.get(0);
    Descriptors.FieldDescriptor valueFieldDescriptor = fieldDescriptors.get(1);
    Map<Object, Object> convertedMap = new HashMap<>();
    for (Message message : messages) {
      Object fieldKey = message.getField(keyFieldDescriptor);
      Object fieldValue = message.getField(valueFieldDescriptor);
      if (fieldKey != null) {
        Object convertedFieldValue = null;
        if (fieldValue != null) {
          convertedFieldValue = convert(new ProtoBufFieldInfo(fieldValue, valueFieldDescriptor));
        }

        if (convertedFieldValue != null) {
          convertedMap
              .put(convertSingleValue(new ProtoBufFieldInfo(fieldKey, keyFieldDescriptor)), convertedFieldValue);
        }
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
  @Nullable
  protected Object convertMultiValue(Object value) {
    ProtoBufFieldInfo protoBufFieldInfo = (ProtoBufFieldInfo) value;
    Collection<Object> fieldValues = (Collection<Object>) protoBufFieldInfo.getFieldValue();

    if (fieldValues.isEmpty()) {
      return null;
    }
    int numValues = fieldValues.size();
    Object[] array = new Object[numValues];
    int index = 0;

    for (Object fieldValue : fieldValues) {
      Object convertedValue = null;
      if (fieldValue != null) {
        convertedValue = convert(new ProtoBufFieldInfo(fieldValue, protoBufFieldInfo.getFieldDescriptor()));
      }
      if (convertedValue != null) {
        array[index++] = convertedValue;
      }
    }

    if (index == numValues) {
      return array;
    } else if (index == 0) {
      return null;
    } else {
      return Arrays.copyOf(array, index);
    }
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
  @Nullable
  protected Object convertRecord(Object value) {
    ProtoBufFieldInfo record = (ProtoBufFieldInfo) value;
    Map<Descriptors.FieldDescriptor, Object> fields = ((Message) record.getFieldValue()).getAllFields();
    if (fields.isEmpty()) {
      return null;
    }

    Map<Object, Object> convertedMap = new HashMap<>();
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fields.entrySet()) {
      Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
      Object fieldValue = entry.getValue();
      if (fieldValue != null) {
        fieldValue = convert(new ProtoBufFieldInfo(fieldValue, fieldDescriptor));
      }
      convertedMap.put(fieldDescriptor.getName(), fieldValue);
    }
    return convertedMap;
  }
}
