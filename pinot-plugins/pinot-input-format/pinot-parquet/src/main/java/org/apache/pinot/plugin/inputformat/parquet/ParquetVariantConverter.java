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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantConverters;
import org.apache.pinot.spi.utils.VariantEnvelope;


/// Converts a top-level Parquet `VARIANT(1)` group into Pinot's self-describing VARIANT byte envelope.
///
/// Detection is based only on [LogicalTypeAnnotation.VariantLogicalTypeAnnotation], never on field names.
/// The parquet-java [VariantConverters] implementation performs the logical reconstruction so unshredded,
/// shredded, and partially shredded encodings all produce complete Variant metadata and value buffers.
///
/// Instances retain parquet-java's converter tree and are intentionally scoped to one record reader because the
/// converter and builder holder are mutable and not thread-safe.
final class ParquetVariantConverter {
  private static final byte SUPPORTED_SPEC_VERSION = 1;

  private final GroupType _variantType;
  private final int _metadataIndex;
  private final int _valueIndex;
  private final int _typedValueIndex;
  private final BuilderHolder _holder = new BuilderHolder();
  private final GroupConverter _converter;

  private ParquetVariantConverter(GroupType variantType) {
    _variantType = variantType;
    _metadataIndex = variantType.getFieldIndex("metadata");
    _valueIndex = variantType.containsField("value") ? variantType.getFieldIndex("value") : -1;
    _typedValueIndex = variantType.containsField("typed_value") ? variantType.getFieldIndex("typed_value") : -1;
    _converter = VariantConverters.newVariantConverter(variantType, _holder::setMetadata, _holder::build);
  }

  /// Validates every VARIANT annotation in the file and returns the supported top-level field names.
  ///
  /// Pinot currently supports only non-repeated, top-level `VARIANT(1)` values. Failing during reader
  /// initialization avoids silently surfacing an unsupported nested/repeated Variant as an ordinary struct.
  static Set<String> validateAndGetTopLevelVariantFields(GroupType schema) {
    return validateAndGetTopLevelVariantFields(schema, fieldName -> true);
  }

  private static Set<String> validateAndGetTopLevelVariantFields(GroupType schema, Predicate<String> fieldSelector) {
    Set<String> variantFields = new HashSet<>();
    for (Type field : schema.getFields()) {
      if (!fieldSelector.test(field.getName())) {
        continue;
      }
      if (isVariant(field)) {
        validateTopLevelVariant(field);
        variantFields.add(field.getName());
      } else if (!field.isPrimitive()) {
        rejectNestedVariants(field.asGroupType(), field.getName());
      }
    }
    return Set.copyOf(variantFields);
  }

  /// Validates the file schema and builds an index-aligned reusable converter tree for each top-level VARIANT column.
  static ParquetVariantConverter[] createTopLevelVariantConverters(GroupType schema) {
    Set<String> variantFields = validateAndGetTopLevelVariantFields(schema);
    return buildTopLevelVariantConverters(schema, variantFields);
  }

  /// Validates selected top-level fields and builds index-aligned converters for selected VARIANT columns.
  static ParquetVariantConverter[] createTopLevelVariantConverters(GroupType schema, Predicate<String> fieldSelector) {
    Set<String> variantFields = validateAndGetTopLevelVariantFields(schema, fieldSelector);
    return buildTopLevelVariantConverters(schema, variantFields);
  }

  private static ParquetVariantConverter[] buildTopLevelVariantConverters(GroupType schema,
      Set<String> variantFields) {
    ParquetVariantConverter[] variantConverters = new ParquetVariantConverter[schema.getFieldCount()];
    for (int fieldIndex = 0; fieldIndex < schema.getFieldCount(); fieldIndex++) {
      Type field = schema.getType(fieldIndex);
      if (variantFields.contains(field.getName())) {
        variantConverters[fieldIndex] = new ParquetVariantConverter(field.asGroupType());
      }
    }
    return variantConverters;
  }

  static boolean isVariant(Type type) {
    return type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation;
  }

  byte[] convert(Group group) {
    if (_valueIndex >= 0 && group.getFieldRepetitionCount(_valueIndex) == 1
        && (_typedValueIndex < 0 || group.getFieldRepetitionCount(_typedValueIndex) == 0)) {
      return pack(group.getBinary(_metadataIndex, 0), group.getBinary(_valueIndex, 0));
    }
    _holder.reset();
    feedVariantGroup(group, _converter, _variantType, _metadataIndex);
    Variant variant = _holder.finish();
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
  }

  static byte[] pack(ByteBuffer metadata, ByteBuffer value) {
    return VariantEnvelope.encode(metadata, value);
  }

  private byte[] pack(Binary metadata, Binary value) {
    // Binary.writeTo(OutputStream) materializes the full payload for a direct or read-only ByteBuffer-backed Binary.
    // Consume the independent views returned by toByteBuffer() so both payloads go straight into the final envelope,
    // without intermediate byte arrays or another duplicate view.
    int metadataLength = metadata.length();
    int valueLength = value.length();
    byte[] envelope = VariantEnvelope.allocate(metadataLength, valueLength);
    copyInto(metadata, envelope, VariantEnvelope.HEADER_SIZE, metadataLength, "metadata");
    copyInto(value, envelope, VariantEnvelope.HEADER_SIZE + metadataLength, valueLength, "value");
    return envelope;
  }

  private static void copyInto(Binary source, byte[] target, int targetOffset, int expectedLength, String payload) {
    ByteBuffer sourceView = source.toByteBuffer();
    if (sourceView.remaining() != expectedLength) {
      throw new IllegalStateException(
          "Parquet VARIANT " + payload + " length changed while copying: expected " + expectedLength + " but found "
              + sourceView.remaining());
    }
    if (sourceView.hasArray()) {
      System.arraycopy(sourceView.array(), sourceView.arrayOffset() + sourceView.position(), target, targetOffset,
          expectedLength);
    } else {
      sourceView.get(target, targetOffset, expectedLength);
    }
  }

  private static void validateTopLevelVariant(Type field) {
    if (!isVariant(field) || field.isPrimitive()) {
      throw new IllegalArgumentException("Parquet VARIANT must be a group: " + field);
    }
    LogicalTypeAnnotation.VariantLogicalTypeAnnotation annotation =
        (LogicalTypeAnnotation.VariantLogicalTypeAnnotation) field.getLogicalTypeAnnotation();
    if (annotation.getSpecVersion() != SUPPORTED_SPEC_VERSION) {
      throw new UnsupportedOperationException(
          "Unsupported Parquet VARIANT spec version: " + annotation.getSpecVersion());
    }
    if (field.isRepetition(Type.Repetition.REPEATED)) {
      throw new UnsupportedOperationException("Repeated Parquet VARIANT is not supported: " + field.getName());
    }

    GroupType group = field.asGroupType();
    for (Type child : group.getFields()) {
      String name = child.getName();
      if (!"metadata".equals(name) && !"value".equals(name) && !"typed_value".equals(name)) {
        throw new IllegalArgumentException("Invalid Parquet VARIANT field: " + child);
      }
    }
    if (!group.containsField("metadata")) {
      throw new IllegalArgumentException("Invalid Parquet VARIANT: missing metadata field");
    }
    Type metadata = group.getType("metadata");
    if (!isBinary(metadata) || !metadata.isRepetition(Type.Repetition.REQUIRED)) {
      throw new IllegalArgumentException("Invalid Parquet VARIANT metadata field: " + metadata);
    }

    boolean hasValue = group.containsField("value");
    boolean hasTypedValue = group.containsField("typed_value");
    if (!hasValue && !hasTypedValue) {
      throw new IllegalArgumentException("Invalid Parquet VARIANT: missing value and typed_value fields");
    }
    if (hasValue) {
      Type value = group.getType("value");
      if (!isBinary(value) || value.isRepetition(Type.Repetition.REPEATED)) {
        throw new IllegalArgumentException("Invalid Parquet VARIANT value field: " + value);
      }
    }
    if (hasTypedValue) {
      Type typedValue = group.getType("typed_value");
      if (!typedValue.isRepetition(Type.Repetition.OPTIONAL)) {
        throw new IllegalArgumentException("Invalid Parquet VARIANT typed_value field: " + typedValue);
      }
    }
  }

  private static void rejectNestedVariants(GroupType group, String path) {
    for (Type child : group.getFields()) {
      String childPath = path + "." + child.getName();
      if (isVariant(child)) {
        throw new UnsupportedOperationException("Nested Parquet VARIANT is not supported: " + childPath);
      }
      if (!child.isPrimitive()) {
        rejectNestedVariants(child.asGroupType(), childPath);
      }
    }
  }

  private static boolean isBinary(Type field) {
    return field.isPrimitive()
        && field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY;
  }

  private static void feedVariantGroup(Group group, GroupConverter converter, GroupType type, int metadataIndex) {
    converter.start();

    // Metadata initializes VariantBuilder and must be delivered before value/typed_value. File field order is not
    // semantically significant (early Spark writers emitted value first). The immutable field index is resolved once
    // when the converter is built instead of by name for every row.
    feedField(group, metadataIndex, converter.getConverter(metadataIndex));
    for (int fieldIndex = 0; fieldIndex < type.getFieldCount(); fieldIndex++) {
      if (fieldIndex != metadataIndex) {
        feedField(group, fieldIndex, converter.getConverter(fieldIndex));
      }
    }
    converter.end();
  }

  private static void feedGroup(Group group, GroupConverter converter) {
    converter.start();
    GroupType type = group.getType();
    for (int fieldIndex = 0; fieldIndex < type.getFieldCount(); fieldIndex++) {
      feedField(group, fieldIndex, converter.getConverter(fieldIndex));
    }
    converter.end();
  }

  private static void feedField(Group group, int fieldIndex, Converter converter) {
    int repetitionCount = group.getFieldRepetitionCount(fieldIndex);
    if (repetitionCount == 0) {
      return;
    }
    if (converter == null) {
      throw new IllegalArgumentException(
          "Unsupported Parquet VARIANT shape at field: " + group.getType().getType(fieldIndex));
    }

    Type field = group.getType().getType(fieldIndex);
    for (int valueIndex = 0; valueIndex < repetitionCount; valueIndex++) {
      if (field.isPrimitive()) {
        feedPrimitive(group, fieldIndex, valueIndex, field.asPrimitiveType(), converter.asPrimitiveConverter());
      } else {
        feedGroup(group.getGroup(fieldIndex, valueIndex), converter.asGroupConverter());
      }
    }
  }

  private static void feedPrimitive(Group group, int fieldIndex, int valueIndex, PrimitiveType type,
      PrimitiveConverter converter) {
    switch (type.getPrimitiveTypeName()) {
      case BOOLEAN:
        converter.addBoolean(group.getBoolean(fieldIndex, valueIndex));
        break;
      case INT32:
        converter.addInt(group.getInteger(fieldIndex, valueIndex));
        break;
      case INT64:
        converter.addLong(group.getLong(fieldIndex, valueIndex));
        break;
      case FLOAT:
        converter.addFloat(group.getFloat(fieldIndex, valueIndex));
        break;
      case DOUBLE:
        converter.addDouble(group.getDouble(fieldIndex, valueIndex));
        break;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        converter.addBinary(group.getBinary(fieldIndex, valueIndex));
        break;
      default:
        throw new IllegalArgumentException("Unsupported Parquet VARIANT physical type: " + type);
    }
  }

  private static final class BuilderHolder {
    private VariantBuilder _builder;

    private void reset() {
      _builder = null;
    }

    private void setMetadata(ByteBuffer metadata) {
      _builder = new VariantBuilder(new ImmutableMetadata(metadata));
    }

    private void build(Consumer<VariantBuilder> consumer) {
      if (_builder == null) {
        throw new IllegalStateException("Cannot build Parquet VARIANT: metadata has not been read");
      }
      consumer.accept(_builder);
    }

    private Variant finish() {
      if (_builder == null) {
        throw new IllegalStateException("Cannot build Parquet VARIANT: missing metadata");
      }
      _builder.appendNullIfEmpty();
      return _builder.build();
    }
  }
}
