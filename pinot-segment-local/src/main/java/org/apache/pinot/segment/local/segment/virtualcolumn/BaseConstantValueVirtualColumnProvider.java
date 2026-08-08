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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueBigDecimalDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueBytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueDoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueFloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueIntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueLongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueStringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.constant.ConstantMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.constant.ConstantMVInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.constant.ConstantSortedIndexReader;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/// Base for virtual columns holding a single constant value for every document of a segment.
///
/// Subclasses supply the value by overriding [#getValue(VirtualColumnContext)]; everything else - a constant sorted
/// forward index, a single-entry dictionary and matching [ColumnMetadataImpl] - is derived from it here.
public abstract class BaseConstantValueVirtualColumnProvider implements VirtualColumnProvider {

  /// Returns the constant value to be stored for every document of the column. The returned object must match the
  /// stored type of the field spec (e.g. a `Long` for a LONG or TIMESTAMP column).
  protected abstract Object getValue(VirtualColumnContext context);

  /// Reads the value via [#getValue(VirtualColumnContext)] and checks it against the field's stored type, so that a
  /// provider returning the wrong box type fails with the offending column and provider named rather than with a bare
  /// `ClassCastException` from deep inside segment loading.
  protected final Object getCheckedValue(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    Object value = getValue(context);
    Class<?> expectedClass = getValueClass(fieldSpec);
    Preconditions.checkState(expectedClass.isInstance(value),
        "Virtual column provider: %s returned value of type: %s for column: %s, expecting: %s", getClass().getName(),
        value != null ? value.getClass().getName() : "null", fieldSpec.getName(), expectedClass.getName());
    return value;
  }

  private static Class<?> getValueClass(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        return Integer.class;
      case LONG:
        return Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case BIG_DECIMAL:
        return BigDecimal.class;
      case STRING:
        return String.class;
      case BYTES:
      case UUID:
        return byte[].class;
      default:
        throw new IllegalStateException(unsupportedStoredType(fieldSpec));
    }
  }

  private static String unsupportedStoredType(FieldSpec fieldSpec) {
    return "Unsupported stored type: " + fieldSpec.getDataType().getStoredType() + " for virtual column: "
        + fieldSpec.getName();
  }

  @Override
  public ForwardIndexReader<?> buildForwardIndex(VirtualColumnContext context) {
    if (context.getFieldSpec().isSingleValueField()) {
      return new ConstantSortedIndexReader(context.getTotalDocCount());
    } else {
      return new ConstantMVForwardIndexReader();
    }
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return buildDictionary(context, getCheckedValue(context));
  }

  /// Builds the dictionary from an already-resolved value, so that callers which need the value more than once can
  /// resolve it a single time.
  protected final Dictionary buildDictionary(VirtualColumnContext context, Object value) {
    FieldSpec fieldSpec = context.getFieldSpec();
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        return new ConstantValueIntDictionary((int) value);
      case LONG:
        return new ConstantValueLongDictionary((long) value);
      case FLOAT:
        return new ConstantValueFloatDictionary((float) value);
      case DOUBLE:
        return new ConstantValueDoubleDictionary((double) value);
      case BIG_DECIMAL:
        return new ConstantValueBigDecimalDictionary((BigDecimal) value);
      case STRING:
        return new ConstantValueStringDictionary((String) value);
      case BYTES:
      case UUID:
        return new ConstantValueBytesDictionary((byte[]) value);
      default:
        throw new IllegalStateException(unsupportedStoredType(fieldSpec));
    }
  }

  @Override
  public InvertedIndexReader<?> buildInvertedIndex(VirtualColumnContext context) {
    if (context.getFieldSpec().isSingleValueField()) {
      return new ConstantSortedIndexReader(context.getTotalDocCount());
    } else {
      return new ConstantMVInvertedIndexReader(context.getTotalDocCount());
    }
  }

  @Override
  public ColumnMetadataImpl buildMetadata(VirtualColumnContext context) {
    return buildMetadata(context, getCheckedValue(context));
  }

  /// Builds the column metadata from an already-resolved value.
  protected final ColumnMetadataImpl buildMetadata(VirtualColumnContext context, Object value) {
    FieldSpec fieldSpec = context.getFieldSpec();
    ColumnMetadataImpl.Builder builder = new ColumnMetadataImpl.Builder().setFieldSpec(fieldSpec)
        .setTotalDocs(context.getTotalDocCount())
        .setCardinality(1)
        .setHasDictionary(true);
    if (fieldSpec.isSingleValueField()) {
      builder.setSorted(true);
    } else {
      // When there is no value for a multi-value column, the maxNumberOfMultiValues and cardinality should be
      // set as 1 because the MV column bitmap uses 1 to delimit the rows for a MV column. Each MV column will have a
      // default null value based on column's data type
      builder.setMaxNumberOfMultiValues(1);
    }

    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        builder.setMinValue((int) value).setMaxValue((int) value);
        break;
      case LONG:
        builder.setMinValue((long) value).setMaxValue((long) value);
        break;
      case FLOAT:
        builder.setMinValue((float) value).setMaxValue((float) value);
        break;
      case DOUBLE:
        builder.setMinValue((double) value).setMaxValue((double) value);
        break;
      case BIG_DECIMAL:
        builder.setMinValue((BigDecimal) value).setMaxValue((BigDecimal) value);
        break;
      case STRING:
        builder.setMinValue((String) value).setMaxValue((String) value);
        break;
      case BYTES:
        builder.setMinValue(new ByteArray((byte[]) value)).setMaxValue(new ByteArray((byte[]) value));
        break;
      default:
        throw new IllegalStateException(unsupportedStoredType(fieldSpec));
    }

    return builder.build();
  }

  /// Builds the column index container from an already-resolved value, so that a subclass whose value comes from
  /// mutable state can resolve it a single time and keep every component consistent.
  protected final ColumnIndexContainer buildColumnIndexContainer(VirtualColumnContext context, Object value,
      @Nullable NullValueVectorReader nullValueVector) {
    return new VirtualColumnIndexContainer(buildForwardIndex(context), buildInvertedIndex(context),
        buildDictionary(context, value), nullValueVector);
  }
}
