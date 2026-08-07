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
package org.apache.pinot.segment.local.segment.index.column;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
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
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/// Provide the default null value.
///
/// This class also serves as the base for virtual columns holding a single constant value per segment: subclasses only
/// need to override [#getValue(VirtualColumnContext)] to supply a value of the column's stored type.
public class DefaultNullValueVirtualColumnProvider implements VirtualColumnProvider {

  /// Returns the constant value to be stored for every document of the column. The returned object must match the
  /// stored type of the field spec (e.g. a `Long` for a LONG column).
  protected Object getValue(VirtualColumnContext context) {
    return context.getFieldSpec().getDefaultNullValue();
  }

  /// Reads the value via [#getValue(VirtualColumnContext)] and checks it against the field's stored type, so that a
  /// provider returning the wrong box type fails with the offending column and provider named rather than with a bare
  /// `ClassCastException` from deep inside segment loading.
  private Object getCheckedValue(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    Object value = getValue(context);
    Class<?> expectedClass;
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        expectedClass = Integer.class;
        break;
      case LONG:
        expectedClass = Long.class;
        break;
      case FLOAT:
        expectedClass = Float.class;
        break;
      case DOUBLE:
        expectedClass = Double.class;
        break;
      case BIG_DECIMAL:
        expectedClass = BigDecimal.class;
        break;
      case STRING:
        expectedClass = String.class;
        break;
      case BYTES:
      case UUID:
        expectedClass = byte[].class;
        break;
      default:
        throw new IllegalStateException(
            "Unsupported stored type: " + fieldSpec.getDataType().getStoredType() + " for virtual column: "
                + fieldSpec.getName());
    }
    Preconditions.checkState(expectedClass.isInstance(value),
        "Virtual column provider: %s returned value of type: %s for column: %s, expecting: %s",
        getClass().getName(), value != null ? value.getClass().getName() : "null", fieldSpec.getName(),
        expectedClass.getName());
    return value;
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
    FieldSpec fieldSpec = context.getFieldSpec();
    Object value = getCheckedValue(context);
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
        throw new IllegalStateException(
            "Unsupported stored type: " + fieldSpec.getDataType().getStoredType() + " for virtual column: "
                + fieldSpec.getName());
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

    Object value = getCheckedValue(context);
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
        throw new IllegalStateException(
            "Unsupported stored type: " + fieldSpec.getDataType().getStoredType() + " for virtual column: "
                + fieldSpec.getName());
    }

    return builder.build();
  }
}
