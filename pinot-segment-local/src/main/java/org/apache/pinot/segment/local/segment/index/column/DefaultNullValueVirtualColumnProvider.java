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
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Provide the default null value.
 */
public class DefaultNullValueVirtualColumnProvider extends BaseVirtualColumnProvider {

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
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        return new ConstantValueIntDictionary((int) fieldSpec.getDefaultNullValue());
      case LONG:
        return new ConstantValueLongDictionary((long) fieldSpec.getDefaultNullValue());
      case FLOAT:
        return new ConstantValueFloatDictionary((float) fieldSpec.getDefaultNullValue());
      case DOUBLE:
        return new ConstantValueDoubleDictionary((double) fieldSpec.getDefaultNullValue());
      case BIG_DECIMAL:
        return new ConstantValueBigDecimalDictionary((BigDecimal) fieldSpec.getDefaultNullValue());
      case STRING:
        return new ConstantValueStringDictionary((String) fieldSpec.getDefaultNullValue());
      case BYTES:
        return new ConstantValueBytesDictionary((byte[]) fieldSpec.getDefaultNullValue());
      default:
        throw new IllegalStateException();
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
    ColumnMetadataImpl.Builder builder = getColumnMetadataBuilder(context).setCardinality(1).setHasDictionary(true);
    if (context.getFieldSpec().isSingleValueField()) {
      builder.setSorted(true);
    } else {
      // When there is no value for a multi-value column, the maxNumberOfMultiValues and cardinality should be
      // set as 1 because the MV column bitmap uses 1 to delimit the rows for a MV column. Each MV column will have a
      // default null value based on column's data type
      builder.setMaxNumberOfMultiValues(1);
    }

    FieldSpec fieldSpec = context.getFieldSpec();
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    switch (fieldSpec.getDataType().getStoredType()) {
      case INT:
        builder.setMinValue((int) defaultNullValue).setMaxValue((int) defaultNullValue);
        break;
      case LONG:
        builder.setMinValue((long) defaultNullValue).setMaxValue((long) defaultNullValue);
        break;
      case FLOAT:
        builder.setMinValue((float) defaultNullValue).setMaxValue((float) defaultNullValue);
        break;
      case DOUBLE:
        builder.setMinValue((double) defaultNullValue).setMaxValue((double) defaultNullValue);
        break;
      case BIG_DECIMAL:
        builder.setMinValue((BigDecimal) defaultNullValue).setMaxValue((BigDecimal) defaultNullValue);
        break;
      case STRING:
        builder.setMinValue((String) defaultNullValue).setMaxValue((String) defaultNullValue);
        break;
      case BYTES:
        builder.setMinValue(new ByteArray((byte[]) defaultNullValue))
            .setMaxValue(new ByteArray((byte[]) defaultNullValue));
        break;
      default:
        throw new IllegalStateException();
    }

    return builder.build();
  }
}
