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
package org.apache.pinot.core.segment.index.column;

import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.impl.ConstantMVForwardIndex;
import org.apache.pinot.core.io.reader.impl.ConstantMVInvertedIndex;
import org.apache.pinot.core.io.reader.impl.ConstantSVSortedIndex;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.ConstantValueBytesDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueDoubleDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueFloatDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueIntDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueLongDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueStringDictionary;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Provide the default null value.
 */
public class DefaultNullValueVirtualColumnProvider extends BaseVirtualColumnProvider {

  @Override
  public DataFileReader buildReader(VirtualColumnContext context) {
    if (context.getFieldSpec().isSingleValueField()) {
      return new ConstantSVSortedIndex(context.getTotalDocCount());
    } else {
      return new ConstantMVForwardIndex();
    }
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    switch (fieldSpec.getDataType()) {
      case INT:
        return new ConstantValueIntDictionary((int) fieldSpec.getDefaultNullValue());
      case LONG:
        return new ConstantValueLongDictionary((long) fieldSpec.getDefaultNullValue());
      case FLOAT:
        return new ConstantValueFloatDictionary((float) fieldSpec.getDefaultNullValue());
      case DOUBLE:
        return new ConstantValueDoubleDictionary((double) fieldSpec.getDefaultNullValue());
      case STRING:
        return new ConstantValueStringDictionary((String) fieldSpec.getDefaultNullValue());
      case BYTES:
        return new ConstantValueBytesDictionary((byte[]) fieldSpec.getDefaultNullValue());
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public InvertedIndexReader buildInvertedIndex(VirtualColumnContext context) {
    if (context.getFieldSpec().isSingleValueField()) {
      return new ConstantSVSortedIndex(context.getTotalDocCount());
    } else {
      return new ConstantMVInvertedIndex(context.getTotalDocCount());
    }
  }

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    return getColumnMetadataBuilder(context).setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true)
        .setIsSorted(context.getFieldSpec().isSingleValueField()).build();
  }
}
