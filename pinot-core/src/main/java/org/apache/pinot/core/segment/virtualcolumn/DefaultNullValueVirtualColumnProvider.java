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
package org.apache.pinot.core.segment.virtualcolumn;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.io.reader.impl.ConstantMultiValueInvertedIndex;
import org.apache.pinot.core.io.reader.impl.ConstantSingleValueInvertedIndex;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.SingleDoubleDictionary;
import org.apache.pinot.core.segment.index.readers.SingleFloatDictionary;
import org.apache.pinot.core.segment.index.readers.SingleIntDictionary;
import org.apache.pinot.core.segment.index.readers.SingleLongDictionary;
import org.apache.pinot.core.segment.index.readers.SingleStringDictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Provide the default null value.
 */
public class DefaultNullValueVirtualColumnProvider extends BaseVirtualColumnProvider {

  Dictionary _dictionary;
  ColumnMetadata _columnMetadata;

  DefaultNullValueVirtualColumnProvider(VirtualColumnContext virtualColumnContext) {
    buildDictionary(virtualColumnContext);
    buildMetadata(virtualColumnContext);
    buildColumnIndexContainer(virtualColumnContext);
  }

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = super.getColumnMetadataBuilder(context);
    columnMetadataBuilder.setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(true);
    _columnMetadata = columnMetadataBuilder.build();
    return _columnMetadata;
  }

  public Dictionary buildDictionary(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    DataType dataType = fieldSpec.getDataType().getStoredType();
    if (dataType.equals(DataType.STRING)) {
      _dictionary = new SingleStringDictionary((String) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(FieldSpec.DataType.FLOAT)) {
      _dictionary = new SingleFloatDictionary((float) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(DataType.DOUBLE)) {
      _dictionary = new SingleDoubleDictionary((double) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(DataType.INT)) {
      _dictionary = new SingleIntDictionary((int) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(DataType.LONG)) {
      _dictionary = new SingleLongDictionary((long) fieldSpec.getDefaultNullValue());
    } else {
      throw new IllegalStateException(
          "Caught exception building dictionary. Unsupported data type: " + dataType.toString());
    }
    return _dictionary;
  }

  public void updateInvertedIndex(String str, VirtualColumnContext virtualColumnContext) {
    Preconditions.checkState(
        _columnIndexContainer.getInvertedIndex() instanceof ConstantSingleValueInvertedIndex || _columnIndexContainer
            .getInvertedIndex() instanceof ConstantMultiValueInvertedIndex, "column index should have constant value");
    if (_columnIndexContainer.getInvertedIndex() instanceof ConstantSingleValueInvertedIndex) {
      ((ConstantSingleValueInvertedIndex) _columnIndexContainer.getInvertedIndex())
          .setLength(virtualColumnContext.getTotalDocCount());
    } else {
      ((ConstantMultiValueInvertedIndex) _columnIndexContainer.getInvertedIndex())
          .setLength(virtualColumnContext.getTotalDocCount());
    }
  }

  public ColumnMetadata getColumnMetadata() {
    return _columnMetadata;
  }
}
