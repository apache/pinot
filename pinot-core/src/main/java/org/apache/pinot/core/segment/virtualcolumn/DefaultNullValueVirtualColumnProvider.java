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

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = super.getColumnMetadataBuilder(context);
    columnMetadataBuilder.setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(true);
    return columnMetadataBuilder.build();
  }

  public Dictionary buildDictionary(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    DataType dataType = fieldSpec.getDataType().getStoredType();
    if (dataType.equals(DataType.STRING)) {
      return new SingleStringDictionary((String) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(FieldSpec.DataType.FLOAT)) {
      return new SingleFloatDictionary((float) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(DataType.DOUBLE)) {
      return new SingleDoubleDictionary((double) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(DataType.INT)) {
      return new SingleIntDictionary((int) fieldSpec.getDefaultNullValue());
    } else if (dataType.equals(DataType.LONG)) {
      return new SingleLongDictionary((long) fieldSpec.getDefaultNullValue());
    }
    throw new IllegalStateException(
        "Caught exception building dictionary. Unsupported data type: " + dataType.toString());
  }
}
