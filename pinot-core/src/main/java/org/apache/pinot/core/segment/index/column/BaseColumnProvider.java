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
import org.apache.pinot.core.io.reader.impl.ConstantMultiValueInvertedIndex;
import org.apache.pinot.core.io.reader.impl.ConstantSingleValueInvertedIndex;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Shared implementation code between column providers.
 */
public abstract class BaseColumnProvider implements ColumnProvider {

  ColumnIndexContainer _columnIndexContainer;
  InvertedIndexReader _invertedIndexReader;

  public DataFileReader buildReader(ColumnContext context) {
    if (context.getFieldSpec().isSingleValueField()) {
      return new ConstantSingleValueInvertedIndex(0);
    } else {
      return new ConstantMultiValueInvertedIndex(0);
    }
  }

  protected ColumnMetadata.Builder getColumnMetadataBuilder(ColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    return new ColumnMetadata.Builder().setVirtual(true).setColumnName(fieldSpec.getName())
        .setFieldType(fieldSpec.getFieldType()).setDataType(fieldSpec.getDataType())
        .setTotalDocs(context.getTotalDocCount()).setSingleValue(fieldSpec.isSingleValueField())
        .setDefaultNullValueString(context.getFieldSpec().getDefaultNullValueString());
  }

  public InvertedIndexReader buildInvertedIndex(ColumnContext context) {
    if (context.getFieldSpec().isSingleValueField()) {
      _invertedIndexReader = new ConstantSingleValueInvertedIndex(context.getTotalDocCount());
    } else {
      _invertedIndexReader = new ConstantMultiValueInvertedIndex(context.getTotalDocCount());
    }
    return _invertedIndexReader;
  }

  @Override
  public ColumnIndexContainer buildColumnIndexContainer(ColumnContext context) {
    _columnIndexContainer =
        new BaseColumnIndexContainer(buildReader(context), buildInvertedIndex(context), buildDictionary(context));
    return _columnIndexContainer;
  }

  public ColumnIndexContainer getColumnIndexContainer() {
    return _columnIndexContainer;
  }
}
