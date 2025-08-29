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

import org.apache.pinot.segment.local.segment.index.readers.ConstantValueIntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.constant.ConstantSortedIndexReader;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Virtual column provider that returns the partition id of the segment.
 * The partition id is extracted from the segment name when possible,
 * otherwise a default partition id -1 is used(for offline tables).
 */
public class PartitionIdVirtualColumnProvider implements VirtualColumnProvider {

  @Override
  public ForwardIndexReader<?> buildForwardIndex(VirtualColumnContext context) {
    return new ConstantSortedIndexReader(context.getTotalDocCount());
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    // Extract partition ID from the default null value set in the field spec
    int partitionId = (int) context.getFieldSpec().getDefaultNullValue();
    return new ConstantValueIntDictionary(partitionId);
  }

  @Override
  public InvertedIndexReader<?> buildInvertedIndex(VirtualColumnContext context) {
    return new ConstantSortedIndexReader(context.getTotalDocCount());
  }

  @Override
  public ColumnMetadataImpl buildMetadata(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    int partitionId = (int) fieldSpec.getDefaultNullValue();
    return new ColumnMetadataImpl.Builder()
        .setFieldSpec(fieldSpec)
        .setTotalDocs(context.getTotalDocCount())
        .setCardinality(1)
        .setSorted(true)
        .setHasDictionary(true)
        .setMinValue(partitionId)
        .setMaxValue(partitionId)
        .build();
  }
}
