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

import java.util.Objects;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueLongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.constant.ConstantSortedIndexReader;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;


/// Virtual column provider that returns the segment creation time for every document.
public class CreationTimeVirtualColumnProvider implements VirtualColumnProvider {
  @Override
  public ForwardIndexReader<?> buildForwardIndex(VirtualColumnContext context) {
    return new ConstantSortedIndexReader(context.getTotalDocCount());
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return new ConstantValueLongDictionary(getCreationTime(context));
  }

  @Override
  public InvertedIndexReader<?> buildInvertedIndex(VirtualColumnContext context) {
    return new ConstantSortedIndexReader(context.getTotalDocCount());
  }

  @Override
  public ColumnMetadataImpl buildMetadata(VirtualColumnContext context) {
    long creationTime = getCreationTime(context);
    return new ColumnMetadataImpl.Builder().setFieldSpec(context.getFieldSpec())
        .setTotalDocs(context.getTotalDocCount())
        .setCardinality(1)
        .setSorted(true)
        .setHasDictionary(true)
        .setMinValue(creationTime)
        .setMaxValue(creationTime)
        .build();
  }

  private static long getCreationTime(VirtualColumnContext context) {
    return Objects.requireNonNull(context.getSegmentMetadata(), "Segment metadata is required for $creationTime")
        .getIndexCreationTime();
  }
}
