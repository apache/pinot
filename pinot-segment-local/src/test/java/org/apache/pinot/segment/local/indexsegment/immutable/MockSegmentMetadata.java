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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;


/// Stubs the column accessors of a mocked [SegmentMetadataImpl].
///
/// Mockito stubs every method of the mock, so stubbing [SegmentMetadataImpl#getColumnMetadataMap()] alone leaves the
/// accessors the segment actually reads (the real implementation holds sorted arrays, not a map) answering `null`.
final class MockSegmentMetadata {
  private MockSegmentMetadata() {
  }

  static SegmentMetadataImpl withColumns(SegmentMetadataImpl metadata, Map<String, ColumnMetadata> columns) {
    TreeMap<String, ColumnMetadata> sorted = new TreeMap<>(columns);
    when(metadata.getColumnMetadataMap()).thenReturn(sorted);
    when(metadata.getAllColumns()).thenReturn(Collections.unmodifiableNavigableSet(sorted.navigableKeySet()));
    when(metadata.getAllColumnMetadata()).thenReturn(Collections.unmodifiableCollection(sorted.values()));
    when(metadata.getNumColumns()).thenReturn(sorted.size());
    when(metadata.getColumnMetadataFor(anyString())).thenAnswer(call -> sorted.get(call.<String>getArgument(0)));
    doAnswer(call -> {
      sorted.forEach(call.<BiConsumer<String, ColumnMetadata>>getArgument(0));
      return null;
    }).when(metadata).forEachColumn(any());
    return metadata;
  }
}
