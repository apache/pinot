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
package org.apache.pinot.segment.local.startree.v2.builder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.configuration2.Configuration;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;


/**
 * The {@code OnHeapSingleTreeBuilder} class is the single star-tree builder that uses on-heap memory.
 */
public class OnHeapSingleTreeBuilder extends BaseSingleTreeBuilder {
  private final List<Record> _records = new ArrayList<>();

  /**
   * Constructor for the on-heap single star-tree builder.
   *
   * @param builderConfig Builder config
   * @param outputDir Directory to store the index files
   * @param segment Index segment
   * @param metadataProperties Segment metadata properties
   */
  public OnHeapSingleTreeBuilder(StarTreeV2BuilderConfig builderConfig, File outputDir, ImmutableSegment segment,
      Configuration metadataProperties) {
    super(builderConfig, outputDir, segment, metadataProperties);
  }

  @Override
  void appendRecord(Record record) {
    _records.add(record);
  }

  @Override
  Record getStarTreeRecord(int docId) {
    return _records.get(docId);
  }

  @Override
  int getDimensionValue(int docId, int dimensionId) {
    return _records.get(docId)._dimensions[dimensionId];
  }

  @Override
  Iterator<Record> sortAndAggregateSegmentRecords(int numDocs) {
    Record[] records = new Record[numDocs];
    for (int i = 0; i < numDocs; i++) {
      records[i] = getSegmentRecord(i);
    }
    Arrays.sort(records, (o1, o2) -> {
      for (int i = 0; i < _numDimensions; i++) {
        if (o1._dimensions[i] != o2._dimensions[i]) {
          return o1._dimensions[i] - o2._dimensions[i];
        }
      }
      return 0;
    });
    return new Iterator<Record>() {
      boolean _hasNext = true;
      Record _currentRecord = records[0];
      int _docId = 1;

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Record next() {
        Record next = mergeSegmentRecord(null, _currentRecord);
        while (_docId < numDocs) {
          Record record = records[_docId++];
          if (!Arrays.equals(record._dimensions, next._dimensions)) {
            _currentRecord = record;
            return next;
          } else {
            next = mergeSegmentRecord(next, record);
          }
        }
        _hasNext = false;
        return next;
      }
    };
  }

  @Override
  Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId) {
    int numDocs = endDocId - startDocId;
    Record[] records = new Record[numDocs];
    for (int i = 0; i < numDocs; i++) {
      records[i] = getStarTreeRecord(startDocId + i);
    }
    Arrays.sort(records, (o1, o2) -> {
      for (int i = dimensionId + 1; i < _numDimensions; i++) {
        if (o1._dimensions[i] != o2._dimensions[i]) {
          return o1._dimensions[i] - o2._dimensions[i];
        }
      }
      return 0;
    });
    return new Iterator<Record>() {
      boolean _hasNext = true;
      Record _currentRecord = records[0];
      int _docId = 1;

      private boolean hasSameDimensions(Record record1, Record record2) {
        for (int i = dimensionId + 1; i < _numDimensions; i++) {
          if (record1._dimensions[i] != record2._dimensions[i]) {
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Record next() {
        Record next = mergeStarTreeRecord(null, _currentRecord);
        next._dimensions[dimensionId] = StarTreeV2Constants.STAR_IN_FORWARD_INDEX;
        while (_docId < numDocs) {
          Record record = records[_docId++];
          if (!hasSameDimensions(record, _currentRecord)) {
            _currentRecord = record;
            return next;
          } else {
            next = mergeStarTreeRecord(next, record);
          }
        }
        _hasNext = false;
        return next;
      }
    };
  }
}
