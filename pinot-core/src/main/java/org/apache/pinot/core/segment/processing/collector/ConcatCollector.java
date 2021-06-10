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
package org.apache.pinot.core.segment.processing.collector;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Arrays;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessingUtils;
import org.apache.pinot.core.segment.processing.utils.SortOrderComparator;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A Collector implementation for collecting and concatenating all incoming rows.
 */
public class ConcatCollector implements Collector {
  private final int _numSortColumns;
  private final SortOrderComparator _sortOrderComparator;
  private final File _workingDir;
  private final GenericRowFileManager _recordFileManager;

  private GenericRowFileWriter _recordFileWriter;
  private GenericRowFileReader _recordFileReader;
  private int _numDocs;

  public ConcatCollector(CollectorConfig collectorConfig, Schema schema) {
    List<String> sortOrder = collectorConfig.getSortOrder();
    List<FieldSpec> fieldSpecs;
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      fieldSpecs = SegmentProcessingUtils.getFieldSpecs(schema, sortOrder);
      _numSortColumns = sortOrder.size();
      _sortOrderComparator = SegmentProcessingUtils.getSortOrderComparator(fieldSpecs, _numSortColumns);
    } else {
      fieldSpecs = SegmentProcessingUtils.getFieldSpecs(schema);
      _numSortColumns = 0;
      _sortOrderComparator = null;
    }

    _workingDir =
        new File(FileUtils.getTempDirectory(), String.format("concat_collector_%d", System.currentTimeMillis()));
    Preconditions.checkState(_workingDir.mkdirs(), "Failed to create dir: %s for %s with config: %s",
        _workingDir.getAbsolutePath(), ConcatCollector.class.getSimpleName(), collectorConfig);

    // TODO: Pass 'includeNullFields' from the config
    _recordFileManager = new GenericRowFileManager(_workingDir, fieldSpecs, true);
    try {
      _recordFileWriter = _recordFileManager.getFileWriter();
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while creating the file writer", e);
    }
  }

  @Override
  public void collect(GenericRow genericRow)
      throws IOException {
    _recordFileWriter.write(genericRow);
    _numDocs++;
  }

  @Override
  public Iterator<GenericRow> iterator()
      throws IOException {
    _recordFileManager.closeFileWriter();
    _recordFileReader = _recordFileManager.getFileReader();

    // TODO: A lot of this code can be made common across Collectors, once {@link RollupCollector} is also converted to off heap implementation
    if (_numSortColumns != 0) {
      int[] sortedDocIds = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        sortedDocIds[i] = i;
      }

      Arrays.quickSort(0, _numDocs, (i1, i2) -> _sortOrderComparator
          .compare(_recordFileReader.partialRead(sortedDocIds[i1], _numSortColumns),
              _recordFileReader.partialRead(sortedDocIds[i2], _numSortColumns)), (i1, i2) -> {
        int temp = sortedDocIds[i1];
        sortedDocIds[i1] = sortedDocIds[i2];
        sortedDocIds[i2] = temp;
      });
      return createIterator(sortedDocIds);
    } else {
      return createIterator(null);
    }
  }

  private Iterator<GenericRow> createIterator(@Nullable int[] sortedDocIds) {
    return new Iterator<GenericRow>() {
      final GenericRow _reuse = new GenericRow();
      int _nextDocId = 0;

      @Override
      public boolean hasNext() {
        return _nextDocId < _numDocs;
      }

      @Override
      public GenericRow next() {
        int docId = sortedDocIds != null ? sortedDocIds[_nextDocId++] : _nextDocId++;
        return _recordFileReader.read(docId, _reuse);
      }
    };
  }

  @Override
  public int size() {
    return _numDocs;
  }

  @Override
  public void reset()
      throws IOException {
    _recordFileManager.cleanUp();
    _recordFileWriter = _recordFileManager.getFileWriter();
    _numDocs = 0;
  }

  @Override
  public void close()
      throws IOException {
    try {
      _recordFileManager.cleanUp();
    } finally {
      FileUtils.deleteQuietly(_workingDir);
    }
  }
}
