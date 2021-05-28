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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A Collector implementation for collecting and concatenating all incoming rows.
 */
public class ConcatCollector implements Collector {
  private static final String RECORD_OFFSET_FILE_NAME = "record.offset";
  private static final String RECORD_DATA_FILE_NAME = "record.data";

  private final List<FieldSpec> _fieldSpecs = new ArrayList<>();
  private final int _numSortColumns;
  private final SortOrderComparator _sortOrderComparator;
  private final File _workingDir;
  private final File _recordOffsetFile;
  private final File _recordDataFile;

  private GenericRowFileWriter _recordFileWriter;
  private GenericRowFileReader _recordFileReader;
  private int _numDocs;

  public ConcatCollector(CollectorConfig collectorConfig, Schema schema) {
    List<String> sortOrder = collectorConfig.getSortOrder();
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      _numSortColumns = sortOrder.size();
      DataType[] sortColumnStoredTypes = new DataType[_numSortColumns];
      for (int i = 0; i < _numSortColumns; i++) {
        String sortColumn = sortOrder.get(i);
        FieldSpec fieldSpec = schema.getFieldSpecFor(sortColumn);
        Preconditions.checkArgument(fieldSpec != null, "Failed to find sort column: %s", sortColumn);
        Preconditions.checkArgument(fieldSpec.isSingleValueField(), "Cannot sort on MV column: %s", sortColumn);
        sortColumnStoredTypes[i] = fieldSpec.getDataType().getStoredType();
        _fieldSpecs.add(fieldSpec);
      }
      _sortOrderComparator = new SortOrderComparator(_numSortColumns, sortColumnStoredTypes);
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        if (!fieldSpec.isVirtualColumn() && !sortOrder.contains(fieldSpec.getName())) {
          _fieldSpecs.add(fieldSpec);
        }
      }
    } else {
      _numSortColumns = 0;
      _sortOrderComparator = null;
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        if (!fieldSpec.isVirtualColumn()) {
          _fieldSpecs.add(fieldSpec);
        }
      }
    }

    _workingDir =
        new File(FileUtils.getTempDirectory(), String.format("concat_collector_%d", System.currentTimeMillis()));
    Preconditions.checkState(_workingDir.mkdirs(), "Failed to create dir: %s for %s with config: %s",
        _workingDir.getAbsolutePath(), ConcatCollector.class.getSimpleName(), collectorConfig);
    _recordOffsetFile = new File(_workingDir, RECORD_OFFSET_FILE_NAME);
    _recordDataFile = new File(_workingDir, RECORD_DATA_FILE_NAME);

    try {
      reset();
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while resetting the collector", e);
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
    _recordFileWriter.close();
    _recordFileReader = new GenericRowFileReader(_recordOffsetFile, _recordDataFile, _fieldSpecs, true);

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
    if (_recordFileWriter != null) {
      _recordFileWriter.close();
    }
    if (_recordFileReader != null) {
      _recordFileReader.close();
    }
    FileUtils.cleanDirectory(_workingDir);
    // TODO: Pass 'includeNullFields' from the config
    _recordFileWriter = new GenericRowFileWriter(_recordOffsetFile, _recordDataFile, _fieldSpecs, true);
    _numDocs = 0;
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (_recordFileWriter != null) {
        _recordFileWriter.close();
      }
      if (_recordFileReader != null) {
        _recordFileReader.close();
      }
    } finally {
      FileUtils.deleteQuietly(_workingDir);
    }
  }
}
