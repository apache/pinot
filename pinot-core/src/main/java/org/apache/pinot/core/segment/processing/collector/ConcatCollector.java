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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.util.GenericRowSerDeUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A Collector implementation for collecting and concatenating all incoming rows.
 */
public class ConcatCollector implements Collector {
  private static final String RECORDS_FILE_NAME = "collector.records";

  private final List<FieldSpec> _fieldSpecs = new ArrayList<>();
  private final Comparator<GenericRow> _genericRowComparator;
  private int _numDocs;

  private final File _workingDir;
  private final File _collectorRecordFile;
  // TODO: Avoid using BufferedOutputStream, and use ByteBuffer directly.
  //  However, ByteBuffer has a limitation that the size cannot exceed 2G.
  //  There are no limits on the size of data inserted into the {@link Collector}.
  //  Hence, would need to implement a hybrid approach or a trigger a flush when size exceeds on Collector.
  private BufferedOutputStream _collectorRecordOutputStream;
  private List<Long> _collectorRecordOffsets;
  private PinotDataBuffer _collectorRecordBuffer;

  public ConcatCollector(CollectorConfig collectorConfig, Schema schema) {

    for (FieldSpec spec : schema.getAllFieldSpecs()) {
      if (!spec.isVirtualColumn()) {
        _fieldSpecs.add(spec);
      }
    }
    List<String> sortOrder = collectorConfig.getSortOrder();
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      GenericRowSorter sorter = new GenericRowSorter(sortOrder, schema);
      _genericRowComparator = sorter.getGenericRowComparator();
    } else {
      _genericRowComparator = null;
    }

    _workingDir =
        new File(FileUtils.getTempDirectory(), String.format("concat_collector_%d", System.currentTimeMillis()));
    Preconditions.checkState(_workingDir.mkdirs(), "Failed to create dir: %s for %s with config: %s",
        _workingDir.getAbsolutePath(), ConcatCollector.class.getSimpleName(), collectorConfig);
    _collectorRecordFile = new File(_workingDir, RECORDS_FILE_NAME);

    initializeBuffer();
  }

  private void initializeBuffer() {

    Preconditions.checkState(!_collectorRecordFile.exists(),
        "Collector record file: " + _collectorRecordFile + " already exists");
    try {
      _collectorRecordOutputStream = new BufferedOutputStream(new FileOutputStream(_collectorRecordFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    _collectorRecordOffsets = new ArrayList<>();
    _collectorRecordOffsets.add(0L);
    _numDocs = 0;
  }

  @Override
  public void collect(GenericRow genericRow)
      throws IOException {
    byte[] genericRowBytes = GenericRowSerDeUtils.serializeGenericRow(genericRow, _fieldSpecs);
    _collectorRecordOutputStream.write(genericRowBytes);
    _collectorRecordOffsets.add(_collectorRecordOffsets.get(_numDocs) + genericRowBytes.length);
    _numDocs++;
  }

  @Override
  public Iterator<GenericRow> iterator()
      throws IOException {

    _collectorRecordOutputStream.flush();
    _collectorRecordBuffer = PinotDataBuffer
        .mapFile(_collectorRecordFile, true, 0, _collectorRecordOffsets.get(_numDocs), PinotDataBuffer.NATIVE_ORDER,
            "ConcatCollector: generic row buffer");

    // TODO: A lot of this code can be made common across Collectors, once {@link RollupCollector} is also converted to off heap implementation
    if (_genericRowComparator != null) {
      int[] sortedDocIds = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        sortedDocIds[i] = i;
      }

      Arrays.quickSort(0, _numDocs, (i1, i2) -> {
        long startOffset1 = _collectorRecordOffsets.get(sortedDocIds[i1]);
        long startOffset2 = _collectorRecordOffsets.get(sortedDocIds[i2]);
        GenericRow row1 = GenericRowSerDeUtils
            .deserializeGenericRow(_collectorRecordBuffer, startOffset1, _fieldSpecs, new GenericRow());
        GenericRow row2 = GenericRowSerDeUtils
            .deserializeGenericRow(_collectorRecordBuffer, startOffset2, _fieldSpecs, new GenericRow());
        return _genericRowComparator.compare(row1, row2);
      }, (i1, i2) -> {
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
        long offset;
        if (sortedDocIds == null) {
          offset = _collectorRecordOffsets.get(_nextDocId++);
        } else {
          offset = _collectorRecordOffsets.get(sortedDocIds[_nextDocId++]);
        }
        return GenericRowSerDeUtils.deserializeGenericRow(_collectorRecordBuffer, offset, _fieldSpecs, _reuse);
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
    try {
      if (_collectorRecordBuffer != null) {
        _collectorRecordBuffer.close();
      }
      if (_collectorRecordOutputStream != null) {
        _collectorRecordOutputStream.close();
      }
    } finally {
      FileUtils.deleteQuietly(_collectorRecordFile);
    }
    initializeBuffer();
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (_collectorRecordBuffer != null) {
        _collectorRecordBuffer.close();
      }
      if (_collectorRecordOutputStream != null) {
        _collectorRecordOutputStream.close();
      }
    } finally {
      FileUtils.deleteQuietly(_workingDir);
    }
  }
}
