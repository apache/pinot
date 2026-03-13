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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for Apache Arrow IPC file format.
 */
public class ArrowRecordReader implements RecordReader {
  private File _dataFile;
  private ArrowToGenericRowConverter _converter;
  private RootAllocator _allocator;
  private FileInputStream _fileInputStream;
  private ArrowFileReader _arrowFileReader;
  private VectorSchemaRoot _root;
  private boolean _hasNextBatch;
  private int _nextRowId;
  private int _currentBatchRowCount;

  public ArrowRecordReader() {
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead,
      @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _converter = new ArrowToGenericRowConverter(fieldsToRead);
    _allocator = new RootAllocator();
    openFile();
  }

  private void openFile()
      throws IOException {
    _fileInputStream = new FileInputStream(_dataFile);
    _arrowFileReader = new ArrowFileReader(_fileInputStream.getChannel(), _allocator);
    _root = _arrowFileReader.getVectorSchemaRoot();
    _nextRowId = 0;
    _currentBatchRowCount = 0;
    _hasNextBatch = _arrowFileReader.loadNextBatch();
    if (_hasNextBatch) {
      _currentBatchRowCount = _root.getRowCount();
    }
  }

  @Override
  public boolean hasNext() {
    return _hasNextBatch && _nextRowId < _currentBatchRowCount;
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    _converter.convertSingleRow(_arrowFileReader, _root, _nextRowId, reuse);
    _nextRowId++;
    if (_nextRowId >= _currentBatchRowCount) {
      _hasNextBatch = _arrowFileReader.loadNextBatch();
      if (_hasNextBatch) {
        _currentBatchRowCount = _root.getRowCount();
        _nextRowId = 0;
      }
    }
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    closeFile();
    openFile();
  }

  @Override
  public void close()
      throws IOException {
    closeFile();
    _allocator.close();
  }

  private void closeFile()
      throws IOException {
    _arrowFileReader.close();
    _fileInputStream.close();
  }
}
