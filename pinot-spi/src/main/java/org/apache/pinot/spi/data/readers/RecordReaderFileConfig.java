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
package org.apache.pinot.spi.data.readers;

import java.io.File;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Wraps RecordReader info to instantiate a reader. Users can either pass in the
 * RecordReader instance directly or the info required to initialize the RecordReader, so that the
 * RecordReader can be initialized just when its about to be used, which avoids early/eager
 * initialization/memory allocation.
 */
public class RecordReaderFileConfig {
  public final FileFormat _fileFormat;
  public final File _dataFile;
  public final Set<String> _fieldsToRead;
  public final RecordReaderConfig _recordReaderConfig;
  private final boolean _isDelegateReader;
  // Record Readers created/passed from clients.
  public RecordReader _recordReader;
  private boolean _isRecordReaderInitialized;
  private boolean _isRecordReaderClosed;

  // Pass in the info needed to initialize the reader
  public RecordReaderFileConfig(FileFormat fileFormat, File dataFile, Set<String> fieldsToRead,
      @Nullable RecordReaderConfig recordReaderConfig) {
    _fileFormat = fileFormat;
    _dataFile = dataFile;
    _fieldsToRead = fieldsToRead;
    _recordReaderConfig = recordReaderConfig;
    _recordReader = null;
    _isDelegateReader = false;
    _isRecordReaderInitialized = false;
    _isRecordReaderClosed = false;
  }

  // Pass in the reader instance directly
  public RecordReaderFileConfig(RecordReader recordReader) {
    _recordReader = recordReader;
    _fileFormat = null;
    _dataFile = null;
    _fieldsToRead = null;
    _recordReaderConfig = null;
    _isDelegateReader = true;
    _isRecordReaderInitialized = true;
    _isRecordReaderClosed = false;
  }

  public RecordReader getRecordReader()
      throws Exception {
    if (!_isRecordReaderInitialized) {
      _recordReader = RecordReaderFactory.getRecordReader(_fileFormat, _dataFile, _fieldsToRead, _recordReaderConfig);
      _isRecordReaderInitialized = true;
    }
    return _recordReader;
  }

  public void closeRecordReader()
      throws Exception {
    // If RecordReaderFileConfig did not create the RecordReader, then it should not close it.
    if (_isRecordReaderInitialized && !_isDelegateReader) {
      _recordReader.close();
      _isRecordReaderClosed = true;
    }
  }

  public boolean isRecordReaderDone() {
    if (_isRecordReaderInitialized) {
      return !_recordReader.hasNext();
    }
    return false;
  }

  public boolean isRecordReaderClosedFromRecordReaderFileConfig() {
    return _isRecordReaderClosed;
  }

  public boolean isRecordReaderInitialized() {
    return _isRecordReaderInitialized;
  }
}
