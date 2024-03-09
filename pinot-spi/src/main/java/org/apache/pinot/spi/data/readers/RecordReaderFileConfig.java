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
 * Placeholder for all RecordReader configs. Manages the lifecycle of a RecordReader by initing/closing within the
 * Segment creation framework.
 */
public class RecordReaderFileConfig {
  public final FileFormat _fileFormat;
  public final File _dataFile;
  public final Set<String> _fieldsToRead;
  public final RecordReaderConfig _recordReaderConfig;
  private final boolean _isDelegateReader;
  // Record Readers created/passed from clients.
  public RecordReader _recordReader;
  // Track if RecordReaderFileConfig initialized the RecordReader for to aid in closing the RecordReader.
  private boolean _isRecordReaderInitialized;
  // Track if RecordReaderFileConfig closed the RecordReader for testing purposes.
  private boolean _isRecordReaderClosed;

  // Pass in the info needed to initialize the reader
  public RecordReaderFileConfig(FileFormat fileFormat, File dataFile, Set<String> fieldsToRead,
      @Nullable RecordReaderConfig recordReaderConfig, @Nullable RecordReader recordReader) {
    _fileFormat = fileFormat;
    _dataFile = dataFile;
    _fieldsToRead = fieldsToRead;
    _recordReaderConfig = recordReaderConfig;
    // Users can pass in custom readers
    _recordReader = recordReader;
    // RecordReaderFileConfig owns the lifecycle of RecordReader, to be inited and closed.
    _isDelegateReader = false;
    _isRecordReaderInitialized = false;
    _isRecordReaderClosed = false;
  }

  // Keeping this for backwards compatibility. We want the lifecycle of the reader to be managed internally
  // (inited/closed) by SegmentProcessorFramework.
  @Deprecated
  public RecordReaderFileConfig(RecordReader recordReader) {
    _recordReader = recordReader;
    _fileFormat = null;
    _dataFile = null;
    _fieldsToRead = null;
    _recordReaderConfig = null;
    // This is a delegate RecordReader i.e. RecordReader instance has been passed to RecordReaderFileConfig instead
    // of the configs. It means RecordReaderFileConfig does not own the RecordReader, so it should not be closed by
    // RecordReaderFileConfig as well. The responsibility of closing the RecordReader lies with the caller.
    _isDelegateReader = true;
    _isRecordReaderInitialized = true;
    _isRecordReaderClosed = false;
  }

  // Return the RecordReader instance. Initialize the RecordReader if not already initialized.
  public RecordReader getRecordReader()
      throws Exception {
    if (!_isRecordReaderInitialized) {
      if (_recordReader == null) {
        // Record reader instance to be created and inited
        _recordReader = RecordReaderFactory.getRecordReader(_fileFormat, _dataFile, _fieldsToRead, _recordReaderConfig);
      } else {
        _recordReader.init(_dataFile, _fieldsToRead, _recordReaderConfig);
      }
      _isRecordReaderInitialized = true;
    }
    return _recordReader;
  }

  // Close the RecordReader instance if RecordReaderFileConfig initialized it.
  public void closeRecordReader()
      throws Exception {
    // If RecordReaderFileConfig did not create the RecordReader, then it should not close it.
    if (_isRecordReaderInitialized && !_isDelegateReader) {
      _recordReader.close();
      _isRecordReaderClosed = true;
    }
  }

  // Return true if RecordReader is done processing.
  public boolean isRecordReaderDone() {
    if (_isRecordReaderInitialized) {
      return !_recordReader.hasNext();
    }
    return false;
  }

  // For testing purposes only.
  public boolean isRecordReaderClosedFromRecordReaderFileConfig() {
    return _isRecordReaderClosed;
  }
}
