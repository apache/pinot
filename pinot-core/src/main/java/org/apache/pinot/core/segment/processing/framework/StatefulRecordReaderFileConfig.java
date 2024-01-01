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
package org.apache.pinot.core.segment.processing.framework;

import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;


public class StatefulRecordReaderFileConfig {
  private final RecordReaderFileConfig _recordReaderFileConfig;
  private RecordReader _recordReader;
  private boolean _isFirstTime = true;

  // Pass in the info needed to initialize the reader
  public StatefulRecordReaderFileConfig(RecordReaderFileConfig recordReaderFileConfig) {
    _recordReaderFileConfig = recordReaderFileConfig;
    _recordReader = null;
  }

  public RecordReader getRecordReader() {
    if (_recordReaderFileConfig._recordReader != null && _isFirstTime) {
      _isFirstTime = false;
      return _recordReaderFileConfig._recordReader;
    }
    return _recordReader;
  }

  public void setRecordReader(RecordReader recordReader) {
    _recordReader = recordReader;
  }

  public RecordReaderFileConfig getRecordReaderFileConfig() {
    return _recordReaderFileConfig;
  }
}
