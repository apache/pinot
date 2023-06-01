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
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Wraps around a RecordReader and does lazy initialization. Initialization of
 * the reader happens when the reader is used (i.e first call to the iterator)
 */
public class LazyRecordReader implements RecordReader {
  RecordReader _delegate;
  File _dataFile;
  Set<String> _fieldsToRead;
  RecordReaderConfig _recordReaderConfig;
  boolean _initied;

  public LazyRecordReader(RecordReader delegate, File dataFile, @Nullable Set<String> fieldsToRead,
      @Nullable RecordReaderConfig recordReaderConfig) {
    _delegate = delegate;
    _dataFile = dataFile;
    _fieldsToRead = fieldsToRead;
    _recordReaderConfig = recordReaderConfig;
    _initied = false;
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    Preconditions.checkState(!_initied, "RecordReader already initialized");
    _delegate.init(dataFile, fieldsToRead, recordReaderConfig);
  }

  @Override
  public boolean hasNext() throws IOException {
    checkAndInit();
    return _delegate.hasNext();
  }

  public GenericRow next()
      throws IOException {
    checkAndInit();
    return _delegate.next();
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    checkAndInit();
    return _delegate.next(reuse);
  }

  @Override
  public void rewind()
      throws IOException {
    checkAndInit();
    _delegate.rewind();
  }

  @Override
  public void close()
      throws IOException {
    _delegate.close();
  }

  private void checkAndInit() throws IOException {
    if (!_initied) {
      init(_dataFile, _fieldsToRead, _recordReaderConfig);
      _initied = true;
    }
  }
}
