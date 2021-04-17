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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Pinot Record reader for Parquet file.<p>
 * It has two implementations: {@link ParquetAvroRecordReader} (Default) and {@link ParquetNativeRecordReader}.
 */
public class ParquetRecordReader implements RecordReader {
  private RecordReader _internalParquetRecordReader;
  private boolean _useAvroParquetRecordReader = true;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    if (recordReaderConfig == null || ((ParquetRecordReaderConfig) recordReaderConfig).useParquetAvroRecordReader()) {
      _internalParquetRecordReader = new ParquetAvroRecordReader();
    } else {
      _useAvroParquetRecordReader = false;
      _internalParquetRecordReader = new ParquetNativeRecordReader();
    }
    _internalParquetRecordReader.init(dataFile, fieldsToRead, recordReaderConfig);
  }

  @Override
  public boolean hasNext() {
    return _internalParquetRecordReader.hasNext();
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    return _internalParquetRecordReader.next(reuse);
  }

  @Override
  public void rewind() throws IOException {
    _internalParquetRecordReader.rewind();
  }

  @Override
  public void close() throws IOException {
    _internalParquetRecordReader.close();
  }

  public boolean useAvroParquetRecordReader() {
    return _useAvroParquetRecordReader;
  }
}
