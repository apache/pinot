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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;

/**
 * Avro Record reader for Parquet file. This reader doesn't read parquet file with incompatible Avro schemas,
 * e.g. INT96, DECIMAL. Please use {@link org.apache.pinot.plugin.inputformat.parquet.ParquetNativeRecordReader}
 * instead.<p><p>
 * For More info on Avro to Parquet schema conversion:
 * <a href="https://javadoc.io/doc/org.apache.parquet/parquet-avro/latest/index.html">
 *   https://javadoc.io/doc/org.apache.parquet/parquet-avro/latest/index.html</a>
 */
public class ParquetAvroRecordReader implements RecordReader {
  private static final String EXTENSION = "parquet";

  private Path _dataFilePath;
  private ParquetAvroRecordExtractor _recordExtractor;
  private ParquetReader<GenericRecord> _parquetReader;
  private GenericRecord _nextRecord;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    File parquetFile = RecordReaderUtils.unpackIfRequired(dataFile, EXTENSION);
    _dataFilePath = new Path(parquetFile.getAbsolutePath());
    _parquetReader = ParquetUtils.getParquetAvroReader(_dataFilePath);
    _recordExtractor = new ParquetAvroRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    _nextRecord = _parquetReader.read();
  }

  @Override
  public boolean hasNext() {
    return _nextRecord != null;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    _recordExtractor.extract(_nextRecord, reuse);
    _nextRecord = _parquetReader.read();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _parquetReader.close();
    _parquetReader = ParquetUtils.getParquetAvroReader(_dataFilePath);
    _nextRecord = _parquetReader.read();
  }

  @Override
  public void close()
      throws IOException {
    _parquetReader.close();
  }
}
