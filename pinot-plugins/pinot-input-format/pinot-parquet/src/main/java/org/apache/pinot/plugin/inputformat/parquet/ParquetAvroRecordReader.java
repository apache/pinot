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
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractorConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordFetchException;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;

/// Avro Record reader for Parquet file. This reader doesn't read parquet file with incompatible Avro schemas,
/// e.g. INT96, DECIMAL. Please use [org.apache.pinot.plugin.inputformat.parquet.ParquetNativeRecordReader]
/// instead.
///
/// For More info on Avro to Parquet schema conversion:
/// [https://javadoc.io/doc/org.apache.parquet/parquet-avro/latest/index.html][ref1]
///
/// [ref1]: https://javadoc.io/doc/org.apache.parquet/parquet-avro/latest/index.html
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
    Path dataFilePath = new Path(parquetFile.getAbsolutePath());
    AvroRecordExtractorConfig extractorConfig = new AvroRecordExtractorConfig();
    if (recordReaderConfig instanceof ParquetRecordReaderConfig) {
      extractorConfig.setExtractRawTimeValues(
          ((ParquetRecordReaderConfig) recordReaderConfig).isExtractRawTimeValues());
    }
    ParquetAvroRecordExtractor recordExtractor = new ParquetAvroRecordExtractor();
    recordExtractor.init(fieldsToRead, extractorConfig);

    ParquetReader<GenericRecord> parquetReader = ParquetUtils.getParquetAvroReader(dataFilePath);
    GenericRecord nextRecord;
    try {
      nextRecord = parquetReader.read();
    } catch (IOException | RuntimeException e) {
      try {
        parquetReader.close();
      } catch (IOException | RuntimeException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
    ParquetReader<GenericRecord> previousReader = _parquetReader;
    // Publish only the fully initialized replacement. A previous-reader close failure must not restore stale state.
    _dataFilePath = dataFilePath;
    _parquetReader = parquetReader;
    _recordExtractor = recordExtractor;
    _nextRecord = nextRecord;
    if (previousReader != null) {
      previousReader.close();
    }
  }

  @Override
  public boolean hasNext() {
    return _nextRecord != null;
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    // Data parsing: extract current record into GenericRow.
    _recordExtractor.extract(_nextRecord, reuse);
    // Record fetch: read next Parquet Avro record.
    try {
      _nextRecord = _parquetReader.read();
    } catch (IOException e) {
      throw new RecordFetchException("Failed to read next Parquet Avro record", e);
    }
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    ParquetReader<GenericRecord> parquetReader = _parquetReader;
    _parquetReader = null;
    _nextRecord = null;
    parquetReader.close();

    ParquetReader<GenericRecord> rewoundReader = ParquetUtils.getParquetAvroReader(_dataFilePath);
    try {
      _nextRecord = rewoundReader.read();
      _parquetReader = rewoundReader;
    } catch (IOException | RuntimeException e) {
      try {
        rewoundReader.close();
      } catch (IOException | RuntimeException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  @Override
  public void close()
      throws IOException {
    ParquetReader<GenericRecord> parquetReader = _parquetReader;
    _dataFilePath = null;
    _recordExtractor = null;
    _parquetReader = null;
    _nextRecord = null;
    if (parquetReader != null) {
      parquetReader.close();
    }
  }
}
