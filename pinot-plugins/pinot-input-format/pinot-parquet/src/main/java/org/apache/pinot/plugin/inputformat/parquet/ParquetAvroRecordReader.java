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

import com.google.common.annotations.VisibleForTesting;
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

/// Reads Parquet records through Avro with logical-type conversion, including INT96 timestamps and DECIMAL values.
/// Parquet schemas that cannot be represented by Avro require [ParquetNativeRecordReader].
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
    _dataFilePath = new Path(parquetFile.getAbsolutePath());
    _parquetReader = ParquetUtils.getParquetAvroReader(_dataFilePath);
    AvroRecordExtractorConfig extractorConfig = new AvroRecordExtractorConfig();
    if (recordReaderConfig instanceof ParquetRecordReaderConfig) {
      extractorConfig.setExtractRawTimeValues(
          ((ParquetRecordReaderConfig) recordReaderConfig).isExtractRawTimeValues());
    }
    _recordExtractor = new ParquetAvroRecordExtractor(ParquetUtils.getUnsignedInt32FieldNames(_dataFilePath));
    _recordExtractor.init(fieldsToRead, extractorConfig);
    _nextRecord = readNextRecord();
  }

  @VisibleForTesting
  @Nullable
  GenericRecord readNextRecord()
      throws IOException {
    return _parquetReader.read();
  }

  @Override
  public boolean hasNext() {
    return _nextRecord != null;
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    Throwable extractFailure = null;
    try {
      // Data parsing: extract current record into GenericRow.
      _recordExtractor.extract(_nextRecord, reuse);
    } catch (Throwable t) {
      extractFailure = t;
      throw t;
    } finally {
      // Advance even if extraction fails and the caller skips the rejected record.
      if (!(extractFailure instanceof Error)) {
        try {
          _nextRecord = readNextRecord();
        } catch (Exception e) {
          RecordFetchException fetchException = e instanceof RecordFetchException
              ? (RecordFetchException) e
              : new RecordFetchException("Failed to read next Parquet Avro record", e);
          if (extractFailure != null) {
            fetchException.addSuppressed(extractFailure);
          }
          throw fetchException;
        }
      }
    }
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _parquetReader.close();
    _parquetReader = ParquetUtils.getParquetAvroReader(_dataFilePath);
    _nextRecord = readNextRecord();
  }

  @Override
  public void close()
      throws IOException {
    _parquetReader.close();
  }
}
