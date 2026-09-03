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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordFetchException;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/// Record reader for Native Parquet file.
public class ParquetNativeRecordReader implements RecordReader {
  private static final String EXTENSION = "parquet";

  private Path _dataFilePath;
  private ParquetNativeRecordExtractor _recordExtractor;
  private MessageType _schema;
  private ParquetFileReader _parquetFileReader;
  private Group _nextRecord;
  private PageReadStore _pageReadStore;
  private MessageColumnIO _columnIO;
  private org.apache.parquet.io.RecordReader<Group> _parquetRecordReader;
  private int _currentPageIdx;
  private Configuration _hadoopConf;
  private ParquetReadOptions _parquetReadOptions;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    File parquetFile = RecordReaderUtils.unpackIfRequired(dataFile, EXTENSION);
    Path dataFilePath = new Path(parquetFile.getAbsolutePath());
    Configuration hadoopConf = ParquetUtils.getParquetHadoopConfiguration();
    ParquetReadOptions parquetReadOptions =
        ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build();

    ParquetFileReader previousReader = _parquetFileReader;
    ParquetFileReader parquetFileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(dataFilePath, hadoopConf), parquetReadOptions);
    MessageType schema;
    ParquetNativeRecordExtractor recordExtractor;
    MessageColumnIO columnIO;
    PageReadStore pageReadStore;
    org.apache.parquet.io.RecordReader<Group> parquetRecordReader;
    try {
      schema = parquetFileReader.getFooter().getFileMetaData().getSchema();
      ParquetNativeRecordExtractorConfig extractorConfig = new ParquetNativeRecordExtractorConfig();
      if (recordReaderConfig instanceof ParquetRecordReaderConfig) {
        extractorConfig.setExtractRawTimeValues(
            ((ParquetRecordReaderConfig) recordReaderConfig).isExtractRawTimeValues());
      }
      recordExtractor = new ParquetNativeRecordExtractor();
      recordExtractor.init(fieldsToRead, extractorConfig);

      columnIO = new ColumnIOFactory().getColumnIO(schema);
      pageReadStore = parquetFileReader.readNextRowGroup();
      parquetRecordReader = pageReadStore != null
          ? columnIO.getRecordReader(pageReadStore, new GroupRecordConverter(schema)) : null;
    } catch (IOException | RuntimeException e) {
      try {
        parquetFileReader.close();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }

    // Publish the fully initialized replacement before cleaning up the old reader. If old-reader cleanup fails, the
    // caller sees the failure but this instance remains attached to the usable replacement instead of stale,
    // partially closed state.
    _dataFilePath = dataFilePath;
    _hadoopConf = hadoopConf;
    _parquetReadOptions = parquetReadOptions;
    _parquetFileReader = parquetFileReader;
    _schema = schema;
    _recordExtractor = recordExtractor;
    _columnIO = columnIO;
    _pageReadStore = pageReadStore;
    _parquetRecordReader = parquetRecordReader;
    _nextRecord = null;
    _currentPageIdx = 0;
    if (previousReader != null) {
      previousReader.close();
    }
  }

  private void init()
      throws IOException {
    _pageReadStore = _parquetFileReader.readNextRowGroup();
    // If the parquet file is initially empty, then we cannot set up the _pageRecordReader.
    // It's expected a user would always call init() -> hasNext() -> next().
    // Without this, an empty parquet file will fail to init.
    if (_pageReadStore != null) {
      _parquetRecordReader = _columnIO.getRecordReader(_pageReadStore, new GroupRecordConverter(_schema));
      _currentPageIdx = 0;
    }
  }

  @Override
  public boolean hasNext() {
    if (_pageReadStore == null) {
      return false;
    }
    if (_pageReadStore.getRowCount() - _currentPageIdx >= 1) {
      return true;
    }
    try {
      _pageReadStore = _parquetFileReader.readNextRowGroup();
      _currentPageIdx = 0;
      if (_pageReadStore == null) {
        return false;
      }
      _parquetRecordReader = _columnIO.getRecordReader(_pageReadStore, new GroupRecordConverter(_schema));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return hasNext();
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    // Record fetch: read next record from Parquet page.
    try {
      _nextRecord = _parquetRecordReader.read();
    } catch (Exception e) {
      throw new RecordFetchException("Failed to read next Parquet native record", e);
    }
    // The physical reader has consumed the row. Advance before logical extraction so continueOnError callers can
    // recover from malformed values without retrying the same row or leaving hasNext() stuck at end-of-file.
    _currentPageIdx++;
    // Data parsing: extract into GenericRow.
    _recordExtractor.extract(_nextRecord, reuse);
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _parquetFileReader.close();
    _parquetFileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(_dataFilePath, _hadoopConf), _parquetReadOptions);
    init();
  }

  @Override
  public void close()
      throws IOException {
    if (_parquetFileReader != null) {
      _parquetFileReader.close();
    }
  }
}
