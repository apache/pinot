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
import java.util.List;
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
import org.apache.parquet.schema.Type;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordFetchException;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/// Record reader for Native Parquet file.
public class ParquetNativeRecordReader implements RecordReader {
  private static final String EXTENSION = "parquet";

  private Path _dataFilePath;
  private File _columnarSource;
  private boolean _useColumnMajorSegmentBuilder;
  private ParquetNativeRecordExtractor _recordExtractor;
  private MessageType _schema;
  private ParquetFileReader _parquetFileReader;
  private Group _nextRecord;
  private PageReadStore _pageReadStore;
  private MessageColumnIO _columnIO;
  private org.apache.parquet.io.RecordReader<Group> _parquetRecordReader;
  private org.apache.parquet.io.RecordReader<GenericRow> _directParquetRecordReader;
  private ParquetNativeRecordMaterializer _directMaterializer;
  private int _currentPageIdx;
  private Configuration _hadoopConf;
  private ParquetReadOptions _parquetReadOptions;
  @Nullable
  private MessageType _initialRequestedSchema;
  @Nullable
  private Set<String> _initialFieldsToRead;
  private boolean _extractRawTimeValues;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    File parquetFile = RecordReaderUtils.unpackIfRequired(dataFile, EXTENSION);
    _dataFilePath = new Path(parquetFile.getAbsolutePath());
    _columnarSource = parquetFile.getAbsoluteFile();
    _useColumnMajorSegmentBuilder = false;
    _initialFieldsToRead = fieldsToRead == null ? null : Set.copyOf(fieldsToRead);
    _extractRawTimeValues = false;
    _hadoopConf = ParquetUtils.getParquetHadoopConfiguration();
    ParquetNativeRecordExtractorConfig extractorConfig = new ParquetNativeRecordExtractorConfig();
    if (recordReaderConfig instanceof ParquetRecordReaderConfig) {
      extractorConfig.setExtractRawTimeValues(
          ((ParquetRecordReaderConfig) recordReaderConfig).isExtractRawTimeValues());
    }
    _recordExtractor = new ParquetNativeRecordExtractor();
    _recordExtractor.init(fieldsToRead, extractorConfig);

    _parquetReadOptions = ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build();

    _parquetFileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(_dataFilePath, _hadoopConf), _parquetReadOptions);
    _schema = _parquetFileReader.getFooter().getFileMetaData().getSchema();
    _directMaterializer = null;
    if (recordReaderConfig instanceof ParquetRecordReaderConfig) {
      ParquetRecordReaderConfig parquetConfig = (ParquetRecordReaderConfig) recordReaderConfig;
      _useColumnMajorSegmentBuilder = parquetConfig.useColumnMajorSegmentBuilder();
      _extractRawTimeValues = parquetConfig.isExtractRawTimeValues();
      if (parquetConfig.useParquetNativeDirectReader() && ParquetNativeRecordMaterializer.supports(_schema)) {
        MessageType requestedSchema = projectSchema(_schema, fieldsToRead);
        // Parquet cannot materialize rows with a zero-column schema. Preserve the existing missing-field behavior.
        _initialRequestedSchema = requestedSchema.getFieldCount() == 0 ? _schema : requestedSchema;
        _directMaterializer = new ParquetNativeRecordMaterializer(_initialRequestedSchema, fieldsToRead,
            _extractRawTimeValues);
        _parquetFileReader.setRequestedSchema(_initialRequestedSchema);
      }
    }
    _columnIO = new ColumnIOFactory().getColumnIO(
        _directMaterializer != null ? _initialRequestedSchema : _schema, _schema);
    init();
  }

  private void init()
      throws IOException {
    _pageReadStore = _parquetFileReader.readNextRowGroup();
    // If the parquet file is initially empty, then we cannot set up the _pageRecordReader.
    // It's expected a user would always call init() -> hasNext() -> next().
    // Without this, an empty parquet file will fail to init.
    if (_pageReadStore != null) {
      initRowGroupReader();
      _currentPageIdx = 0;
    }
  }

  @Nullable
  @Override
  public ColumnReaderFactory tryCreateColumnReaderFactory(Schema targetSchema)
      throws IOException {
    if (!_useColumnMajorSegmentBuilder || _directMaterializer == null) {
      return null;
    }
    return ParquetColumnReaderFactory.tryCreate(_columnarSource, _initialFieldsToRead, _extractRawTimeValues,
        targetSchema);
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
      initRowGroupReader();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return hasNext();
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    if (_directMaterializer != null) {
      _directMaterializer.setRecord(reuse);
      try {
        _directParquetRecordReader.read();
      } catch (Exception e) {
        throw new RecordFetchException("Failed to read next Parquet native record", e);
      }
      _currentPageIdx++;
      return reuse;
    }
    // Record fetch: read next record from Parquet page.
    try {
      _nextRecord = _parquetRecordReader.read();
    } catch (Exception e) {
      throw new RecordFetchException("Failed to read next Parquet native record", e);
    }
    // Data parsing: extract into GenericRow.
    _recordExtractor.extract(_nextRecord, reuse);
    _currentPageIdx++;
    return reuse;
  }

  private void initRowGroupReader() {
    if (_directMaterializer != null) {
      _directParquetRecordReader = _columnIO.getRecordReader(_pageReadStore, _directMaterializer);
    } else {
      _parquetRecordReader = _columnIO.getRecordReader(_pageReadStore, new GroupRecordConverter(_schema));
    }
  }

  @VisibleForTesting
  boolean isDirectRecordReader() {
    return _directMaterializer != null;
  }

  @VisibleForTesting
  PageReadStore getPageReadStore() {
    return _pageReadStore;
  }

  @Override
  public void rewind()
      throws IOException {
    reopen(_initialRequestedSchema);
  }

  @Override
  public boolean rewind(@Nullable Set<String> fieldsToRead)
      throws IOException {
    if (_directMaterializer == null) {
      return false;
    }
    MessageType requestedSchema = projectSchema(projectSchema(_schema, _initialFieldsToRead), fieldsToRead);
    if (requestedSchema.getFieldCount() == 0) {
      return false;
    }
    reopen(requestedSchema);
    return true;
  }

  private static MessageType projectSchema(MessageType schema, @Nullable Set<String> fieldsToRead) {
    if (fieldsToRead == null || fieldsToRead.isEmpty()) {
      return schema;
    }
    List<Type> fields = schema.getFields().stream().filter(field -> fieldsToRead.contains(field.getName())).toList();
    return new MessageType(schema.getName(), fields);
  }

  private void reopen(@Nullable MessageType requestedSchema)
      throws IOException {
    _parquetFileReader.close();
    _parquetFileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(_dataFilePath, _hadoopConf), _parquetReadOptions);
    if (_directMaterializer != null) {
      _parquetFileReader.setRequestedSchema(requestedSchema);
      _columnIO = new ColumnIOFactory().getColumnIO(requestedSchema, _schema);
      _directMaterializer = new ParquetNativeRecordMaterializer(requestedSchema, _initialFieldsToRead,
          _extractRawTimeValues);
    }
    init();
  }

  @Override
  public void close()
      throws IOException {
    _parquetFileReader.close();
  }
}
