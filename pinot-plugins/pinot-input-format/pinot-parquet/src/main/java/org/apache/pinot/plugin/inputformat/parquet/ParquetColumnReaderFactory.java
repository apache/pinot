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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.apache.pinot.spi.utils.PinotDataType;


/// Sequential column readers for required Parquet fields. Required String MAP values targeting JSON are converted to
/// canonical JSON; other values retain their native logical types. Remaining normalization belongs to the caller.
/// A reader materializes just one projected top-level column, sequentially, with at most one live row group.
/// Construction and factory initialization read metadata only; column pages are opened on the first value request.
final class ParquetColumnReaderFactory implements ColumnReaderFactory {
  private static final long serialVersionUID = 1L;
  private final File _source;
  @Nullable
  private final Set<String> _sourceFields;
  private final boolean _extractRawTimeValues;
  private Map<String, ColumnReader> _readers = Map.of();
  private Set<String> _availableColumns = Set.of();
  private boolean _initialized;

  private ParquetColumnReaderFactory(File source, @Nullable Set<String> sourceFields, boolean extractRawTimeValues) {
    _source = Objects.requireNonNull(source, "source").getAbsoluteFile();
    _sourceFields = sourceFields == null || sourceFields.isEmpty() ? null : Set.copyOf(sourceFields);
    _extractRawTimeValues = extractRawTimeValues;
  }

  /// Checks only footer metadata. A successful result owns no row reader, open file, or mutable configuration.
  @Nullable
  static ParquetColumnReaderFactory tryCreate(File source, @Nullable Set<String> sourceFields,
      boolean extractRawTimeValues, Schema targetSchema)
      throws IOException {
    ParquetColumnReaderFactory factory = new ParquetColumnReaderFactory(source, sourceFields, extractRawTimeValues);
    try (ParquetFileReader input = openFile(factory._source)) {
      long rows = input.getRecordCount();
      if (rows < 0) {
        throw new IOException("Negative Parquet row count: " + rows);
      }
      if (rows > Integer.MAX_VALUE
          || !factory.supports(input.getFooter().getFileMetaData().getSchema(), targetSchema)) {
        return null;
      }
    }
    return factory;
  }

  @Override
  public void init(Schema targetSchema)
      throws IOException {
    init(targetSchema, targetSchema.getPhysicalColumnNames());
  }

  @Override
  public void init(Schema targetSchema, Set<String> colsToRead)
      throws IOException {
    Objects.requireNonNull(targetSchema, "targetSchema");
    Objects.requireNonNull(colsToRead, "colsToRead");
    close();
    if (!_source.isFile()) {
      throw new IOException("Parquet source is not a regular file: " + _source);
    }

    MessageType fileSchema;
    int totalDocs;
    try (ParquetFileReader input = openFile(_source)) {
      fileSchema = input.getFooter().getFileMetaData().getSchema();
      if (!supports(fileSchema, targetSchema)) {
        throw new IOException(
            "Unsupported required Parquet columns for target schema: " + targetSchema.getSchemaName());
      }
      try {
        totalDocs = Math.toIntExact(input.getRecordCount());
      } catch (ArithmeticException e) {
        throw new IOException("ColumnReader requires an int row count: " + input.getRecordCount(), e);
      }
      if (totalDocs < 0) {
        throw new IOException("Negative Parquet row count: " + totalDocs);
      }
    }

    Set<String> available = new LinkedHashSet<>();
    fileSchema.getFields().forEach(field -> available.add(field.getName()));
    Set<String> targetColumns = targetSchema.getPhysicalColumnNames();
    Map<String, ColumnReader> readers = new LinkedHashMap<>();
    for (String name : targetColumns) {
      if (!colsToRead.contains(name)) {
        continue;
      }
      if (!available.contains(name)) {
        throw new IOException("Target column is absent from Parquet source: " + name);
      }
      MessageType projection = new MessageType(fileSchema.getName(), fileSchema.getType(name));
      readers.put(name,
          new ProjectedColumnReader(_source, fileSchema, projection, name, totalDocs, _extractRawTimeValues,
              targetSchema.getFieldSpecFor(name).getDataType() == DataType.JSON));
    }
    for (String name : colsToRead) {
      if (!targetColumns.contains(name)) {
        throw new IOException("Selected column is not a physical target column: " + name);
      }
    }
    _availableColumns = Collections.unmodifiableSet(available);
    _readers = Collections.unmodifiableMap(readers);
    _initialized = true;
  }

  private boolean supports(MessageType sourceSchema, Schema targetSchema) {
    Set<String> columns = targetSchema.getPhysicalColumnNames();
    if (columns.isEmpty()) {
      return false;
    }
    for (String name : columns) {
      FieldSpec target = targetSchema.getFieldSpecFor(name);
      if (!target.isSingleValueField() || !sourceSchema.containsField(name)
          || _sourceFields != null && !_sourceFields.contains(name)) {
        return false;
      }
      Type source = sourceSchema.getType(name);
      if (!source.isRepetition(Type.Repetition.REQUIRED)) {
        return false;
      }
      if (target.getDataType() == DataType.JSON) {
        if (!isRequiredStringMap(source)) {
          return false;
        }
        continue;
      }
      if (!source.isPrimitive()) {
        return false;
      }
      LogicalTypeAnnotation logical = source.getLogicalTypeAnnotation();
      PrimitiveTypeName physical = source.asPrimitiveType().getPrimitiveTypeName();
      switch (target.getDataType()) {
        case INT:
          if (physical != PrimitiveTypeName.INT32
              || logical != null && !(logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation)) {
            return false;
          }
          break;
        case LONG:
          if (physical != PrimitiveTypeName.INT64
              || logical != null && !(logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation)) {
            return false;
          }
          break;
        case STRING:
          if ((physical != PrimitiveTypeName.BINARY && physical != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              || !(logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
              || logical instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation)) {
            return false;
          }
          break;
        case TIMESTAMP:
          if (physical != PrimitiveTypeName.INT64
              || !(logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)) {
            return false;
          }
          break;
        case BOOLEAN:
          if (physical != PrimitiveTypeName.BOOLEAN || logical != null) {
            return false;
          }
          break;
        case FLOAT:
          if (physical != PrimitiveTypeName.FLOAT || logical != null) {
            return false;
          }
          break;
        case DOUBLE:
          if (physical != PrimitiveTypeName.DOUBLE || logical != null) {
            return false;
          }
          break;
        case BYTES:
          if ((physical != PrimitiveTypeName.BINARY && physical != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              || logical != null) {
            return false;
          }
          break;
        default:
          return false;
      }
    }
    return true;
  }

  private static boolean isRequiredStringMap(Type source) {
    if (source.isPrimitive() || !LogicalTypeAnnotation.mapType().equals(source.getLogicalTypeAnnotation())) {
      return false;
    }
    GroupType map = source.asGroupType();
    if (map.getFieldCount() != 1 || map.getType(0).isPrimitive()
        || !map.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      return false;
    }
    GroupType entry = map.getType(0).asGroupType();
    // Some producers repeat MAP or legacy MAP_KEY_VALUE on the entry wrapper.
    LogicalTypeAnnotation annotation = entry.getLogicalTypeAnnotation();
    if (annotation != null && !(annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation)
        && !(annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation)) {
      return false;
    }
    if (entry.getFieldCount() != 2 || !entry.containsField("key") || !entry.containsField("value")) {
      return false;
    }
    for (String name : Set.of("key", "value")) {
      Type child = entry.getType(name);
      if (!child.isRepetition(Type.Repetition.REQUIRED) || !child.isPrimitive()
          || child.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.BINARY
          || !LogicalTypeAnnotation.stringType().equals(child.getLogicalTypeAnnotation())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<String> getAvailableColumns() {
    requireInitialized();
    return _availableColumns;
  }

  @Override
  public ColumnReader getColumnReader(String columnName) {
    requireInitialized();
    return _readers.get(columnName);
  }

  @Override
  public Map<String, ColumnReader> getAllColumnReaders() {
    requireInitialized();
    return _readers;
  }

  private void requireInitialized() {
    if (!_initialized) {
      throw new IllegalStateException("Call init() before using the factory");
    }
  }

  @Override
  public void close()
      throws IOException {
    IOException failure = null;
    for (ColumnReader reader : _readers.values()) {
      try {
        reader.close();
      } catch (IOException | RuntimeException e) {
        if (failure == null) {
          failure = e instanceof IOException ? (IOException) e : new IOException("Failed to close column reader", e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    _readers = Map.of();
    _availableColumns = Set.of();
    _initialized = false;
    if (failure != null) {
      throw failure;
    }
  }

  private static ParquetFileReader openFile(File source)
      throws IOException {
    ParquetReadOptions options =
        ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build();
    return ParquetFileReader.open(HadoopInputFile.fromPath(new Path(source.getAbsolutePath()),
        ParquetUtils.getParquetHadoopConfiguration()), options);
  }

  private static final class ProjectedColumnReader implements ColumnReader {
    private static final long serialVersionUID = 1L;

    private final File _source;
    private final MessageType _fileSchema;
    private final MessageType _projection;
    private final String _columnName;
    private final int _totalDocs;
    private final boolean _extractRawTimeValues;
    private final boolean _canonicalJson;
    private final GenericRow _row = new GenericRow();
    private transient ParquetFileReader _input;
    private transient PageReadStore _pageStore;
    private transient org.apache.parquet.io.RecordReader<GenericRow> _recordReader;
    private transient ParquetNativeRecordMaterializer _materializer;
    private transient MessageColumnIO _columnIO;
    private long _remainingGroupRows;
    private int _currentDocId = -1;
    private Object _value;
    private boolean _closed;

    private ProjectedColumnReader(File source, MessageType fileSchema, MessageType projection, String columnName,
        int totalDocs, boolean extractRawTimeValues, boolean canonicalJson) {
      _extractRawTimeValues = extractRawTimeValues;
      _canonicalJson = canonicalJson;
      _source = source;
      _fileSchema = fileSchema;
      _projection = projection;
      _columnName = columnName;
      _totalDocs = totalDocs;
      // Keep this one HashMap node for the reader's entire lifetime. Materializer.start() overwrites its value.
      _row.putValue(columnName, null);
    }

    @Override
    public String getColumnName() {
      return _columnName;
    }

    @Override
    public PinotDataType getValueType() {
      // JSON values are canonical Strings; other values retain their native logical types, including Timestamp.
      return _canonicalJson ? PinotDataType.STRING : null;
    }

    @Override
    public boolean isCanonicalJson() {
      return _canonicalJson;
    }

    @Override
    public int getTotalDocs() {
      return _totalDocs;
    }

    @Override
    public void rewind()
        throws IOException {
      requireOpen();
      try {
        closeSource();
      } catch (IOException e) {
        _closed = true;
        throw e;
      } finally {
        _currentDocId = -1;
        _value = null;
        _row.putValue(_columnName, null);
      }
    }

    @Override
    public boolean isNull(int docId)
        throws IOException {
      return getValue(docId) == null;
    }

    @Override
    public Object getValue(int docId)
        throws IOException {
      requireOpen();
      if (docId < 0 || docId >= _totalDocs) {
        throw new IndexOutOfBoundsException("Document " + docId + " outside [0, " + _totalDocs + ")");
      }
      if (docId == _currentDocId) {
        return _value;
      }
      if (docId < _currentDocId) {
        if (docId != 0) {
          throw new UnsupportedOperationException("Sequential column reader cannot move backward from "
              + _currentDocId + " to " + docId + "; call rewind()");
        }
        rewind();
      }
      try {
        while (_currentDocId < docId) {
          if (_remainingGroupRows == 0) {
            nextRowGroup();
          }
          if (_recordReader.read() == null) {
            throw new IOException("Unexpected missing Parquet record for column " + _columnName);
          }
          _remainingGroupRows--;
          _currentDocId++;
          _value = _row.getValue(_columnName);
        }
        if (_currentDocId == _totalDocs - 1) {
          // All pages can be released now; keep the last owned value for isNull()+getValue() idempotence.
          closeSource();
        }
        return _value;
      } catch (IOException | RuntimeException e) {
        try {
          close();
        } catch (IOException cleanup) {
          e.addSuppressed(cleanup);
        }
        throw e;
      }
    }

    private void requireOpen() {
      if (_closed) {
        throw new IllegalStateException("Column reader is closed: " + _columnName);
      }
    }

    private void nextRowGroup()
        throws IOException {
      if (_input == null) {
        _input = openFile(_source);
        if (!_fileSchema.equals(_input.getFooter().getFileMetaData().getSchema())
            || _input.getRecordCount() != _totalDocs) {
          throw new IOException("Parquet schema or row count changed after factory initialization: " + _source);
        }
        // Set the projection before the first readNextRowGroup(): never fetch the other columns' pages.
        _input.setRequestedSchema(_projection);
        _columnIO = new ColumnIOFactory().getColumnIO(_projection, _fileSchema, true);
        _materializer = new ParquetNativeRecordMaterializer(_projection, Set.of(_columnName), _extractRawTimeValues,
            _canonicalJson ? Set.of(_columnName) : Set.of());
        _materializer.setRecord(_row);
      }
      do {
        releaseRowGroup();
        _pageStore = _input.readNextRowGroup();
        if (_pageStore == null) {
          throw new IOException("Parquet ended before footer row count for column " + _columnName);
        }
        _remainingGroupRows = _pageStore.getRowCount();
      } while (_remainingGroupRows == 0);
      _materializer.resetCanonicalJsonCaches();
      _recordReader = _columnIO.getRecordReader(_pageStore, _materializer);
    }

    private void releaseRowGroup()
        throws IOException {
      PageReadStore pageStore = _pageStore;
      _recordReader = null;
      _pageStore = null;
      _remainingGroupRows = 0;
      if (pageStore != null) {
        try {
          pageStore.close();
        } catch (RuntimeException e) {
          throw new IOException("Failed to release Parquet row group for column " + _columnName, e);
        }
      }
    }

    private void closeSource()
        throws IOException {
      IOException failure = null;
      try {
        releaseRowGroup();
      } catch (IOException e) {
        failure = e;
      }
      ParquetFileReader input = _input;
      _input = null;
      _columnIO = null;
      _materializer = null;
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }

    @Override
    public void close()
        throws IOException {
      if (_closed) {
        return;
      }
      _closed = true;
      _value = null;
      _row.putValue(_columnName, null);
      closeSource();
    }

    private UnsupportedOperationException rawOnly() {
      return new UnsupportedOperationException("Use getValue() for raw logical column " + _columnName);
    }

    @Override
    public int getInt(int docId) {
      throw rawOnly();
    }

    @Override
    public long getLong(int docId) {
      throw rawOnly();
    }

    @Override
    public float getFloat(int docId) {
      throw rawOnly();
    }

    @Override
    public double getDouble(int docId) {
      throw rawOnly();
    }

    @Override
    public BigDecimal getBigDecimal(int docId) {
      throw rawOnly();
    }

    @Override
    public String getString(int docId)
        throws IOException {
      if (!_canonicalJson) {
        throw rawOnly();
      }
      return (String) getValue(docId);
    }

    @Override
    public byte[] getBytes(int docId) {
      throw rawOnly();
    }

    @Override
    public MultiValueResult<int[]> getIntMV(int docId) {
      throw rawOnly();
    }

    @Override
    public MultiValueResult<long[]> getLongMV(int docId) {
      throw rawOnly();
    }

    @Override
    public MultiValueResult<float[]> getFloatMV(int docId) {
      throw rawOnly();
    }

    @Override
    public MultiValueResult<double[]> getDoubleMV(int docId) {
      throw rawOnly();
    }

    @Override
    public BigDecimal[] getBigDecimalMV(int docId) {
      throw rawOnly();
    }

    @Override
    public String[] getStringMV(int docId) {
      throw rawOnly();
    }

    @Override
    public byte[][] getBytesMV(int docId) {
      throw rawOnly();
    }
  }
}
