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
package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;


public class GenericRowArrowFileWriter implements Closeable, FileWriter<GenericRow> {
  public static final String SORT_COLUMNS_DATA_DIR = "sort_columns";
  public static final String NON_SORT_COLUMNS_DATA_DIR = "non_sort_columns";
  public static final String CHUNK_METADATA_JSON_FILE = "chunk_metadata.json";
  private final String _outputDir;
  private final Schema _pinotSchema;
  private final int _maxBatchRows;
  private final long _maxBatchBytes;
  private final Set<String> _sortColumns;
  private final CompressionCodec.Factory _compressionFactory;
  private final CompressionUtil.CodecType _codecType;
  private final Optional<Integer> _compressionLevel;

  private VectorSchemaRoot _sortColumnsVectorRoot;
  private VectorSchemaRoot _nonSortColumnsVectorRoot;
  private ArrowFileWriter _sortColumnsWriter;
  private ArrowFileWriter _nonSortColumnsWriter;
  private int _batchRowCount;
  private long _sortColumnsBatchByteCount;
  private long _nonSortColumnsBatchByteCount;
  private int _batchNumber;
  private BufferAllocator _allocator;
  private Map<String, UnionListWriter> _listWriters = new HashMap<>();

  private Map<String, FileMetadata> _fileMetadata = new HashMap<>();

  private static class FileMetadata {
    @JsonProperty("rowCount")
    public int _rowCount;

    @JsonProperty("byteCount")
    public long _byteCount;

    // Default constructor for Jackson
    public FileMetadata() {
    }

    public FileMetadata(int rowCount, long byteCount) {
      _rowCount = rowCount;
      _byteCount = byteCount;
    }
  }

  public enum ArrowCompressionType {
    NONE, LZ4_FRAME, ZSTD
  }

  public GenericRowArrowFileWriter(String outputDir, Schema pinotSchema, int maxBatchRows, long maxBatchBytes,
      Set<String> sortColumns, ArrowCompressionType compressionType, Integer compressionLevel)
      throws IOException {
    _outputDir = outputDir;
    _pinotSchema = pinotSchema;
    _maxBatchRows = maxBatchRows;
    _maxBatchBytes = maxBatchBytes;
    _sortColumns = sortColumns;
    _batchNumber = 0;
    _allocator = new RootAllocator(Long.MAX_VALUE);

    // Set up compression
    switch (compressionType) {
      case LZ4_FRAME:
        _compressionFactory = new CommonsCompressionFactory();
        _codecType = CompressionUtil.CodecType.LZ4_FRAME;
        _compressionLevel = Optional.empty();
        break;
      case ZSTD:
        _compressionFactory = new CommonsCompressionFactory();
        _codecType = CompressionUtil.CodecType.ZSTD;
        _compressionLevel = Optional.ofNullable(compressionLevel);
        if (_compressionLevel.isPresent() && (_compressionLevel.get() < 1 || _compressionLevel.get() > 22)) {
          throw new IllegalArgumentException("ZSTD compression level must be between 1 and 22");
        }
        break;
      case NONE:
      default:
        _compressionFactory = new NoCompressionCodec.Factory();
        _codecType = CompressionUtil.CodecType.NO_COMPRESSION;
        _compressionLevel = Optional.empty();
        break;
    }

    initNewBatch();
  }

  private void initNewBatch()
      throws IOException {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    resetListWriters();

    org.apache.arrow.vector.types.pojo.Schema sortColumnsSchema =
        getArrowSchemaFromPinotSchema(_pinotSchema, _sortColumns);
    org.apache.arrow.vector.types.pojo.Schema nonSortColumnsSchema =
        getArrowSchemaFromPinotSchema(_pinotSchema, _pinotSchema.getColumnNames().stream()
            .filter(col -> !_sortColumns.contains(col))
            .collect(Collectors.toSet()));

    _sortColumnsVectorRoot = VectorSchemaRoot.create(sortColumnsSchema, allocator);
    _nonSortColumnsVectorRoot = VectorSchemaRoot.create(nonSortColumnsSchema, allocator);

    String sortColFileName =
        StringUtils.join(new String[]{_outputDir, SORT_COLUMNS_DATA_DIR, _batchNumber + ".arrow"},
            File.separator);
    String nonSortColFileName =
        StringUtils.join(new String[]{_outputDir, NON_SORT_COLUMNS_DATA_DIR, _batchNumber + ".arrow"},
            File.separator);

    // ensure dirs are created
    new File(_outputDir + File.separator + SORT_COLUMNS_DATA_DIR).mkdirs();
    new File(_outputDir + File.separator + NON_SORT_COLUMNS_DATA_DIR).mkdirs();

    _sortColumnsWriter =
        new ArrowFileWriter(_sortColumnsVectorRoot, null, new FileOutputStream(sortColFileName).getChannel(),
            Collections.emptyMap(), IpcOption.DEFAULT, _compressionFactory, _codecType, _compressionLevel);
    _nonSortColumnsWriter =
        new ArrowFileWriter(_nonSortColumnsVectorRoot, null, new FileOutputStream(nonSortColFileName).getChannel(),
            Collections.emptyMap(), IpcOption.DEFAULT, _compressionFactory, _codecType, _compressionLevel);

    _sortColumnsWriter.start();
    _nonSortColumnsWriter.start();

    _batchRowCount = 0;
    _sortColumnsBatchByteCount = 0;
    _nonSortColumnsBatchByteCount = 0;

    _fileMetadata.put(sortColFileName, new FileMetadata());
    _fileMetadata.put(nonSortColFileName, new FileMetadata());
  }

  private org.apache.arrow.vector.types.pojo.Schema getArrowSchemaFromPinotSchema(Schema pinotSchema,
      @Nullable Set<String> columns) {
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();

    for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
      if (columns != null && !columns.contains(fieldSpec.getName())) {
        continue;
      }
      FieldSpec.DataType storedType = fieldSpec.getDataType().getStoredType();
      FieldType fieldType;
      org.apache.arrow.vector.types.pojo.Field arrowField;
      if (fieldSpec.isSingleValueField()) {
        switch (storedType) {
          case INT:
            fieldType = FieldType.nullable(new ArrowType.Int(32, true));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            break;
          case LONG:
            fieldType = FieldType.nullable(new ArrowType.Int(64, true));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            break;
          case FLOAT:
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            break;
          case DOUBLE:
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            break;
          case STRING:
            fieldType = FieldType.nullable(new ArrowType.Utf8());
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            break;
          case BYTES:
            fieldType = FieldType.nullable(new ArrowType.Binary());
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      } else {
        FieldType listType = new FieldType(true, new ArrowType.List(), null);
        FieldType childType;
        switch (storedType) {
          case INT:
            childType = new FieldType(true, new ArrowType.Int(32, true), null);
            break;
          case LONG:
            childType = new FieldType(true, new ArrowType.Int(64, true), null);
            break;
          case FLOAT:
            childType = new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
            break;
          case DOUBLE:
            childType = new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
            break;
          case STRING:
            childType = new FieldType(true, new ArrowType.Utf8(), null);
            break;
          case BYTES:
            childType = new FieldType(true, new ArrowType.Binary(), null);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
        org.apache.arrow.vector.types.pojo.Field childField =
            new org.apache.arrow.vector.types.pojo.Field("item", childType, null);
        List<org.apache.arrow.vector.types.pojo.Field> childFields = Collections.singletonList(childField);
        arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), listType, childFields);
      }
      arrowFields.add(arrowField);
    }
    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
  }

  private void fillVectorsFromGenericRow(VectorSchemaRoot sortColumnsRoot, VectorSchemaRoot nonSortColumnsRoot,
      GenericRow row) {
    for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
      FieldVector fieldVector =
          _sortColumns != null && _sortColumns.contains(fieldSpec.getName()) ? sortColumnsRoot.getVector(
              fieldSpec.getName()) : nonSortColumnsRoot.getVector(fieldSpec.getName());

      byte[] bytes;
      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType().getStoredType()) {
          case INT:
            ((IntVector) fieldVector).setSafe(_batchRowCount, (Integer) row.getValue(fieldSpec.getName()));
            break;
          case LONG:
            ((BigIntVector) fieldVector).setSafe(_batchRowCount, (Long) row.getValue(fieldSpec.getName()));
            break;
          case FLOAT:
            ((Float4Vector) fieldVector).setSafe(_batchRowCount, (Float) row.getValue(fieldSpec.getName()));
            break;
          case DOUBLE:
            ((Float8Vector) fieldVector).setSafe(_batchRowCount, (Double) row.getValue(fieldSpec.getName()));
            break;
          case STRING:
            bytes = row.getValue(fieldSpec.getName()).toString().getBytes();
            ((VarCharVector) fieldVector).setSafe(_batchRowCount, bytes, 0, bytes.length);
            break;
          case BYTES:
            bytes = (byte[]) row.getValue(fieldSpec.getName());
            ((VarBinaryVector) fieldVector).setSafe(_batchRowCount, bytes, 0, bytes.length);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }

        fieldVector.setValueCount(_batchRowCount + 1);
      } else {
        Object[] values = (Object[]) row.getValue(fieldSpec.getName());
        UnionListWriter listWriter =
            _listWriters.computeIfAbsent(fieldSpec.getName(), k -> ((ListVector) fieldVector).getWriter());
        listWriter.startList();
        switch (fieldSpec.getDataType().getStoredType()) {
          case INT:
            for (Object value : values) {
              listWriter.writeInt((Integer) value);
            }
            break;
          case LONG:
            for (Object value : values) {
              listWriter.writeBigInt((Long) value);
            }
            break;
          case FLOAT:
            for (Object value : values) {
              listWriter.writeFloat4((Float) value);
            }
            break;
          case DOUBLE:
            for (Object value : values) {
              listWriter.writeFloat8((Double) value);
            }
            break;
          case STRING:
            for (Object value : values) {
              listWriter.writeVarChar(value.toString());
            }
            break;
          case BYTES:
            for (Object value : values) {
              listWriter.writeVarBinary((byte[]) value);
            }
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }
        listWriter.endList();
        listWriter.setValueCount(_batchRowCount + 1);
      }
      if (_sortColumns != null && _sortColumns.contains(fieldSpec.getName())) {
        _sortColumnsBatchByteCount += fieldVector.getBufferSize();
      } else {
        _nonSortColumnsBatchByteCount += fieldVector.getBufferSize();
      }
    }
    _batchRowCount++;
    sortColumnsRoot.setRowCount(_batchRowCount);
    nonSortColumnsRoot.setRowCount(_batchRowCount);
  }

  private void resetListWriters() {
    for (UnionListWriter writer : _listWriters.values()) {
      writer.setValueCount(0);
    }
    _listWriters.clear();
  }

  @Override
  public long writeData(GenericRow genericRow)
      throws IOException {
    write(genericRow);
    return (_sortColumnsBatchByteCount + _nonSortColumnsBatchByteCount);
  }

  public void write(GenericRow genericRow)
      throws IOException {
    fillVectorsFromGenericRow(_sortColumnsVectorRoot, _nonSortColumnsVectorRoot, genericRow);

    if (_batchRowCount >= _maxBatchRows
        || (_sortColumnsBatchByteCount + _nonSortColumnsBatchByteCount) >= _maxBatchBytes) {
      flushBatch();
    }
  }

  private void flushBatch()
      throws IOException {
    sortAllColumns();

    _sortColumnsWriter.writeBatch();
    _sortColumnsWriter.end();

    _nonSortColumnsWriter.writeBatch();
    _nonSortColumnsWriter.end();

    String sortColFileName =
        StringUtils.join(new String[]{_outputDir, SORT_COLUMNS_DATA_DIR, _batchNumber + ".arrow"}, File.separator);
    String nonSortColFileName =
        StringUtils.join(new String[]{_outputDir, NON_SORT_COLUMNS_DATA_DIR, _batchNumber + ".arrow"}, File.separator);

    _fileMetadata.put(sortColFileName, new FileMetadata(_batchRowCount, _sortColumnsBatchByteCount));
    _fileMetadata.put(nonSortColFileName, new FileMetadata(_batchRowCount, _nonSortColumnsBatchByteCount));

    _batchNumber++;
    initNewBatch();
  }

  @Override
  public void close()
      throws IOException {
    if (_batchRowCount > 0) {
      flushBatch();
    }
    _sortColumnsWriter.close();
    _nonSortColumnsWriter.close();

    writeMetadataAsJson();
  }

  private void sortAllColumns() {
    int[] sortIndices = getSortIndices();
    ArrowSortUtils.inPlaceSortAll(_sortColumnsVectorRoot, sortIndices);
    ArrowSortUtils.inPlaceSortAll(_nonSortColumnsVectorRoot, sortIndices);
  }

  private int[] getSortIndices() {
    IntVector indices = new IntVector("sort_indices", _allocator);
    indices.allocateNew(_batchRowCount);
    indices.setValueCount(_batchRowCount);

    for (int i = 0; i < _batchRowCount; i++) {
      indices.set(i, i);
    }

    //TODO: creating this for every batch might have overhead, check if moving to constructor doesn't impact correctness
    IndexSorter indexSorter = new IndexSorter();

    for (String sortColumn : _sortColumns) {
      FieldVector sortedVectorRootVector = _sortColumnsVectorRoot.getVector(sortColumn);
      VectorValueComparator comparator = DefaultVectorComparators.createDefaultComparator(sortedVectorRootVector);
      indexSorter.sort(sortedVectorRootVector, indices, comparator);
    }

    int[] sortIndices = new int[_batchRowCount];
    for (int i = 0; i < _batchRowCount; i++) {
      sortIndices[i] = indices.get(i);
    }

    indices.close();
    return sortIndices;
  }

  public Map<String, FileMetadata> getFileMetadata() {
    return Collections.unmodifiableMap(_fileMetadata);
  }

  private void writeMetadataAsJson()
      throws IOException {
    String metadataFileContent = JsonUtils.objectToString(_fileMetadata);
    Files.write(Paths.get(_outputDir, CHUNK_METADATA_JSON_FILE), metadataFileContent.getBytes());
  }
}
