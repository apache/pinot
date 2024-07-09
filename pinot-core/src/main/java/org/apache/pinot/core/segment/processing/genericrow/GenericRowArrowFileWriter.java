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

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.apache.arrow.memory.ArrowBuf;
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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


public class GenericRowArrowFileWriter implements Closeable, FileWriter<GenericRow> {

  private final Schema _pinotSchema;
  private final int _maxBatchRows;
  private final long _maxBatchBytes;
  private final Set<String> _sortColumns;
  private final String _baseFileName;
  private final CompressionCodec.Factory _compressionFactory;
  private final CompressionUtil.CodecType _codecType;
  private final Optional<Integer> _compressionLevel;

  private VectorSchemaRoot _sortedVectorRoot;
  private VectorSchemaRoot _unsortedVectorRoot;
  private ArrowFileWriter _sortedWriter;
  private ArrowFileWriter _unsortedWriter;
  private int _batchRowCount;
  private long _sortedBatchByteCount;
  private long _unsortedBatchByteCount;
  private int _batchNumber;

  private BufferAllocator _allocator;


  private Map<String, FileMetadata> _fileMetadata = new HashMap<>();

  private static class FileMetadata {
    int rowCount;
    long byteCount;
  }

  public enum ArrowCompressionType {
    NONE,
    LZ4_FRAME,
    ZSTD
  }

  public GenericRowArrowFileWriter(String baseFileName, Schema pinotSchema, int maxBatchRows, long maxBatchBytes,
      Set<String> sortColumns, ArrowCompressionType compressionType,
      Integer compressionLevel) throws IOException {
    _pinotSchema = pinotSchema;
    _maxBatchRows = maxBatchRows;
    _maxBatchBytes = maxBatchBytes;
    _sortColumns = sortColumns;
    _baseFileName = baseFileName;
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


  private void initNewBatch() throws IOException {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    org.apache.arrow.vector.types.pojo.Schema sortedSchema = getArrowSchemaFromPinotSchema(_pinotSchema, _sortColumns);
    org.apache.arrow.vector.types.pojo.Schema unsortedSchema = getArrowSchemaFromPinotSchema(_pinotSchema, _pinotSchema.getColumnNames().stream()
        .filter(col -> !_sortColumns.contains(col))
        .collect(Collectors.toSet()));

    _sortedVectorRoot = VectorSchemaRoot.create(sortedSchema, allocator);
    _unsortedVectorRoot = VectorSchemaRoot.create(unsortedSchema, allocator);

    String sortedFileName = _baseFileName + "_sorted_" + _batchNumber + ".arrow";
    String unsortedFileName = _baseFileName + "_unsorted_" + _batchNumber + ".arrow";

    _sortedWriter = new ArrowFileWriter(_sortedVectorRoot, null,
        new FileOutputStream(sortedFileName).getChannel(), Collections.emptyMap(),
        IpcOption.DEFAULT, _compressionFactory, _codecType, _compressionLevel);
    _unsortedWriter = new ArrowFileWriter(_unsortedVectorRoot, null,
        new FileOutputStream(unsortedFileName).getChannel(), Collections.emptyMap(),
        IpcOption.DEFAULT, _compressionFactory, _codecType, _compressionLevel);

    _sortedWriter.start();
    _unsortedWriter.start();

    _batchRowCount = 0;
    _sortedBatchByteCount = 0;
    _unsortedBatchByteCount = 0;

    _fileMetadata.put(sortedFileName, new FileMetadata());
    _fileMetadata.put(unsortedFileName, new FileMetadata());
  }

  private org.apache.arrow.vector.types.pojo.Schema getArrowSchemaFromPinotSchema(Schema pinotSchema, @Nullable Set<String> columns) {
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
        org.apache.arrow.vector.types.pojo.Field childField = new org.apache.arrow.vector.types.pojo.Field("item", childType, null);
        List<org.apache.arrow.vector.types.pojo.Field> childFields = Collections.singletonList(childField);
        arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), listType, childFields);
      }
      arrowFields.add(arrowField);
    }
    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
  }

  private void fillVectorsFromGenericRow(VectorSchemaRoot sortedRoot, VectorSchemaRoot unsortedRoot, GenericRow row) {
    for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
      FieldVector fieldVector =
          _sortColumns != null && _sortColumns.contains(fieldSpec.getName()) ? sortedRoot.getVector(fieldSpec.getName())
              : unsortedRoot.getVector(fieldSpec.getName());

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
      } else {
        Object[] values = (Object[]) row.getValue(fieldSpec.getName());
        int numValues = values.length;
        switch (fieldSpec.getDataType().getStoredType()) {
          case INT:
            UnionListWriter listWriter = ((ListVector) fieldVector).getWriter();
            listWriter.setPosition(_batchRowCount);
            listWriter.startList();
            for (Object value : values) {
              listWriter.writeInt((Integer) value);
            }
            listWriter.setValueCount(numValues);
            listWriter.endList();
            break;
          case LONG:
            UnionListWriter listWriterLong = ((ListVector) fieldVector).getWriter();
            listWriterLong.setPosition(_batchRowCount);
            listWriterLong.startList();
            for (Object value : values) {
              listWriterLong.writeBigInt((Long) value);
            }
            listWriterLong.setValueCount(numValues);
            listWriterLong.endList();
            break;
          case FLOAT:
            UnionListWriter listWriterFloat = ((ListVector) fieldVector).getWriter();
            listWriterFloat.setPosition(_batchRowCount);
            listWriterFloat.startList();
            for (Object value : values) {
              listWriterFloat.writeFloat4((Float) value);
            }
            listWriterFloat.setValueCount(numValues);
            listWriterFloat.endList();
            break;
          case DOUBLE:
            UnionListWriter listWriterDouble = ((ListVector) fieldVector).getWriter();
            listWriterDouble.setPosition(_batchRowCount);
            listWriterDouble.startList();
            for (Object value : values) {
              listWriterDouble.writeFloat8((Double) value);
            }
            listWriterDouble.setValueCount(numValues);
            listWriterDouble.endList();
            break;
          case STRING:
            UnionListWriter listWriterString = ((ListVector) fieldVector).getWriter();
            listWriterString.setPosition(_batchRowCount);
            listWriterString.startList();
            for (Object value : values) {
              listWriterString.writeVarChar(value.toString());
            }
            listWriterString.setValueCount(numValues);
            listWriterString.endList();
            break;
          case BYTES:
            UnionListWriter listWriterBytes = ((ListVector) fieldVector).getWriter();
            listWriterBytes.setPosition(_batchRowCount);
            listWriterBytes.startList();
            for (Object value : values) {
              listWriterBytes.writeVarBinary((byte[]) value);
            }
            listWriterBytes.setValueCount(numValues);
            listWriterBytes.endList();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }
      }
      fieldVector.setValueCount(_batchRowCount + 1);
      if (_sortColumns != null && _sortColumns.contains(fieldSpec.getName())) {
        _sortedBatchByteCount += fieldVector.getBufferSize();
      } else {
        _unsortedBatchByteCount += fieldVector.getBufferSize();
      }
    }
    _batchRowCount++;
    sortedRoot.setRowCount(_batchRowCount);
    unsortedRoot.setRowCount(_batchRowCount);
  }


  @Override
  public long writeData(GenericRow genericRow) throws IOException {
    write(genericRow);
    return (_sortedBatchByteCount + _unsortedBatchByteCount);
  }

  public void write(GenericRow genericRow) throws IOException {
    fillVectorsFromGenericRow(_sortedVectorRoot, _unsortedVectorRoot, genericRow);

    if (_batchRowCount >= _maxBatchRows ||
        (_sortedBatchByteCount  + _unsortedBatchByteCount) >= _maxBatchBytes) {
      flushBatch();
    }
  }

  private void flushBatch() throws IOException {
    sortAllColumns();

    _sortedWriter.writeBatch();
    _unsortedWriter.writeBatch();

    String sortedFileName = _baseFileName + "_sorted_" + _batchNumber + ".arrow";
    String unsortedFileName = _baseFileName + "_unsorted_" + _batchNumber + ".arrow";

    _fileMetadata.get(sortedFileName).rowCount = _batchRowCount;
    _fileMetadata.get(sortedFileName).byteCount = _sortedBatchByteCount;
    _fileMetadata.get(unsortedFileName).rowCount = _batchRowCount;
    _fileMetadata.get(unsortedFileName).byteCount = _unsortedBatchByteCount;

    _batchNumber++;
    initNewBatch();
  }

  @Override
  public void close() throws IOException {
    if (_batchRowCount > 0) {
      flushBatch();
    }
    _sortedWriter.end();
    _unsortedWriter.end();
    _sortedWriter.close();
    _unsortedWriter.close();
  }

  private void sortAllColumns() {
    int[] sortIndices = getSortIndices();
    ArrowUtils.inPlaceSortAll(_sortedVectorRoot, sortIndices);
    ArrowUtils.inPlaceSortAll(_unsortedVectorRoot, sortIndices);
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

    for (String sortColumn: _sortColumns) {
      FieldVector sortedVectorRootVector = _sortedVectorRoot.getVector(sortColumn);
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
}
