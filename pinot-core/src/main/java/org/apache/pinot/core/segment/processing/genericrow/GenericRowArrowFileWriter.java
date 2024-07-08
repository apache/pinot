package org.apache.pinot.core.segment.processing.genericrow;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


public class GenericRowArrowFileWriter implements Closeable, FileWriter<GenericRow> {

  private static final int DEFAULT_BATCH_SIZE = 1024;
  private final DataOutputStream _offsetStream;
  private final FileOutputStream _dataStream;
  private final org.apache.arrow.vector.types.pojo.Schema _arrowSchema;
  private final Schema _pinotSchema;
  private VectorSchemaRoot _vectorSchemaRoot;
  private int _batchRowCount;

  public GenericRowArrowFileWriter(File offsetFile, File dataFile, Schema pinotSchema)
      throws FileNotFoundException {
    _offsetStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(offsetFile)));
    _dataStream = new FileOutputStream(dataFile);
    _arrowSchema = getArrowSchemaFromPinotSchema(pinotSchema);
    _pinotSchema = pinotSchema;
    _vectorSchemaRoot = VectorSchemaRoot.create(_arrowSchema, new RootAllocator(Long.MAX_VALUE));
    _batchRowCount = 0;
  }

  private org.apache.arrow.vector.types.pojo.Schema getArrowSchemaFromPinotSchema(Schema pinotSchema) {
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();

    for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
      FieldSpec.DataType storedType = fieldSpec.getDataType().getStoredType();
      FieldType fieldType;
      org.apache.arrow.vector.types.pojo.Field arrowField;
      if (fieldSpec.isSingleValueField()) {
        switch (storedType) {
          case INT:
            fieldType = FieldType.nullable(new ArrowType.Int(32, true));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case LONG:
            fieldType = FieldType.nullable(new ArrowType.Int(64, true));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case FLOAT:
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case DOUBLE:
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case STRING:
            fieldType = FieldType.nullable(new ArrowType.Utf8());
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case BYTES:
            fieldType = FieldType.nullable(new ArrowType.Binary());
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      } else {
        switch (storedType) {
          case INT:
            fieldType = new FieldType(true, new ArrowType.List.Int(32, true), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case LONG:
            fieldType = new FieldType(true, new ArrowType.List.Int(64, true), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case FLOAT:
            fieldType = new FieldType(true, new ArrowType.List.FloatingPoint(FloatingPointPrecision.SINGLE), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case DOUBLE:
            fieldType = new FieldType(true, new ArrowType.List.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case STRING:
            fieldType = new FieldType(true, new ArrowType.List.Utf8(), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      }
    }
    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
  }

  private void fillVectorsFromGenericRow(VectorSchemaRoot root, GenericRow row) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    int count = 0;
    for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
      FieldVector fieldVector = fieldVectors.get(count++);
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
              bytes = value.toString().getBytes();
              BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
              ArrowBuf arrowBuf = allocator.buffer(bytes.length);
              arrowBuf.writeBytes(bytes);
              listWriterString.writeVarChar(0, bytes.length, arrowBuf);
            }
            listWriterString.setValueCount(numValues);
            listWriterString.endList();
            break;
          case BYTES:
            UnionListWriter listWriterBytes = ((ListVector) fieldVector).getWriter();
            listWriterBytes.setPosition(_batchRowCount);
            listWriterBytes.startList();
            for (Object value : values) {
              bytes = (byte[]) value;
              BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
              ArrowBuf arrowBuf = allocator.buffer(bytes.length);
              arrowBuf.writeBytes(bytes);
              listWriterBytes.writeVarBinary(0, bytes.length, arrowBuf);
            }
            listWriterBytes.setValueCount(numValues);
            listWriterBytes.endList();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }
      }
      _batchRowCount++;
    }
  }

  public void write(GenericRow genericRow) {
    fillVectorsFromGenericRow(_vectorSchemaRoot, genericRow);
    _vectorSchemaRoot.setRowCount(_batchRowCount);
    if (_batchRowCount >= DEFAULT_BATCH_SIZE) {
      try (ArrowFileWriter writer = new ArrowFileWriter(_vectorSchemaRoot, null, _dataStream.getChannel());) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Arrow file", e);
      }
    }
  }

  public long writeData(GenericRow genericRow) {
    fillVectorsFromGenericRow(_vectorSchemaRoot, genericRow);
    _vectorSchemaRoot.setRowCount(_batchRowCount);
    if (_batchRowCount >= DEFAULT_BATCH_SIZE) {
      try (ArrowFileWriter writer = new ArrowFileWriter(_vectorSchemaRoot, null, _dataStream.getChannel());) {
        writer.start();
        writer.writeBatch();
        writer.end();
        return writer.bytesWritten();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Arrow file", e);
      }
    }
    return 0;
  }

  @Override
  public void close() {
    try {
      _offsetStream.close();
      _dataStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close Arrow writer", e);
    }
  }
}
