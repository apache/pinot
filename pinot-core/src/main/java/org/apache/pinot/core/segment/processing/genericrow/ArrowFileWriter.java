package org.apache.pinot.core.segment.processing.genericrow;
import java.io.Closeable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArrowFileWriter implements Closeable, FileWriter<GenericRow> {
  private final ArrowFileWriter fileWriter;
  private final VectorSchemaRoot root;
  private final BufferAllocator allocator;
  private final List<FieldVector> vectors;
  private final int chunkSizeBytes;
  private int rowCount;

  public ArrowFileWriter(File outputFile, List<FieldSpec> fieldSpecs, int chunkSizeBytes) throws IOException {
    this.chunkSizeBytes = chunkSizeBytes;
    this.allocator = new RootAllocator();
    this.vectors = new ArrayList<>();
    List<Field> fields = new ArrayList<>();

    for (FieldSpec fieldSpec : fieldSpecs) {
      Field field = createField(fieldSpec);
      fields.add(field);
      FieldVector vector = createVector(field);
      vectors.add(vector);
    }

    Schema schema = new Schema(fields);
    this.root = new VectorSchemaRoot(schema, vectors, 0);
    this.fileWriter = new ArrowFileWriter(root, null, new FileOutputStream(outputFile));
    this.fileWriter.start();
    this.rowCount = 0;
  }

  private Field createField(FieldSpec fieldSpec) {
    String name = fieldSpec.getName();
    FieldSpec.DataType dataType = fieldSpec.getDataType();
    boolean nullable = !fieldSpec.isSingleValueField();

    ArrowType arrowType;
    switch (dataType) {
      case INT:
        arrowType = new ArrowType.Int(32, true);
        break;
      case LONG:
        arrowType = new ArrowType.Int(64, true);
        break;
      case FLOAT:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        break;
      case STRING:
        arrowType = new ArrowType.Utf8();
        break;
      case BYTES:
        arrowType = new ArrowType.Binary();
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    return new Field(name, new FieldType(nullable, arrowType, null), null);
  }

  private FieldVector createVector(Field field) {
    return field.createVector(allocator);
  }

  @Override
  public void write(GenericRow genericRow) throws IOException {
    for (int i = 0; i < vectors.size(); i++) {
      FieldVector vector = vectors.get(i);
      String fieldName = vector.getField().getName();
      Object value = genericRow.getValue(fieldName);

      if (value == null || genericRow.isNullValue(fieldName)) {
        vector.setNull(rowCount);
      } else {
        writeValueToVector(vector, rowCount, value);
      }
    }

    rowCount++;

    if (root.getBufferSize() >= chunkSizeBytes) {
      flush();
    }
  }

  private void writeValueToVector(FieldVector vector, int index, Object value) {
    if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(index, (Integer) value);
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(index, (Long) value);
    } else if (vector instanceof Float4Vector) {
      ((Float4Vector) vector).setSafe(index, (Float) value);
    } else if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(index, (Double) value);
    } else if (vector instanceof VarCharVector) {
      ((VarCharVector) vector).setSafe(index, value.toString().getBytes());
    } else if (vector instanceof VarBinaryVector) {
      ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
    } else {
      throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }
  }

  private void flush() throws IOException {
    root.setRowCount(rowCount);
    fileWriter.writeBatch();
    root.clear();
    rowCount = 0;
  }

  @Override
  public void close() throws IOException {
    if (rowCount > 0) {
      flush();
    }
    fileWriter.end();
    fileWriter.close();
    root.close();
    allocator.close();
  }
}
