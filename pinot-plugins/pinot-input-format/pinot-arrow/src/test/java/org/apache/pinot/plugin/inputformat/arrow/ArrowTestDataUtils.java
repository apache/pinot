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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


/// Helpers that produce Arrow IPC stream-format byte payloads for the decoder tests in [ArrowMessageDecoderTest].
/// Per-type extraction lives in [ArrowRecordExtractorTest], so this util only covers the shapes the decoder tests
/// need: a small two-column batch, a multi-row batch with a `batch_num` / `value` schema, and an empty zero-batch
/// stream.
public class ArrowTestDataUtils {
  private ArrowTestDataUtils() {
  }

  /// Two-column (`id` INT, `name` STRING) batch with `numRows` rows. Row `i` has `id = i + 1` and
  /// `name = "name_" + (i + 1)`.
  public static byte[] createValidArrowIpcData(int numRows)
      throws IOException {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
      Schema schema = new Schema(Arrays.asList(idField, nameField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");

        root.allocateNew();
        idVector.allocateNew(numRows);
        nameVector.allocateNew(numRows * 10, numRows);

        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);
          nameVector.set(i, ("name_" + (i + 1)).getBytes());
        }

        idVector.setValueCount(numRows);
        nameVector.setValueCount(numRows);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root);
      }
    }
  }

  /// Three-column (`id` INT, `batch_num` INT, `value` STRING) data with `batchCount` batches of
  /// `rowsPerBatch` rows each. `id` is a global running counter (1-based); `batch_num` is the batch
  /// index; `value = "batch_<batch>_row_<row>"`.
  public static byte[] createMultiBatchArrowIpcData(int batchCount, int rowsPerBatch)
      throws IOException {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field batchField =
          new Field("batch_num", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field valueField = new Field("value", FieldType.nullable(new ArrowType.Utf8()), null);
      Schema schema = new Schema(Arrays.asList(idField, batchField, valueField));

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      try (WritableByteChannel channel = Channels.newChannel(outputStream);
          VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {

        writer.start();

        IntVector idVector = (IntVector) root.getVector("id");
        IntVector batchVector = (IntVector) root.getVector("batch_num");
        VarCharVector valueVector = (VarCharVector) root.getVector("value");

        int totalRowId = 1;
        for (int batch = 0; batch < batchCount; batch++) {
          root.allocateNew();
          idVector.allocateNew(rowsPerBatch);
          batchVector.allocateNew(rowsPerBatch);
          valueVector.allocateNew(rowsPerBatch * 15, rowsPerBatch);

          for (int row = 0; row < rowsPerBatch; row++) {
            idVector.set(row, totalRowId++);
            batchVector.set(row, batch);
            valueVector.set(row, ("batch_" + batch + "_row_" + row).getBytes());
          }

          idVector.setValueCount(rowsPerBatch);
          batchVector.setValueCount(rowsPerBatch);
          valueVector.setValueCount(rowsPerBatch);
          root.setRowCount(rowsPerBatch);

          writer.writeBatch();
        }

        writer.end();
        return outputStream.toByteArray();
      }
    }
  }

  /// Stream with the schema header but no record batches.
  public static byte[] createEmptyArrowIpcData()
      throws IOException {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
      Schema schema = new Schema(Arrays.asList(idField, nameField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        root.setRowCount(0);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (WritableByteChannel channel = Channels.newChannel(outputStream);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {
          writer.start();
          writer.end();
        }

        return outputStream.toByteArray();
      }
    }
  }

  private static byte[] writeArrowDataToBytes(VectorSchemaRoot root)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (WritableByteChannel channel = Channels.newChannel(outputStream);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return outputStream.toByteArray();
  }
}
