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
package org.apache.pinot.plugin.inputformat.arrow.util;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


public class ArrowTestDataUtil {

  private ArrowTestDataUtil() {
  }

  public static byte[] createValidArrowIpcData(int numRows)
      throws Exception {
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

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  public static byte[] createMultiTypeArrowIpcData(int numRows)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
      Field priceField =
          new Field(
              "price",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              null);
      Field activeField = new Field("active", FieldType.nullable(new ArrowType.Bool()), null);
      Field timestampField =
          new Field(
              "timestamp",
              FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
              null);

      Schema schema =
          new Schema(Arrays.asList(idField, nameField, priceField, activeField, timestampField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        Float8Vector priceVector = (Float8Vector) root.getVector("price");
        BitVector activeVector = (BitVector) root.getVector("active");
        TimeStampMilliVector timestampVector = (TimeStampMilliVector) root.getVector("timestamp");

        root.allocateNew();
        idVector.allocateNew(numRows);
        nameVector.allocateNew(numRows * 20, numRows);
        priceVector.allocateNew(numRows);
        activeVector.allocateNew(numRows);
        timestampVector.allocateNew(numRows);

        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);
          nameVector.set(i, ("product_" + (i + 1)).getBytes());
          priceVector.set(i, 10.99 + (i * 5.0));
          activeVector.set(i, i % 2 == 0 ? 1 : 0);
          timestampVector.set(i, baseTime + (i * 1000L));
        }

        idVector.setValueCount(numRows);
        nameVector.setValueCount(numRows);
        priceVector.setValueCount(numRows);
        activeVector.setValueCount(numRows);
        timestampVector.setValueCount(numRows);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  public static byte[] createMultiBatchArrowIpcData(int batchCount, int rowsPerBatch)
      throws Exception {
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

  public static byte[] createEmptyArrowIpcData()
      throws Exception {
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

  public static byte[] createDictionaryEncodedArrowIpcData(int numRows)
      throws Exception {
    List<String> dictionaryValues = Arrays.asList("Electronics", "Books", "Clothing", "Home");
    DictionaryEncoding dictionaryEncoding =
        new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VarCharVector dictionaryVector = new VarCharVector("category_dict", allocator);
        IntVector idVector = new IntVector("id", allocator);
        Float8Vector priceVector = new Float8Vector("price", allocator);
        VarCharVector categoryUnencoded =
            new VarCharVector(
                "category",
                new FieldType(true, new ArrowType.Utf8(), dictionaryEncoding),
                allocator)) {

      dictionaryVector.allocateNew();
      for (int i = 0; i < dictionaryValues.size(); i++) {
        dictionaryVector.set(i, dictionaryValues.get(i).getBytes());
      }
      dictionaryVector.setValueCount(dictionaryValues.size());

      Dictionary dictionary = new Dictionary(dictionaryVector, dictionaryEncoding);
      DictionaryProvider.MapDictionaryProvider dictionaryProvider =
          new DictionaryProvider.MapDictionaryProvider();
      dictionaryProvider.put(dictionary);

      idVector.allocateNew(numRows);
      priceVector.allocateNew(numRows);
      categoryUnencoded.allocateNew(numRows);

      for (int i = 0; i < numRows; i++) {
        idVector.set(i, i + 1);
        categoryUnencoded.set(i, dictionaryValues.get(i % dictionaryValues.size()).getBytes());
        priceVector.set(i, 19.99 + (i * 10.0));
      }
      idVector.setValueCount(numRows);
      priceVector.setValueCount(numRows);
      categoryUnencoded.setValueCount(numRows);

      try (org.apache.arrow.vector.FieldVector encodedCategoryVector =
          (org.apache.arrow.vector.FieldVector)
              DictionaryEncoder.encode(categoryUnencoded, dictionary)) {
        List<Field> fields =
            Arrays.asList(
                idVector.getField(), encodedCategoryVector.getField(), priceVector.getField());
        List<org.apache.arrow.vector.FieldVector> vectors =
            Arrays.asList(idVector, encodedCategoryVector, priceVector);
        try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors)) {
          return writeArrowDataToBytes(root, dictionaryProvider);
        }
      }
    }
  }

  public static byte[] createListArrowIpcData(int numRows)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field numbersElementField =
          new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field numbersField =
          new Field(
              "numbers",
              FieldType.nullable(new ArrowType.List()),
              Arrays.asList(numbersElementField));

      Field tagsElementField = new Field("$data$", FieldType.nullable(new ArrowType.Utf8()), null);
      Field tagsField =
          new Field(
              "tags", FieldType.nullable(new ArrowType.List()), Arrays.asList(tagsElementField));

      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Arrays.asList(idField, numbersField, tagsField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        ListVector numbersVector = (ListVector) root.getVector("numbers");
        ListVector tagsVector = (ListVector) root.getVector("tags");
        IntVector numbersChild = (IntVector) numbersVector.getDataVector();
        VarCharVector tagsChild = (VarCharVector) tagsVector.getDataVector();

        root.allocateNew();
        idVector.allocateNew(numRows);
        numbersVector.allocateNew();
        tagsVector.allocateNew();

        int numbersElemIndex = 0;
        int tagsElemIndex = 0;

        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);

          numbersVector.startNewValue(i);
          for (int j = 0; j <= i; j++) {
            numbersChild.setSafe(numbersElemIndex++, (i + 1) * 10 + j);
          }
          numbersVector.endValue(i, i + 1);

          tagsVector.startNewValue(i);
          for (int j = 0; j < 2; j++) {
            tagsChild.setSafe(tagsElemIndex++, ("tag_" + i + "_" + j).getBytes());
          }
          tagsVector.endValue(i, 2);
        }

        idVector.setValueCount(numRows);
        numbersChild.setValueCount(numbersElemIndex);
        numbersVector.setValueCount(numRows);
        tagsChild.setValueCount(tagsElemIndex);
        tagsVector.setValueCount(numRows);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  public static byte[] createStructArrowIpcData(int numRows)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
      Field ageField = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field streetField = new Field("street", FieldType.nullable(new ArrowType.Utf8()), null);
      Field cityField = new Field("city", FieldType.nullable(new ArrowType.Utf8()), null);
      Field addressField =
          new Field(
              "address",
              FieldType.nullable(new ArrowType.Struct()),
              Arrays.asList(streetField, cityField));

      Field personField =
          new Field(
              "person",
              FieldType.nullable(new ArrowType.Struct()),
              Arrays.asList(nameField, ageField, addressField));

      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Arrays.asList(idField, personField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        StructVector personVector = (StructVector) root.getVector("person");

        root.allocateNew();
        idVector.allocateNew(numRows);
        personVector.allocateNew();

        VarCharVector nameVector = (VarCharVector) personVector.getChild("name");
        IntVector ageVector = (IntVector) personVector.getChild("age");
        StructVector addressVector = (StructVector) personVector.getChild("address");
        VarCharVector streetVector = (VarCharVector) addressVector.getChild("street");
        VarCharVector cityVector = (VarCharVector) addressVector.getChild("city");

        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);
          personVector.setIndexDefined(i);
          addressVector.setIndexDefined(i);
          nameVector.setSafe(i, ("Person_" + (i + 1)).getBytes());
          ageVector.setSafe(i, 25 + i);
          streetVector.setSafe(i, ((i + 1) + " Main St").getBytes());
          cityVector.setSafe(i, ("City_" + (i + 1)).getBytes());
        }

        idVector.setValueCount(numRows);
        personVector.setValueCount(numRows);
        nameVector.setValueCount(numRows);
        ageVector.setValueCount(numRows);
        addressVector.setValueCount(numRows);
        streetVector.setValueCount(numRows);
        cityVector.setValueCount(numRows);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  public static byte[] createMapArrowIpcData(int numRows)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field keyField =
          new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Utf8()), null);
      Field valField =
          new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field entriesField =
          new Field(
              MapVector.DATA_VECTOR_NAME,
              FieldType.notNullable(new ArrowType.Struct()),
              Arrays.asList(keyField, valField));
      Field mapField =
          new Field(
              "metadata",
              FieldType.nullable(new ArrowType.Map(false)),
              Arrays.asList(entriesField));

      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Arrays.asList(idField, mapField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        MapVector mapVector = (MapVector) root.getVector("metadata");
        StructVector entries = (StructVector) mapVector.getDataVector();
        VarCharVector keyVector = (VarCharVector) entries.getChild(MapVector.KEY_NAME);
        IntVector valueVector = (IntVector) entries.getChild(MapVector.VALUE_NAME);

        root.allocateNew();
        idVector.allocateNew(numRows);
        mapVector.allocateNew();

        int entryIndex = 0;
        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);
          int entriesCount = 2 + (i % 2);
          mapVector.startNewValue(i);
          for (int j = 0; j < entriesCount; j++) {
            keyVector.setSafe(entryIndex, ("key_" + i + "_" + j).getBytes());
            valueVector.setSafe(entryIndex, (i + 1) * 100 + j);
            entries.setIndexDefined(entryIndex);
            entryIndex++;
          }
          mapVector.endValue(i, entriesCount);
        }

        idVector.setValueCount(numRows);
        keyVector.setValueCount(entryIndex);
        valueVector.setValueCount(entryIndex);
        entries.setValueCount(entryIndex);
        mapVector.setValueCount(numRows);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  public static byte[] createNestedMapArrowIpcData(int numRows)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // Define inner map (value of outer map)
      Field innerKeyField =
          new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Utf8()), null);
      Field innerValField =
          new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field innerEntriesField =
          new Field(
              MapVector.DATA_VECTOR_NAME,
              FieldType.notNullable(new ArrowType.Struct()),
              Arrays.asList(innerKeyField, innerValField));
      Field innerMapField =
          new Field(
              MapVector.VALUE_NAME,
              FieldType.nullable(new ArrowType.Map(false)),
              Arrays.asList(innerEntriesField));

      // Define outer map with value as the inner map
      Field outerKeyField =
          new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Utf8()), null);
      Field outerEntriesField =
          new Field(
              MapVector.DATA_VECTOR_NAME,
              FieldType.notNullable(new ArrowType.Struct()),
              Arrays.asList(outerKeyField, innerMapField));
      Field outerMapField =
          new Field(
              "metadata",
              FieldType.nullable(new ArrowType.Map(false)),
              Arrays.asList(outerEntriesField));

      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Arrays.asList(idField, outerMapField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        MapVector outerMapVector = (MapVector) root.getVector("metadata");
        StructVector outerEntries = (StructVector) outerMapVector.getDataVector();
        VarCharVector outerKeyVector = (VarCharVector) outerEntries.getChild(MapVector.KEY_NAME);
        MapVector innerMapVector = (MapVector) outerEntries.getChild(MapVector.VALUE_NAME);
        StructVector innerEntries = (StructVector) innerMapVector.getDataVector();
        VarCharVector innerKeyVector = (VarCharVector) innerEntries.getChild(MapVector.KEY_NAME);
        IntVector innerValueVector = (IntVector) innerEntries.getChild(MapVector.VALUE_NAME);

        root.allocateNew();
        idVector.allocateNew(numRows);
        outerMapVector.allocateNew();

        int outerEntryIndex = 0;
        int innerEntryIndex = 0;
        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);

          int outerEntriesCount = 2 + (i % 2); // 2 or 3 outer entries
          outerMapVector.startNewValue(i);
          for (int j = 0; j < outerEntriesCount; j++) {
            // Set outer key
            outerKeyVector.setSafe(outerEntryIndex, ("outer_key_" + i + "_" + j).getBytes());

            // Populate inner map for this outer entry at aligned index
            innerMapVector.startNewValue(outerEntryIndex);
            int innerEntriesCount = 2 + (j % 2); // 2 or 3 inner entries
            for (int k = 0; k < innerEntriesCount; k++) {
              innerKeyVector.setSafe(
                  innerEntryIndex, ("inner_key_" + i + "_" + j + "_" + k).getBytes());
              innerValueVector.setSafe(innerEntryIndex, (i + 1) * 1000 + j * 10 + k);
              innerEntries.setIndexDefined(innerEntryIndex);
              innerEntryIndex++;
            }
            innerMapVector.endValue(outerEntryIndex, innerEntriesCount);

            outerEntries.setIndexDefined(outerEntryIndex);
            outerEntryIndex++;
          }
          outerMapVector.endValue(i, outerEntriesCount);
        }

        idVector.setValueCount(numRows);
        outerKeyVector.setValueCount(outerEntryIndex);
        innerKeyVector.setValueCount(innerEntryIndex);
        innerValueVector.setValueCount(innerEntryIndex);
        innerEntries.setValueCount(innerEntryIndex);
        innerMapVector.setValueCount(outerEntryIndex);
        outerEntries.setValueCount(outerEntryIndex);
        outerMapVector.setValueCount(numRows);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  public static byte[] createNestedListStructArrowIpcData(int numRows)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field itemNameField = new Field("item_name", FieldType.nullable(new ArrowType.Utf8()), null);
      Field itemPriceField =
          new Field(
              "item_price",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              null);
      Field itemStructField =
          new Field(
              "$data$",
              FieldType.nullable(new ArrowType.Struct()),
              Arrays.asList(itemNameField, itemPriceField));

      Field itemsField =
          new Field(
              "items", FieldType.nullable(new ArrowType.List()), Arrays.asList(itemStructField));

      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Arrays.asList(idField, itemsField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector idVector = (IntVector) root.getVector("id");
        ListVector itemsVector = (ListVector) root.getVector("items");
        StructVector itemStructVector = (StructVector) itemsVector.getDataVector();
        VarCharVector itemNameVector = (VarCharVector) itemStructVector.getChild("item_name");
        Float8Vector itemPriceVector = (Float8Vector) itemStructVector.getChild("item_price");

        root.allocateNew();
        idVector.allocateNew(numRows);
        itemsVector.allocateNew();

        int structIndex = 0;
        for (int i = 0; i < numRows; i++) {
          idVector.set(i, i + 1);
          int itemsCount = 1 + (i % 3);
          itemsVector.startNewValue(i);
          for (int j = 0; j < itemsCount; j++) {
            itemNameVector.setSafe(structIndex, ("item_" + i + "_" + j).getBytes());
            itemPriceVector.setSafe(structIndex, 10.99 + (i * 5.0) + j);
            itemStructVector.setIndexDefined(structIndex);
            structIndex++;
          }
          itemsVector.endValue(i, itemsCount);
        }

        idVector.setValueCount(numRows);
        itemsVector.setValueCount(numRows);
        itemNameVector.setValueCount(structIndex);
        itemPriceVector.setValueCount(structIndex);
        root.setRowCount(numRows);

        return writeArrowDataToBytes(root, null);
      }
    }
  }

  private static byte[] writeArrowDataToBytes(
      VectorSchemaRoot root, DictionaryProvider dictionaryProvider)
      throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (WritableByteChannel channel = Channels.newChannel(outputStream);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, dictionaryProvider, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return outputStream.toByteArray();
  }
}
