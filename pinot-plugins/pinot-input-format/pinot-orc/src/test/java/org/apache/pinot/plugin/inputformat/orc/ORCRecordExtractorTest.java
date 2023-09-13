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
package org.apache.pinot.plugin.inputformat.orc;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Tests the {@link ORCRecordReader} using a schema containing groovy transform functions
 */
public class ORCRecordExtractorTest extends AbstractRecordExtractorTest {
  private final File _dataFile = new File(_tempDir, "events.orc");

  /**
   * Create an ORCRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    ORCRecordReader orcRecordReader = new ORCRecordReader();
    orcRecordReader.init(_dataFile, fieldsToRead, null);
    return orcRecordReader;
  }

  /**
   * Create an ORC input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {
    // CHECKSTYLE:OFF
    // @format:off
    TypeDescription schema = TypeDescription.fromString(
        "struct<" + "userID:int," + "firstName:string," + "bids:array<int>," + "cost:double," + "timestamp:bigint,"
            + "simpleStruct:struct<structString:string,structLong:bigint,structDouble:double>,"
            + "complexStruct:struct<structString:string,nestedStruct:struct<nestedStructInt:int,"
            + "nestedStructLong:bigint>>,"
            + "complexList:array<struct<complexListInt:int,complexListDouble:decimal(10,5)>>,"
            + "simpleMap:map<string,int>,"
            + "complexMap:map<string,struct<doubleField:double,stringField:string>>" + ">");
    // @format:on
    // CHECKSTYLE:ON

    int numRecords = _inputRecords.size();
    VectorizedRowBatch rowBatch = schema.createRowBatch(numRecords);
    LongColumnVector userIdVector = (LongColumnVector) rowBatch.cols[0];
    userIdVector.noNulls = false;
    BytesColumnVector firstNameVector = (BytesColumnVector) rowBatch.cols[1];
    firstNameVector.noNulls = false;

    // simple list containing long
    ListColumnVector bidsVector = (ListColumnVector) rowBatch.cols[2];
    bidsVector.noNulls = false;
    LongColumnVector bidsElementVector = (LongColumnVector) bidsVector.child;
    bidsElementVector.ensureSize(6, false);

    DoubleColumnVector costVector = (DoubleColumnVector) rowBatch.cols[3];
    LongColumnVector timestampVector = (LongColumnVector) rowBatch.cols[4];

    // simple struct - string, long, and double
    StructColumnVector simpleStructVector = (StructColumnVector) rowBatch.cols[5];
    simpleStructVector.noNulls = false;
    BytesColumnVector simpleStructBytesVector = (BytesColumnVector) simpleStructVector.fields[0];
    LongColumnVector simpleStructLongVector = (LongColumnVector) simpleStructVector.fields[1];
    DoubleColumnVector simpleStructDoubleVector = (DoubleColumnVector) simpleStructVector.fields[2];

    // complex struct - string and struct containing int and long
    StructColumnVector complexStructVector = (StructColumnVector) rowBatch.cols[6];
    complexStructVector.noNulls = false;
    BytesColumnVector complexStructBytesVector = (BytesColumnVector) complexStructVector.fields[0];
    StructColumnVector complexStructInnerVector = (StructColumnVector) complexStructVector.fields[1];
    LongColumnVector complexStructIntVector = (LongColumnVector) complexStructInnerVector.fields[0];
    LongColumnVector complexStructLongVector = (LongColumnVector) complexStructInnerVector.fields[1];

    // complex list elements - each element is a struct containing int and long
    ListColumnVector complexListVector = (ListColumnVector) rowBatch.cols[7];
    complexListVector.noNulls = false;
    StructColumnVector complexListElementVector = (StructColumnVector) complexListVector.child;
    LongColumnVector complexListIntVector = (LongColumnVector) complexListElementVector.fields[0];
    complexListIntVector.ensureSize(5, false);
    DecimalColumnVector complexListDoubleVector = (DecimalColumnVector) complexListElementVector.fields[1];
    complexListDoubleVector.ensureSize(5, false);

    // simple map - string key and value long
    MapColumnVector simpleMapVector = (MapColumnVector) rowBatch.cols[8];
    simpleMapVector.noNulls = false;
    BytesColumnVector simpleMapKeysVector = (BytesColumnVector) simpleMapVector.keys;
    LongColumnVector simpleMapValuesVector = (LongColumnVector) simpleMapVector.values;
    simpleMapKeysVector.ensureSize(6, false);
    simpleMapValuesVector.ensureSize(6, false);

    // complex map - string key and struct value containing double and string
    MapColumnVector complexMapVector = (MapColumnVector) rowBatch.cols[9];
    complexMapVector.noNulls = false;
    BytesColumnVector complexMapKeysVector = (BytesColumnVector) complexMapVector.keys;
    complexMapKeysVector.ensureSize(6, false);
    StructColumnVector complexMapValuesVector = (StructColumnVector) complexMapVector.values;
    DoubleColumnVector complexMapValueDoubleVector = (DoubleColumnVector) complexMapValuesVector.fields[0];
    complexMapValueDoubleVector.ensureSize(6, false);
    BytesColumnVector complexMapValueBytesVector = (BytesColumnVector) complexMapValuesVector.fields[1];
    complexMapValueBytesVector.ensureSize(6, false);

    Writer writer = OrcFile.createWriter(new Path(_dataFile.getAbsolutePath()),
        OrcFile.writerOptions(new Configuration()).setSchema(schema).overwrite(true));
    for (int i = 0; i < numRecords; i++) {
      Map<String, Object> record = _inputRecords.get(i);

      Integer userId = (Integer) record.get("userID");
      if (userId != null) {
        userIdVector.vector[i] = userId;
      } else {
        userIdVector.isNull[i] = true;
      }

      String firstName = (String) record.get("firstName");
      if (firstName != null) {
        firstNameVector.setVal(i, firstName.getBytes(UTF_8));
      } else {
        firstNameVector.isNull[i] = true;
      }

      List<Integer> bids = (List<Integer>) record.get("bids");
      if (bids != null) {
        bidsVector.offsets[i] = bidsVector.childCount;
        bidsVector.lengths[i] = bids.size();
        for (int bid : bids) {
          bidsElementVector.vector[bidsVector.childCount++] = bid;
        }
      } else {
        bidsVector.isNull[i] = true;
      }

      costVector.vector[i] = (double) record.get("cost");
      timestampVector.vector[i] = (long) record.get("timestamp");

      // simple map with string key and int value
      Map<String, Integer> simpleMap = (Map<String, Integer>) record.get("simpleMap");
      if (simpleMap != null) {
        simpleMapVector.offsets[i] = simpleMapVector.childCount;
        simpleMapVector.lengths[i] = simpleMap.size();
        for (Map.Entry<String, Integer> entry : simpleMap.entrySet()) {
          simpleMapKeysVector.setVal(simpleMapVector.childCount, entry.getKey().getBytes(UTF_8));
          simpleMapValuesVector.vector[simpleMapVector.childCount] = entry.getValue();
          simpleMapVector.childCount++;
        }
      } else {
        simpleMapVector.isNull[i] = true;
      }

      // simple struct with long and double values
      Map<String, Object> struct1 = (Map<String, Object>) record.get("simpleStruct");
      if (struct1 != null) {
        simpleStructBytesVector.setVal(i, ((String) struct1.get("structString")).getBytes(UTF_8));
        simpleStructLongVector.vector[i] = (long) struct1.get("structLong");
        simpleStructDoubleVector.vector[i] = (double) struct1.get("structDouble");
      } else {
        simpleStructVector.isNull[i] = true;
      }

      // complex struct - string, struct containing int and long
      Map<String, Object> complexStruct = (Map<String, Object>) record.get("complexStruct");
      if (complexStruct != null) {
        complexStructBytesVector.setVal(i, ((String) complexStruct.get("structString")).getBytes(UTF_8));
        // Set nested struct vector
        complexStructIntVector.vector[i] =
            (Integer) ((Map<String, Object>) complexStruct.get("nestedStruct")).get("nestedStructInt");
        complexStructLongVector.vector[i] =
            (Long) ((Map<String, Object>) complexStruct.get("nestedStruct")).get("nestedStructLong");
      } else {
        complexStructVector.isNull[i] = true;
      }

      // complex list elements
      List<Map<String, Object>> complexList = (List<Map<String, Object>>) record.get("complexList");
      if (complexList != null) {
        complexListVector.offsets[i] = complexListVector.childCount;
        complexListVector.lengths[i] = complexList.size();
        for (Map<String, Object> complexElement : complexList) {
          complexListIntVector.vector[complexListVector.childCount] = (int) complexElement.get("complexListInt");
          complexListDoubleVector.vector[complexListVector.childCount] =
          new HiveDecimalWritable(HiveDecimal.create((String) complexElement.get("complexListDouble")));
          complexListVector.childCount++;
        }
      } else {
        complexListVector.isNull[i] = true;
      }

      // complex map with key string and struct. struct contains double and string.
      Map<String, Map<String, Object>> complexMap = (Map<String, Map<String, Object>>) record.get("complexMap");
      if (complexMap != null) {
        complexMapVector.offsets[i] = complexMapVector.childCount;
        complexMapVector.lengths[i] = complexMap.size();
        for (Map.Entry<String, Map<String, Object>> entry : complexMap.entrySet()) {
          complexMapKeysVector.setVal(complexMapVector.childCount, entry.getKey().getBytes(UTF_8));
          complexMapValueDoubleVector.vector[complexMapVector.childCount] =
              (double) entry.getValue().get("doubleField");
          complexMapValueBytesVector.setVal(complexMapVector.childCount,
              ((String) entry.getValue().get("stringField")).getBytes(UTF_8));
          complexMapVector.childCount++;
        }
      } else {
        complexMapVector.isNull[i] = true;
      }

      rowBatch.size++;
    }

    writer.addRowBatch(rowBatch);
    rowBatch.reset();
    writer.close();
  }

  @Override
  protected List<Map<String, Object>> getInputRecords() {
    // simple struct - contains a string, long and double array
    Map[] simpleStructs = new Map[]{
        null, createStructInput("structString", "abc", "structLong", 1000L, "structDouble", 5.99999),
        createStructInput("structString", "def", "structLong", 2000L, "structDouble", 6.99999),
        createStructInput("structString", "ghi", "structLong", 3000L, "structDouble", 7.99999)
    };

    // complex struct - contains a string and nested struct of int and long
    Map[] complexStructs = new Map[]{
        createStructInput("structString", "abc", "nestedStruct",
            createStructInput("nestedStructInt", 4, "nestedStructLong", 4000L)),
        createStructInput("structString", "def", "nestedStruct",
            createStructInput("nestedStructInt", 5, "nestedStructLong", 5000L)), null,
        createStructInput("structString", "ghi", "nestedStruct",
            createStructInput("nestedStructInt", 6, "nestedStructLong", 6000L))
    };

    // complex list element - each element contains a struct of int and double
    List[] complexLists = new List[]{
        Arrays.asList(createStructInput("complexListInt", 10, "complexListDouble", "100"),
            createStructInput("complexListInt", 20, "complexListDouble", "200.212")), null,
        Collections.singletonList(createStructInput("complexListInt", 30, "complexListDouble", "300.378")),
        Arrays.asList(createStructInput("complexListInt", 40, "complexListDouble", "400.1"),
            createStructInput("complexListInt", 50, "complexListDouble", "500.2323"))
    };

    // single value integer
    Integer[] userID = new Integer[]{1, 2, null, 4};

    // single value string
    String[] firstName = new String[]{null, "John", "Ringo", "George"};

    // collection of integers
    List[] bids = new List[]{Arrays.asList(10, 20), null, Collections.singletonList(1), Arrays.asList(1, 2, 3)};

    // single value double
    double[] cost = new double[]{10000, 20000, 30000, 25000};

    // single value long
    long[] timestamp = new long[]{1570863600000L, 1571036400000L, 1571900400000L, 1574000000000L};

    // simple map with string keys and integer values
    Map[] simpleMaps = new Map[]{
        createStructInput("key1", 10, "key2", 20), null, createStructInput("key3", 30),
        createStructInput("key4", 40, "key5", 50)
    };

    // complex map with struct values - struct contains double and string
    Map[] complexMap = new Map[]{
        createStructInput("key1", createStructInput("doubleField", 2.0, "stringField", "abc")), null,
        createStructInput("key1", createStructInput("doubleField", 3.0, "stringField", "xyz"), "key2",
            createStructInput("doubleField", 4.0, "stringField", "abc123")),
        createStructInput("key1", createStructInput("doubleField", 3.0, "stringField", "xyz"), "key2",
            createStructInput("doubleField", 4.0, "stringField", "abc123"), "key3",
            createStructInput("doubleField", 4.0, "stringField", "asdf"))
    };

    List<Map<String, Object>> inputRecords = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      Map<String, Object> record = new HashMap<>();
      record.put("userID", userID[i]);
      record.put("firstName", firstName[i]);
      record.put("bids", bids[i]);
      record.put("cost", cost[i]);
      record.put("timestamp", timestamp[i]);
      record.put("simpleStruct", simpleStructs[i]);
      record.put("complexStruct", complexStructs[i]);
      record.put("complexList", complexLists[i]);
      record.put("simpleMap", simpleMaps[i]);
      record.put("complexMap", complexMap[i]);

      inputRecords.add(record);
    }
    return inputRecords;
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets
        .newHashSet("userID", "firstName", "bids", "cost", "timestamp", "simpleMap", "simpleStruct", "complexStruct",
            "complexList", "complexMap");
  }

  private Map<String, Object> createStructInput(String fieldName1, Object value1) {
    Map<String, Object> struct = new HashMap<>(1);
    struct.put(fieldName1, value1);
    return struct;
  }

  private Map<String, Object> createStructInput(String fieldName1, Object value1, String fieldName2, Object value2) {
    Map<String, Object> struct = new HashMap<>(2);
    struct.put(fieldName1, value1);
    struct.put(fieldName2, value2);
    return struct;
  }

  private Map<String, Object> createStructInput(String fieldName1, Object value1, String fieldName2, Object value2,
      String fieldName3, Object value3) {
    Map<String, Object> struct = new HashMap<>(3);
    struct.put(fieldName1, value1);
    struct.put(fieldName2, value2);
    struct.put(fieldName3, value3);
    return struct;
  }
}
