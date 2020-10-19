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
package org.apache.pinot.plugin.inputformat.thrift;

import com.google.common.collect.Sets;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;


/**
 * Tests for the {@link ThriftRecordExtractor}
 */
public class ThriftRecordExtractorTest extends AbstractRecordExtractorTest {

  private File _tempFile = new File(_tempDir, "test_complex_thrift.data");

  private static final String INT_FIELD = "intField";
  private static final String LONG_FIELD = "longField";
  private static final String BOOL_FIELD = "booleanField";
  private static final String DOUBLE_FIELD = "doubleField";
  private static final String STRING_FIELD = "stringField";
  private static final String ENUM_FIELD = "enumField";
  private static final String OPTIONAL_STRING_FIELD = "optionalStringField";
  private static final String NESTED_STRUCT_FIELD = "nestedStructField";
  private static final String SIMPLE_LIST = "simpleListField";
  private static final String COMPLEX_LIST = "complexListField";
  private static final String SIMPLE_MAP = "simpleMapField";
  private static final String COMPLEX_MAP = "complexMapField";
  private static final String NESTED_STRING_FIELD = "nestedStringField";
  private static final String NESTED_INT_FIELD = "nestedIntField";

  @Override
  protected List<Map<String, Object>> getInputRecords() {
    return Arrays.asList(createRecord1(), createRecord2());
  }

  @Override
  protected Set<String> getSourceFields() {
    return Sets.newHashSet(INT_FIELD, LONG_FIELD, BOOL_FIELD, DOUBLE_FIELD, STRING_FIELD, ENUM_FIELD,
        OPTIONAL_STRING_FIELD, NESTED_STRUCT_FIELD, SIMPLE_LIST, COMPLEX_LIST, SIMPLE_MAP, COMPLEX_MAP);
  }

  /**
   * Creates a ThriftRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    ThriftRecordReader recordReader = new ThriftRecordReader();
    recordReader.init(_tempFile, getSourceFields(), getThriftRecordReaderConfig());
    return recordReader;
  }

  private ThriftRecordReaderConfig getThriftRecordReaderConfig() {
    ThriftRecordReaderConfig config = new ThriftRecordReaderConfig();
    config.setThriftClass("org.apache.pinot.plugin.inputformat.thrift.ComplexTypes");
    return config;
  }

  /**
   * Create a data input file using input records containing various Thrift record types
   */
  @Override
  protected void createInputFile()
      throws IOException {
    List<ComplexTypes> thriftRecords = new ArrayList<>(2);

    for (Map<String, Object> inputRecord : _inputRecords) {
      ComplexTypes thriftRecord = new ComplexTypes();
      thriftRecord.setIntField((int) inputRecord.get(INT_FIELD));
      thriftRecord.setLongField((long) inputRecord.get(LONG_FIELD));

      Map<String, Object> nestedStructValues = (Map<String, Object>) inputRecord.get(NESTED_STRUCT_FIELD);
      thriftRecord.setNestedStructField(createNestedType(
          (String) nestedStructValues.get(NESTED_STRING_FIELD),
          (int) nestedStructValues.get(NESTED_INT_FIELD))
      );

      thriftRecord.setSimpleListField((List<String>) inputRecord.get(SIMPLE_LIST));

      List<NestedType> nestedTypeList = new ArrayList<>();
      for (Map element : (List<Map>) inputRecord.get(COMPLEX_LIST)) {
        nestedTypeList.add(createNestedType((String) element.get(NESTED_STRING_FIELD),
            (Integer) element.get(NESTED_INT_FIELD)));
      }

      thriftRecord.setComplexListField(nestedTypeList);
      thriftRecord.setBooleanField(Boolean.valueOf((String) inputRecord.get(BOOL_FIELD)));
      thriftRecord.setDoubleField((Double) inputRecord.get(DOUBLE_FIELD));
      thriftRecord.setStringField((String) inputRecord.get(STRING_FIELD));
      thriftRecord.setEnumField(TestEnum.valueOf((String) inputRecord.get(ENUM_FIELD)));
      thriftRecord.setSimpleMapField((Map<String, Integer>) inputRecord.get(SIMPLE_MAP));

      Map<String, NestedType> complexMap = new HashMap<>();
      for (Map.Entry<String, Map<String, Object>> entry :
          ((Map<String, Map<String, Object>>) inputRecord.get(COMPLEX_MAP)).entrySet()) {
        complexMap.put(entry.getKey(), createNestedType(
            (String) entry.getValue().get(NESTED_STRING_FIELD),
            (int) entry.getValue().get(NESTED_INT_FIELD)));
      }
      thriftRecord.setComplexMapField(complexMap);
      thriftRecords.add(thriftRecord);
    }

    BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(_tempFile));
    TBinaryProtocol binaryOut = new TBinaryProtocol(new TIOStreamTransport(bufferedOut));
    for (ComplexTypes record : thriftRecords) {
      try {
        record.write(binaryOut);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
    bufferedOut.close();
  }

  private Map<String, Object> createRecord1() {
    Map<String, Object> record = new HashMap<>();
    record.put(STRING_FIELD, "hello");
    record.put(INT_FIELD, 10);
    record.put(LONG_FIELD, 1000L);
    record.put(DOUBLE_FIELD, 1.0);
    record.put(BOOL_FIELD, "false");
    record.put(ENUM_FIELD, TestEnum.DELTA.toString());
    record.put(NESTED_STRUCT_FIELD, createNestedMap(NESTED_STRING_FIELD, "ice cream", NESTED_INT_FIELD, 5));
    record.put(SIMPLE_LIST, Arrays.asList("aaa", "bbb", "ccc"));
    record.put(COMPLEX_LIST,
        Arrays.asList(
            createNestedMap(NESTED_STRING_FIELD, "hows", NESTED_INT_FIELD, 10),
            createNestedMap(NESTED_STRING_FIELD, "it", NESTED_INT_FIELD, 20),
            createNestedMap(NESTED_STRING_FIELD, "going", NESTED_INT_FIELD, 30)
        )
    );
    record.put(SIMPLE_MAP, createNestedMap("Tuesday", 3, "Wednesday", 4));
    record.put(
        COMPLEX_MAP,
        createNestedMap(
            "fruit1", createNestedMap(NESTED_STRING_FIELD, "apple", NESTED_INT_FIELD, 1),
            "fruit2", createNestedMap(NESTED_STRING_FIELD, "orange", NESTED_INT_FIELD, 2)
        )
    );
    return record;
  }

  private Map<String, Object> createRecord2() {
    Map<String, Object> record = new HashMap<>();
    record.put(STRING_FIELD, "world");
    record.put(INT_FIELD, 20);
    record.put(LONG_FIELD, 2000L);
    record.put(DOUBLE_FIELD, 2.0);
    record.put(BOOL_FIELD, "false");
    record.put(ENUM_FIELD, TestEnum.GAMMA.toString());
    record.put(NESTED_STRUCT_FIELD, createNestedMap(NESTED_STRING_FIELD, "ice cream", NESTED_INT_FIELD, 5));
    record.put(SIMPLE_LIST, Arrays.asList("aaa", "bbb", "ccc"));
    record.put(COMPLEX_LIST,
        Arrays.asList(
            createNestedMap(NESTED_STRING_FIELD, "hows", NESTED_INT_FIELD, 10),
            createNestedMap(NESTED_STRING_FIELD, "it", NESTED_INT_FIELD, 20),
            createNestedMap(NESTED_STRING_FIELD, "going", NESTED_INT_FIELD, 30)
        )
    );
    record.put(SIMPLE_MAP, createNestedMap("Tuesday", 3, "Wednesday", 4));
    record.put(
        COMPLEX_MAP,
        createNestedMap(
            "fruit1", createNestedMap(NESTED_STRING_FIELD, "apple", NESTED_INT_FIELD, 1),
            "fruit2", createNestedMap(NESTED_STRING_FIELD, "orange", NESTED_INT_FIELD, 2)
        )
    );
    return record;
  }

  private Map<String, Object> createNestedMap(String key1, Object value1, String key2, Object value2) {
    Map<String, Object> nestedMap = new HashMap<>(2);
    nestedMap.put(key1, value1);
    nestedMap.put(key2, value2);
    return nestedMap;
  }

  private NestedType createNestedType(String stringField, int intField) {
    NestedType nestedRecord = new NestedType();
    nestedRecord.setNestedStringField(stringField);
    nestedRecord.setNestedIntField(intField);
    return nestedRecord;
  }
}
