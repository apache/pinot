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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for the {@link ProtoBufRecordExtractor}
 */
public class ProtoBufRecordExtractorLowLevelTest {
  protected final File _tempDir = Files.createTempDirectory("ProtoBufRecordExtractorLowLevelTest").toFile();

  private final File _dataFile = new File(_tempDir, "test_complex_proto.data");
  private ProtoBufRecordExtractor _extractor;

  private static final String DESCRIPTOR_FILE = "complex_types.desc";

  private static final String STRING_FIELD = "string_field";
  private static final String INT_FIELD = "int_field";
  private static final String LONG_FIELD = "long_field";
  private static final String DOUBLE_FIELD = "double_field";
  private static final String FLOAT_FIELD = "float_field";
  private static final String BOOL_FIELD = "bool_field";
  private static final String BYTES_FIELD = "bytes_field";
  private static final String NULLABLE_STRING_FIELD = "nullable_string_field";
  private static final String NULLABLE_INT_FIELD = "nullable_int_field";
  private static final String NULLABLE_LONG_FIELD = "nullable_long_field";
  private static final String NULLABLE_DOUBLE_FIELD = "nullable_double_field";
  private static final String NULLABLE_FLOAT_FIELD = "nullable_float_field";
  private static final String NULLABLE_BOOL_FIELD = "nullable_bool_field";
  private static final String NULLABLE_BYTES_FIELD = "nullable_bytes_field";
  private static final String REPEATED_STRINGS = "repeated_strings";
  private static final String NESTED_MESSAGE = "nested_message";
  private static final String REPEATED_NESTED_MESSAGES = "repeated_nested_messages";
  private static final String COMPLEX_MAP = "complex_map";
  private static final String SIMPLE_MAP = "simple_map";
  private static final String ENUM_FIELD = "enum_field";
  private static final String NESTED_INT_FIELD = "nested_int_field";
  private static final String NESTED_STRING_FIELD = "nested_string_field";

  public ProtoBufRecordExtractorLowLevelTest()
      throws IOException {
  }

  @AfterClass
  public void cleanUp()
      throws IOException {
    FileUtils.forceDelete(_tempDir);
  }

  @BeforeMethod
  public void beforeMethod() {
    _extractor = new ProtoBufRecordExtractor();
    _extractor.init(null, new RecordExtractorConfig() {
    });
  }

  /**
   * For each case, we have:
   * <ol>
   *   <li>The name of the field to change</li>
   *   <li>A valid protobuf value which is not the default field value</li>
   *   <li>The expected pinot value</li>
   * </ol>
   */
  @DataProvider(name = "normalCases")
  public Object[][] normalCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, "some text", "some text"},
        new Object[] {NULLABLE_STRING_FIELD, "some text", "some text"},

        new Object[] {INT_FIELD, 123, 123},
        new Object[] {NULLABLE_INT_FIELD, 123, 123},

        new Object[] {LONG_FIELD, 123L, 123L},
        new Object[] {NULLABLE_LONG_FIELD, 123L, 123L},

        new Object[] {DOUBLE_FIELD, 0.5d, 0.5d},
        new Object[] {NULLABLE_DOUBLE_FIELD, 0.5d, 0.5d},

        new Object[] {FLOAT_FIELD, 0.5f, 0.5f},
        new Object[] {NULLABLE_FLOAT_FIELD, 0.5f, 0.5f},

        new Object[] {BYTES_FIELD, ByteString.copyFrom(new byte[] {0, 1, 2, 3}), new byte[] {0, 1, 2, 3}},
        new Object[] {NULLABLE_BYTES_FIELD, ByteString.copyFrom(new byte[] {0, 1, 2, 3}), new byte[] {0, 1, 2, 3}},

        new Object[] {BOOL_FIELD, true, "true"},
        new Object[] {NULLABLE_BOOL_FIELD, true, "true"}
    };
  }

  /**
   * For each case, we have:
   * <ol>
   *   <li>The name of the field to change</li>
   *   <li>A valid protobuf value which <b>is</b> the default field value</li>
   *   <li>The expected pinot value</li>
   * </ol>
   */
  @Test(dataProvider = "normalCases")
  public void whenNormalCases(String fieldName, Object protoVal, Object pinotVal) {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();
    messageBuilder.setField(fd, protoVal);

    GenericRow row = new GenericRow();
    _extractor.extract(messageBuilder.build(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  /**
   * For each case, we have:
   * <ol>
   *   <li>The name of the field read</li>
   *   <li>The expected pinot value when the value is not set</li>
   * </ol>
   */
  @DataProvider(name = "defaultCases")
  public Object[][] defaultCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, "", ""},
        new Object[] {NULLABLE_STRING_FIELD, "", ""},

        new Object[] {INT_FIELD, 0, 0},
        new Object[] {NULLABLE_INT_FIELD, 0, 0},

        new Object[] {LONG_FIELD, 0L, 0L},
        new Object[] {NULLABLE_LONG_FIELD, 0L, 0L},

        new Object[] {DOUBLE_FIELD, 0d, 0d},
        new Object[] {NULLABLE_DOUBLE_FIELD, 0d, 0d},

        new Object[] {FLOAT_FIELD, 0f, 0f},
        new Object[] {NULLABLE_FLOAT_FIELD, 0f, 0f},

        new Object[] {BYTES_FIELD, ByteString.empty(), new byte[] {}},
        new Object[] {NULLABLE_BYTES_FIELD, ByteString.empty(), new byte[] {}},

        new Object[] {BOOL_FIELD, false, "false"},
        new Object[] {NULLABLE_BOOL_FIELD, false, "false"}
    };
  }

  @Test(dataProvider = "defaultCases")
  public void whenDefaultCases(String fieldName, Object protoValue, Object pinotVal) {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();

    Assert.assertEquals(protoValue, fd.getDefaultValue());
    messageBuilder.setField(fd, protoValue);

    GenericRow row = new GenericRow();
    _extractor.extract(messageBuilder.build(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  @DataProvider(name = "unsetCases")
  public Object[][] unsetCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, ""},
        new Object[] {NULLABLE_STRING_FIELD, null},

        new Object[] {INT_FIELD, 0},
        new Object[] {NULLABLE_INT_FIELD, null},

        new Object[] {LONG_FIELD, 0L},
        new Object[] {NULLABLE_LONG_FIELD, null},

        new Object[] {DOUBLE_FIELD, 0d},
        new Object[] {NULLABLE_DOUBLE_FIELD, null},

        new Object[] {FLOAT_FIELD, 0f},
        new Object[] {NULLABLE_FLOAT_FIELD, null},

        new Object[] {BYTES_FIELD, new byte[] {}},
        new Object[] {NULLABLE_BYTES_FIELD, null},

        new Object[] {BOOL_FIELD, "false"},
        new Object[] {NULLABLE_BOOL_FIELD, null}
    };
  }

  @Test(dataProvider = "unsetCases")
  public void whenUnset(String fieldName, Object pinotVal) {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();

    GenericRow row = new GenericRow();
    _extractor.extract(messageBuilder.build(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }

  @DataProvider(name = "clearCases")
  public Object[][] clearCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, ""},
        new Object[] {NULLABLE_STRING_FIELD, null},

        new Object[] {INT_FIELD, 0},
        new Object[] {NULLABLE_INT_FIELD, null},

        new Object[] {LONG_FIELD, 0L},
        new Object[] {NULLABLE_LONG_FIELD, null},

        new Object[] {DOUBLE_FIELD, 0d},
        new Object[] {NULLABLE_DOUBLE_FIELD, null},

        new Object[] {FLOAT_FIELD, 0f},
        new Object[] {NULLABLE_FLOAT_FIELD, null},

        new Object[] {BYTES_FIELD, new byte[] {}},
        new Object[] {NULLABLE_BYTES_FIELD, null},

        new Object[] {BOOL_FIELD, "false"},
        new Object[] {NULLABLE_BOOL_FIELD, null}
    };
  }

  @Test(dataProvider = "clearCases")
  public void whenClear(String fieldName, Object pinotVal) {
    Descriptors.FieldDescriptor fd = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    ComplexTypes.TestMessage.Builder messageBuilder = ComplexTypes.TestMessage.newBuilder();
    messageBuilder.clearField(fd);

    GenericRow row = new GenericRow();
    _extractor.extract(messageBuilder.build(), row);

    Assert.assertEquals(row.getValue(fd.getName()), pinotVal);
  }
}
