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
package org.apache.pinot.plugin.inputformat.clplog;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.sql.parsers.rewriter.CLPDecodeRewriter;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractorConfig.FIELDS_FOR_CLP_ENCODING_CONFIG_KEY;
import static org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractorConfig.FIELDS_FOR_CLP_ENCODING_SEPARATOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


public class CLPLogRecordExtractorTest {
  private static final String _TIMESTAMP_FIELD_NAME = "timestamp";
  private static final int _TIMESTAMP_FIELD_VALUE = 10;
  private static final String _LEVEL_FIELD_NAME = "level";
  private static final String _LEVEL_FIELD_VALUE = "INFO";
  private static final String _MESSAGE_1_FIELD_NAME = "message1";
  private static final String _MESSAGE_1_FIELD_VALUE = "Started job_123 on node-987: 4 cores, 8 threads and "
      + "51.4% memory used.";
  private static final String _MESSAGE_2_FIELD_NAME = "message2";
  private static final String _MESSAGE_2_FIELD_VALUE = "Stopped job_123 on node-987: 3 cores, 6 threads and "
      + "22.0% memory used.";

  @Test
  public void testCLPEncoding() {
    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = new HashSet<>();
    // Add some fields for CLP encoding
    props.put(FIELDS_FOR_CLP_ENCODING_CONFIG_KEY,
        _MESSAGE_1_FIELD_NAME + FIELDS_FOR_CLP_ENCODING_SEPARATOR + _MESSAGE_2_FIELD_NAME);
    addCLPEncodedField(_MESSAGE_1_FIELD_NAME, fieldsToRead);
    addCLPEncodedField(_MESSAGE_2_FIELD_NAME, fieldsToRead);
    // Add some unencoded fields
    fieldsToRead.add(_TIMESTAMP_FIELD_NAME);

    GenericRow row;

    // Test extracting specific fields
    row = extract(props, fieldsToRead);
    assertEquals(row.getValue(_TIMESTAMP_FIELD_NAME), _TIMESTAMP_FIELD_VALUE);
    assertNull(row.getValue(_LEVEL_FIELD_NAME));
    validateClpEncodedField(row, _MESSAGE_1_FIELD_NAME, _MESSAGE_1_FIELD_VALUE);
    validateClpEncodedField(row, _MESSAGE_2_FIELD_NAME, _MESSAGE_2_FIELD_VALUE);

    // Test extracting all fields
    row = extract(props, null);
    assertEquals(row.getValue(_TIMESTAMP_FIELD_NAME), _TIMESTAMP_FIELD_VALUE);
    assertEquals(row.getValue(_LEVEL_FIELD_NAME), _LEVEL_FIELD_VALUE);
    validateClpEncodedField(row, _MESSAGE_1_FIELD_NAME, _MESSAGE_1_FIELD_VALUE);
    validateClpEncodedField(row, _MESSAGE_2_FIELD_NAME, _MESSAGE_2_FIELD_VALUE);
  }

  @Test
  public void testBadCLPEncodingConfig() {
    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = new HashSet<>();
    // Add some fields for CLP encoding with some mistakenly empty field names
    String separator = FIELDS_FOR_CLP_ENCODING_SEPARATOR;
    props.put(FIELDS_FOR_CLP_ENCODING_CONFIG_KEY, separator + _MESSAGE_1_FIELD_NAME
        + separator + separator + _MESSAGE_2_FIELD_NAME + separator);
    addCLPEncodedField(_MESSAGE_1_FIELD_NAME, fieldsToRead);
    addCLPEncodedField(_MESSAGE_2_FIELD_NAME, fieldsToRead);
    // Add some unencoded fields
    fieldsToRead.add(_TIMESTAMP_FIELD_NAME);

    GenericRow row;

    // Test extracting specific fields
    row = extract(props, fieldsToRead);
    assertEquals(row.getValue(_TIMESTAMP_FIELD_NAME), _TIMESTAMP_FIELD_VALUE);
    assertNull(row.getValue(_LEVEL_FIELD_NAME));
    validateClpEncodedField(row, _MESSAGE_1_FIELD_NAME, _MESSAGE_1_FIELD_VALUE);
    validateClpEncodedField(row, _MESSAGE_2_FIELD_NAME, _MESSAGE_2_FIELD_VALUE);

    // Test extracting all fields
    row = extract(props, null);
    assertEquals(row.getValue(_TIMESTAMP_FIELD_NAME), _TIMESTAMP_FIELD_VALUE);
    assertEquals(row.getValue(_LEVEL_FIELD_NAME), _LEVEL_FIELD_VALUE);
    validateClpEncodedField(row, _MESSAGE_1_FIELD_NAME, _MESSAGE_1_FIELD_VALUE);
    validateClpEncodedField(row, _MESSAGE_2_FIELD_NAME, _MESSAGE_2_FIELD_VALUE);
  }

  @Test
  public void testEmptyCLPEncodingConfig() {
    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = new HashSet<>();
    // Add some unencoded fields
    fieldsToRead.add(_MESSAGE_1_FIELD_NAME);
    fieldsToRead.add(_MESSAGE_2_FIELD_NAME);
    fieldsToRead.add(_TIMESTAMP_FIELD_NAME);

    GenericRow row;

    // Test extracting specific fields
    row = extract(props, fieldsToRead);
    assertEquals(row.getValue(_TIMESTAMP_FIELD_NAME), _TIMESTAMP_FIELD_VALUE);
    assertNull(row.getValue(_LEVEL_FIELD_NAME));
    assertEquals(row.getValue(_MESSAGE_1_FIELD_NAME), _MESSAGE_1_FIELD_VALUE);
    assertEquals(row.getValue(_MESSAGE_2_FIELD_NAME), _MESSAGE_2_FIELD_VALUE);

    // Test extracting all fields
    row = extract(props, null);
    assertEquals(row.getValue(_TIMESTAMP_FIELD_NAME), _TIMESTAMP_FIELD_VALUE);
    assertEquals(row.getValue(_LEVEL_FIELD_NAME), _LEVEL_FIELD_VALUE);
    assertEquals(row.getValue(_MESSAGE_1_FIELD_NAME), _MESSAGE_1_FIELD_VALUE);
    assertEquals(row.getValue(_MESSAGE_2_FIELD_NAME), _MESSAGE_2_FIELD_VALUE);
  }

  private void addCLPEncodedField(String fieldName, Set<String> fields) {
    fields.add(fieldName + CLPDecodeRewriter.LOGTYPE_COLUMN_SUFFIX);
    fields.add(fieldName + CLPDecodeRewriter.DICTIONARY_VARS_COLUMN_SUFFIX);
    fields.add(fieldName + CLPDecodeRewriter.ENCODED_VARS_COLUMN_SUFFIX);
  }

  private GenericRow extract(Map<String, String> props, Set<String> fieldsToRead) {
    CLPLogRecordExtractorConfig extractorConfig = new CLPLogRecordExtractorConfig();
    CLPLogRecordExtractor extractor = new CLPLogRecordExtractor();
    extractorConfig.init(props);
    extractor.init(fieldsToRead, extractorConfig);

    // Assemble record
    Map<String, Object> record = new HashMap<>();
    record.put(_TIMESTAMP_FIELD_NAME, _TIMESTAMP_FIELD_VALUE);
    record.put(_MESSAGE_1_FIELD_NAME, _MESSAGE_1_FIELD_VALUE);
    record.put(_MESSAGE_2_FIELD_NAME, _MESSAGE_2_FIELD_VALUE);
    record.put(_LEVEL_FIELD_NAME, _LEVEL_FIELD_VALUE);

    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row;
  }

  private void validateClpEncodedField(GenericRow row, String fieldName, String expectedFieldValue) {
    try {
      // Decode and validate field
      assertNull(row.getValue(fieldName));
      String logtype = (String) row.getValue(fieldName + CLPDecodeRewriter.LOGTYPE_COLUMN_SUFFIX);
      assertNotEquals(logtype, null);
      String[] dictionaryVars =
          (String[]) row.getValue(fieldName + CLPDecodeRewriter.DICTIONARY_VARS_COLUMN_SUFFIX);
      assertNotEquals(dictionaryVars, null);
      Long[] encodedVars = (Long[]) row.getValue(fieldName + CLPDecodeRewriter.ENCODED_VARS_COLUMN_SUFFIX);
      assertNotEquals(encodedVars, null);
      long[] encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();

      MessageDecoder messageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
          BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
      String decodedMessage = messageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
      assertEquals(expectedFieldValue, decodedMessage);
    } catch (ClassCastException e) {
      fail(e.getMessage(), e);
    } catch (IOException e) {
      fail("Could not decode " + fieldName + " with CLP - " + e.getMessage());
    }
  }
}
