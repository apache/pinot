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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.sql.parsers.rewriter.ClpRewriter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractorConfig.FIELDS_FOR_CLP_ENCODING_CONFIG_KEY;
import static org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractorConfig.FIELDS_FOR_CLP_ENCODING_SEPARATOR;
import static org.apache.pinot.plugin.inputformat.clplog.CLPLogRecordExtractorConfig.REMOVE_PROCESSED_FIELDS_CONFIG_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;


public class CLPLogRecordExtractorTest {
  private static final String MESSAGE_1_FIELD_NAME = "message1";
  private static final String MESSAGE_1_FIELD_VALUE =
      "Started job_123 on node-987: 4 cores, 8 threads and 51.4% memory used.";
  private static final String MESSAGE_2_FIELD_NAME = "message2";
  private static final String MESSAGE_2_FIELD_VALUE =
      "Stopped job_123 on node-987: 3 cores, 6 threads and 22.0% memory used.";
  private static final String TOPIC_NAME = "testTopic";
  private static final String TOPIC_NAME_DEST_COLUMN = "topicName";

  @DataProvider(name = "removeProcessedField")
  public Object[][] removeProcessedField() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "removeProcessedField")
  public void testCLPEncoding(boolean removeProcessedField)
      throws IOException {
    GenericRow row = extract(Map.of(
        FIELDS_FOR_CLP_ENCODING_CONFIG_KEY,
        MESSAGE_1_FIELD_NAME + FIELDS_FOR_CLP_ENCODING_SEPARATOR + MESSAGE_2_FIELD_NAME,
        REMOVE_PROCESSED_FIELDS_CONFIG_KEY, String.valueOf(removeProcessedField)
    ));
    validateClpEncodedField(row, MESSAGE_1_FIELD_NAME, MESSAGE_1_FIELD_VALUE, removeProcessedField);
    validateClpEncodedField(row, MESSAGE_2_FIELD_NAME, MESSAGE_2_FIELD_VALUE, removeProcessedField);
  }

  @Test
  public void testBadCLPEncodingConfig()
      throws IOException {
    // Empty entries in the field-list config (leading / trailing / consecutive separators) are tolerated;
    // valid entries still get encoded.
    String separator = FIELDS_FOR_CLP_ENCODING_SEPARATOR;
    GenericRow row = extract(Map.of(
        FIELDS_FOR_CLP_ENCODING_CONFIG_KEY,
        separator + MESSAGE_1_FIELD_NAME + separator + separator + MESSAGE_2_FIELD_NAME + separator,
        REMOVE_PROCESSED_FIELDS_CONFIG_KEY, "true"
    ));
    validateClpEncodedField(row, MESSAGE_1_FIELD_NAME, MESSAGE_1_FIELD_VALUE, true);
    validateClpEncodedField(row, MESSAGE_2_FIELD_NAME, MESSAGE_2_FIELD_VALUE, true);
  }

  @Test
  public void testEmptyCLPEncodingConfig() {
    // No fields configured for CLP encoding — message fields pass through as plain Strings, not split into
    // logtype / dict / encoded triples.
    GenericRow row = extract(Map.of());
    assertEquals(row.getValue(MESSAGE_1_FIELD_NAME), MESSAGE_1_FIELD_VALUE);
    assertEquals(row.getValue(MESSAGE_2_FIELD_NAME), MESSAGE_2_FIELD_VALUE);
    assertNull(row.getValue(MESSAGE_1_FIELD_NAME + ClpRewriter.LOGTYPE_COLUMN_SUFFIX));
  }

  // === Un-encoded field conversion (same dispatch as JSONRecordExtractor) ===

  @Test
  public void testBigIntegerWidenedToBigDecimal() {
    // Default Jackson parses integer literals that overflow `Long` as `BigInteger`. The extractor widens
    // to `BigDecimal` since Pinot has no `BigInteger` type.
    BigInteger value = new BigInteger("99999999999999999999999999");
    GenericRow row = extractUnencoded("payload", value);
    assertEquals(row.getValue("payload"), new BigDecimal(value));
  }

  @Test
  public void testListExtractedAsArray() {
    GenericRow row = extractUnencoded("payload", List.of(1, "a", true));
    assertEquals((Object[]) row.getValue("payload"), new Object[]{1, "a", true});
  }

  @Test
  public void testNestedMapRecursivelyConverted() {
    // Inner List values become Object[]; inner BigInteger widens to BigDecimal.
    GenericRow row = extractUnencoded("payload", Map.of(
        "list", List.of(1, 2),
        "big", new BigInteger("100")
    ));
    Map<?, ?> result = (Map<?, ?>) row.getValue("payload");
    assertEquals((Object[]) result.get("list"), new Object[]{1, 2});
    assertEquals(result.get("big"), new BigDecimal("100"));
  }

  /// Run the extractor with no CLP-encoded fields configured, so `payload` flows through the un-encoded
  /// path (the same `convert` dispatch as JSON).
  private GenericRow extractUnencoded(String fieldName, Object value) {
    CLPLogRecordExtractorConfig extractorConfig = new CLPLogRecordExtractorConfig();
    extractorConfig.init(Map.of());
    CLPLogRecordExtractor extractor = new CLPLogRecordExtractor();
    extractor.init(null, extractorConfig, TOPIC_NAME);
    Map<String, Object> input = new HashMap<>();
    input.put(fieldName, value);
    GenericRow row = new GenericRow();
    extractor.extract(input, row);
    return row;
  }

  @Test
  public void testPreserveTopicName() {
    // Without the destination-column config, the topic is not surfaced.
    assertNull(extract(Map.of()).getValue(TOPIC_NAME_DEST_COLUMN));

    // With the destination-column config, the topic name is surfaced under the configured column.
    GenericRow row = extract(Map.of(
        CLPLogRecordExtractorConfig.TOPIC_NAME_DESTINATION_COLUMN_CONFIG_KEY, TOPIC_NAME_DEST_COLUMN
    ));
    assertEquals(row.getValue(TOPIC_NAME_DEST_COLUMN), TOPIC_NAME);
  }

  private GenericRow extract(Map<String, String> props) {
    CLPLogRecordExtractorConfig extractorConfig = new CLPLogRecordExtractorConfig();
    CLPLogRecordExtractor extractor = new CLPLogRecordExtractor();
    extractorConfig.init(props);
    extractor.init(null, extractorConfig, TOPIC_NAME);

    GenericRow row = new GenericRow();
    extractor.extract(Map.of(
        MESSAGE_1_FIELD_NAME, MESSAGE_1_FIELD_VALUE,
        MESSAGE_2_FIELD_NAME, MESSAGE_2_FIELD_VALUE
    ), row);
    return row;
  }

  private void validateClpEncodedField(GenericRow row, String fieldName, String expectedFieldValue,
      boolean removeProcessedField)
      throws IOException {
    if (removeProcessedField) {
      assertNull(row.getValue(fieldName));
    }
    String logtype = (String) row.getValue(fieldName + ClpRewriter.LOGTYPE_COLUMN_SUFFIX);
    assertNotEquals(logtype, null);
    String[] dictionaryVars = (String[]) row.getValue(fieldName + ClpRewriter.DICTIONARY_VARS_COLUMN_SUFFIX);
    assertNotEquals(dictionaryVars, null);
    Long[] encodedVars = (Long[]) row.getValue(fieldName + ClpRewriter.ENCODED_VARS_COLUMN_SUFFIX);
    assertNotEquals(encodedVars, null);
    long[] encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();

    MessageDecoder messageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    String decodedMessage = messageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
    assertEquals(expectedFieldValue, decodedMessage);
  }
}
