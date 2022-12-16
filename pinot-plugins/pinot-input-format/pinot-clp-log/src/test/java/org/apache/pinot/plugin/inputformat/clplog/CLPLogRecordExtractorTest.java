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

import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


public class CLPLogRecordExtractorTest {
  @Test
  public void testCLPEncoding() {
    // Setup extractor
    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = new HashSet<>();
    // Add two fields for CLP encoding
    props.put("fieldsForClpEncoding", "message1,message2");
    fieldsToRead.add("message1_logtype");
    fieldsToRead.add("message1_encodedVars");
    fieldsToRead.add("message1_dictionaryVars");
    fieldsToRead.add("message2_logtype");
    fieldsToRead.add("message2_encodedVars");
    fieldsToRead.add("message2_dictionaryVars");
    // Add an unencoded field
    fieldsToRead.add("timestamp");
    CLPLogRecordExtractorConfig extractorConfig = new CLPLogRecordExtractorConfig();
    CLPLogRecordExtractor extractor = new CLPLogRecordExtractor();
    extractorConfig.init(props);
    extractor.init(fieldsToRead, extractorConfig);

    // Assemble record
    Map<String, Object> record = new HashMap<>();
    record.put("timestamp", 10);
    String message1 = "Started job_123 on node-987: 4 cores, 8 threads and 51.4% memory used.";
    record.put("message1", message1);
    String message2 = "Stopped job_123 on node-987: 3 cores, 6 threads and 22.0% memory used.";
    record.put("message2", message2);

    // Test decode
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    assertEquals(row.getValue("timestamp"), 10);
    try {
      // Validate message1 field
      assertNull(row.getValue("message1"));
      String logtype = (String) row.getValue("message1_logtype");
      assertNotEquals(logtype, null);
      String[] dictionaryVars = (String[]) row.getValue("message1_dictionaryVars");
      assertNotEquals(dictionaryVars, null);
      Long[] encodedVars = (Long[]) row.getValue("message1_encodedVars");
      assertNotEquals(encodedVars, null);
      long[] encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();
      String decodedMessage = MessageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
      assertEquals(message1, decodedMessage);

      // Validate message2 field
      assertNull(row.getValue("message2"));
      logtype = (String) row.getValue("message2_logtype");
      assertNotEquals(logtype, null);
      dictionaryVars = (String[]) row.getValue("message2_dictionaryVars");
      assertNotEquals(dictionaryVars, null);
      encodedVars = (Long[]) row.getValue("message2_encodedVars");
      assertNotEquals(encodedVars, null);
      encodedVarsAsPrimitives = Arrays.stream(encodedVars).mapToLong(Long::longValue).toArray();
      decodedMessage = MessageDecoder.decodeMessage(logtype, dictionaryVars, encodedVarsAsPrimitives);
      assertEquals(message2, decodedMessage);
    } catch (ClassCastException e) {
      fail(e.getMessage(), e);
    } catch (IOException e) {
      fail("Could not decode message with CLP - " + e.getMessage());
    }
  }
}
