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

package org.apache.pinot.plugin.inputformat.csv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class CSVMessageDecoderTest {

  @Test
  public void testHappyCase()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender"), "");
    String incomingRecord = "Alice;18;F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    assertNotNull(destination.getValue("name"));
    assertNotNull(destination.getValue("age"));
    assertNotNull(destination.getValue("gender"));

    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("age"), "18");
    assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testMultivalue()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("header", "name;age;gender;subjects");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;18;F;maths,German,history";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    assertNotNull(destination.getValue("name"));
    assertNotNull(destination.getValue("age"));
    assertNotNull(destination.getValue("gender"));
    assertNotNull(destination.getValue("subjects"));

    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("age"), "18");
    assertEquals(destination.getValue("gender"), "F");
    assertEquals(destination.getValue("subjects"), new String[]{"maths", "German", "history"});
  }

  @Test(expectedExceptions = java.util.NoSuchElementException.class)
  public void testCommentMarker()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("header", "name,age,gender");
    decoderProps.put("delimiter", ",");
    decoderProps.put("commentMarker", "#");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender"), "");
    String incomingRecord = "#Alice,18,F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
  }

  @Test
  public void testHeaderFromRecord()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.remove("header");
    decoderProps.put("delimiter", ",");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender"), "");
    String incomingRecord = "name,age,gender\nAlice,18,F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);

    assertNotNull(destination.getValue("name"));
    assertNotNull(destination.getValue("age"));
    assertNotNull(destination.getValue("gender"));

    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("age"), "18");
    assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testEscapeCharacter()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("header", "name;age;gender;subjects");
    decoderProps.put("delimiter", ";");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;18;F;mat\\;hs";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    assertNotNull(destination.getValue("name"));
    assertNotNull(destination.getValue("age"));
    assertNotNull(destination.getValue("gender"));
    assertNotNull(destination.getValue("subjects"));

    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("age"), "18");
    assertEquals(destination.getValue("gender"), "F");
    assertEquals(destination.getValue("subjects"), "mat;hs");
  }

  @Test
  public void testNullString()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("header", "name;age;gender;subjects");
    decoderProps.put("delimiter", ";");
    decoderProps.put("nullStringValue", "null");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;null;F;null";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    assertNotNull(destination.getValue("name"));
    assertNull(destination.getValue("age"));
    assertNotNull(destination.getValue("gender"));
    assertNull(destination.getValue("subjects"));

    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testDefaultProps()
      throws Exception {
    Map<String, String> decoderProps = ImmutableMap.of();
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "name,age,gender,subjects\nAlice,18,F,maths";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);

    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("age"), "18");
    assertEquals(destination.getValue("gender"), "F");
    assertEquals(destination.getValue("subjects"), "maths");
  }

  private static Map<String, String> getStandardDecoderProps() {
    //setup
    Map<String, String> props = new HashMap<>();
    props.put("header", "name;age;gender");
    props.put("delimiter", ";");
    props.put("multiValueDelimiter", ",");
    props.put("escapeCharacter", "\\");
    return props;
  }
}
