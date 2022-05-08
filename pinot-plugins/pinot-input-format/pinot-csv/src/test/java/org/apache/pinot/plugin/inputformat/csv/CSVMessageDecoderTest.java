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

import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


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
    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testMultivalue()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("csv.hdr", "name;age;gender;subjects");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;18;F;maths,German,history";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));
    Assert.assertNotNull(destination.getValue("subjects"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
    Assert.assertEquals(destination.getValue("subjects"), new String[]{"maths", "German", "history"});
  }

  @Test(expectedExceptions = java.util.NoSuchElementException.class)
  public void testCommentMarker()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("csv.hdr", "name,age,gender");
    decoderProps.put("csv.delim", ",");
    decoderProps.put("csv.commentMarker", "#");
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
    decoderProps.remove("csv.hdr");
    decoderProps.put("csv.delim", ",");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender"), "");
    String incomingRecord = "name,age,gender\nAlice,18,F";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);

    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
  }

  @Test
  public void testEscapeCharacter()
      throws Exception {
    Map<String, String> decoderProps = getStandardDecoderProps();
    decoderProps.put("csv.hdr", "name;age;gender;subjects");
    decoderProps.put("csv.delim", ";");
    CSVMessageDecoder messageDecoder = new CSVMessageDecoder();
    messageDecoder.init(decoderProps, ImmutableSet.of("name", "age", "gender", "subjects"), "");
    String incomingRecord = "Alice;18;F;mat\\;hs";
    GenericRow destination = new GenericRow();
    messageDecoder.decode(incomingRecord.getBytes(StandardCharsets.UTF_8), destination);
    Assert.assertNotNull(destination.getValue("name"));
    Assert.assertNotNull(destination.getValue("age"));
    Assert.assertNotNull(destination.getValue("gender"));
    Assert.assertNotNull(destination.getValue("subjects"));

    Assert.assertEquals(destination.getValue("name"), "Alice");
    Assert.assertEquals(destination.getValue("age"), "18");
    Assert.assertEquals(destination.getValue("gender"), "F");
    Assert.assertEquals(destination.getValue("subjects"), "mat;hs");
  }

  private static Map<String, String> getStandardDecoderProps() {
    //setup
    Map<String, String> props = new HashMap<>();
    props.put("csv.hdr", "name;age;gender");
    props.put("csv.delim", ";");
    props.put("csv.multiValDelim", ",");
    props.put("csv.EscChar", "\\");
    return props;
  }
}
