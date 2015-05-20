/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common;

import java.io.File;
import java.io.IOException;

import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;


public class TestSchema {

  private static Schema schema;
  private static File schemaFile;

  @BeforeClass
  public void setup() throws JsonParseException, JsonMappingException, IOException {
    schemaFile = new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource("data1.schema")));
    schema = Schema.fromFile(schemaFile);
  }

  @Test
  public void test1() throws IllegalArgumentException, IllegalAccessException, JsonParseException,
      JsonMappingException, IOException {
    ZNRecord record = Schema.toZNRecord(schema);
    Schema.fromZNRecordV2(record);
  }
}
