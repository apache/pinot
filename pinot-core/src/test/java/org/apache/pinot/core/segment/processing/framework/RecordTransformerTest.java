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
package org.apache.pinot.core.segment.processing.framework;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.segment.processing.transformer.NoOpRecordTransformer;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformer;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerFactory;
import org.apache.pinot.core.segment.processing.transformer.TransformFunctionRecordTransformer;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for {@link RecordTransformer}
 */
public class RecordTransformerTest {

  @Test
  public void testRecordTransformerFactory() {
    RecordTransformerConfig config = new RecordTransformerConfig.Builder().build();
    RecordTransformer recordTransformer = RecordTransformerFactory.getRecordTransformer(config);
    assertEquals(recordTransformer.getClass(), NoOpRecordTransformer.class);

    Map<String, String> transformFunctionMap = new HashMap<>();
    config = new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build();
    recordTransformer = RecordTransformerFactory.getRecordTransformer(config);
    assertEquals(recordTransformer.getClass(), TransformFunctionRecordTransformer.class);

    transformFunctionMap.put("foo", "toEpochDays(foo)");
    config = new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build();
    recordTransformer = RecordTransformerFactory.getRecordTransformer(config);
    assertEquals(recordTransformer.getClass(), TransformFunctionRecordTransformer.class);

    transformFunctionMap.put("bar", "bad function");
    config = new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build();
    try {
      RecordTransformerFactory.getRecordTransformer(config);
      fail("Should not create record transformer with invalid transform function");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testRecordTransformer() {
    Map<String, String> transformFunctionMap = new HashMap<>();
    transformFunctionMap.put("foo", "toEpochDays(foo)");
    transformFunctionMap.put("bar", "Groovy({bar + \"_\" + zoo}, bar, zoo)");
    RecordTransformerConfig config = new RecordTransformerConfig.Builder().setTransformFunctionsMap(transformFunctionMap).build();
    RecordTransformer recordTransformer = RecordTransformerFactory.getRecordTransformer(config);
    GenericRow row = new GenericRow();
    row.putValue("foo", 1587410614000L);
    row.putValue("bar", "dimValue1");
    row.putValue("zoo", "dimValue2");
    GenericRow transformRecord = recordTransformer.transformRecord(row);
    assertEquals(transformRecord.getValue("foo"), 18372L);
    assertEquals(transformRecord.getValue("bar"), "dimValue1_dimValue2");
    assertEquals(transformRecord.getValue("zoo"), "dimValue2");

  }
}
