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
package org.apache.pinot.core.data.recordtransformer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ExpressionTransformerTest {

  private Schema _pinotSchema;

  @BeforeClass
  public void setup()
      throws IOException {
    URL resource =
        AbstractRecordExtractorTest.class.getClassLoader().getResource("data/expression_transformer/groovy_expression_transformer.json");
    File schemaFile = new File(resource.getFile());
    _pinotSchema = Schema.fromFile(schemaFile);
  }

  @Test
  public void testGroovyExpressionTransformer() {
    ExpressionTransformer expressionTransformer = new ExpressionTransformer(_pinotSchema);
    DataTypeTransformer dataTypeTransformer = new DataTypeTransformer(_pinotSchema);

    // test functions from schema
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("userID", 1L);
    genericRow.putValue("firstName", "John");
    genericRow.putValue("lastName", "Denver");
    genericRow.putValue("bids", Arrays.asList(10, 20));
    HashMap<String, String> map1 = new HashMap<>(); // keys in Map from avro are always in STRING
    map1.put("30", "foo");
    map1.put("200", "bar");
    genericRow.putValue("map1", map1);
    HashMap<String, Integer> map2 = new HashMap<>();
    map2.put("k1", 10);
    map2.put("k2", 20);
    genericRow.putValue("map2", map2);
    genericRow.putValue("cost", 1000.0);
    genericRow.putValue("timestamp", 1574000000000L);

    // expression transformer
    expressionTransformer.transform(genericRow);

    // extract userId
    Assert.assertEquals(genericRow.getValue("userId"), 1L);
    // concat fullName
    Assert.assertEquals(genericRow.getValue("fullName"), "John Denver");
    Assert.assertTrue(((List) genericRow.getValue("bids")).containsAll(Arrays.asList(10, 20)));
    // find max bid from bids
    Assert.assertEquals(genericRow.getValue("maxBid"), 20);
     // Backward compatible way to support MAP - __KEYS indicates keys of map1
    ArrayList map1__keys = (ArrayList) genericRow.getValue("map1__KEYS");
    Assert.assertEquals(map1__keys.get(0), "200");
    Assert.assertEquals(map1__keys.get(1), "30");
    // Backward compatible way to support MAP - __VALUES indicates values of map1
    ArrayList map1__values = (ArrayList) genericRow.getValue("map1__VALUES");
    Assert.assertEquals(map1__values.get(0), "bar");
    Assert.assertEquals(map1__values.get(1), "foo");
    // handle Map through transform functions
    ArrayList map2__keys = (ArrayList) genericRow.getValue("map2_keys");
    Assert.assertEquals(map2__keys.get(0), "k1");
    Assert.assertEquals(map2__keys.get(1), "k2");
    ArrayList map2__values = (ArrayList) genericRow.getValue("map2_values");
    Assert.assertEquals(map2__values.get(0), 10);
    Assert.assertEquals(map2__values.get(1), 20);
    Assert.assertEquals(genericRow.getValue("cost"), 1000.0);
    // calculate hoursSinceEpoch
    Assert.assertEquals(genericRow.getValue("hoursSinceEpoch").toString(), "437222.2222222222");

    // data type transformer
    dataTypeTransformer.transform(genericRow);

    Assert.assertEquals(genericRow.getValue("userId"), 1L);
    Assert.assertEquals(genericRow.getValue("fullName"), "John Denver");
    Assert.assertEquals(((Integer[]) genericRow.getValue("bids")), new Integer[] {10, 20});
    Assert.assertEquals(genericRow.getValue("maxBid"), 20);
    Integer[] map1Keys = (Integer[]) genericRow.getValue("map1__KEYS");
    Assert.assertEquals(map1Keys[0].intValue(), 200);
    Assert.assertEquals(map1Keys[1].intValue(), 30);
    // Convert to INT array
    Object[] map1Values = (Object[]) genericRow.getValue("map1__VALUES");
    Assert.assertEquals(map1Values[0], "bar");
    Assert.assertEquals(map1Values[1], "foo");
    // handle Map through transform functions
    Object[] map2Keys = (Object[]) genericRow.getValue("map2_keys");
    Assert.assertEquals(map2Keys[0], "k1");
    Assert.assertEquals(map2Keys[1], "k2");
    Object[] map2Values = (Object[]) genericRow.getValue("map2_values");
    Assert.assertEquals(map2Values[0], 10);
    Assert.assertEquals(map2Values[1], 20);
    Assert.assertEquals(genericRow.getValue("cost"), 1000.0);
    // convert to LONG
    Assert.assertEquals(genericRow.getValue("hoursSinceEpoch"), 437222L);

  }
}
