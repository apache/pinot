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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeNativeTextIndexReaderWriterTest {

  @Test
  public void testIndexWriterReader()
      throws IOException {
    String[] uniqueValues = new String[4];
    uniqueValues[0] = "hello-world";
    uniqueValues[1] = "hello-world123";
    uniqueValues[2] = "still";
    uniqueValues[3] = "zoobar";

    try (RealtimeNativeTextIndex textIndex = new RealtimeNativeTextIndex("testFSTColumn")) {
      for (int i = 0; i < 4; i++) {
        textIndex.add(uniqueValues[i]);
      }

      int[] matchedDocIds = textIndex.getDocIds("hello.*").toArray();
      Assert.assertEquals(2, matchedDocIds.length);
      Assert.assertEquals(0, matchedDocIds[0]);
      Assert.assertEquals(1, matchedDocIds[1]);

      matchedDocIds = textIndex.getDocIds(".*llo").toArray();
      Assert.assertEquals(2, matchedDocIds.length);
      Assert.assertEquals(0, matchedDocIds[0]);
      Assert.assertEquals(1, matchedDocIds[1]);

      matchedDocIds = textIndex.getDocIds("wor.*").toArray();
      Assert.assertEquals(2, matchedDocIds.length);
      Assert.assertEquals(0, matchedDocIds[0]);
      Assert.assertEquals(1, matchedDocIds[1]);

      matchedDocIds = textIndex.getDocIds("zoo.*").toArray();
      Assert.assertEquals(1, matchedDocIds.length);
      Assert.assertEquals(3, matchedDocIds[0]);
    }
  }
}
