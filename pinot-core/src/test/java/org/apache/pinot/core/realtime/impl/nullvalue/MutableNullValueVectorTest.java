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
package org.apache.pinot.core.realtime.impl.nullvalue;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MutableNullValueVectorTest {
  private static final Random RANDOM = new Random();
  private static final int NUM_DOCS = 100;
  private static final int MAX_DOC_ID = 10000;

  private final MutableNullValueVector _nullValueVector = new MutableNullValueVector();

  @Test
  public void testMutableNullValueVector() {
    int[] docIds = new int[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      int docId = RANDOM.nextInt(MAX_DOC_ID);
      _nullValueVector.setNull(docId);
      docIds[i] = docId;
    }
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(_nullValueVector.isNull(docIds[i]));
    }
  }
}
