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
package org.apache.pinot.core.operator.filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NotFilterOperatorTest {

  @Test
  public void testNotOperator() {
    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 17, 18, 21, 22, 23, 24, 26, 28};
    Set<Integer> expectedResult = new HashSet();
    expectedResult.addAll(Arrays.asList(0, 1, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 19, 20, 25, 27, 29));
    Iterator<Integer> expectedIterator = expectedResult.iterator();
    NotFilterOperator notFilterOperator = new NotFilterOperator(new TestFilterOperator(docIds1), 30);
    BlockDocIdIterator iterator = notFilterOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      Assert.assertEquals(docId, expectedIterator.next().intValue());
    }
  }
}
