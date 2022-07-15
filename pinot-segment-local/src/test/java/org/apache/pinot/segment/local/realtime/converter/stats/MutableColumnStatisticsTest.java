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
package org.apache.pinot.segment.local.realtime.converter.stats;

import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class MutableColumnStatisticsTest {

  @Test
  public void testElementLength() {
    int numElements = 10;
    String[] elements = new String[numElements];
    int minElementLength = Integer.MAX_VALUE;
    int maxElementLength = 0;
    for (int i = 0; i < numElements; i++) {
      String randomString = RandomStringUtils.random(100);
      elements[i] = randomString;
      int elementLength = randomString.getBytes(StandardCharsets.UTF_8).length;
      minElementLength = Math.min(minElementLength, elementLength);
      maxElementLength = Math.max(maxElementLength, elementLength);
    }

    DataSource dataSource = mock(DataSource.class);
    Dictionary dictionary = mock(Dictionary.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(numElements);
    when(dictionary.getStringValue(anyInt())).thenAnswer(
        (Answer<String>) invocation -> elements[(int) invocation.getArgument(0)]);

    MutableColumnStatistics columnStatistics = new MutableColumnStatistics(dataSource, null);
    assertEquals(columnStatistics.getLengthOfShortestElement(), minElementLength);
    assertEquals(columnStatistics.getLengthOfLargestElement(), maxElementLength);
  }
}
