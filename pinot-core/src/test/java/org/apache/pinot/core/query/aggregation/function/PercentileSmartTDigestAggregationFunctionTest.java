/*
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
 *
 */

package org.apache.pinot.core.query.aggregation.function;

import java.util.Collections;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.testng.annotations.Test;


public class PercentileSmartTDigestAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void simpleTest() {
    givenNullableIntTable()
        .withSegment(
            new Object[] {1}
        )
        .andSegment(
            new Object[] {2}
        )
        .whenQuery("select count(*) from testTable")
        .thenResultIs(new Object[] {2L});
  }

  @Test
  public void countNull() {
    givenNullableIntTable()
        .withSegment(
            new Object[] {1}
        )
        .andSegment(
            new Object[] {2},
            new Object[] {null}
        )
        .whenQuery("select count(*) from testTable")
        .thenResultIs(new Object[] {2L});
  }


  @Override
  protected List<TestTable> getTestCases() {
    return Collections.emptyList();
  }

  @Override
  protected PropertiesConfiguration getQueryExecutorConfig() {
    return new PropertiesConfiguration();
  }
}
