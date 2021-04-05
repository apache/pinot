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
package org.apache.pinot.core.data.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.function.GroovyFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


/**
 * Tests Groovy functions for transforming schema columns
 */
public class GroovyFunctionEvaluatorTest {

  @Test(dataProvider = "groovyFunctionEvaluationDataProvider")
  public void testGroovyFunctionEvaluation(String transformFunction, List<String> arguments, GenericRow genericRow, Object expectedResult) {

    GroovyFunctionEvaluator groovyExpressionEvaluator = new GroovyFunctionEvaluator(transformFunction);
    Assert.assertEquals(groovyExpressionEvaluator.getArguments(), arguments);

    Object result = groovyExpressionEvaluator.evaluate(genericRow);
    Assert.assertEquals(result, expectedResult);
  }

  @DataProvider(name = "groovyFunctionEvaluationDataProvider")
  public Object[][] groovyFunctionEvaluationDataProvider() {

    List<Object[]> entries = new ArrayList<>();

    GenericRow genericRow1 = new GenericRow();
    genericRow1.putValue("userID", 101);
    entries.add(new Object[]{"Groovy({userID}, userID)", Lists.newArrayList("userID"), genericRow1, 101});

    GenericRow genericRow2 = new GenericRow();
    Map<String, Integer> map1 = new HashMap<>();
    map1.put("def", 10);
    map1.put("xyz", 30);
    map1.put("abc", 40);
    genericRow2.putValue("map1", map1);
    entries.add(new Object[]{"Groovy({map1.sort()*.value}, map1)", Lists.newArrayList("map1"), genericRow2, Lists.newArrayList(40, 10, 30)});

    GenericRow genericRow3 = new GenericRow();
    genericRow3.putValue("campaigns", new Object[]{3, 2});
    entries.add(new Object[]{"Groovy({campaigns.max{ it.toBigDecimal() }}, campaigns)", Lists.newArrayList("campaigns"), genericRow3, 3});

    GenericRow genericRow4 = new GenericRow();
    genericRow4.putValue("millis", "1584040201500");
    entries.add(new Object[]{"Groovy({(long)(Long.parseLong(millis)/(1000*60*60))}, millis)", Lists.newArrayList("millis"), genericRow4, 440011L});

    GenericRow genericRow5 = new GenericRow();
    genericRow5.putValue("firstName", "John");
    genericRow5.putValue("lastName", "Doe");
    entries.add(new Object[]{"Groovy({firstName + ' ' + lastName}, firstName, lastName)", Lists.newArrayList("firstName", "lastName"), genericRow5, "John Doe"});

    GenericRow genericRow6 = new GenericRow();
    genericRow6.putValue("eventType", "IMPRESSION");
    entries.add(new Object[]{"Groovy({eventType == 'IMPRESSION' ? 1: 0}, eventType)", Lists.newArrayList("eventType"), genericRow6, 1});

    GenericRow genericRow7 = new GenericRow();
    genericRow7.putValue("eventType", "CLICK");
    entries.add(new Object[]{"Groovy({eventType == 'IMPRESSION' ? 1: 0}, eventType)", Lists.newArrayList("eventType"), genericRow7, 0});

    return entries.toArray(new Object[entries.size()][]);
  }
}
