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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.function.GroovyFunctionEvaluator;
import org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig.getDefaultAllowedImports;
import static org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/**
 * Tests Groovy functions for transforming schema columns
 */
public class GroovyFunctionEvaluatorTest {
  @Test
  public void testLegalGroovyScripts()
      throws JsonProcessingException {
    // TODO: Add separate tests for these rules: receivers, imports, static imports, and method names.
    List<String> scripts = List.of(
        "Groovy({2})",
        "Groovy({![\"pinot_minion_totalOutputSegmentSize_Value\"].contains(\"\");2})",
        "Groovy({airtime == null ? (arrdelay == null ? 0 : arrdelay.value) : airtime.value; 2}, airtime, arrdelay)"
    );

    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(
        getDefaultAllowedReceivers(),
        getDefaultAllowedImports(),
        getDefaultAllowedImports(),
        List.of("invoke", "execute"),
        false);
    GroovyFunctionEvaluator.setConfig(config);

    for (String script : scripts) {
      GroovyFunctionEvaluator groovyFunctionEvaluator = new GroovyFunctionEvaluator(script);
      GenericRow row = new GenericRow();
      Object result = groovyFunctionEvaluator.evaluate(row);
      assertEquals(2, result);
    }
  }

  @Test
  public void testIllegalGroovyScripts()
      throws JsonProcessingException {
    // TODO: Add separate tests for these rules: receivers, imports, static imports, and method names.
    List<String> scripts = List.of(
        "Groovy({\"ls\".execute()})",
        "Groovy({[\"ls\"].execute()})",
        "Groovy({System.exit(5)})",
        "Groovy({System.metaClass.methods.each { method -> if (method.name.md5() == "
            + "\"f24f62eeb789199b9b2e467df3b1876b\") {method.invoke(System, 10)} }})",
        "Groovy({System.metaClass.methods.each { method -> if (method.name.reverse() == (\"ti\" + \"xe\")) "
            + "{method.invoke(System, 10)} }})",
        "groovy({def args = [\"QuickStart\", \"-type\", \"REALTIME\"] as String[]; "
            + "org.apache.pinot.tools.admin.PinotAdministrator.main(args); 2})",
        "Groovy({return [\"bash\", \"-c\", \"env\"].execute().text})"
    );

    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(
        getDefaultAllowedReceivers(),
        getDefaultAllowedImports(),
        getDefaultAllowedImports(),
        List.of("invoke", "execute"),
        false);
    GroovyFunctionEvaluator.setConfig(config);

    for (String script : scripts) {
      try {
        new GroovyFunctionEvaluator(script);
        fail("Groovy analyzer failed to catch malicious script");
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testUpdatingConfiguration()
      throws JsonProcessingException {
    // TODO: Figure out how to test this with the singleton initializer
    // These tests would pass by default but the configuration will be updated so that they fail
    List<String> scripts = List.of(
        "Groovy({2})",
        "Groovy({![\"pinot_minion_totalOutputSegmentSize_Value\"].contains(\"\");2})",
        "Groovy({airtime == null ? (arrdelay == null ? 0 : arrdelay.value) : airtime.value; 2}, airtime, arrdelay)"
    );

    GroovyStaticAnalyzerConfig config =
        new GroovyStaticAnalyzerConfig(List.of(), List.of(), List.of(), List.of(), false);
    GroovyFunctionEvaluator.setConfig(config);

    for (String script : scripts) {
      try {
        GroovyFunctionEvaluator groovyFunctionEvaluator = new GroovyFunctionEvaluator(script);
        GenericRow row = new GenericRow();
        groovyFunctionEvaluator.evaluate(row);
        fail(String.format("Groovy analyzer failed to catch malicious script: %s", script));
      } catch (Exception ignored) {
      }
    }
  }

  @Test(dataProvider = "groovyFunctionEvaluationDataProvider")
  public void testGroovyFunctionEvaluation(String transformFunction, List<String> arguments, GenericRow genericRow,
      Object expectedResult) {

    GroovyFunctionEvaluator groovyExpressionEvaluator = new GroovyFunctionEvaluator(transformFunction);
    assertEquals(groovyExpressionEvaluator.getArguments(), arguments);

    Object result = groovyExpressionEvaluator.evaluate(genericRow);
    assertEquals(result, expectedResult);
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
    entries.add(new Object[]{
        "Groovy({map1.sort()*.value}, map1)", Lists.newArrayList("map1"), genericRow2, Lists.newArrayList(40, 10, 30)
    });

    GenericRow genericRow3 = new GenericRow();
    genericRow3.putValue("campaigns", new Object[]{3, 2});
    entries.add(new Object[]{
        "Groovy({campaigns.max{ it.toBigDecimal() }}, campaigns)", Lists.newArrayList("campaigns"), genericRow3, 3
    });

    GenericRow genericRow4 = new GenericRow();
    genericRow4.putValue("millis", "1584040201500");
    entries.add(new Object[]{
        "Groovy({(long)(Long.parseLong(millis)/(1000*60*60))}, millis)", Lists.newArrayList("millis"), genericRow4,
        440011L
    });

    GenericRow genericRow5 = new GenericRow();
    genericRow5.putValue("firstName", "John");
    genericRow5.putValue("lastName", "Doe");
    entries.add(new Object[]{
        "Groovy({firstName + ' ' + lastName}, firstName, lastName)", Lists.newArrayList("firstName", "lastName"),
        genericRow5, "John Doe"
    });

    GenericRow genericRow6 = new GenericRow();
    genericRow6.putValue("eventType", "IMPRESSION");
    entries.add(new Object[]{
        "Groovy({eventType == 'IMPRESSION' ? 1: 0}, eventType)", Lists.newArrayList("eventType"), genericRow6, 1
    });

    GenericRow genericRow7 = new GenericRow();
    genericRow7.putValue("eventType", "CLICK");
    entries.add(new Object[]{
        "Groovy({eventType == 'IMPRESSION' ? 1: 0}, eventType)", Lists.newArrayList("eventType"), genericRow7, 0
    });

    GenericRow genericRow8 = new GenericRow();
    genericRow8.putValue("ssn", "123-45-6789");
    entries.add(new Object[]{
        "Groovy({org.apache.commons.codec.digest.DigestUtils.sha256Hex(ssn)}, ssn)", Lists.newArrayList("ssn"),
        genericRow8, "01a54629efb952287e554eb23ef69c52097a75aecc0e3a93ca0855ab6d7a31a0"
    });

    GenericRow genericRow9 = new GenericRow();
    genericRow9.putValue("ArrTime", 101);
    genericRow9.putValue("ArrTimeV2", null);
    entries.add(new Object[]{
        "Groovy({ArrTimeV2 != null ? ArrTimeV2: ArrTime }, ArrTime, ArrTimeV2)",
        Lists.newArrayList("ArrTime", "ArrTimeV2"), genericRow9, 101
    });

    GenericRow genericRow10 = new GenericRow();
    String jello = "Jello";
    genericRow10.putValue("jello", jello);
    entries.add(new Object[]{
        "Groovy({jello != null ? jello.length() : \"Jello\" }, jello)",
        Lists.newArrayList("jello"), genericRow10, 5
    });

    //Invalid groovy script
    GenericRow genericRow11 = new GenericRow();
    genericRow11.putValue("nullValue", null);
    entries.add(new Object[]{
        "Groovy({nullValue == null ? nullValue.length() : \"Jello\" }, nullValue)",
        Lists.newArrayList("nullValue"), genericRow11, null
    });
    return entries.toArray(new Object[entries.size()][]);
  }
}
