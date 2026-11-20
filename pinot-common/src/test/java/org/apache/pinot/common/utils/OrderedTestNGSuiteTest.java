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
package org.apache.pinot.common.utils;

import java.util.ArrayList;
import java.util.List;
import org.junit.platform.suite.api.SelectClasses;
import org.testng.annotations.Test;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that [OrderedTestNGSuite] restores the class order declared with `@SelectClasses` on the suite.
public class OrderedTestNGSuiteTest {

  @Test
  public void testRestoresDeclaredClassOrder() {
    XmlTest xmlTest = xmlTest(Third.class, First.class, Second.class);
    new DeclaredOrderSuite().alter(List.of(xmlTest.getSuite()));
    assertOrder(xmlTest, First.class, Second.class, Third.class);
  }

  @Test
  public void testFollowsTheAnnotationRatherThanClassNames() {
    XmlTest xmlTest = xmlTest(First.class, Second.class, Third.class);
    new ReversedOrderSuite().alter(List.of(xmlTest.getSuite()));
    assertOrder(xmlTest, Third.class, Second.class, First.class);
  }

  private static XmlTest xmlTest(Class<?>... classes) {
    XmlTest xmlTest = new XmlTest(new XmlSuite());
    List<XmlClass> xmlClasses = new ArrayList<>();
    for (Class<?> clazz : classes) {
      xmlClasses.add(new XmlClass(clazz));
    }
    xmlTest.setXmlClasses(xmlClasses);
    return xmlTest;
  }

  private static void assertOrder(XmlTest xmlTest, Class<?>... expected) {
    List<XmlClass> xmlClasses = xmlTest.getXmlClasses();
    assertEquals(xmlClasses.size(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(xmlClasses.get(i).getSupportClass(), expected[i]);
      assertEquals(xmlClasses.get(i).getIndex(), i);
    }
    assertTrue(xmlTest.getPreserveOrder());
  }

  @SelectClasses({First.class, Second.class, Third.class})
  private static class DeclaredOrderSuite extends OrderedTestNGSuite {
  }

  @SelectClasses({Third.class, Second.class, First.class})
  private static class ReversedOrderSuite extends OrderedTestNGSuite {
  }

  private static class First {
  }

  private static class Second {
  }

  private static class Third {
  }
}
