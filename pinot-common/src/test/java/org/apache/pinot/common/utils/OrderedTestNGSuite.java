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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.junit.platform.suite.api.SelectClasses;
import org.testng.IAlterSuiteListener;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;


/// Restores the declared class order after the TestNG engine converts class selectors to method selectors.
/// TestNG collects those methods' classes in a HashSet, so preserveOrder alone cannot retain the selection order.
/// Stateless and thread-safe; register the concrete @SelectClasses suite as a testng.listeners parameter.
public abstract class OrderedTestNGSuite implements IAlterSuiteListener {
  @Override
  public final void alter(List<XmlSuite> suites) {
    List<Class<?>> classOrder = Arrays.asList(getClass().getAnnotation(SelectClasses.class).value());
    for (XmlSuite suite : suites) {
      for (XmlTest test : suite.getTests()) {
        List<XmlClass> classes = test.getXmlClasses();
        classes.sort(Comparator.comparingInt(xmlClass -> classOrder.indexOf(xmlClass.getSupportClass())));
        for (int i = 0; i < classes.size(); i++) {
          classes.get(i).setIndex(i);
        }
        test.setPreserveOrder(true);
      }
    }
  }
}
