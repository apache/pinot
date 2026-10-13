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
package org.apache.pinot.integration.tests.suites;

import java.util.List;
import org.apache.pinot.integration.tests.MultiNodesOfflineClusterIntegrationTest;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;


/// Runs the focused multi-node offline scenarios with one shared cluster.
/// Includes new tests declared by the selected class automatically, while excluding inherited tests that already run
/// in the offline suite. Inherited configuration methods still run. Stateless and thread-safe; tests run sequentially.
@Suite
@IncludeEngines("testng")
@ConfigurationParameter(key = "testng.preserveOrder", value = "true")
@ConfigurationParameter(key = "testng.listeners",
    value = "org.apache.pinot.integration.tests.suites.MultiNodesOfflineSuite")
@SelectClasses(MultiNodesOfflineClusterIntegrationTest.class)
public class MultiNodesOfflineSuite implements IMethodInterceptor {
  @Override
  public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context) {
    return methods.stream()
        .filter(method -> method.getMethod().getConstructorOrMethod().getDeclaringClass()
            == method.getInstance().getClass())
        .toList();
  }
}
