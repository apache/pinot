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

import org.apache.pinot.integration.tests.DateTimeFieldSpecHybridClusterIntegrationTest;
import org.apache.pinot.integration.tests.HybridClusterIntegrationTest;
import org.apache.pinot.integration.tests.IngestionConfigHybridIntegrationTest;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;


/// Runs the hybrid scenarios with one shared cluster.
/// Stateless suite definition; the selected tests run sequentially in one fork.
@Suite
@IncludeEngines("testng")
@SelectClasses({
    HybridClusterIntegrationTest.class,
    DateTimeFieldSpecHybridClusterIntegrationTest.class,
    IngestionConfigHybridIntegrationTest.class
})
public class SharedHybridSuite {
}
