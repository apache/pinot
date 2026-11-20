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

import org.junit.platform.suite.api.ExcludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;


/// Runs the logical-table scenarios with one shared cluster.
///
/// `BaseLogicalTableIntegrationTest` starts its cluster from `@BeforeSuite`, so its subclasses only share that
/// cluster when they run in a single suite. Selecting the whole package keeps every class in one fork and keeps a
/// newly added class from being silently skipped.
///
/// `KafkaPartitionSubsetChaosIntegrationTest` is excluded because it does not extend
/// `BaseLogicalTableIntegrationTest`: it starts and stops its own cluster from `@BeforeClass`, which inside this
/// suite would run alongside the shared cluster for the whole of its (long) duration. The lane profile selects it
/// directly instead, so it gets a fork where only its own cluster is up.
///
/// Stateless suite definition; the selected tests run sequentially in one fork.
@Suite
@IncludeEngines("testng")
@SelectPackages("org.apache.pinot.integration.tests.logicaltable")
@IncludeClassNamePatterns(".*")
@ExcludeClassNamePatterns(".*\\.KafkaPartitionSubsetChaosIntegrationTest")
public class LogicalTableSuite {
}
