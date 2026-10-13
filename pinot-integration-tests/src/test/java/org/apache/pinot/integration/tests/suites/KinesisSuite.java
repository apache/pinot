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

import org.apache.pinot.common.utils.OrderedTestNGSuite;
import org.apache.pinot.integration.tests.realtime.ingestion.KinesisShardChangeTest;
import org.apache.pinot.integration.tests.realtime.ingestion.RealtimeKinesisIntegrationTest;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;


/// Runs the Kinesis scenarios with one shared cluster.
/// Stateless suite definition; the selected tests run sequentially in one fork.
@Suite
@IncludeEngines("testng")
@ConfigurationParameter(key = "testng.listeners",
    value = "org.apache.pinot.integration.tests.suites.KinesisSuite")
@SelectClasses({
    RealtimeKinesisIntegrationTest.class,
    KinesisShardChangeTest.class
})
public class KinesisSuite extends OrderedTestNGSuite {
}
