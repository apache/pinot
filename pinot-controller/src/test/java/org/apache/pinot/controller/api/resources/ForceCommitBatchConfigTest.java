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
package org.apache.pinot.controller.api.resources;

import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;


public class ForceCommitBatchConfigTest {

  @Test
  public void testForceCommitBatchConfig() {
    ForceCommitBatchConfig forceCommitBatchConfig = ForceCommitBatchConfig.of(Integer.MAX_VALUE, 5, 180);
    assert Integer.MAX_VALUE == forceCommitBatchConfig.getBatchSize();
    assert 5000 == forceCommitBatchConfig.getBatchStatusCheckIntervalMs();
    assert 180000 == forceCommitBatchConfig.getBatchStatusCheckTimeoutMs();

    forceCommitBatchConfig = ForceCommitBatchConfig.of(1, 5, 180);
    assert 1 == forceCommitBatchConfig.getBatchSize();
    assert 5000 == forceCommitBatchConfig.getBatchStatusCheckIntervalMs();
    assert 180000 == forceCommitBatchConfig.getBatchStatusCheckTimeoutMs();

    forceCommitBatchConfig = ForceCommitBatchConfig.of(1, 23, 37);
    assert 1 == forceCommitBatchConfig.getBatchSize();
    assert 23000 == forceCommitBatchConfig.getBatchStatusCheckIntervalMs();
    assert 37000 == forceCommitBatchConfig.getBatchStatusCheckTimeoutMs();

    assertThrows(IllegalArgumentException.class, () -> ForceCommitBatchConfig.of(0, 5, 180));
    assertThrows(IllegalArgumentException.class, () -> ForceCommitBatchConfig.of(32, 0, 0));
  }
}
