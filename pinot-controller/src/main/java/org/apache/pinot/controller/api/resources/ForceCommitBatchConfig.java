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

import com.google.common.base.Preconditions;


public class ForceCommitBatchConfig {
  private final int _batchSize;
  private final int _batchStatusCheckIntervalMs;
  private final int _batchStatusCheckTimeoutMs;

  private ForceCommitBatchConfig(int batchSize, int batchStatusCheckIntervalMs, int batchStatusCheckTimeoutMs) {
    _batchSize = batchSize;
    _batchStatusCheckIntervalMs = batchStatusCheckIntervalMs;
    _batchStatusCheckTimeoutMs = batchStatusCheckTimeoutMs;
  }

  public static ForceCommitBatchConfig of(int batchSize, int batchStatusCheckIntervalSec,
      int batchStatusCheckTimeoutSec) {
    Preconditions.checkArgument(batchSize > 0, "Batch size should be greater than zero");
    Preconditions.checkArgument(batchStatusCheckIntervalSec > 0,
        "Batch status check interval should be greater than zero");
    Preconditions.checkArgument(batchStatusCheckTimeoutSec > 0,
        "Batch status check timeout should be greater than zero");
    return new ForceCommitBatchConfig(batchSize, batchStatusCheckIntervalSec * 1000, batchStatusCheckTimeoutSec * 1000);
  }

  public int getBatchSize() {
    return _batchSize;
  }

  public int getBatchStatusCheckIntervalMs() {
    return _batchStatusCheckIntervalMs;
  }

  public int getBatchStatusCheckTimeoutMs() {
    return _batchStatusCheckTimeoutMs;
  }
}