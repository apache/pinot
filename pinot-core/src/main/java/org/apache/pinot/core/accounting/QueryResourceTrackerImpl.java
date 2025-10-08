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
package org.apache.pinot.core.accounting;

import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.query.QueryExecutionContext;


public class QueryResourceTrackerImpl implements QueryResourceTracker {
  private final QueryExecutionContext _executionContext;
  private long _cpuTimeNs;
  private long _allocatedBytes;

  public QueryResourceTrackerImpl(QueryExecutionContext executionContext, long cpuTimeNs, long allocatedBytes) {
    _executionContext = executionContext;
    _cpuTimeNs = cpuTimeNs;
    _allocatedBytes = allocatedBytes;
  }

  @Override
  public QueryExecutionContext getExecutionContext() {
    return _executionContext;
  }

  @Override
  public long getCpuTimeNs() {
    return _cpuTimeNs;
  }

  @Override
  public long getAllocatedBytes() {
    return _allocatedBytes;
  }

  public QueryResourceTrackerImpl merge(long cpuTimeNs, long allocatedBytes) {
    _cpuTimeNs += cpuTimeNs;
    _allocatedBytes += allocatedBytes;
    return this;
  }
}
