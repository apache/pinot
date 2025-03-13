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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;


public class AggregateByQueryIdAccountantFactoryForTest implements ThreadAccountantFactory {
  @Override
  public ThreadResourceUsageAccountant init(PinotConfiguration config, String instanceId) {
    return new AggregateByQueryIdAccountant(config, instanceId);
  }

  /**
   * PerQueryCPUMemResourceUsageAccountant clears state at the end of a query. It cannot be used in tests to check
   * if resources are being accounted. This class is a simple extension of PerQueryCPUMemResourceUsageAccountant that
   * aggregates memory usage by query id.
   * Note that this is useful only in simple scenarios when one query is running.
   * This class has to be defined in this package so that Starter classes can find it. It is not meant to be used
   * outside of tests.
   */
  public static class AggregateByQueryIdAccountant
      extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
    Map<String, Long> _queryMemUsage = new ConcurrentHashMap<>();

    public AggregateByQueryIdAccountant(PinotConfiguration config, String instanceId) {
      super(config, instanceId);
    }

    @Override
    public void sampleThreadBytesAllocated() {
      super.sampleThreadBytesAllocated();
      ThreadExecutionContext context = getThreadExecutionContext();
      _queryMemUsage.put(context.getQueryId(),
          _queryMemUsage.getOrDefault(context.getQueryId(), 0L) + getThreadEntry().getAllocatedBytes());
    }

    public Map<String, Long> getQueryMemUsage() {
      return _queryMemUsage;
    }
  }
}
