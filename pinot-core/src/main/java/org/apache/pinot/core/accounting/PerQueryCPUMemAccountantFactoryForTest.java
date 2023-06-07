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
import org.apache.pinot.core.query.utils.QueryIdUtils;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * For broker integration test, remove tracking for severs
 */
public class PerQueryCPUMemAccountantFactoryForTest implements ThreadAccountantFactory {
  @Override
  public ThreadResourceUsageAccountant init(PinotConfiguration config, String instanceId) {
    return new PerQueryCPUMemResourceUsageAccountantBrokerKillingTest(config, instanceId);
  }

  public static class PerQueryCPUMemResourceUsageAccountantBrokerKillingTest
      extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
    public PerQueryCPUMemResourceUsageAccountantBrokerKillingTest(PinotConfiguration config, String instanceId) {
      super(config, instanceId);
    }

    public void postAggregation(Map<String, AggregatedStats> aggregatedUsagePerActiveQuery) {
      if (aggregatedUsagePerActiveQuery != null) {
        aggregatedUsagePerActiveQuery.entrySet().removeIf(item -> item.getKey().endsWith(QueryIdUtils.OFFLINE_SUFFIX));
      }
    }
  }
}
