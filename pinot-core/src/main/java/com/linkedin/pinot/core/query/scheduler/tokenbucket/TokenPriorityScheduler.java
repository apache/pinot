/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.scheduler.tokenbucket;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.core.query.scheduler.MultiLevelPriorityQueue;
import com.linkedin.pinot.core.query.scheduler.PriorityScheduler;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroup;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroupFactory;
import com.linkedin.pinot.core.query.scheduler.TableBasedGroupMapper;
import com.linkedin.pinot.core.query.scheduler.resources.PolicyBasedResourceManager;
import com.linkedin.pinot.core.query.scheduler.resources.ResourceManager;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;

/**
 * Schedules queries from a {@link SchedulerGroup} with highest number of tokens on priority.
 * This is a thin wrapper factory class that configures {@link PriorityScheduler} with
 * the right concrete classes. All the priority based scheduling logic is in {@link PriorityScheduler}
 */
public class TokenPriorityScheduler extends PriorityScheduler {
  public static final String TOKENS_PER_MS_KEY = "tokens_per_ms";
  public static final String TOKEN_LIFETIME_MS_KEY = "token_lifetime_ms";
  private static final int DEFAULT_TOKEN_LIFETIME_MS = 100;

  public static TokenPriorityScheduler create(@Nonnull Configuration config, @Nonnull QueryExecutor queryExecutor,
      @Nonnull ServerMetrics metrics) {
    final ResourceManager rm = new PolicyBasedResourceManager(config);
    final SchedulerGroupFactory groupFactory =  new SchedulerGroupFactory() {
      @Override
      public SchedulerGroup create(Configuration config, String groupName) {
        // max available tokens per millisecond equals number of threads (total execution capacity)
        // we are over provisioning tokens here because its better to keep pipe full rather than empty
        int maxTokensPerMs = rm.getNumQueryRunnerThreads() + rm.getNumQueryWorkerThreads();
        int tokensPerMs = config.getInt(TOKENS_PER_MS_KEY, maxTokensPerMs);
        int tokenLifetimeMs = config.getInt(TOKEN_LIFETIME_MS_KEY, DEFAULT_TOKEN_LIFETIME_MS);

        return new TokenSchedulerGroup(groupName, tokensPerMs, tokenLifetimeMs);
      }
    };

    MultiLevelPriorityQueue queue = new MultiLevelPriorityQueue(config, rm, groupFactory, new TableBasedGroupMapper());
    return new TokenPriorityScheduler(rm, queryExecutor, queue, metrics);
  }

  private TokenPriorityScheduler(@Nonnull ResourceManager resourceManager, @Nonnull QueryExecutor queryExecutor,
      @Nonnull MultiLevelPriorityQueue queue, @Nonnull ServerMetrics metrics) {
    super(resourceManager, queryExecutor, queue, metrics);
  }

  @Override
  public String name() {
    return "TokenBucket";
  }
}
