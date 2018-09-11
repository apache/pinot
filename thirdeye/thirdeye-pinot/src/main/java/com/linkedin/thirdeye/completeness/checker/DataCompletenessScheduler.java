/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.completeness.checker;

import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The scheduler which will run periodically and schedule jobs and tasks for data completeness
 */
public class DataCompletenessScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessScheduler.class);

  private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  private DataCompletenessJobRunner dataCompletenessJobRunner;
  private DataCompletenessJobContext dataCompletenessJobContext;

  public void start() {
    LOG.info("Starting data completeness checker service");

    dataCompletenessJobContext = new DataCompletenessJobContext();
    dataCompletenessJobRunner = new DataCompletenessJobRunner(dataCompletenessJobContext);

    scheduledExecutorService.scheduleAtFixedRate(dataCompletenessJobRunner, 0, 15, TimeUnit.MINUTES);
  }

  public void shutdown() {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
  }

}
