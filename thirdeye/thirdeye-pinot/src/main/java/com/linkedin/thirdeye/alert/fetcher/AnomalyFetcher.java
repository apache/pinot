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

package com.linkedin.thirdeye.alert.fetcher;

import com.linkedin.thirdeye.alert.commons.AnomalyFetcherConfig;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collection;
import java.util.Properties;
import org.joda.time.DateTime;


public interface AnomalyFetcher {
  /**
   * Initialize the AnomalyFetcher with properties
   * @param anomalyFetcherConfig the configuration of the anomaly fetcher
   */
  void init(AnomalyFetcherConfig anomalyFetcherConfig);

  /**
   * Get the alert candidates with the given current date time
   * @param current the current DateTime
   * @param alertSnapShot the snapshot of the alert, containing the time and
   */
  Collection<MergedAnomalyResultDTO> getAlertCandidates(DateTime current, AlertSnapshotDTO alertSnapShot);
}