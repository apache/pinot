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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.alert.commons.AnomalyFetcherConfig;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Collection;
import java.util.Properties;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseAnomalyFetcher implements AnomalyFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAnomalyFetcher.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String ANOMALY_SOURCE_TYPE = "anomalySourceType";
  public static final String ANOMALY_SOURCE = "anomalySource";

  protected Properties properties;
  protected AnomalyFetcherConfig anomalyFetcherConfig;
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected boolean active = true;

  public BaseAnomalyFetcher(){
  }

  @Override
  public void init(AnomalyFetcherConfig anomalyFetcherConfig) {
    mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.anomalyFetcherConfig = anomalyFetcherConfig;
    this.properties = StringUtils.decodeCompactedProperties(anomalyFetcherConfig.getProperties());
  }

  @Override
  public abstract Collection<MergedAnomalyResultDTO> getAlertCandidates(DateTime current, AlertSnapshotDTO alertSnapShot);

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }
}
