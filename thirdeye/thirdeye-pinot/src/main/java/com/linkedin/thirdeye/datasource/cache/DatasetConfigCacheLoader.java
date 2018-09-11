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

package com.linkedin.thirdeye.datasource.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class DatasetConfigCacheLoader extends CacheLoader<String, DatasetConfigDTO> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetConfigCacheLoader.class);
  private DatasetConfigManager datasetConfigDAO;

  public DatasetConfigCacheLoader(DatasetConfigManager datasetConfigDAO) {
    this.datasetConfigDAO = datasetConfigDAO;
  }

  @Override
  public DatasetConfigDTO load(String collection) throws Exception {
    LOGGER.debug("Loading DatasetConfigCache for {}", collection);
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
    return datasetConfig;
  }

}
