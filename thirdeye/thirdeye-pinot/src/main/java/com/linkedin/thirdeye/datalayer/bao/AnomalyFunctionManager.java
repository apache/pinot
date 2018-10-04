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

package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;


public interface AnomalyFunctionManager extends AbstractManager<AnomalyFunctionDTO> {

  List<AnomalyFunctionDTO> findAllByCollection(String collection);

  /**
   * Get the list of anomaly functions under the given application
   * @param application name of the application
   * @return return the list of anomaly functions under the application
   */
  List<AnomalyFunctionDTO> findAllByApplication(String application);

  List<String> findDistinctTopicMetricsByCollection(String collection);

  List<AnomalyFunctionDTO> findAllActiveFunctions();

  List<AnomalyFunctionDTO> findWhereNameLike(String name);

  AnomalyFunctionDTO findWhereNameEquals(String name);

}
