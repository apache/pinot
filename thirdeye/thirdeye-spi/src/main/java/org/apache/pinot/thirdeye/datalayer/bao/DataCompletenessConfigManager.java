/*
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
 *
 */

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.List;

import org.apache.pinot.thirdeye.datalayer.dto.DataCompletenessConfigDTO;

public interface DataCompletenessConfigManager extends AbstractManager<DataCompletenessConfigDTO>{

  List<DataCompletenessConfigDTO> findAllByDataset(String dataset);
  List<DataCompletenessConfigDTO> findAllInTimeRange(long startTime, long endTime);
  List<DataCompletenessConfigDTO> findAllByDatasetAndInTimeRange(String dataset, long startTime, long endTime);
  List<DataCompletenessConfigDTO> findAllByDatasetAndInTimeRangeAndStatus(String dataset, long startTime, long endTime, boolean dataComplete);
  List<DataCompletenessConfigDTO> findAllByTimeOlderThan(long time);
  List<DataCompletenessConfigDTO> findAllByTimeOlderThanAndStatus(long time, boolean dataComplete);
  List<DataCompletenessConfigDTO> findAllByDatasetAndInTimeRangeAndPercentCompleteGT(String dataset, long startTime,
      long endTime, double percentComplete);
  DataCompletenessConfigDTO findByDatasetAndDateSDF(String dataset, String dateToCheckInSDF);
  DataCompletenessConfigDTO findByDatasetAndDateMS(String dataset, Long dateToCheckInMS);

}
