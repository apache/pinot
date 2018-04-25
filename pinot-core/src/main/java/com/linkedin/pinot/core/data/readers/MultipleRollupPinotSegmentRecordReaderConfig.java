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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.core.operator.transform.transformer.datetime.EpochToEpochTransformer;
import java.util.List;
import java.util.Map;


/**
 * Configuration for multiple roll-up pinot segment record reader.
 */
public class MultipleRollupPinotSegmentRecordReaderConfig implements RecordReaderConfig {

  private Map<String, String> _aggregatorTypeMap;
  private List<String> _sortOrder;
  private EpochToEpochTransformer _dateTimeTransformer;

  public Map<String, String> getAggregatorTypeMap() {
    return _aggregatorTypeMap;
  }

  public void setAggregatorTypeMap(Map<String, String> aggregatorTypeMap) {
    this._aggregatorTypeMap = aggregatorTypeMap;
  }

  public List<String> getSortOrder() {
    return _sortOrder;
  }

  public void setSortOrder(List<String> sortOrder) {
    this._sortOrder = sortOrder;
  }

  public EpochToEpochTransformer getDateTimeTransformer() {
    return _dateTimeTransformer;
  }

  public void setDateTimeTransformer(EpochToEpochTransformer dateTimeTransformer) {
    _dateTimeTransformer = dateTimeTransformer;
  }
}
