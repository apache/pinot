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

package org.apache.pinot.thirdeye.datalayer.dto;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.pojo.DatasetConfigBean;
import java.util.concurrent.TimeUnit;

public class DatasetConfigDTO extends DatasetConfigBean {

  private TimeGranularity bucketTimeGranularity;

  /**
   * Returns the granularity of a bucket (i.e., a data point) of this dataset if such information is available.
   *
   * The granularity that is defined in dataset configuration actually defines the granularity of the timestamp of each
   * data point. For instance, timestamp's granularity (in database) could be 1-MILLISECONDS but the bucket's
   * granularity is 1-HOURS. In real applications, the granularity of timestamp is never being used. Therefore, this
   * method returns the actual granularity of the bucket (data point) if such information is available in the cnofig.
   * This information is crucial for non-additive dataset.
   *
   * @return the granularity of a bucket (a data point) of this dataset.
   */
  public TimeGranularity bucketTimeGranularity() {
    if (bucketTimeGranularity == null) {
        int size = getNonAdditiveBucketSize() != null ? getNonAdditiveBucketSize() : getTimeDuration();
        TimeUnit timeUnit = getNonAdditiveBucketUnit() != null ? getNonAdditiveBucketUnit() : getTimeUnit();
        bucketTimeGranularity = new TimeGranularity(size, timeUnit);
    }
    return bucketTimeGranularity;
  }

  /**
   * Get the dataset name for display on UI.
   * @return the dataset's name. Use display name if it's available, otherwise, use 'dataset' field.
   */
  public String getName() {
    return StringUtils.isNotBlank(getDisplayName()) ? getDisplayName() : getDataset();
  }
}
