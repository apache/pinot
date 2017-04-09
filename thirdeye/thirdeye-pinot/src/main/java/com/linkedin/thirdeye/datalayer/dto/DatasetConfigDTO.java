package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
import java.util.concurrent.TimeUnit;

public class DatasetConfigDTO extends DatasetConfigBean {

  private TimeGranularity bucketTimeGranularity;

  /**
   * Returns the granularity of a bucket (a data point) of this dataset. The granularity of a bucket, which may be
   * different from the granularity that is defined in dataset configuration, which actually defines the granularity of
   * the timestamp of each data point. This variable provides the actual granularity when the granularity defined in
   * the dataset config is different from the granularity of data point. This information is crucial for non-additive
   * dataset.
   *
   * @return the granularity of a bucket (a data point) of this dataset
   */
  public TimeGranularity bucketTimeGranularity() {
    if (bucketTimeGranularity == null) {
      if (this.isAdditive()) {
        bucketTimeGranularity = new TimeGranularity(getTimeDuration(), getTimeUnit());
      } else {
        int size = getNonAdditiveBucketSize() != null ? getNonAdditiveBucketSize() : getTimeDuration();
        TimeUnit timeUnit = getNonAdditiveBucketUnit() != null ? getNonAdditiveBucketUnit() : getTimeUnit();
        bucketTimeGranularity = new TimeGranularity(size, timeUnit);
      }
    }
    return bucketTimeGranularity;
  }
}
