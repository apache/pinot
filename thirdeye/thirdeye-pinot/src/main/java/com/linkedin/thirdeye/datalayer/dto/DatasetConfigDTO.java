package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;
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
}
