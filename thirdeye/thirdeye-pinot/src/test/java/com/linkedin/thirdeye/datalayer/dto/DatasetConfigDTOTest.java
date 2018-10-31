package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.api.TimeGranularity;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetConfigDTOTest {

  @Test
  public void testBucketTimeGranularity() {
    DatasetConfigDTO datasetConfigDTOEmptyBucketInfo = new DatasetConfigDTO();
    datasetConfigDTOEmptyBucketInfo.setTimeDuration(1);
    datasetConfigDTOEmptyBucketInfo.setTimeUnit(TimeUnit.MILLISECONDS);
    Assert.assertEquals(datasetConfigDTOEmptyBucketInfo.bucketTimeGranularity(),
        new TimeGranularity(1, TimeUnit.MILLISECONDS));

    DatasetConfigDTO datasetConfigDTOEmptyBucketSize = new DatasetConfigDTO();
    datasetConfigDTOEmptyBucketSize.setTimeDuration(1);
    datasetConfigDTOEmptyBucketSize.setTimeUnit(TimeUnit.MILLISECONDS);
    datasetConfigDTOEmptyBucketSize.setNonAdditiveBucketUnit(TimeUnit.MINUTES);
    Assert.assertEquals(datasetConfigDTOEmptyBucketSize.bucketTimeGranularity(),
        new TimeGranularity(1, TimeUnit.MINUTES));

    DatasetConfigDTO datasetConfigDTOEmptyBucketUnit = new DatasetConfigDTO();
    datasetConfigDTOEmptyBucketUnit.setTimeDuration(1);
    datasetConfigDTOEmptyBucketUnit.setTimeUnit(TimeUnit.MILLISECONDS);
    datasetConfigDTOEmptyBucketUnit.setNonAdditiveBucketSize(5);
    Assert.assertEquals(datasetConfigDTOEmptyBucketUnit.bucketTimeGranularity(),
        new TimeGranularity(5, TimeUnit.MILLISECONDS));

    DatasetConfigDTO datasetConfigDTOFullOverride = new DatasetConfigDTO();
    datasetConfigDTOFullOverride.setTimeDuration(1);
    datasetConfigDTOFullOverride.setTimeUnit(TimeUnit.MILLISECONDS);
    datasetConfigDTOFullOverride.setNonAdditiveBucketSize(5);
    datasetConfigDTOFullOverride.setNonAdditiveBucketUnit(TimeUnit.MINUTES);
    Assert.assertEquals(datasetConfigDTOFullOverride.bucketTimeGranularity(),
        new TimeGranularity(5, TimeUnit.MINUTES));
  }

}
