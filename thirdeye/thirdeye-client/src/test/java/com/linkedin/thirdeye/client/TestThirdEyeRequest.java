package com.linkedin.thirdeye.client;

import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;

public class TestThirdEyeRequest {

  // Ensure that old legacy behavior of using "!" as dimension value for group by is retained.
  @Test
  public void getGroupBy() {
    String testDimKey = "test1";
    String testGroup = "test2";
    ThirdEyeRequest req =
        new ThirdEyeRequestBuilder().addDimensionValue(testDimKey, ThirdEyeRequest.GROUP_BY_VALUE)
            .addGroupBy(testGroup).build();
    HashSet<String> groups = new HashSet<>();
    groups.add(testDimKey);
    groups.add(testGroup);
    Assert.assertEquals(req.getGroupBy(), groups);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cannot group by fixed dimension .*")
  public void build() {
    String key = "test";
    new ThirdEyeRequestBuilder().addDimensionValue(key, key).addGroupBy(key).build();
  }

  // default should be always grouped by time.
  @Test
  public void isGroupedByTime() {
    ThirdEyeRequest request = new ThirdEyeRequestBuilder().build();
    Assert.assertTrue(request.shouldGroupByTime());
  }
}
