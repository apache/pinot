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

package com.linkedin.thirdeye.anomaly.views;

import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCondensedAnomalyTimelinesView {
  private AnomalyTimelinesView getTestData(int num) {
    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    AnomalyTimelinesView view = new AnomalyTimelinesView();
    for(int i = 0; i < num; i++) {
      view.addTimeBuckets(new TimeBucket(date.getMillis(), date.plusHours(1).getMillis(), date.getMillis(), date.plusHours(1).getMillis()));
      view.addCurrentValues((double) i);
      view.addBaselineValues(i + 0.1);
      date = date.plusHours(1);
    }
    view.addSummary("test", "test");
    return view;
  }
  @Test
  public void testFromAnomalyTimelinesView() {
    int testNum = 100;
    long minBucketMillis = CondensedAnomalyTimelinesView.DEFAULT_MIN_BUCKET_UNIT;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));
    Assert.assertEquals(condensedView.bucketMillis.longValue(), Hours.ONE.toStandardDuration().getMillis() / minBucketMillis);
    Assert.assertEquals(condensedView.getTimeStamps().size(), testNum);

    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < testNum; i++) {
      Assert.assertEquals(condensedView.getTimeStamps().get(i).longValue(),
          (date.getMillis() - condensedView.timestampOffset)/minBucketMillis);
      Assert.assertEquals(condensedView.getCurrentValues().get(i), i + 0d);
      Assert.assertEquals(condensedView.getBaselineValues().get(i), i + 0.1);
      date = date.plusHours(1);
    }
  }

  @Test
  public void testFromJsonString() throws Exception{
    int testNum = 100;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));

    AnomalyTimelinesView anomalyTimelinesView = CondensedAnomalyTimelinesView
        .fromJsonString(condensedView.toJsonString()).toAnomalyTimelinesView();

    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < testNum; i++) {
      TimeBucket timeBucket = anomalyTimelinesView.getTimeBuckets().get(i);
      Assert.assertEquals(timeBucket.getCurrentStart(), date.getMillis());
      Assert.assertEquals(timeBucket.getBaselineEnd(), date.plusHours(1).getMillis());
      Assert.assertEquals(condensedView.getCurrentValues().get(i), i + 0d);
      Assert.assertEquals(condensedView.getBaselineValues().get(i), i + 0.1);
      date = date.plusHours(1);
    }
  }

  @Test
  public void testCompress() throws Exception {
    int testNum = 1500;
    long minBucketMillis = CondensedAnomalyTimelinesView.DEFAULT_MIN_BUCKET_UNIT;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));
    Assert.assertTrue(condensedView.toJsonString().length() > CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);

    CondensedAnomalyTimelinesView compressedView = condensedView.compress();
    Assert.assertTrue(compressedView.toJsonString().length() < CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    Assert.assertEquals(compressedView.bucketMillis.longValue(), 240l);
    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < compressedView.getTimeStamps().size(); i++) {
      Assert.assertEquals(compressedView.getTimeStamps().get(i).longValue(),
          (date.getMillis() - condensedView.timestampOffset)/minBucketMillis);
      Assert.assertEquals(compressedView.getCurrentValues().get(i), i * 4 + 1.5, 0.000001);
      Assert.assertEquals(compressedView.getBaselineValues().get(i), i * 4 + 1.6, 0.000001);
      date = date.plusHours(4);
    }

    AnomalyTimelinesView decompressedView = compressedView.toAnomalyTimelinesView();
    date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < decompressedView.getBaselineValues().size(); i++) {
      TimeBucket timeBucket = decompressedView.getTimeBuckets().get(i);
      Assert.assertEquals(timeBucket.getCurrentStart(), date.getMillis());
      Assert.assertEquals(timeBucket.getCurrentEnd(), date.plusHours(4).getMillis());
      Assert.assertEquals(decompressedView.getCurrentValues().get(i), i * 4 + 1.5, 0.000001);
      Assert.assertEquals(decompressedView.getBaselineValues().get(i), i * 4 + 1.6, 0.000001);
      date = date.plusHours(4);
    }

  }
}
