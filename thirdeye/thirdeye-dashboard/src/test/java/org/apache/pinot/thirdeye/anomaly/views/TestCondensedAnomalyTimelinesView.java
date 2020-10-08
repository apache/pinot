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

package org.apache.pinot.thirdeye.anomaly.views;

import org.apache.pinot.thirdeye.dashboard.views.TimeBucket;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCondensedAnomalyTimelinesView {
  /** Create 5-min granularity test data */
  private AnomalyTimelinesView getTestData(int num) {
    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    AnomalyTimelinesView view = new AnomalyTimelinesView();
    for(int i = 0; i < num; i++) {
      view.addTimeBuckets(new TimeBucket(date.getMillis(), date.plusMinutes(5).getMillis(), date.getMillis(), date.plusMinutes(5).getMillis()));
      view.addCurrentValues((double) i);
      view.addBaselineValues(i + 0.333333333333);  // mimic the situation with many decimal digits
      date = date.plusMinutes(5);
    }
    view.addSummary("test", "test");
    return view;
  }
  @Test
  public void testFromAnomalyTimelinesView() {
    int testNum = 100;
    long minBucketMillis = CondensedAnomalyTimelinesView.DEFAULT_MIN_BUCKET_UNIT;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));
    Assert.assertEquals(condensedView.bucketMillis.longValue(), Period.minutes(5).toStandardDuration().getMillis() / minBucketMillis);
    Assert.assertEquals(condensedView.getTimeStamps().size(), testNum);

    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < testNum; i++) {
      Assert.assertEquals(condensedView.getTimeStamps().get(i).longValue(),
          (date.getMillis() - condensedView.timestampOffset)/minBucketMillis);
      Assert.assertEquals(condensedView.getCurrentValues().get(i), i + 0d);
      Assert.assertEquals(condensedView.getBaselineValues().get(i), i + 0.333333333333);
      date = date.plusMinutes(5);
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
      Assert.assertEquals(timeBucket.getBaselineEnd(), date.plusMinutes(5).getMillis());
      Assert.assertEquals(condensedView.getCurrentValues().get(i), i + 0d);
      Assert.assertEquals(condensedView.getBaselineValues().get(i), i + 0.333333333333);
      date = date.plusMinutes(5);
    }
  }

  /** Compression Test case 1: anomaly view could satisfy requirement after rounding up the decimals.*/
  @Test
  public void testCompressWithRoundUp() throws Exception {
    int testNum = 500;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));
    Assert.assertTrue(condensedView.toJsonString().length() > CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    CondensedAnomalyTimelinesView compressedView = condensedView.compress();
    Assert.assertTrue(compressedView.toJsonString().length() < CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    Assert.assertEquals(compressedView.timeStamps.size(), testNum);

    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    long minBucketMillis = CondensedAnomalyTimelinesView.DEFAULT_MIN_BUCKET_UNIT;
    for (int i = 0; i < compressedView.getTimeStamps().size(); i++) {
      Assert.assertEquals(compressedView.getTimeStamps().get(i).longValue(),
          (date.getMillis() - condensedView.timestampOffset)/minBucketMillis);
      Assert.assertEquals(compressedView.getCurrentValues().get(i), i + 0.0d);
      Assert.assertEquals(compressedView.getBaselineValues().get(i), i + 0.33);
      date = date.plusMinutes(5);
    }
  }

  /** Compression Test case 2:  The anomaly view is still too large after rounding up, and needed to be further compressed */
  @Test
  public void testCompress() throws Exception {
    int testNum = 600;
    long minBucketMillis = CondensedAnomalyTimelinesView.DEFAULT_MIN_BUCKET_UNIT;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));
    Assert.assertTrue(condensedView.toJsonString().length() > CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    CondensedAnomalyTimelinesView compressedView = condensedView.compress();
    Assert.assertTrue(compressedView.toJsonString().length() < CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    Assert.assertEquals(300, compressedView.timeStamps.size());
    Assert.assertEquals(compressedView.bucketMillis.longValue(), 10);
    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < compressedView.getTimeStamps().size(); i++) {
      Assert.assertEquals(compressedView.getTimeStamps().get(i).longValue(),
          (date.getMillis() - condensedView.timestampOffset)/minBucketMillis);
      Assert.assertEquals(compressedView.getCurrentValues().get(i), i * 2 + 0d);
      Assert.assertEquals(compressedView.getBaselineValues().get(i), i * 2 + 0.33);
      date = date.plusMinutes(10);
    }

    AnomalyTimelinesView decompressedView = compressedView.toAnomalyTimelinesView();
    date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < decompressedView.getBaselineValues().size(); i++) {
      TimeBucket timeBucket = decompressedView.getTimeBuckets().get(i);
      Assert.assertEquals(timeBucket.getCurrentStart(), date.getMillis());
      Assert.assertEquals(timeBucket.getCurrentEnd(), date.plusMinutes(10).getMillis());
      Assert.assertEquals(decompressedView.getCurrentValues().get(i), i * 2 + 0d);
      Assert.assertEquals(decompressedView.getBaselineValues().get(i), i * 2 + 0.33);
      date = date.plusMinutes(10);
    }
  }

  /** Compression Test case 3:  compressed 2 times */
  @Test
  public void testCompressTwice() throws Exception {
    int testNum = 1200;
    long minBucketMillis = CondensedAnomalyTimelinesView.DEFAULT_MIN_BUCKET_UNIT;
    CondensedAnomalyTimelinesView condensedView = CondensedAnomalyTimelinesView.fromAnomalyTimelinesView(getTestData(testNum));
    Assert.assertTrue(condensedView.toJsonString().length() > CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    CondensedAnomalyTimelinesView compressedView = condensedView.compress();
    Assert.assertTrue(compressedView.toJsonString().length() < CondensedAnomalyTimelinesView.DEFAULT_MAX_LENGTH);
    Assert.assertEquals(300, compressedView.timeStamps.size());
    Assert.assertEquals(compressedView.bucketMillis.longValue(), 20);
    DateTime date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < compressedView.getTimeStamps().size(); i++) {
      Assert.assertEquals(compressedView.getTimeStamps().get(i).longValue(),
          (date.getMillis() - condensedView.timestampOffset)/minBucketMillis);
      Assert.assertEquals(compressedView.getCurrentValues().get(i), i * 4 + 0d);
      Assert.assertEquals(compressedView.getBaselineValues().get(i), i * 4 + 0.33);
      date = date.plusMinutes(20);
    }

    AnomalyTimelinesView decompressedView = compressedView.toAnomalyTimelinesView();
    date = new DateTime(2018, 1, 1, 0, 0, 0);
    for (int i = 0; i < decompressedView.getBaselineValues().size(); i++) {
      TimeBucket timeBucket = decompressedView.getTimeBuckets().get(i);
      Assert.assertEquals(timeBucket.getCurrentStart(), date.getMillis());
      Assert.assertEquals(timeBucket.getCurrentEnd(), date.plusMinutes(20).getMillis());
      Assert.assertEquals(decompressedView.getCurrentValues().get(i), i * 4 + 0d);
      Assert.assertEquals(decompressedView.getBaselineValues().get(i), i * 4 + 0.33);
      date = date.plusMinutes(20);
    }
  }
}
