package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DataFrameTest {
  final static long[] INDEX = new long[] { -1, 1, -2, 4, 3 };
  final static double[] VALUES_DOUBLE = new double[] { -2.1, -0.1, 0.0, 0.5, 1.3 };
  final static long[] VALUES_LONG = new long[] { -2, 1, 0, 1, 2 };
  final static String[] VALUES_STRING = new String[] { "-2.3", "-1", "0.0", "0.5", "0.13e1" };
  final static boolean[] VALUES_BOOLEAN = new boolean[] { true, true, false, true, true };

  // TODO thorough testing for longs
  // TODO thorough testing for strings
  // TODO thorough testing for booleans

  // TODO test double batch function
  // TODO test string batch function
  // TODO test boolean batch function

  DataFrame df;

  @BeforeMethod
  public void before() {
    df = new DataFrame(INDEX);
    df.addSeries("double", VALUES_DOUBLE);
    df.addSeries("long", VALUES_LONG);
    df.addSeries("string", VALUES_STRING);
    df.addSeries("boolean", VALUES_BOOLEAN);
  }

  @Test
  public void testEnforceSeriesLengthPass() {
    df.addSeries("series", VALUES_DOUBLE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEnforceSeriesLengthFail() {
    df.addSeries("series", new double[] { 0.1, 3.2});
  }

  @Test
  public void testDoubleSeriesDataDuplication() {
    DoubleSeries first = DataFrame.toSeries(VALUES_DOUBLE);
    DoubleSeries second = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(first.values(), new double[] { -2.1, -0.1, 0.0, 0.5, 1.3 });
    Assert.assertEquals(first.values(), second.values());
    Assert.assertNotSame(first.values(), second.values());
  }

  @Test
  public void testDoubleToBoolean() {
    BooleanSeries s = DataFrame.toSeries(VALUES_DOUBLE).toBooleans();
    Assert.assertEquals(s.values(), new boolean[] { true, true, false, true, true });
  }

  @Test
  public void testDoubleToLong() {
    LongSeries s = DataFrame.toSeries(VALUES_DOUBLE).toLongs();
    Assert.assertEquals(s.values(), new long[] { -2, 0, 0, 0, 1 });
  }

  @Test
  public void testDoubleToString() {
    StringSeries s = DataFrame.toSeries(VALUES_DOUBLE).toStrings();
    Assert.assertEquals(s.values(), new String[] { "-2.1", "-0.1", "0.0", "0.5", "1.3" });
  }

  @Test
  public void testMapSeriesDoubleToDouble() {
    DoubleSeries in = DataFrame.toSeries(VALUES_DOUBLE);
    DoubleSeries out = in.map(new DoubleSeries.DoubleFunction() {
      public double apply(double value) {
        return value * 2;
      }
    });
    Assert.assertEquals(out.values(), new double[] { -4.2, -0.2, 0.0, 1.0, 2.6 });
  }

  @Test
  public void testMapSeriesDoubleToBoolean() {
    DoubleSeries in = DataFrame.toSeries(VALUES_DOUBLE);
    BooleanSeries out = in.map(new DoubleSeries.DoubleConditional() {
      public boolean apply(double value) {
        return value <= 0.3;
      }
    });
    Assert.assertEquals(out.values(), new boolean[] { true, true, true, false, false });
  }

  @Test
  public void testMapDataFrameAsDouble() {
    DoubleSeries out = df.mapAsDouble(new DoubleSeries.DoubleBatchFunction() {
      public double apply(double[] values) {
        return values[0] + values[1] + values[2];
      }
    }, "long", "string", "boolean");
    Assert.assertEquals(out.values(), new double[] { -3.3, 1.0, 0.0, 2.5, 4.3 });
  }

  @Test
  public void testOverrideWithGeneratedSeries() {
    DoubleSeries out = df.toDoubles("double").map(new DoubleSeries.DoubleFunction() {
      public double apply(double value) {
        return value * 2;
      }
    });
    df.addSeries("double", out);
    Assert.assertEquals(df.toDoubles("double").values(), new double[] { -4.2, -0.2, 0.0, 1.0, 2.6 });
  }

  @Test
  public void testTransparentConversionStringToBooleanViaDouble() {
    BooleanSeries s = DataFrame.toSeries(VALUES_STRING).toBooleans();
    Assert.assertEquals(s.values(), new boolean[] { true, true, false, true, true });
  }

  @Test
  public void testTransparentConversionStringToLongViaDouble() {
    LongSeries s = DataFrame.toSeries(VALUES_STRING).toLongs();
    Assert.assertEquals(s.values(), new long[] { -2, -1, 0, 0, 1 });
  }

  @Test
  public void testSortDouble() {
    DoubleSeries in = DataFrame.toSeries(new double[] { 3, 1.5, 1.3, 5, 1.9 });
    Assert.assertEquals(in.sort().values(), new double[] { 1.3, 1.5, 1.9, 3, 5 });
  }

  @Test
  public void testSortLong() {
    LongSeries in = DataFrame.toSeries(new long[] { 3, 15, 13, 5, 19 });
    Assert.assertEquals(in.sort().values(), new long[] { 3, 5, 13, 15, 19 });
  }

  @Test
  public void testSortString() {
    StringSeries in = DataFrame.toSeries(new String[] { "b", "a", "ba", "ab", "aa" });
    Assert.assertEquals(in.sort().values(), new String[] { "a", "aa", "ab", "b", "ba" });
  }

  @Test
  public void testSortBoolean() {
    BooleanSeries in = DataFrame.toSeries(new boolean[] { true, false, false, true, false });
    Assert.assertEquals(in.sort().values(), new boolean[] { false, false, false, true, true });
  }

  @Test
  public void testSortByIndex() {
    DataFrame ndf = df.sortByIndex();
    // NOTE: internal logic uses reorder() for all sorting
    Assert.assertEquals(ndf.getIndex().values(), new long[] { -2, -1, 1, 3, 4 });
    Assert.assertEquals(ndf.toDoubles("double").values(), new double[] { 0.0, -2.1, -0.1, 1.3, 0.5 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { 0, -2, 1, 2, 1 });
    Assert.assertEquals(ndf.toStrings("string").values(), new String[] { "0.0", "-2.3", "-1", "0.13e1", "0.5" });
    Assert.assertEquals(ndf.toBooleans("boolean").values(), new boolean[] { false, true, true, true, true });
  }

  @Test
  public void testSortByDouble() {
    df.addSeries("myseries", 0.1, -2.1, 3.3, 4.6, -7.8 );
    DataFrame ndf = df.sortBySeries("myseries");
    Assert.assertEquals(ndf.getIndex().values(), new long[] { 3, 1, -1, -2, 4 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { 2, 1, -2, 0, 1 });
  }

  @Test
  public void testSortByLong() {
    df.addSeries("myseries", 1, -21, 33, 46, -78 );
    DataFrame ndf = df.sortBySeries("myseries");
    Assert.assertEquals(ndf.getIndex().values(), new long[] { 3, 1, -1, -2, 4 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { 2, 1, -2, 0, 1 });
  }

  @Test
  public void testSortByString() {
    df.addSeries("myseries", "b", "aa", "bb", "c", "a" );
    DataFrame ndf = df.sortBySeries("myseries");
    Assert.assertEquals(ndf.getIndex().values(), new long[] { 3, 1, -1, -2, 4 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { 2, 1, -2, 0, 1 });
  }

  @Test
  public void testSortByBoolean() {
    // NOTE: boolean sort should be stable
    df.addSeries("myseries", true, true, false, false, true );
    DataFrame ndf = df.sortBySeries("myseries");
    Assert.assertEquals(ndf.getIndex().values(), new long[] { -2, 4, -1, 1, 3 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { 0, 1, -2, 1, 2 });
  }

  @Test
  public void testReverse() {
    // NOTE: uses separate reverse() implementation by each series
    DataFrame ndf = df.reverse();
    Assert.assertEquals(ndf.getIndex().values(), new long[] { 3, 4, -2, 1, -1 });
    Assert.assertEquals(ndf.toDoubles("double").values(), new double[] { 1.3, 0.5, 0.0, -0.1, -2.1 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { 2, 1, 0, 1, -2 });
    Assert.assertEquals(ndf.toStrings("string").values(), new String[] { "0.13e1", "0.5", "0.0", "-1", "-2.3" });
    Assert.assertEquals(ndf.toBooleans("boolean").values(), new boolean[] { true, true, false, true, true });
  }

  @Test
  public void testLongBucketsByInterval() {
    LongSeries in = DataFrame.toSeries(new long[] { 3, 15, 13, 5, 19, 20 });
    List<Series.Bucket> buckets = in.bucketsByInterval(4);

    Assert.assertEquals(buckets.size(), 6);
    Assert.assertEquals(buckets.get(0).fromIndex, new int[] { 0 });
    Assert.assertEquals(buckets.get(1).fromIndex, new int[] { 3 });
    Assert.assertEquals(buckets.get(2).fromIndex, new int[] {});
    Assert.assertEquals(buckets.get(3).fromIndex, new int[] { 1, 2 });
    Assert.assertEquals(buckets.get(4).fromIndex, new int[] { 4 });
    Assert.assertEquals(buckets.get(5).fromIndex, new int[] { 5 });
  }

  @Test
  public void testLongBucketsByCountAligned() {
    LongSeries in = DataFrame.toSeries(new long[] { 3, 15, 13, 5, 19, 20 });
    List<Series.Bucket> buckets = in.bucketsByCount(3);

    Assert.assertEquals(buckets.size(), 2);
    Assert.assertEquals(buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
    Assert.assertEquals(buckets.get(1).fromIndex, new int[] { 3, 4, 5 });

  }

  @Test
  public void testLongBucketsByCountUnaligned() {
    LongSeries in = DataFrame.toSeries(new long[] { 3, 15, 13, 5, 19, 11, 12, 9 });
    List<Series.Bucket> buckets = in.bucketsByCount(3);

    Assert.assertEquals(buckets.size(), 3);
    Assert.assertEquals(buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
    Assert.assertEquals(buckets.get(1).fromIndex, new int[] { 3, 4, 5 });
    Assert.assertEquals(buckets.get(2).fromIndex, new int[] { 6, 7 });
  }

  @Test
  public void testLongBucketsGroupBySum() {
    List<Series.Bucket> buckets = new ArrayList<>();
    buckets.add(new Series.Bucket(new int[] { 1, 3, 4 }));
    buckets.add(new Series.Bucket(new int[] {}));
    buckets.add(new Series.Bucket(new int[] { 0, 2 }));

    LongSeries in = DataFrame.toSeries(new long[] { 3, 15, 13, 5, 19 });
    LongSeries out = in.groupBy(buckets, 0, new LongSeries.LongBatchAdd());
    Assert.assertEquals(out.values(), new long[] { 39, 0, 16 });
  }

  @Test
  public void testLongBucketsGroupByLast() {
    List<Series.Bucket> buckets = new ArrayList<>();
    buckets.add(new Series.Bucket(new int[] { 1, 3, 4 }));
    buckets.add(new Series.Bucket(new int[] {}));
    buckets.add(new Series.Bucket(new int[] { 0, 2 }));

    LongSeries in = DataFrame.toSeries(new long[] { 3, 15, 13, 5, 19 });
    LongSeries out = in.groupBy(buckets, -1, new LongSeries.LongBatchLast());
    Assert.assertEquals(out.values(), new long[] { 19, -1, 13 });
  }

  @Test
  public void testLongBucketsGroupByEndToEnd() {
    LongSeries in = DataFrame.toSeries(new long[] { 0, 3, 12, 2, 4, 8, 5, 1, 7, 9, 6, 10, 11 });
    List<Series.Bucket> buckets = in.bucketsByInterval(4);
    Assert.assertEquals(buckets.size(), 4);

    LongSeries out = in.groupBy(buckets, 0, new LongSeries.LongBatchAdd());
    Assert.assertEquals(out.values(), new long[] { 6, 22, 38, 12 });
  }

  @Test
  public void testResampleEndToEnd() {
    DataFrame ndf = df.resample(2, new DataFrame.ResampleLast());

    Assert.assertEquals(ndf.getIndex().size(), 4);
    Assert.assertEquals(ndf.getSeriesNames().size(), 4);

    Assert.assertEquals(ndf.getIndex().values(), new long[] { -2, 0, 2, 4 });
    Assert.assertEquals(ndf.toDoubles("double").values(), new double[] { -2.1, -0.1, 1.3, 0.5 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { -2, 1, 2, 1 });
    Assert.assertEquals(ndf.toStrings("string").values(), new String[] { "-2.3", "-1", "0.13e1", "0.5" });
    Assert.assertEquals(ndf.toBooleans("boolean").values(), new boolean[] { true, true, true, true });
  }
}
