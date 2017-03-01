package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

  // TODO test double batch function
  // TODO test string batch function
  // TODO test boolean batch function

  // TODO string test head, tail, accessors
  // TODO boolean test head, tail, accessors

  // TODO shift double, long, boolean
  // TODO fill double, long, boolean

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
    df.addSeries("series", 0.1, 3.2);
  }

  @Test
  public void testDoubleNoDataDuplication() {
    DoubleSeries first = DataFrame.toSeries(VALUES_DOUBLE);
    DoubleSeries second = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertSame(first.values(), second.values());
  }

  @Test
  public void testDoubleToDouble() {
    Series s = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(s.toDoubles().values(), VALUES_DOUBLE);
  }

  @Test
  public void testDoubleToLong() {
    Series s = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(s.toLongs().values(), new long[] { -2, 0, 0, 0, 1 });
  }

  @Test
  public void testDoubleToBoolean() {
    Series s = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, true, false, true, true });
  }

  @Test
  public void testDoubleToString() {
    Series s = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(s.toStrings().values(), new String[] { "-2.1", "-0.1", "0.0", "0.5", "1.3" });
  }

  @Test
  public void testLongToDouble() {
    Series s = DataFrame.toSeries(VALUES_LONG);
    Assert.assertEquals(s.toDoubles().values(), new double[] { -2.0, 1.0, 0.0, 1.0, 2.0 });
  }

  @Test
  public void testLongToLong() {
    Series s = DataFrame.toSeries(VALUES_LONG);
    Assert.assertEquals(s.toLongs().values(), VALUES_LONG);
  }

  @Test
  public void testLongToBoolean() {
    Series s = DataFrame.toSeries(VALUES_LONG);
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, true, false, true, true });
  }

  @Test
  public void testLongToString() {
    Series s = DataFrame.toSeries(VALUES_LONG);
    Assert.assertEquals(s.toStrings().values(), new String[] { "-2", "1", "0", "1", "2" });
  }

  @Test
  public void testBooleanToDouble() {
    Series s = DataFrame.toSeries(VALUES_BOOLEAN);
    Assert.assertEquals(s.toDoubles().values(), new double[] { 1.0, 1.0, 0.0, 1.0, 1.0 });
  }

  @Test
  public void testBooleanToLong() {
    Series s = DataFrame.toSeries(VALUES_BOOLEAN);
    Assert.assertEquals(s.toLongs().values(), new long[] { 1, 1, 0, 1, 1 });
  }

  @Test
  public void testBooleanToBoolean() {
    Series s = DataFrame.toSeries(VALUES_BOOLEAN);
    Assert.assertEquals(s.toBooleans().values(), VALUES_BOOLEAN);
  }

  @Test
  public void testBooleanToString() {
    Series s = DataFrame.toSeries(VALUES_BOOLEAN);
    Assert.assertEquals(s.toStrings().values(), new String[] { "true", "true", "false", "true", "true" });
  }

  @Test
  public void testStringToDouble() {
    Series s = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s.toDoubles().values(), new double[] { -2.3, -1.0, 0.0, 0.5, 1.3 });
  }

  @Test
  public void testStringToLong() {
    // NOTE: transparent conversion via double
    Series s = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s.toLongs().values(), new long[] { -2, -1, 0, 0, 1 });
  }

  @Test
  public void testStringToBoolean() {
    // NOTE: transparent conversion via double
    Series s = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, true, false, true, true });
  }

  @Test
  public void testStringToString() {
    Series s = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s.toStrings().values(), VALUES_STRING);
  }

  @Test
  public void testDoubleNull() {
    Series s = DataFrame.toSeries(1.0, DoubleSeries.NULL_VALUE, 2.0);
    Assert.assertEquals(s.toDoubles().values(), new double[] { 1.0, DoubleSeries.NULL_VALUE, 2.0 });
    Assert.assertEquals(s.toLongs().values(), new long[] { 1, LongSeries.NULL_VALUE, 2 });
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, BooleanSeries.NULL_VALUE, true });
    Assert.assertEquals(s.toStrings().values(), new String[] { "1.0", StringSeries.NULL_VALUE, "2.0" });
  }

  @Test
  public void testLongNull() {
    Series s = DataFrame.toSeries(1, LongSeries.NULL_VALUE, 2);
    Assert.assertEquals(s.toDoubles().values(), new double[] { 1.0, DoubleSeries.NULL_VALUE, 2.0 });
    Assert.assertEquals(s.toLongs().values(), new long[] { 1, LongSeries.NULL_VALUE, 2 });
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, BooleanSeries.NULL_VALUE, true });
    Assert.assertEquals(s.toStrings().values(), new String[] { "1", StringSeries.NULL_VALUE, "2" });
  }

  @Test
  public void testBooleanNull() {
    Series s = DataFrame.toSeries(true, BooleanSeries.NULL_VALUE, false);
    Assert.assertEquals(s.toDoubles().values(), new double[] { 1.0, 0.0, 0.0 });
    Assert.assertEquals(s.toLongs().values(), new long[] { 1, 0, 0 });
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, false, false });
    Assert.assertEquals(s.toStrings().values(), new String[] { "true", "false", "false" });
  }

  @Test
  public void testStringNull() {
    Series s = DataFrame.toSeries("1.0", StringSeries.NULL_VALUE, "2.0");
    Assert.assertEquals(s.toDoubles().values(), new double[] { 1.0, DoubleSeries.NULL_VALUE, 2.0 });
    Assert.assertEquals(s.toLongs().values(), new long[] { 1, LongSeries.NULL_VALUE, 2 });
    Assert.assertEquals(s.toBooleans().values(), new boolean[] { true, BooleanSeries.NULL_VALUE, true });
    Assert.assertEquals(s.toStrings().values(), new String[] { "1.0", StringSeries.NULL_VALUE, "2.0" });
  }

  @Test
  public void testMapDoubleToDouble() {
    DoubleSeries in = DataFrame.toSeries(VALUES_DOUBLE);
    DoubleSeries out = in.map(new DoubleSeries.DoubleFunction() {
      public double apply(double value) {
        return value * 2;
      }
    });
    Assert.assertEquals(out.values(), new double[] { -4.2, -0.2, 0.0, 1.0, 2.6 });
  }

  @Test
  public void testMapDoubleToBoolean() {
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
    DoubleSeries out = df.map(new DoubleSeries.DoubleBatchFunction() {
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
  public void testSortDouble() {
    DoubleSeries in = DataFrame.toSeries(3, 1.5, 1.3, 5, 1.9);
    Assert.assertEquals(in.sort().values(), new double[] { 1.3, 1.5, 1.9, 3, 5 });
  }

  @Test
  public void testSortLong() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19);
    Assert.assertEquals(in.sort().values(), new long[] { 3, 5, 13, 15, 19 });
  }

  @Test
  public void testSortString() {
    StringSeries in = DataFrame.toSeries("b", "a", "ba", "ab", "aa");
    Assert.assertEquals(in.sort().values(), new String[] { "a", "aa", "ab", "b", "ba" });
  }

  @Test
  public void testSortBoolean() {
    BooleanSeries in = DataFrame.toSeries(true, false, false, true, false);
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
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20);
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
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20);
    List<Series.Bucket> buckets = in.bucketsByCount(3);

    Assert.assertEquals(buckets.size(), 2);
    Assert.assertEquals(buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
    Assert.assertEquals(buckets.get(1).fromIndex, new int[] { 3, 4, 5 });

  }

  @Test
  public void testLongBucketsByCountUnaligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 11, 12, 9);
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

    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19 );
    LongSeries out = in.groupBy(buckets, 0, new LongSeries.LongBatchSum());
    Assert.assertEquals(out.values(), new long[] { 39, 0, 16 });
  }

  @Test
  public void testLongBucketsGroupByLast() {
    List<Series.Bucket> buckets = new ArrayList<>();
    buckets.add(new Series.Bucket(new int[] { 1, 3, 4 }));
    buckets.add(new Series.Bucket(new int[] {}));
    buckets.add(new Series.Bucket(new int[] { 0, 2 }));

    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19 );
    LongSeries out = in.groupBy(buckets, -1, new LongSeries.LongBatchLast());
    Assert.assertEquals(out.values(), new long[] { 19, -1, 13 });
  }

  @Test
  public void testLongBucketsGroupByEndToEnd() {
    LongSeries in = DataFrame.toSeries(0, 3, 12, 2, 4, 8, 5, 1, 7, 9, 6, 10, 11);
    List<Series.Bucket> buckets = in.bucketsByInterval(4);
    Assert.assertEquals(buckets.size(), 4);

    LongSeries out = in.groupBy(buckets, 0, new LongSeries.LongBatchSum());
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

  @Test
  public void testStableMultiSortDoubleLong() {
    DataFrame mydf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    mydf.addSeries("double", 1.0, 1.0, 2.0, 2.0, 1.0, 1.0, 2.0, 2.0);
    mydf.addSeries("long", 2, 2, 2, 2, 1, 1, 1, 1);

    DataFrame sdfa = mydf.sortBySeries("double", "long");
    Assert.assertEquals(sdfa.getIndex().values(), new long[] { 5, 6, 1, 2, 7, 8, 3, 4 });

    DataFrame sdfb = mydf.sortBySeries("long", "double");
    Assert.assertEquals(sdfb.getIndex().values(), new long[] { 3, 4, 7, 8, 1, 2, 5, 6 });
  }

  @Test
  public void testStableMultiSortStringBoolean() {
    DataFrame mydf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    mydf.addSeries("string", "a", "a", "b", "b", "a", "a", "b", "b");
    mydf.addSeries("boolean", true, true, true, true, false, false, false, false);

    DataFrame sdfa = mydf.sortBySeries("string", "boolean");
    Assert.assertEquals(sdfa.getIndex().values(), new long[] { 5, 6, 1, 2, 7, 8, 3, 4 });

    DataFrame sdfb = mydf.sortBySeries("boolean", "string");
    Assert.assertEquals(sdfb.getIndex().values(), new long[] { 3, 4, 7, 8, 1, 2, 5, 6 });
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFilterUnequalLengthFail() {
    df.filter(DataFrame.toSeries(false, true));
  }

  @Test
  public void testFilter() {
    DataFrame ndf = df.filter(DataFrame.toSeries(true, false, true, true, false));

    Assert.assertEquals(ndf.getIndex().size(), 3);
    Assert.assertEquals(ndf.getIndex().values(), new long[] { -1, -2, 4 });
    Assert.assertEquals(ndf.toDoubles("double").values(), new double[] { -2.1, 0.0, 0.5 });
    Assert.assertEquals(ndf.toLongs("long").values(), new long[] { -2, 0, 1 });
    Assert.assertEquals(ndf.toStrings("string").values(), new String[] { "-2.3", "0.0", "0.5"  });
    Assert.assertEquals(ndf.toBooleans("boolean").values(), new boolean[] { true, false, true });
  }

  @Test
  public void testFilterAll() {
    DataFrame ndf = df.filter(DataFrame.toSeries(true, true, true, true, true));
    Assert.assertEquals(ndf.getIndex().size(), 5);
  }

  @Test
  public void testFilterNone() {
    DataFrame ndf = df.filter(DataFrame.toSeries(false, false, false, false, false));
    Assert.assertEquals(ndf.getIndex().size(), 0);
  }

  @Test
  public void testGetSingleValue() {
    DataFrame ndf = df.filter(DataFrame.toSeries(true, false, false, false, false));

    Assert.assertEquals(ndf.getDouble("double"), -2.1);
    Assert.assertEquals(ndf.getLong("long"), -2);
    Assert.assertEquals(ndf.getString("string"), "-2.3");
    Assert.assertEquals(ndf.getBoolean("boolean"), true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetSingleValueMultipleDoubleFail() {
    df.getDouble("double");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetSingleValueNoDoubleFail() {
    DataFrame ndf = df.filter(DataFrame.toSeries(false, false, false, false, false));
    ndf.getDouble("double");
  }

  @Test
  public void testRenameSeries() {
    df.renameSeries("double", "new");
    df.toDoubles("new");
    try {
      df.toDoubles("double");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      // left blank
    }
  }

  @Test
  public void testRenameSeriesOverride() {
    df.renameSeries("double", "long");
    Assert.assertEquals(df.toDoubles("long").values(), VALUES_DOUBLE);
  }

  @Test
  public void testContains() {
    Assert.assertTrue(df.contains("double"));
    Assert.assertFalse(df.contains("NOT_VALID"));
  }

  @Test
  public void testCopy() {
    DataFrame ndf = df.copy();

    ndf.toDoubles("double").values()[0] = 100.0;
    Assert.assertNotEquals(df.toDoubles("double").first(), ndf.toDoubles("double").first());

    ndf.toLongs("long").values()[0] = 100;
    Assert.assertNotEquals(df.toLongs("long").first(), ndf.toLongs("long").first());

    ndf.toStrings("string").values()[0] = "other string";
    Assert.assertNotEquals(df.toStrings("string").first(), ndf.toStrings("string").first());

    ndf.toBooleans("boolean").values()[0] = false;
    Assert.assertNotEquals(df.toBooleans("boolean").first(), ndf.toBooleans("boolean").first());
  }

  @Test
  public void testDoubleHead() {
    DoubleSeries s = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(s.head(0).values(), new double[]{});
    Assert.assertEquals(s.head(3).values(), Arrays.copyOfRange(VALUES_DOUBLE, 0, 3));
    Assert.assertEquals(s.head(6).values(), Arrays.copyOfRange(VALUES_DOUBLE, 0, 5));
  }

  @Test
  public void testDoubleTail() {
    DoubleSeries s = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertEquals(s.tail(0).values(), new double[] {});
    Assert.assertEquals(s.tail(3).values(), Arrays.copyOfRange(VALUES_DOUBLE, 2, 5));
    Assert.assertEquals(s.tail(6).values(), Arrays.copyOfRange(VALUES_DOUBLE, 0, 5));
  }

  @Test
  public void testDoubleAccessorsEmpty() {
    DoubleSeries s = new DoubleSeries();
    Assert.assertEquals(s.sum(), 0.0d);

    try {
      s.first();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.last();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.min();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.max();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.mean();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }
  }

  @Test
  public void testLongHead() {
    LongSeries s = DataFrame.toSeries(VALUES_LONG);
    Assert.assertEquals(s.head(0).values(), new long[] {});
    Assert.assertEquals(s.head(3).values(), Arrays.copyOfRange(VALUES_LONG, 0, 3));
    Assert.assertEquals(s.head(6).values(), Arrays.copyOfRange(VALUES_LONG, 0, 5));
  }

  @Test
  public void testLongTail() {
    LongSeries s = DataFrame.toSeries(VALUES_LONG);
    Assert.assertEquals(s.tail(0).values(), new long[] {});
    Assert.assertEquals(s.tail(3).values(), Arrays.copyOfRange(VALUES_LONG, 2, 5));
    Assert.assertEquals(s.tail(6).values(), Arrays.copyOfRange(VALUES_LONG, 0, 5));
  }

  @Test
  public void testLongAccessorsEmpty() {
    LongSeries s = new LongSeries();
    Assert.assertEquals(s.sum(), 0);

    try {
      s.first();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.last();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.min();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.max();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }

    try {
      s.mean();
      Assert.fail();
    } catch(IllegalStateException e) {
      // left blank
    }
  }

  @Test
  public void testLongUnique() {
    LongSeries s1 = DataFrame.toSeries(new long[] {});
    Assert.assertEquals(s1.unique().values(), new long[] {});

    LongSeries s2 = DataFrame.toSeries(4, 5, 2, 1);
    Assert.assertEquals(s2.unique().values(), new long[] {1, 2, 4, 5});

    LongSeries s3 = DataFrame.toSeries(9, 1, 2, 3, 6, 1, 2, 9, 2, 7);
    Assert.assertEquals(s3.unique().values(), new long[] {1, 2, 3, 6, 7, 9});
  }

  @Test
  public void testDoubleUnique() {
    DoubleSeries s1 = DataFrame.toSeries(new double[] {});
    Assert.assertEquals(s1.unique().values(), new double[] {});

    DoubleSeries s2 = DataFrame.toSeries(4.1, 5.2, 2.3, 1.4);
    Assert.assertEquals(s2.unique().values(), new double[] {1.4, 2.3, 4.1, 5.2});

    DoubleSeries s3 = DataFrame.toSeries(9.0, 1.1, 2.2, 3.0, 6.0, 1.1, 2.3, 9.0, 2.3, 7.0);
    Assert.assertEquals(s3.unique().values(), new double[] {1.1, 2.2, 2.3, 3.0, 6.0, 7.0, 9.0});
  }

  @Test
  public void testStringUnique() {
    StringSeries s1 = DataFrame.toSeries(new String[] {});
    Assert.assertEquals(s1.unique().values(), new String[] {});

    StringSeries s2 = DataFrame.toSeries("a", "A", "b", "Cc");
    Assert.assertEquals(new HashSet<>(s2.unique().toList()), new HashSet<>(Arrays.asList("a", "A", "b", "Cc")));

    StringSeries s3 = DataFrame.toSeries("a", "A", "b", "Cc", "A", "cC", "a", "cC");
    Assert.assertEquals(new HashSet<>(s3.unique().toList()), new HashSet<>(Arrays.asList("a", "A", "b", "Cc", "cC")));
  }

  @Test
  public void testStringFillNull() {
    StringSeries s = DataFrame.toSeries("a", null, null, "b", null);
    Assert.assertEquals(s.fillNull("N").values(), new String[] { "a", "N", "N", "b", "N" });
  }

  @Test
  public void testStringShift() {
    StringSeries s1 = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s1.shift(0).values(), VALUES_STRING);

    StringSeries s2 = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s2.shift(2).values(), new String[] { null, null, "-2.3", "-1", "0.0" });

    StringSeries s3 = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s3.shift(4).values(), new String[] { null, null, null, null, "-2.3" });

    StringSeries s4 = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s4.shift(-4).values(), new String[] { "0.13e1", null, null, null, null });

    StringSeries s5 = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s5.shift(100).values(), new String[] { null, null, null, null, null });

    StringSeries s6 = DataFrame.toSeries(VALUES_STRING);
    Assert.assertEquals(s6.shift(-100).values(), new String[] { null, null, null, null, null });
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDoubleMapNullConditionalFail() {
    DoubleSeries s = DataFrame.toSeries(1.0, DoubleSeries.NULL_VALUE, 2.0);
    s.map(new DoubleSeries.DoubleConditional() {
      @Override
      public boolean apply(double value) {
        return false;
      }
    });
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testLongMapNullConditionalFail() {
    LongSeries s = DataFrame.toSeries(1, LongSeries.NULL_VALUE, 2);
    s.map(new LongSeries.LongConditional() {
      @Override
      public boolean apply(long value) {
        return false;
      }
    });
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testStringMapNullConditionalFail() {
    StringSeries s = DataFrame.toSeries("1.0", StringSeries.NULL_VALUE, "2.0");
    s.map(new StringSeries.StringConditional() {
      @Override
      public boolean apply(String value) {
        return false;
      }
    });
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDoubleMapNullFunctionFail() {
    DoubleSeries s = DataFrame.toSeries(1.0, DoubleSeries.NULL_VALUE, 2.0);
    s.map(new DoubleSeries.DoubleFunction() {
      @Override
      public double apply(double value) {
        return value;
      }
    });
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testLongMapNullFunctionFail() {
    LongSeries s = DataFrame.toSeries(1, LongSeries.NULL_VALUE, 2);
    s.map(new LongSeries.LongFunction() {
      @Override
      public long apply(long value) {
        return value;
      }
    });
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testStringMapNullFunctionFail() {
    StringSeries s = DataFrame.toSeries("1.0", StringSeries.NULL_VALUE, "2.0");
    s.map(new StringSeries.StringFunction() {
      @Override
      public String apply(String value) {
        return value;
      }
    });
  }

  @Test
  public void testDropNullRows() {
    DataFrame mdf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6 });
    mdf.addSeries("double", 1.0, 2.0, DoubleSeries.NULL_VALUE, 4.0, 5.0, 6.0);
    mdf.addSeries("long", LongSeries.NULL_VALUE, 2, 3, 4, 5, 6);
    mdf.addSeries("string", "1.0", "2", "bbb", "true", StringSeries.NULL_VALUE, "aaa");
    mdf.addSeries("boolean", true, true, false, false, false, false);

    DataFrame ddf = mdf.dropNullRows();
    Assert.assertEquals(ddf.getIndex().size(), 3);
    Assert.assertEquals(ddf.getIndex().values(), new long[] { 2, 4, 6 });
    Assert.assertEquals(ddf.toDoubles("double").values(), new double[] { 2.0, 4.0, 6.0 });
    Assert.assertEquals(ddf.toLongs("long").values(), new long[] { 2, 4, 6 });
    Assert.assertEquals(ddf.toStrings("string").values(), new String[] { "2", "true", "aaa" });
    Assert.assertEquals(ddf.toBooleans("boolean").values(), new boolean[] { true, false, false });
  }

  @Test
  public void testDropNullRowsIdentity() {
    Assert.assertEquals(df.dropNullRows().getIndex().size(), df.getIndex().size());
  }

  @Test
  public void testDropNullColumns() {
    DataFrame mdf = new DataFrame(new long[] { 1, 2, 3 });
    mdf.addSeries("double_null", 1.0, 2.0, DoubleSeries.NULL_VALUE);
    mdf.addSeries("double", 1.0, 2.0, 3.0);
    mdf.addSeries("long_null", LongSeries.NULL_VALUE, 2, 3);
    mdf.addSeries("long", 1, 2, 3);
    mdf.addSeries("string_null", "true", StringSeries.NULL_VALUE, "aaa");
    mdf.addSeries("string", "true", "this", "aaa");
    mdf.addSeries("boolean", true, true, false);

    DataFrame ddf = mdf.dropNullColumns();
    Assert.assertEquals(ddf.getIndex().size(), 3);
    Assert.assertEquals(new HashSet<>(ddf.getSeriesNames()), new HashSet<>(Arrays.asList("double", "long", "string", "boolean")));
  }
}
