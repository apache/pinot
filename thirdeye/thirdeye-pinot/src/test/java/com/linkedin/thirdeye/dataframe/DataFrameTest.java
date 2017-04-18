package com.linkedin.thirdeye.dataframe;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DataFrameTest {
  final static byte TRUE = BooleanSeries.TRUE;
  final static byte FALSE = BooleanSeries.FALSE;
  final static double DNULL = DoubleSeries.NULL;
  final static long LNULL = LongSeries.NULL;
  final static String SNULL = StringSeries.NULL;
  final static byte BNULL = BooleanSeries.NULL;

  final static double COMPARE_DOUBLE_DELTA = 0.001;

  final static long[] INDEX = new long[] { -1, 1, -2, 4, 3 };
  final static double[] VALUES_DOUBLE = new double[] { -2.1, -0.1, 0.0, 0.5, 1.3 };
  final static long[] VALUES_LONG = new long[] { -2, 1, 0, 1, 2 };
  final static String[] VALUES_STRING = new String[] { "-2.3", "-1", "0.0", "0.5", "0.13e1" };
  final static byte[] VALUES_BOOLEAN = new byte[] { 1, 1, 0, 1, 1 };

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
    df = new DataFrame(INDEX)
        .addSeries("double", VALUES_DOUBLE)
        .addSeries("long", VALUES_LONG)
        .addSeries("string", VALUES_STRING)
        .addSeries("boolean", VALUES_BOOLEAN);
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
  public void testSeriesName() {
    df.addSeries("ab", VALUES_DOUBLE);
    df.addSeries("_a", VALUES_DOUBLE);
    df.addSeries("a1", VALUES_DOUBLE);
  }

  @Test
  public void testChainedEqualsSeparate() {
    DataFrame dfChained = new DataFrame()
        .addSeries("test", 1, 2, 3)
        .addSeries("drop", 1, 2, 3)
        .renameSeries("test", "checkme")
        .dropSeries("drop");

    DataFrame dfSeparate = new DataFrame();
    dfSeparate.addSeries("test", 1, 2, 3);
    dfSeparate.addSeries("drop", 1, 2, 3);
    dfSeparate.renameSeries("test", "checkme");
    dfSeparate.dropSeries("drop");

    Assert.assertEquals(dfChained.getSeriesNames().size(), 1);
    Assert.assertEquals(dfSeparate.getSeriesNames().size(), 1);
    Assert.assertEquals(dfChained, dfSeparate);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "testSeriesNameFailProvider")
  public void testSeriesNameFail(String name) {
    df.addSeries(name, VALUES_DOUBLE);
  }

  @DataProvider(name = "testSeriesNameFailProvider")
  public Object[][] testSeriesNameFailProvider() {
    return new Object[][] { { null }, { "" }, { "1a" }, { "a,b" }, { "a-b" }, { "a+b" }, { "a*b" }, { "a/b" }, { "a=b" }, { "a>b" } };
  }

  @Test
  public void testIndexColumn() {
    DataFrame dfEmpty = new DataFrame();
    Assert.assertTrue(dfEmpty.getSeriesNames().isEmpty());

    DataFrame dfIndexRange = new DataFrame(0);
    Assert.assertEquals(dfIndexRange.getSeriesNames(), Collections.singleton("index"));
  }

  @Test
  public void testDoubleNoDataDuplication() {
    DoubleSeries first = DataFrame.toSeries(VALUES_DOUBLE);
    DoubleSeries second = DataFrame.toSeries(VALUES_DOUBLE);
    Assert.assertSame(first.values(), second.values());
  }

  @Test
  public void testDoubleToDouble() {
    assertEquals(DataFrame.toSeries(VALUES_DOUBLE).getDoubles(), VALUES_DOUBLE);
  }

  @Test
  public void testDoubleToLong() {
    assertEquals(DataFrame.toSeries(VALUES_DOUBLE).getLongs(), -2, 0, 0, 0, 1);
  }

  @Test
  public void testDoubleToBoolean() {
   assertEquals(DataFrame.toSeries(VALUES_DOUBLE).getBooleans(), TRUE, TRUE, FALSE, TRUE, TRUE);
  }

  @Test
  public void testDoubleToString() {
    assertEquals(DataFrame.toSeries(VALUES_DOUBLE).getStrings(), "-2.1", "-0.1", "0.0", "0.5", "1.3");
  }

  @Test
  public void testLongToDouble() {
    assertEquals(DataFrame.toSeries(VALUES_LONG).getDoubles(), -2.0, 1.0, 0.0, 1.0, 2.0);
  }

  @Test
  public void testLongToLong() {
    assertEquals(DataFrame.toSeries(VALUES_LONG).getLongs(), VALUES_LONG);
  }

  @Test
  public void testLongToBoolean() {
    assertEquals(DataFrame.toSeries(VALUES_LONG).getBooleans(), TRUE, TRUE, FALSE, TRUE, TRUE);
  }

  @Test
  public void testLongToString() {
    assertEquals(DataFrame.toSeries(VALUES_LONG).getStrings(), "-2", "1", "0", "1", "2");
  }

  @Test
  public void testBooleanToDouble() {
    assertEquals(DataFrame.toSeries(VALUES_BOOLEAN).getDoubles(), 1.0, 1.0, 0.0, 1.0, 1.0);
  }

  @Test
  public void testBooleanToLong() {
    assertEquals(DataFrame.toSeries(VALUES_BOOLEAN).getLongs(), TRUE, TRUE, FALSE, TRUE, TRUE);
  }

  @Test
  public void testBooleanToBoolean() {
    assertEquals(DataFrame.toSeries(VALUES_BOOLEAN).getBooleans(), VALUES_BOOLEAN);
  }

  @Test
  public void testBooleanToString() {
    assertEquals(DataFrame.toSeries(VALUES_BOOLEAN).getStrings(), "true", "true", "false", "true", "true");
  }

  @Test
  public void testStringToDouble() {
    assertEquals(DataFrame.toSeries(VALUES_STRING).getDoubles(), -2.3, -1.0, 0.0, 0.5, 1.3);
  }

  @Test
  public void testStringToDoubleNulls() {
    Series s = DataFrame.toSeries("", null, "-2.1e1");
    assertEquals(s.getDoubles(), DNULL, DNULL, -21.0d);
  }

  @Test
  public void testStringToLong() {
    // NOTE: transparent conversion via double
    assertEquals(DataFrame.toSeries(VALUES_STRING).getLongs(), -2, -1, 0, 0, 1);
  }

  @Test
  public void testStringToLongNulls() {
    // NOTE: transparent conversion via double
    Series s = DataFrame.toSeries("", null, "-1.0");
    assertEquals(s.getLongs(), LNULL, LNULL, -1);
  }

  @Test
  public void testStringToBoolean() {
    // NOTE: transparent conversion via double
    assertEquals(DataFrame.toSeries(VALUES_STRING).getBooleans(), TRUE, TRUE, FALSE, TRUE, TRUE);
  }

  @Test
  public void testStringToBooleanNulls() {
    // NOTE: transparent conversion via double
    Series s = DataFrame.toSeries("", null, "true");
    assertEquals(s.getBooleans(), BNULL, BNULL, TRUE);
  }

  @Test
  public void testStringToString() {
    assertEquals(DataFrame.toSeries(VALUES_STRING).getStrings(), VALUES_STRING);
  }

  @Test
  public void testDoubleBuilderNull() {
    assertEquals(DoubleSeries.builder().addValues((Double)null).build(), DNULL);
  }

  @Test
  public void testLongBuilderNull() {
    assertEquals(LongSeries.builder().addValues((Long)null).build(), LNULL);
  }

  @Test
  public void testStringBuilderNull() {
    assertEquals(StringSeries.builder().addValues((String)null).build(), SNULL);
  }

  @Test
  public void testBooleanBuilderNull() {
    assertEquals(BooleanSeries.builder().addValues((Byte)null).build(), BNULL);
  }

  @Test
  public void testBooleanBuilderNullBoolean() {
    assertEquals(BooleanSeries.builder().addBooleanValues((Boolean)null).build(), BNULL);
  }

  @Test
  public void testDoubleNull() {
    Series s = DataFrame.toSeries(1.0, DNULL, 2.0);
    assertEquals(s.getDoubles(), 1.0, DNULL, 2.0);
    assertEquals(s.getLongs(), 1, LNULL, 2);
    assertEquals(s.getBooleans(), TRUE, BNULL, TRUE);
    assertEquals(s.getStrings(), "1.0", SNULL, "2.0");
  }

  @Test
  public void testLongNull() {
    Series s = DataFrame.toSeries(1, LNULL, 2);
    assertEquals(s.getDoubles(), 1.0, DNULL, 2.0);
    assertEquals(s.getLongs(), 1, LNULL, 2);
    assertEquals(s.getBooleans(), TRUE, BNULL, TRUE);
    assertEquals(s.getStrings(), "1", SNULL, "2");
  }

  @Test
  public void testBooleanNull() {
    Series s = DataFrame.toSeries(TRUE, BNULL, FALSE);
    assertEquals(s.getDoubles(), 1.0, DNULL, 0.0);
    assertEquals(s.getLongs(), 1, LNULL, 0);
    assertEquals(s.getBooleans(), TRUE, BNULL, FALSE);
    assertEquals(s.getStrings(), "true", SNULL, "false");
  }

  @Test
  public void testStringNull() {
    Series s = DataFrame.toSeries("1.0", SNULL, "2.0");
    assertEquals(s.getDoubles(), 1.0, DNULL, 2.0);
    assertEquals(s.getLongs(), 1, LNULL, 2);
    assertEquals(s.getBooleans(), TRUE, BNULL, TRUE);
    assertEquals(s.getStrings(), "1.0", SNULL, "2.0");
  }

  @Test
  public void testDoubleInfinity() {
    Series s = DataFrame.toSeries(DoubleSeries.POSITIVE_INFINITY, DoubleSeries.NEGATIVE_INFINITY);
    assertEquals(s.getDoubles(), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    assertEquals(s.getLongs(), LongSeries.MAX_VALUE, LongSeries.MIN_VALUE);
    assertEquals(s.getBooleans(), BooleanSeries.TRUE, BooleanSeries.TRUE);
    assertEquals(s.getStrings(), "Infinity", "-Infinity");

    assertEquals(DataFrame.toSeries("Infinity", "-Infinity").getDoubles(),
        Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testMapDoubleToDouble() {
    DoubleSeries in = DataFrame.toSeries(VALUES_DOUBLE);
    DoubleSeries out = in.map(new DoubleSeries.DoubleFunction() {
      public double apply(double... values) {
        return values[0] * 2;
      }
    });
    assertEquals(out, -4.2, -0.2, 0.0, 1.0, 2.6);
  }

  @Test
  public void testMapDoubleToBoolean() {
    DoubleSeries in = DataFrame.toSeries(VALUES_DOUBLE);
    BooleanSeries out = in.map(new DoubleSeries.DoubleConditional() {
      public boolean apply(double... values) {
        return values[0] <= 0.3;
      }
    });
    assertEquals(out, TRUE, TRUE, TRUE, FALSE, FALSE);
  }

  @Test
  public void testMapDataFrameAsDouble() {
    DoubleSeries out = df.map(new Series.DoubleFunction() {
      public double apply(double[] values) {
        return values[0] + values[1] + values[2];
      }
    }, "long", "string", "boolean");
    assertEquals(out, -3.3, 1.0, 0.0, 2.5, 4.3);
  }

  @Test
  public void testOverrideWithGeneratedSeries() {
    DoubleSeries out = df.getDoubles("double").map(new DoubleSeries.DoubleFunction() {
      public double apply(double... values) {
        return values[0] * 2;
      }
    });
    df = df.addSeries("double", out);
    assertEquals(df.getDoubles("double"), -4.2, -0.2, 0.0, 1.0, 2.6);
  }

  @Test
  public void testSortDouble() {
    DoubleSeries in = DataFrame.toSeries(3, 1.5, 1.3, 5, 1.9, DNULL);
    assertEquals(in.sorted(), DNULL, 1.3, 1.5, 1.9, 3, 5);
  }

  @Test
  public void testSortLong() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, LNULL);
    assertEquals(in.sorted(), LNULL, 3, 5, 13, 15, 19);
  }

  @Test
  public void testSortString() {
    StringSeries in = DataFrame.toSeries("b", "a", "ba", "ab", "aa", SNULL);
    assertEquals(in.sorted(), SNULL, "a", "aa", "ab", "b", "ba");
  }

  @Test
  public void testSortBoolean() {
    BooleanSeries in = DataFrame.toSeries(TRUE, FALSE, FALSE, TRUE, FALSE, BNULL);
    assertEquals(in.sorted(), BNULL, FALSE, FALSE, FALSE, TRUE, TRUE);
  }

  @Test
  public void testProject() {
    int[] fromIndex = new int[] { 1, -1, 4, 0 };
    DataFrame ndf = df.project(fromIndex);
    assertEquals(ndf.getLongs("index"), 1, LNULL, 3, -1);
    assertEquals(ndf.getDoubles("double"), -0.1, DNULL, 1.3, -2.1);
    assertEquals(ndf.getLongs("long"), 1, LNULL, 2, -2);
    assertEquals(ndf.getStrings("string"), "-1", SNULL, "0.13e1", "-2.3");
    assertEquals(ndf.getBooleans("boolean"), TRUE, BNULL, TRUE, TRUE);
  }

  @Test
  public void testSortByIndex() {
    df = df.sortedBy("index");
    // NOTE: internal logic uses reorder() for all sorting
    assertEquals(df.getLongs("index"), -2, -1, 1, 3, 4);
    assertEquals(df.getDoubles("double"), 0.0, -2.1, -0.1, 1.3, 0.5);
    assertEquals(df.getLongs("long"), 0, -2, 1, 2, 1);
    assertEquals(df.getStrings("string"), "0.0", "-2.3", "-1", "0.13e1", "0.5");
    assertEquals(df.getBooleans("boolean"), FALSE, TRUE, TRUE, TRUE, TRUE);
  }

  @Test
  public void testSortByDouble() {
    df = df.addSeries("myseries", 0.1, -2.1, 3.3, 4.6, -7.8 );
    df = df.sortedBy("myseries");
    assertEquals(df.getLongs("index"), 3, 1, -1, -2, 4);
    assertEquals(df.getLongs("long"), 2, 1, -2, 0, 1);
  }

  @Test
  public void testSortByLong() {
    df = df.addSeries("myseries", 1, -21, 33, 46, -78 );
    df = df.sortedBy("myseries");
    assertEquals(df.getLongs("index"), 3, 1, -1, -2, 4);
    assertEquals(df.getLongs("long"), 2, 1, -2, 0, 1);
  }

  @Test
  public void testSortByString() {
    df = df.addSeries("myseries", "b", "aa", "bb", "c", "a" );
    df = df.sortedBy("myseries");
    assertEquals(df.getLongs("index"), 3, 1, -1, -2, 4);
    assertEquals(df.getLongs("long"), 2, 1, -2, 0, 1);
  }

  @Test
  public void testSortByBoolean() {
    // NOTE: boolean sorted should be stable
    df = df.addSeries("myseries", true, true, false, false, true );
    df = df.sortedBy("myseries");
    assertEquals(df.getLongs("index"), -2, 4, -1, 1, 3);
    assertEquals(df.getLongs("long"), 0, 1, -2, 1, 2);
  }

  @Test
  public void testReverse() {
    // NOTE: uses separate reverse() implementation by each series
    df = df.reverse();
    assertEquals(df.getLongs("index"), 3, 4, -2, 1, -1);
    assertEquals(df.getDoubles("double"), 1.3, 0.5, 0.0, -0.1, -2.1);
    assertEquals(df.getLongs("long"), 2, 1, 0, 1, -2);
    assertEquals(df.getStrings("string"), "0.13e1", "0.5", "0.0", "-1", "-2.3");
    assertEquals(df.getBooleans("boolean"), TRUE, TRUE, FALSE, TRUE, TRUE);
  }

  @Test
  public void testAppendLongDouble() {
    Series s = df.get("long").append(df.get("double"));
    Assert.assertEquals(s.type(), Series.SeriesType.LONG);
    assertEquals(s.getLongs(), -2, 1, 0, 1, 2, -2, 0, 0, 0, 1);
  }

  @Test
  public void testAppendLongBoolean() {
    Series s = df.get("long").append(df.get("boolean"));
    Assert.assertEquals(s.type(), Series.SeriesType.LONG);
    assertEquals(s.getLongs(), -2, 1, 0, 1, 2, 1, 1, 0, 1, 1);
  }

  @Test
  public void testAppendLongString() {
    Series s = df.get("long").append(df.get("string"));
    Assert.assertEquals(s.type(), Series.SeriesType.LONG);
    assertEquals(s.getLongs(), -2, 1, 0, 1, 2, -2, -1, 0, 0, 1);
  }

  @Test
  public void testLongGroupByIntervalEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new long[0]).groupByInterval(1).isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testLongGroupByIntervalFailZero() {
    DataFrame.toSeries(-1).groupByInterval(0);
  }

  @Test
  public void testLongGroupByInterval() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20);
    Series.SeriesGrouping grouping = in.groupByInterval(4);

    Assert.assertEquals(grouping.size(), 6);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 3 });
    Assert.assertEquals(grouping.buckets.get(2).fromIndex, new int[] {});
    Assert.assertEquals(grouping.buckets.get(3).fromIndex, new int[] { 1, 2 });
    Assert.assertEquals(grouping.buckets.get(4).fromIndex, new int[] { 4 });
    Assert.assertEquals(grouping.buckets.get(5).fromIndex, new int[] { 5 });
  }

  @Test
  public void testLongGroupByCountEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new long[0]).groupByCount(1).isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testLongGroupByCountFailZero() {
    DataFrame.toSeries(-1).groupByCount(0);
  }

  @Test
  public void testLongGroupByCountAligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20);
    Series.SeriesGrouping grouping = in.groupByCount(3);

    Assert.assertEquals(grouping.size(), 2);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 3, 4, 5 });
  }

  @Test
  public void testLongBucketsByCountUnaligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 11, 12, 9);
    Series.SeriesGrouping grouping = in.groupByCount(3);

    Assert.assertEquals(grouping.size(), 3);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 3, 4, 5 });
    Assert.assertEquals(grouping.buckets.get(2).fromIndex, new int[] { 6, 7 });
  }

  @Test
  public void testLongGroupByPartitionsEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new long[0]).groupByPartitions(1).isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testLongGroupByPartitionsFailZero() {
    DataFrame.toSeries(-1).groupByPartitions(0);
  }

  @Test
  public void testLongGroupByPartitionsAligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20, 5, 5, 8, 1);
    Series.SeriesGrouping grouping = in.groupByPartitions(5);

    Assert.assertEquals(grouping.size(), 5);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0, 1 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 2, 3 });
    Assert.assertEquals(grouping.buckets.get(2).fromIndex, new int[] { 4, 5 });
    Assert.assertEquals(grouping.buckets.get(3).fromIndex, new int[] { 6, 7 });
    Assert.assertEquals(grouping.buckets.get(4).fromIndex, new int[] { 8, 9 });
  }

  @Test
  public void testLongGroupByPartitionsUnaligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20, 5, 5, 8, 1);
    Series.SeriesGrouping grouping = in.groupByPartitions(3);

    Assert.assertEquals(grouping.size(), 3);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 3, 4, 5, 6 });
    Assert.assertEquals(grouping.buckets.get(2).fromIndex, new int[] { 7, 8, 9 });
  }

  @Test
  public void testLongGroupByPartitionsUnalignedSmall() {
    LongSeries in = DataFrame.toSeries(3, 15, 1);
    Series.SeriesGrouping grouping = in.groupByPartitions(7);

    Assert.assertEquals(grouping.size(), 7);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] {});
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 0 });
    Assert.assertEquals(grouping.buckets.get(2).fromIndex, new int[] {});
    Assert.assertEquals(grouping.buckets.get(3).fromIndex, new int[] { 1 });
    Assert.assertEquals(grouping.buckets.get(4).fromIndex, new int[] {});
    Assert.assertEquals(grouping.buckets.get(5).fromIndex, new int[] { 2 });
    Assert.assertEquals(grouping.buckets.get(6).fromIndex, new int[] {});
  }

  @Test
  public void testLongGroupByValueEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new long[0]).groupByValue().isEmpty());
  }

  @Test
  public void testLongGroupByValue() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5);
    Series.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 4);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 5 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 0, 4 });
    Assert.assertEquals(grouping.buckets.get(2).fromIndex, new int[] { 1 });
    Assert.assertEquals(grouping.buckets.get(3).fromIndex, new int[] { 2, 3, 6 });
  }

  @Test
  public void testBooleanGroupByValueEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new boolean[0]).groupByValue().isEmpty());
  }

  @Test
  public void testBooleanGroupByValue() {
    BooleanSeries in = DataFrame.toSeries(true, false, false, true, false, true, false);
    Series.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 2);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 1, 2, 4, 6 });
    Assert.assertEquals(grouping.buckets.get(1).fromIndex, new int[] { 0, 3, 5 });
  }

  @Test
  public void testBooleanGroupByValueTrueOnly() {
    BooleanSeries in = DataFrame.toSeries(true, true, true);
    Series.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 1);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
  }

  @Test
  public void testBooleanGroupByValueFalseOnly() {
    BooleanSeries in = DataFrame.toSeries(false, false, false);
    Series.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 1);
    Assert.assertEquals(grouping.buckets.get(0).fromIndex, new int[] { 0, 1, 2 });
  }

  @Test
  public void testLongAggregateSum() {
    Series keys = DataFrame.toSeries(3, 5, 7);
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19 );
    List<Series.Bucket> buckets = new ArrayList<>();
    buckets.add(new Series.Bucket(new int[] { 1, 3, 4 }));
    buckets.add(new Series.Bucket(new int[] {}));
    buckets.add(new Series.Bucket(new int[] { 0, 2 }));

    Series.SeriesGrouping grouping = new Series.SeriesGrouping(keys, in, buckets);

    DataFrame out = grouping.aggregate(new LongSeries.LongSum());
    assertEquals(out.getLongs("key"), 3, 5, 7);
    assertEquals(out.getLongs("value"), 39, LNULL, 16);
  }

  @Test
  public void testLongAggregateLast() {
    Series keys = DataFrame.toSeries(3, 5, 7);
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19 );
    List<Series.Bucket> buckets = new ArrayList<>();
    buckets.add(new Series.Bucket(new int[] { 1, 3, 4 }));
    buckets.add(new Series.Bucket(new int[] {}));
    buckets.add(new Series.Bucket(new int[] { 0, 2 }));

    Series.SeriesGrouping grouping = new Series.SeriesGrouping(keys, in, buckets);

    DataFrame out = grouping.aggregate(new LongSeries.LongLast());
    assertEquals(out.getLongs("key"), 3, 5, 7);
    assertEquals(out.getLongs("value"), 19, LNULL, 13);
  }

  @Test
  public void testLongGroupByAggregateEndToEnd() {
    LongSeries in = DataFrame.toSeries(0, 3, 12, 2, 4, 8, 5, 1, 7, 9, 6, 10, 11);
    Series.SeriesGrouping grouping = in.groupByInterval(4);
    Assert.assertEquals(grouping.size(), 4);

    DataFrame out = grouping.aggregate(new LongSeries.LongSum());
    assertEquals(out.getLongs("key"), 0, 4, 8, 12);
    assertEquals(out.getLongs("value"), 6, 22, 38, 12);
  }

  @Test
  public void testAggregateWithoutData() {
    DoubleSeries s = DataFrame.toSeries(new double[0]);
    Assert.assertEquals(s.sum(), DNULL);
  }

  @Test
  public void testDoubleAggregateWithNull() {
    DoubleSeries s = DataFrame.toSeries(1.0, 2.0, DNULL, 4.0);
    Assert.assertEquals(s.sum(), DNULL);
    Assert.assertEquals(s.fillNull().sum(), 7.0);
    Assert.assertEquals(s.dropNull().sum(), 7.0);
  }

  @Test
  public void testLongAggregateWithNull() {
    LongSeries s = DataFrame.toSeries(1, 2, LNULL, 4);
    Assert.assertEquals(s.sum(), LNULL);
    Assert.assertEquals(s.fillNull().sum(), 7);
    Assert.assertEquals(s.dropNull().sum(), 7);
  }

  @Test
  public void testStringAggregateWithNull() {
    StringSeries s = DataFrame.toSeries("a", "b", SNULL, "d");
    Assert.assertEquals(s.join(), SNULL);
    Assert.assertEquals(s.fillNull().join(), "abd");
    Assert.assertEquals(s.dropNull().join(), "abd");
  }

  @Test
  public void testBooleanAggregateWithNull() {
    BooleanSeries s = DataFrame.toSeries(new byte[] { 1, 0, BNULL, 1 });
    Assert.assertEquals(s.aggregate(BooleanSeries.HAS_TRUE).value(), BNULL);
    Assert.assertEquals(s.fillNull().aggregate(BooleanSeries.HAS_TRUE).value(), 1);
    Assert.assertEquals(s.dropNull().aggregate(BooleanSeries.HAS_TRUE).value(), 1);
  }

  @Test
  public void testDataFrameGroupBy() {
    DataFrame.DataFrameGrouping grouping = df.groupBy("boolean");
    DoubleSeries ds = grouping.aggregate("double", new DoubleSeries.DoubleSum()).getDoubles("double");
    assertEquals(ds, 0.0, -0.4);

    LongSeries ls = grouping.aggregate("long", new LongSeries.LongSum()).get("long").getLongs();
    assertEquals(ls, 0, 2);

    StringSeries ss = grouping.aggregate("string", new StringSeries.StringConcat("|")).get("string").getStrings();
    assertEquals(ss, "0.0", "-2.3|-1|0.5|0.13e1");
  }

  @Test
  public void testResampleEndToEnd() {
    df = df.resampledBy("index", 2, new DataFrame.ResampleLast());

    Assert.assertEquals(df.size(), 4);
    Assert.assertEquals(df.getSeriesNames().size(), 5);

    assertEquals(df.getLongs("index"), -2, 0, 2, 4);
    assertEquals(df.getDoubles("double"), -2.1, -0.1, 1.3, 0.5);
    assertEquals(df.getLongs("long"), -2, 1, 2, 1);
    assertEquals(df.getStrings("string"), "-2.3", "-1", "0.13e1", "0.5");
    assertEquals(df.getBooleans("boolean"), TRUE, TRUE, TRUE, TRUE);
  }

  @Test
  public void testStableMultiSortDoubleLong() {
    DataFrame mydf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7, 8 })
        .addSeries("double", 1.0, 1.0, 2.0, 2.0, 1.0, 1.0, 2.0, 2.0)
        .addSeries("long", 2, 2, 2, 2, 1, 1, 1, 1);

    DataFrame sdfa = mydf.sortedBy("double", "long");
    assertEquals(sdfa.getLongs("index"), 5, 6, 1, 2, 7, 8, 3, 4);

    DataFrame sdfb = mydf.sortedBy("long", "double");
    assertEquals(sdfb.getLongs("index"), 3, 4, 7, 8, 1, 2, 5, 6);
  }

  @Test
  public void testStableMultiSortStringBoolean() {
    DataFrame mydf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7, 8 })
        .addSeries("string", "a", "a", "b", "b", "a", "a", "b", "b")
        .addSeries("boolean", true, true, true, true, false, false, false, false);

    DataFrame sdfa = mydf.sortedBy("string", "boolean");
    assertEquals(sdfa.getLongs("index"), 5, 6, 1, 2, 7, 8, 3, 4);

    DataFrame sdfb = mydf.sortedBy("boolean", "string");
    assertEquals(sdfb.getLongs("index"), 3, 4, 7, 8, 1, 2, 5, 6);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFilterUnequalLengthFail() {
    df.filter(DataFrame.toSeries(false, true));
  }

  @Test
  public void testFilter() {
    df = df.filter(DataFrame.toSeries(true, false, true, true, false));

    Assert.assertEquals(df.size(), 3);
    assertEquals(df.getLongs("index"),-1, -2, 4);
    assertEquals(df.getDoubles("double"), -2.1, 0.0, 0.5);
    assertEquals(df.getLongs("long"), -2, 0, 1);
    assertEquals(df.getStrings("string"),"-2.3", "0.0", "0.5");
    assertEquals(df.getBooleans("boolean"), TRUE, FALSE, TRUE);
  }

  @Test
  public void testFilterAll() {
    df = df.filter(DataFrame.toSeries(true, true, true, true, true));
    Assert.assertEquals(df.size(), 5);
  }

  @Test
  public void testFilterNone() {
    df = df.filter(DataFrame.toSeries(false, false, false, false, false));
    Assert.assertEquals(df.size(), 0);
  }

  @Test
  public void testFilterNull() {
    df = df.filter(DataFrame.toSeries(new byte[] { BNULL, 0, 1, BNULL, 0 }));
    Assert.assertEquals(df.size(), 1);
  }

  @Test
  public void testRenameSeries() {
    df = df.renameSeries("double", "new");

    df.getDoubles("new");

    try {
      df.getDoubles("double");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      // left blank
    }
  }

  @Test
  public void testRenameSeriesOverride() {
    df = df.renameSeries("double", "long");
    assertEquals(df.getDoubles("long"), VALUES_DOUBLE);
  }

  @Test
  public void testContains() {
    Assert.assertTrue(df.contains("double"));
    Assert.assertFalse(df.contains("NOT_VALID"));
  }

  @Test
  public void testCopy() {
    DataFrame ndf = df.copy();

    ndf.getDoubles("double").values()[0] = 100.0;
    Assert.assertNotEquals(df.getDoubles("double").first(), ndf.getDoubles("double").first());

    ndf.getLongs("long").values()[0] = 100;
    Assert.assertNotEquals(df.getLongs("long").first(), ndf.getLongs("long").first());

    ndf.getStrings("string").values()[0] = "other string";
    Assert.assertNotEquals(df.getStrings("string").first(), ndf.getStrings("string").first());

    ndf.getBooleans("boolean").values()[0] = 0;
    Assert.assertNotEquals(df.getBooleans("boolean").first(), ndf.getBooleans("boolean").first());
  }

  @Test
  public void testDoubleHead() {
    DoubleSeries s = DataFrame.toSeries(VALUES_DOUBLE);
    assertEquals(s.head(0), new double[0]);
    assertEquals(s.head(3), Arrays.copyOfRange(VALUES_DOUBLE, 0, 3));
    assertEquals(s.head(6), Arrays.copyOfRange(VALUES_DOUBLE, 0, 5));
  }

  @Test
  public void testDoubleTail() {
    DoubleSeries s = DataFrame.toSeries(VALUES_DOUBLE);
    assertEquals(s.tail(0), new double[0]);
    assertEquals(s.tail(3), Arrays.copyOfRange(VALUES_DOUBLE, 2, 5));
    assertEquals(s.tail(6), Arrays.copyOfRange(VALUES_DOUBLE, 0, 5));
  }

  @Test
  public void testDoubleAccessorsEmpty() {
    DoubleSeries s = DoubleSeries.empty();
    Assert.assertTrue(DoubleSeries.isNull(s.sum()));
    Assert.assertTrue(DoubleSeries.isNull(s.min()));
    Assert.assertTrue(DoubleSeries.isNull(s.max()));
    Assert.assertTrue(DoubleSeries.isNull(s.mean()));
    Assert.assertTrue(DoubleSeries.isNull(s.std()));

    try {
      s.first();
      Assert.fail();
    } catch(IllegalStateException ignore) {
      // left blank
    }

    try {
      s.last();
      Assert.fail();
    } catch(IllegalStateException ignore) {
      // left blank
    }

    try {
      s.value();
      Assert.fail();
    } catch(IllegalStateException ignore) {
      // left blank
    }
  }

  @Test
  public void testLongHead() {
    LongSeries s = DataFrame.toSeries(VALUES_LONG);
    assertEquals(s.head(0), new long[0]);
    assertEquals(s.head(3), Arrays.copyOfRange(VALUES_LONG, 0, 3));
    assertEquals(s.head(6), Arrays.copyOfRange(VALUES_LONG, 0, 5));
  }

  @Test
  public void testLongTail() {
    LongSeries s = DataFrame.toSeries(VALUES_LONG);
    assertEquals(s.tail(0), new long[0]);
    assertEquals(s.tail(3), Arrays.copyOfRange(VALUES_LONG, 2, 5));
    assertEquals(s.tail(6), Arrays.copyOfRange(VALUES_LONG, 0, 5));
  }

  @Test
  public void testLongAccessorsEmpty() {
    LongSeries s = LongSeries.empty();
    Assert.assertTrue(LongSeries.isNull(s.sum()));
    Assert.assertTrue(LongSeries.isNull(s.min()));
    Assert.assertTrue(LongSeries.isNull(s.max()));

    try {
      s.first();
      Assert.fail();
    } catch(IllegalStateException ignore) {
      // left blank
    }

    try {
      s.last();
      Assert.fail();
    } catch(IllegalStateException ignore) {
      // left blank
    }

    try {
      s.value();
      Assert.fail();
    } catch(IllegalStateException ignore) {
      // left blank
    }
  }

  @Test
  public void testLongUnique() {
    LongSeries s1 = DataFrame.toSeries(new long[0]);
    assertEquals(s1.unique(), new long[0]);

    LongSeries s2 = DataFrame.toSeries(4, 5, 2, 1);
    assertEquals(s2.unique(), 1, 2, 4, 5);

    LongSeries s3 = DataFrame.toSeries(9, 1, 2, 3, 6, 1, 2, 9, 2, 7);
    assertEquals(s3.unique(), 1, 2, 3, 6, 7, 9);
  }

  @Test
  public void testDoubleUnique() {
    DoubleSeries s1 = DataFrame.toSeries(new double[] {});
    assertEquals(s1.unique(), new double[0]);

    DoubleSeries s2 = DataFrame.toSeries(4.1, 5.2, 2.3, 1.4);
    assertEquals(s2.unique(), 1.4, 2.3, 4.1, 5.2);

    DoubleSeries s3 = DataFrame.toSeries(9.0, 1.1, 2.2, 3.0, 6.0, 1.1, 2.3, 9.0, 2.3, 7.0);
    assertEquals(s3.unique(), 1.1, 2.2, 2.3, 3.0, 6.0, 7.0, 9.0);
  }

  @Test
  public void testStringUnique() {
    StringSeries s1 = DataFrame.toSeries(new String[] {});
    assertEquals(s1.unique(), new String[0]);

    StringSeries s2 = DataFrame.toSeries("a", "A", "b", "Cc");
    Assert.assertEquals(new HashSet<>(s2.unique().toList()), new HashSet<>(Arrays.asList("a", "A", "b", "Cc")));

    StringSeries s3 = DataFrame.toSeries("a", "A", "b", "Cc", "A", "cC", "a", "cC");
    Assert.assertEquals(new HashSet<>(s3.unique().toList()), new HashSet<>(Arrays.asList("a", "A", "b", "Cc", "cC")));
  }

  @Test
  public void testStringFillNull() {
    StringSeries s = DataFrame.toSeries("a", SNULL, SNULL, "b", SNULL);
    assertEquals(s.fillNull("N"), "a", "N", "N", "b", "N");
  }

  @Test
  public void testStringShift() {
    StringSeries s1 = DataFrame.toSeries(VALUES_STRING);
    assertEquals(s1.shift(0), VALUES_STRING);

    StringSeries s2 = DataFrame.toSeries(VALUES_STRING);
    assertEquals(s2.shift(2), SNULL, SNULL, "-2.3", "-1", "0.0");

    StringSeries s3 = DataFrame.toSeries(VALUES_STRING);
    assertEquals(s3.shift(4), SNULL, SNULL, SNULL, SNULL, "-2.3");

    StringSeries s4 = DataFrame.toSeries(VALUES_STRING);
    assertEquals(s4.shift(-4), "0.13e1", SNULL, SNULL, SNULL, SNULL);

    StringSeries s5 = DataFrame.toSeries(VALUES_STRING);
    assertEquals(s5.shift(100), SNULL, SNULL, SNULL, SNULL, SNULL);

    StringSeries s6 = DataFrame.toSeries(VALUES_STRING);
    assertEquals(s6.shift(-100), SNULL, SNULL, SNULL, SNULL, SNULL);
  }

  @Test
  public void testDoubleMapNullConditional() {
    DoubleSeries in = DataFrame.toSeries(1.0, DNULL, 2.0);
    BooleanSeries out = in.map(new Series.DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return true;
      }
    });
    assertEquals(out, TRUE, BNULL, TRUE);
  }

  @Test
  public void testLongMapNullConditional() {
    LongSeries in = DataFrame.toSeries(1, LNULL, 2);
    BooleanSeries out = in.map(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return true;
      }
    });
    assertEquals(out, TRUE, BNULL, TRUE);
  }

  @Test
  public void testStringMapNullConditional() {
    StringSeries in = DataFrame.toSeries("1.0", SNULL, "2.0");
    BooleanSeries out = in.map(new Series.StringConditional() {
      @Override
      public boolean apply(String... values) {
        return true;
      }
    });
    assertEquals(out, TRUE, BNULL, TRUE);
  }

  @Test
  public void testDoubleMapNullFunction() {
    DoubleSeries in = DataFrame.toSeries(1.0, DNULL, 2.0);
    DoubleSeries out = in.map(new DoubleSeries.DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] + 1.0;
      }
    });
    assertEquals(out, 2.0, DNULL, 3.0);
  }

  @Test
  public void testLongMapNullFunction() {
    LongSeries in = DataFrame.toSeries(1, LNULL, 2);
    LongSeries out = in.map(new LongSeries.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] + 1;
      }
    });
    assertEquals(out, 2, LNULL, 3);
  }

  @Test
  public void testStringMapNullFunction() {
    StringSeries in = DataFrame.toSeries("1.0", SNULL, "2.0");
    StringSeries out = in.map(new StringSeries.StringFunction() {
      @Override
      public String apply(String... values) {
        return values[0] + "+";
      }
    });
    assertEquals(out, "1.0+", SNULL, "2.0+");
  }

  @Test
  public void testDropNullRows() {
    DataFrame mdf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6 })
        .addSeries("double", 1.0, 2.0, DNULL, 4.0, 5.0, 6.0)
        .addSeries("long", LNULL, 2, 3, 4, 5, 6)
        .addSeries("string", "1.0", "2", "bbb", "true", SNULL, "aaa")
        .addSeries("boolean", true, true, false, false, false, false);

    DataFrame ddf = mdf.dropNull();
    Assert.assertEquals(ddf.size(), 3);
    assertEquals(ddf.getLongs("index"), 2, 4, 6);
    assertEquals(ddf.getDoubles("double"), 2.0, 4.0, 6.0);
    assertEquals(ddf.getLongs("long"), 2, 4, 6);
    assertEquals(ddf.getStrings("string"), "2", "true", "aaa");
    assertEquals(ddf.getBooleans("boolean"), TRUE, FALSE, FALSE);
  }

  @Test
  public void testDropNullRowsIdentity() {
    Assert.assertEquals(df.dropNull().size(), df.size());
  }

  @Test
  public void testDropNullColumns() {
    DataFrame mdf = new DataFrame()
        .addSeries("double_null", 1.0, 2.0, DNULL)
        .addSeries("double", 1.0, 2.0, 3.0)
        .addSeries("long_null", LNULL, 2, 3)
        .addSeries("long", 1, 2, 3)
        .addSeries("string_null", "true", SNULL, "aaa")
        .addSeries("string", "true", "this", "aaa")
        .addSeries("boolean", true, true, false);

    DataFrame ddf = mdf.dropNullColumns();
    Assert.assertEquals(ddf.size(), 3);
    Assert.assertEquals(new HashSet<>(ddf.getSeriesNames()), new HashSet<>(Arrays.asList("double", "long", "string", "boolean")));
  }

  @Test
  public void testMapExpression() {
    DoubleSeries s = df.map("(double * 2 + long + boolean) / 2");
    assertEquals(s, -2.6, 0.9, 0.0, 1.5, 2.8);
  }

  @Test
  public void testMapExpressionNull() {
    DataFrame mdf = new DataFrame(VALUES_LONG)
        .addSeries("null", 1.0, 1.0, DNULL, 1.0, 1.0);
    DoubleSeries out = mdf.map("null + 1");
    assertEquals(out, 2.0, 2.0, DNULL, 2.0, 2.0);
  }

  @Test
  public void testMapExpressionOtherNullPass() {
    DataFrame mdf = new DataFrame(VALUES_LONG)
        .addSeries("null", 1.0, 1.0, DNULL, 1.0, 1.0)
        .addSeries("notnull", 1.0, 1.0, 1.0, 1.0, 1.0);
    mdf.map("notnull + 1");
  }

  @Test
  public void testMapExpressionWithNull() {
    DataFrame mdf = new DataFrame(VALUES_LONG)
        .addSeries("null", 1.0, 1.0, DNULL, 1.0, 1.0);
    DoubleSeries s = mdf.map("null + 1");
    assertEquals(s, 2.0, 2.0, DNULL, 2.0, 2.0);
  }

  @Test
  public void testDoubleMovingWindow() {
    DoubleSeries s = DataFrame.toSeries(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
    DoubleSeries out = s.applyMovingWindow(2, 1, new DoubleSeries.DoubleSum());
    assertEquals(out, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0);
  }

  @Test
  public void testSeriesEquals() {
    Assert.assertTrue(DataFrame.toSeries(0.0, 3.0, 4.0).equals(DataFrame.toSeries(0.0, 3.0, 4.0)));
    Assert.assertTrue(DataFrame.toSeries(0, 3, 4).equals(DataFrame.toSeries(0, 3, 4)));
    Assert.assertTrue(DataFrame.toSeries(false, true, true).equals(DataFrame.toSeries(false, true, true)));
    Assert.assertTrue(DataFrame.toSeries("1", "3", "4").equals(DataFrame.toSeries("1", "3", "4")));

    Assert.assertFalse(DataFrame.toSeries(0.0, 3.0, 4.0).equals(DataFrame.toSeries(0, 3, 4)));
    Assert.assertFalse(DataFrame.toSeries(0, 3, 4).equals(DataFrame.toSeries(0.0, 3.0, 4.0)));
    Assert.assertFalse(DataFrame.toSeries(false, true, true).equals(DataFrame.toSeries("0", "1", "1")));
    Assert.assertFalse(DataFrame.toSeries("1", "3", "4").equals(DataFrame.toSeries(1, 3, 4)));

    Assert.assertTrue(DataFrame.toSeries(0.0, 3.0, 4.0).equals(DataFrame.toSeries(0, 3, 4).getDoubles()));
    Assert.assertTrue(DataFrame.toSeries(0, 3, 4).equals(DataFrame.toSeries(0.0, 3.0, 4.0).getLongs()));
    Assert.assertTrue(DataFrame.toSeries(false, true, true).equals(DataFrame.toSeries("0", "1", "1").getBooleans()));
    Assert.assertTrue(DataFrame.toSeries("1", "3", "4").equals(DataFrame.toSeries(1, 3, 4).getStrings()));
  }

  @Test
  public void testLongJoinInner() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5, 4, 3, 3, 0);

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.INNER);

    Assert.assertEquals(pairs.size(), 3);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(1, 2));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(1, 3));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(0, 1));
  }

  @Test
  public void testLongJoinLeft() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5, 4, 3, 3, 0);

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.LEFT);

    Assert.assertEquals(pairs.size(), 5);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(2, -1));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(3, -1));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(1, 2));
    Assert.assertEquals(pairs.get(3), new Series.JoinPair(1, 3));
    Assert.assertEquals(pairs.get(4), new Series.JoinPair(0, 1));
  }

  @Test
  public void testLongJoinRight() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5, 4, 3, 3, 0);

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.RIGHT);

    Assert.assertEquals(pairs.size(), 5);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(-1, 4));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(1, 2));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(1, 3));
    Assert.assertEquals(pairs.get(3), new Series.JoinPair(0, 1));
    Assert.assertEquals(pairs.get(4), new Series.JoinPair(-1, 0));
  }

  @Test
  public void testLongJoinOuter() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5, 4, 3, 3, 0);

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.OUTER);

    Assert.assertEquals(pairs.size(), 7);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(-1, 4));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(2, -1));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(3, -1));
    Assert.assertEquals(pairs.get(3), new Series.JoinPair(1, 2));
    Assert.assertEquals(pairs.get(4), new Series.JoinPair(1, 3));
    Assert.assertEquals(pairs.get(5), new Series.JoinPair(0, 1));
    Assert.assertEquals(pairs.get(6), new Series.JoinPair(-1, 0));
  }

  @Test
  public void testLongDoubleJoinInner() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5.0, 4.0, 3.0, 3.0, 0.0);

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.INNER);

    Assert.assertEquals(pairs.size(), 3);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(1, 2));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(1, 3));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(0, 1));
  }

  @Test
  public void testStringJoinInner() {
    Series sLeft = DataFrame.toSeries("4", "3", "1", "2");
    Series sRight = DataFrame.toSeries("5", "4", "3", "3", "0");

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.INNER);

    Assert.assertEquals(pairs.size(), 3);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(1, 2));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(1, 3));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(0, 1));
  }

  @Test
  public void testBooleanJoinInner() {
    Series sLeft = DataFrame.toSeries(true, false, false);
    Series sRight = DataFrame.toSeries(false, true, true);

    List<Series.JoinPair> pairs = sLeft.join(sRight, Series.JoinType.INNER);

    Assert.assertEquals(pairs.size(), 4);
    Assert.assertEquals(pairs.get(0), new Series.JoinPair(1, 0));
    Assert.assertEquals(pairs.get(1), new Series.JoinPair(2, 0));
    Assert.assertEquals(pairs.get(2), new Series.JoinPair(0, 1));
    Assert.assertEquals(pairs.get(3), new Series.JoinPair(0, 2));
  }

  @Test
  public void testJoinInner() {
    DataFrame left = new DataFrame()
        .addSeries("leftKey", 4, 2, 1, 3)
        .addSeries("leftValue", "a", "d", "c", "b");

    DataFrame right = new DataFrame()
        .addSeries("rightKey", 5.0, 2.0, 1.0, 3.0, 1.0, 0.0)
        .addSeries("rightValue", "v", "z", "w", "x", "y", "u");

    DataFrame joined = left.joinInner(right, "leftKey", "rightKey");

    Assert.assertEquals(joined.size(), 4);
    Assert.assertEquals(joined.get("leftKey").type(), Series.SeriesType.LONG);
    Assert.assertEquals(joined.get("leftValue").type(), Series.SeriesType.STRING);
    Assert.assertEquals(joined.get("rightKey").type(), Series.SeriesType.DOUBLE);
    Assert.assertEquals(joined.get("rightValue").type(), Series.SeriesType.STRING);
    assertEquals(joined.getLongs("leftKey"), 1, 1, 2, 3);
    assertEquals(joined.getDoubles("rightKey"),1.0, 1.0, 2.0, 3.0);
    assertEquals(joined.getStrings("leftValue"), "c", "c", "d", "b");
    assertEquals(joined.getStrings("rightValue"), "w", "y", "z", "x");
  }

  @Test
  public void testJoinOuter() {
    DataFrame left = new DataFrame()
        .addSeries("leftKey", 4, 2, 1, 3)
        .addSeries("leftValue", "a", "d", "c", "b");

    DataFrame right = new DataFrame()
        .addSeries("rightKey", 5.0, 2.0, 1.0, 3.0, 1.0, 0.0)
        .addSeries("rightValue", "v", "z", "w", "x", "y", "u");

    DataFrame joined = left.joinOuter(right, "leftKey", "rightKey");

    Assert.assertEquals(joined.size(), 7);
    Assert.assertEquals(joined.get("leftKey").type(), Series.SeriesType.LONG);
    Assert.assertEquals(joined.get("leftValue").type(), Series.SeriesType.STRING);
    Assert.assertEquals(joined.get("rightKey").type(), Series.SeriesType.DOUBLE);
    Assert.assertEquals(joined.get("rightValue").type(), Series.SeriesType.STRING);
    assertEquals(joined.getLongs("leftKey"), LNULL, 1, 1, 2, 3, 4, LNULL);
    assertEquals(joined.getDoubles("rightKey"), 0.0, 1.0, 1.0, 2.0, 3.0, DNULL, 5.0);
    assertEquals(joined.getStrings("leftValue"), SNULL, "c", "c", "d", "b", "a", SNULL);
    assertEquals(joined.getStrings("rightValue"), "u", "w", "y", "z", "x", SNULL, "v");
  }

  @Test
  public void testJoinSameNameSameContent() {
    DataFrame left = new DataFrame()
        .addSeries("name", 1, 2, 3, 4);

    DataFrame right = new DataFrame()
        .addSeries("name", 3, 4, 5, 6);

    DataFrame df = left.joinInner(right, "name", "name");

    Assert.assertEquals(df.getSeriesNames().size(), 1);
    Assert.assertTrue(df.contains("name"));
    Assert.assertFalse(df.contains("name_right"));
  }

  @Test
  public void testJoinSameNameDifferentContent() {
    DataFrame left = new DataFrame()
        .addSeries("name", 1, 2, 3, 4);

    DataFrame right = new DataFrame()
        .addSeries("name", 3, 4, 5, 6);

    DataFrame df = left.joinOuter(right, "name", "name");

    Assert.assertEquals(df.getSeriesNames().size(), 2);
    Assert.assertTrue(df.contains("name"));
    Assert.assertTrue(df.contains("name_right"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testJoinIndexFailNoIndex() {
    DataFrame dfIndex = new DataFrame(5);
    DataFrame dfNoIndex = new DataFrame().addSeries(DataFrame.COLUMN_INDEX_DEFAULT, DataFrame.toSeries(VALUES_DOUBLE));
    dfIndex.joinOuter(dfNoIndex);
  }

  @Test
  public void testJoinIndex() {
    DataFrame dfLeft = new DataFrame(5).addSeries("one", 5, 4, 3, 2, 1);
    DataFrame dfRight = new DataFrame(3).addSeries("two", "A", "B", "C");
    DataFrame joined = dfLeft.joinLeft(dfRight);

    assertEquals(joined.getStrings("one"), "5", "4", "3", "2", "1");
    assertEquals(joined.getStrings("two"), "A", "B", "C", null, null);
  }

  @Test
  public void testBooleanHasTrueFalseNull() {
    BooleanSeries s1 = DataFrame.toSeries(new boolean[0]);
    Assert.assertFalse(s1.hasFalse());
    Assert.assertFalse(s1.hasTrue());
    Assert.assertFalse(s1.hasNull());

    BooleanSeries s2 = DataFrame.toSeries(true, true);
    Assert.assertFalse(s2.hasFalse());
    Assert.assertTrue(s2.hasTrue());
    Assert.assertFalse(s2.hasNull());

    BooleanSeries s3 = DataFrame.toSeries(false, false);
    Assert.assertTrue(s3.hasFalse());
    Assert.assertFalse(s3.hasTrue());
    Assert.assertFalse(s3.hasNull());

    BooleanSeries s4 = DataFrame.toSeries(true, false);
    Assert.assertTrue(s4.hasFalse());
    Assert.assertTrue(s4.hasTrue());
    Assert.assertFalse(s4.hasNull());

    BooleanSeries s5 = DataFrame.toSeries(TRUE, FALSE, BNULL);
    Assert.assertFalse(s5.hasFalse());
    Assert.assertFalse(s5.hasTrue());
    Assert.assertTrue(s5.hasNull());
  }

  @Test
  public void testBooleanAllTrueFalse() {
    BooleanSeries s1 = BooleanSeries.empty();
    Assert.assertFalse(s1.allTrue());
    Assert.assertFalse(s1.allFalse());

    BooleanSeries s2 = DataFrame.toSeries(true, true);
    Assert.assertFalse(s2.allFalse());
    Assert.assertTrue(s2.allTrue());

    BooleanSeries s3 = DataFrame.toSeries(false, false);
    Assert.assertTrue(s3.allFalse());
    Assert.assertFalse(s3.allTrue());

    BooleanSeries s4 = DataFrame.toSeries(true, false);
    Assert.assertFalse(s4.allFalse());
    Assert.assertFalse(s4.allTrue());

    BooleanSeries s5 = DataFrame.toSeries(TRUE, TRUE, BNULL);
    Assert.assertFalse(s5.allFalse());
    Assert.assertFalse(s5.allTrue());

    BooleanSeries s6 = DataFrame.toSeries(FALSE, FALSE, BNULL);
    Assert.assertFalse(s6.allFalse());
    Assert.assertFalse(s6.allTrue());

    BooleanSeries s7 = DataFrame.toSeries(TRUE, FALSE, BNULL);
    Assert.assertFalse(s7.allFalse());
    Assert.assertFalse(s7.allTrue());
  }

  @Test
  public void testStringInferSeriesTypeDoubleDot() {
    Series.SeriesType t = StringSeries.buildFrom("1", "2", "3.", "", null).inferType();
    Assert.assertEquals(t, Series.SeriesType.DOUBLE);
  }

  @Test
  public void testStringInferSeriesTypeDoubleExp() {
    Series.SeriesType t = StringSeries.buildFrom("1", "2e1", "3", "", null).inferType();
    Assert.assertEquals(t, Series.SeriesType.DOUBLE);
  }

  @Test
  public void testStringInferSeriesTypeLong() {
    Series.SeriesType t = StringSeries.buildFrom("2", "-4", "-0", "", null).inferType();
    Assert.assertEquals(t, Series.SeriesType.LONG);
  }

  @Test
  public void testStringInferSeriesTypeBoolean() {
    Series.SeriesType t = StringSeries.buildFrom("true", "False", "false", "", null).inferType();
    Assert.assertEquals(t, Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testStringInferSeriesTypeString() {
    Series.SeriesType t = StringSeries.buildFrom("true", "", "-0.2e1", null).inferType();
    Assert.assertEquals(t, Series.SeriesType.STRING);
  }

  @Test
  public void testCompareInversion() {
    StringSeries string = StringSeries.buildFrom("0", "", "true");
    BooleanSeries bool = BooleanSeries.buildFrom(FALSE, BNULL, TRUE);

    Assert.assertTrue(string.compare(bool, 0, 0) < 0); // "0" < "false"
    Assert.assertTrue(bool.compare(string, 0, 0) == 0);

    Assert.assertTrue(string.compare(bool, 1, 1) > 0); // "" > null
    Assert.assertTrue(bool.compare(string, 1, 1) == 0);

    Assert.assertTrue(string.compare(bool, 2, 2) == 0);
    Assert.assertTrue(bool.compare(string, 2, 2) == 0);
  }

  @Test
  public void testDataFrameFromCsv() throws IOException {
    Reader in = new InputStreamReader(this.getClass().getResourceAsStream("test.csv"));
    DataFrame df = DataFrame.fromCsv(in);

    Assert.assertEquals(df.getSeriesNames().size(), 3);
    Assert.assertEquals(df.size(), 6);

    Series a = df.get("header_A");
    Assert.assertEquals(a.type(), Series.SeriesType.STRING);
    assertEquals(a.getStrings(), "a1", "A2", "two words", "", "with comma, semicolon; and more", "");

    Series b = df.get("_1headerb");
    Assert.assertEquals(b.type(), Series.SeriesType.LONG);
    assertEquals(b.getLongs(), 1, 2, 3, 4, 5, 6);

    Series c = df.get("Header_C");
    Assert.assertEquals(c.type(), Series.SeriesType.BOOLEAN);
    assertEquals(c.getBooleans(), BNULL, TRUE, FALSE, FALSE, BNULL, TRUE);
  }

  @Test
  public void testDoubleFunctionConversion() {
    Series out = df.map(new Series.DoubleFunction() {
      @Override
      public double apply(double... values) {
        return values[0] + 1;
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.DOUBLE);
  }

  @Test
  public void testLongFunctionConversion() {
    Series out = df.map(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] + 1;
      }
    }, "double");
    Assert.assertEquals(out.type(), Series.SeriesType.LONG);
  }

  @Test
  public void testStringFunctionConversion() {
    Series out = df.map(new Series.StringFunction() {
      @Override
      public String apply(String... values) {
        return values[0] + "-";
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.STRING);
  }

  @Test
  public void testBooleanFunctionConversion() {
    Series out = df.map(new Series.BooleanFunction() {
      @Override
      public boolean apply(boolean... values) {
        return !values[0];
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testBooleanFunctionExConversion() {
    Series out = df.map(new Series.BooleanFunctionEx() {
      @Override
      public byte apply(boolean... values) {
        return TRUE;
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testDoubleConditionalConversion() {
    Series out = df.map(new Series.DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return true;
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testLongConditionalConversion() {
    Series out = df.map(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return true;
      }
    }, "double");
    Assert.assertEquals(out.type(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testStringConditionalConversion() {
    Series out = df.map(new Series.StringConditional() {
      @Override
      public boolean apply(String... values) {
        return true;
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testBooleanConditionalConversion() {
    Series out = df.map(new Series.BooleanConditional() {
      @Override
      public boolean apply(boolean... values) {
        return true;
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testFillForward() {
    // must pass
    LongSeries.empty().fillNullForward();

    // must pass
    LongSeries.buildFrom(LNULL).fillNullForward();

    LongSeries in = LongSeries.buildFrom(LNULL, 1, LNULL, 2, 3, LNULL);
    assertEquals(in.fillNullForward(), LNULL, 1, 1, 2, 3, 3);
  }

  @Test
  public void testFillBackward() {
    // must pass
    LongSeries.empty().fillNullBackward();

    // must pass
    LongSeries.buildFrom(LNULL).fillNullBackward();

    LongSeries in = LongSeries.buildFrom(LNULL, 1, LNULL, 2, 3, LNULL);
    assertEquals(in.fillNullBackward(), 1, 1, 2, 2, 3, LNULL);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIndexNone() {
    DataFrame df = new DataFrame();
    Assert.assertFalse(df.hasIndex());
    df.getIndex();
  }

  @Test
  public void testIndexDefault() {
    Assert.assertTrue(new DataFrame(0).hasIndex());
    Assert.assertTrue(new DataFrame(1, 2, 3).hasIndex());
    Assert.assertTrue(new DataFrame(DataFrame.toSeries(VALUES_STRING)).hasIndex());
  }

  @Test
  public void testIndexCopy() {
    DataFrame df = new DataFrame(5)
        .addSeries("test", DataFrame.toSeries(VALUES_BOOLEAN))
        .setIndex("test");
    Assert.assertEquals(df.copy().getIndexName(), "test");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIndexSetInvalid() {
    DataFrame df = new DataFrame(0);
    df.setIndex("test");
  }

  @Test
  public void testIndexRename() {
    DataFrame df = new DataFrame(0);
    Series index = df.getIndex();
    df.renameSeries(df.getIndexName(), "test");
    df.addSeries(DataFrame.COLUMN_INDEX_DEFAULT, DataFrame.toSeries(new double[0]));
    Assert.assertEquals(df.getIndexName(), "test");
    Assert.assertEquals(df.getIndex(), index);
  }

  @Test
  public void testDoubleNormalize() {
    DoubleSeries s = DataFrame.toSeries(1.5, 2.0, 3.5).normalize();
    assertEquals(s, 0, 0.25, 1.0);
  }

  @Test
  public void testDoubleNormalizeFailInvalid() {
    DoubleSeries s = DataFrame.toSeries(1.5, 1.5, 1.5).normalize();
    assertEquals(s, DoubleSeries.nulls(3));
  }

  @Test
  public void testDoubleZScore() {
    DoubleSeries s = DataFrame.toSeries(0.0, 1.0, 2.0).zscore();
    assertEquals(s, -0.707, 0.0, 0.707);
  }

  @Test
  public void testDoubleZScoreFailInvalid() {
    DoubleSeries s = DataFrame.toSeries(1.5, 1.5, 1.5).zscore();
    assertEquals(s, DoubleSeries.nulls(3));
  }

  @Test
  public void testDoubleOperationsSeries() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    DoubleSeries mod = DataFrame.toSeries(1, 1, 1, 0, DNULL);

    assertEquals(base.add(mod), DNULL, 1, 2, 1.5, DNULL);
    assertEquals(base.subtract(mod), DNULL, -1, 0, 1.5, DNULL);
    assertEquals(base.multiply(mod), DNULL, 0, 1, 0, DNULL);
    assertEquals(base.divide(mod.replace(0, 1)), DNULL, 0, 1, 1.5, DNULL);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);

    try {
      base.divide(mod);
      Assert.fail();
    } catch(ArithmeticException ignore) {
      // left blank
    }
  }

  @Test
  public void testDoubleOperationAddConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.add(1), DNULL, 1, 2, 2.5, 1.003);
    assertEquals(base.add(0), DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.add(-1), DNULL, -1, 0, 0.5, -0.997);
    assertEquals(base.add(DNULL), DoubleSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationSubtractConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.subtract(1), DNULL, -1, 0, 0.5, -0.997);
    assertEquals(base.subtract(0), DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.subtract(-1), DNULL, 1, 2, 2.5, 1.003);
    assertEquals(base.subtract(DNULL), DoubleSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationMultiplyConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.multiply(1), DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.multiply(0), DNULL, 0, 0, 0, 0);
    assertEquals(base.multiply(-1), DNULL, 0, -1, -1.5, -0.003);
    assertEquals(base.multiply(DNULL), DoubleSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationDivideConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.divide(1), DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.divide(-1), DNULL, 0, -1, -1.5, -0.003);
    assertEquals(base.divide(DNULL), DoubleSeries.nulls(5));

    try {
      base.divide(0);
      Assert.fail();
    } catch(ArithmeticException ignore) {
      // left blank
    }
  }

  @Test
  public void testDoubleOperationEqConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.eq(1), BNULL, FALSE, TRUE, FALSE, FALSE);
    assertEquals(base.eq(0), BNULL, TRUE, FALSE, FALSE, FALSE);
    assertEquals(base.eq(-1), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.eq(DNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testLongOperationsSeries() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    LongSeries mod = DataFrame.toSeries(1, 1, 1, 0, LNULL);

    assertEquals(base.add(mod), LNULL, 1, 2, 5, LNULL);
    assertEquals(base.subtract(mod), LNULL, -1, 0, 5, LNULL);
    assertEquals(base.multiply(mod), LNULL, 0, 1, 0, LNULL);
    assertEquals(base.divide(mod.replace(0, 1)), LNULL, 0, 1, 5, LNULL);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);

    try {
      base.divide(mod);
      Assert.fail();
    } catch(ArithmeticException ignore) {
      // left blank
    }
  }

  @Test
  public void testLongOperationAddConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.add(1), LNULL, 1, 2, 6, 11);
    assertEquals(base.add(0), LNULL, 0, 1, 5, 10);
    assertEquals(base.add(-1), LNULL, -1, 0, 4, 9);
    assertEquals(base.add(LNULL), LongSeries.nulls(5));
  }

  @Test
  public void testLongOperationSubtractConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.subtract(1), LNULL, -1, 0, 4, 9);
    assertEquals(base.subtract(0), LNULL, 0, 1, 5, 10);
    assertEquals(base.subtract(-1), LNULL, 1, 2, 6, 11);
    assertEquals(base.subtract(LNULL), LongSeries.nulls(5));
  }

  @Test
  public void testLongOperationMultiplyConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.multiply(1), LNULL, 0, 1, 5, 10);
    assertEquals(base.multiply(0), LNULL, 0, 0, 0, 0);
    assertEquals(base.multiply(-1), LNULL, 0, -1, -5, -10);
    assertEquals(base.multiply(LNULL), LongSeries.nulls(5));
  }

  @Test
  public void testLongOperationDivideConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.divide(1), LNULL, 0, 1, 5, 10);
    assertEquals(base.divide(-1), LNULL, 0, -1, -5, -10);
    assertEquals(base.divide(LNULL), LongSeries.nulls(5));
    try {
      base.divide(0);
      Assert.fail();
    } catch(ArithmeticException ignore) {
      // left blank
    }
  }

  @Test
  public void testLongOperationEqConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.eq(1), BNULL, FALSE, TRUE, FALSE, FALSE);
    assertEquals(base.eq(0), BNULL, TRUE, FALSE, FALSE, FALSE);
    assertEquals(base.eq(-1), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.eq(LNULL), BooleanSeries.nulls(5));
  }


  @Test
  public void testStringOperationsSeries() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b", "c", "d");
    StringSeries mod = DataFrame.toSeries("A", "A", "b", "B", SNULL);

    assertEquals(base.concat(mod), SNULL, "aA", "bb", "cB", SNULL);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);
  }

  @Test
  public void testStringOperationConcatConstant() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b", "c", "d");
    assertEquals(base.concat("X"), SNULL, "aX", "bX", "cX", "dX");
    assertEquals(base.concat(""), SNULL, "a", "b", "c", "d");
    assertEquals(base.concat(SNULL), StringSeries.nulls(5));
  }

  @Test
  public void testStringOperationEqConstant() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b", "c", "d");
    assertEquals(base.eq("a"), BNULL, TRUE, FALSE, FALSE, FALSE);
    assertEquals(base.eq("b"), BNULL, FALSE, TRUE, FALSE, FALSE);
    assertEquals(base.eq(""), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.eq(SNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testBooleanOperationsSeries() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    BooleanSeries mod = DataFrame.toSeries(TRUE, TRUE, TRUE, FALSE, BNULL);

    assertEquals(base.and(mod), BNULL, TRUE, FALSE, FALSE, BNULL);
    assertEquals(base.or(mod), BNULL, TRUE, TRUE, TRUE, BNULL);
    assertEquals(base.xor(mod), BNULL, FALSE, TRUE, TRUE, BNULL);
    assertEquals(base.implies(mod), BNULL, TRUE, TRUE, FALSE, BNULL);
    assertEquals(base.eq(mod), BNULL, TRUE, FALSE, FALSE, BNULL);
  }

  @Test
  public void testBooleanOperationAndConstant() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.and(true), BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.and(false), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.and(BNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testBooleanOperationOrConstant() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.or(true), BNULL, TRUE, TRUE, TRUE, TRUE);
    assertEquals(base.or(false), BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.or(BNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testBooleanOperationXorConstant() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.xor(true), BNULL, FALSE, TRUE, FALSE, TRUE);
    assertEquals(base.xor(false), BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.xor(BNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testBooleanOperationImpliesConstant() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.implies(true), BNULL, TRUE, TRUE, TRUE, TRUE);
    assertEquals(base.implies(false), BNULL, FALSE, TRUE, FALSE, TRUE);
    assertEquals(base.implies(BNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testBooleanOperationEqConstant() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.eq(true), BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.eq(false), BNULL, FALSE, TRUE, FALSE, TRUE);
    assertEquals(base.eq(BNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testAppend() {
    DataFrame base = new DataFrame();
    base.addSeries("A", 1, 2, 3, 4);
    base.addSeries("B", "a", "b", "c", "d");
    base.setIndex("B");

    DataFrame other = new DataFrame();
    other.addSeries("A", 5.0d, 6.3d, 7.1d);
    other.addSeries("C", true, true, false);

    DataFrame another = new DataFrame();
    another.addSeries("C", false, false);

    DataFrame res = base.append(other, another);

    Assert.assertEquals(res.getSeriesNames(), new HashSet<>(Arrays.asList("A", "B")));
    Assert.assertEquals(res.get("A").type(), Series.SeriesType.LONG);
    Assert.assertEquals(res.get("B").type(), Series.SeriesType.STRING);

    assertEquals(res.getLongs("A"), 1, 2, 3, 4, 5, 6, 7, LongSeries.NULL, LongSeries.NULL);
    assertEquals(res.getStrings("B"), "a", "b", "c", "d", null, null, null, null, null);
  }

  /* **************************************************************************
   * Helpers
   ***************************************************************************/

  static void assertEquals(Series actual, Series expected) {
    Assert.assertEquals(actual, expected);
  }

  static void assertEquals(DoubleSeries actual, double... expected) {
    assertEquals(actual.getDoubles().values(), expected);
  }

  static void assertEquals(double[] actual, double... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      if(Double.isNaN(actual[i]) && Double.isNaN(expected[i]))
        continue;
      Assert.assertEquals(actual[i], expected[i], COMPARE_DOUBLE_DELTA, "index=" + i);
    }
  }

  static void assertEquals(LongSeries actual, long... expected) {
    assertEquals(actual.getLongs().values(), expected);
  }

  static void assertEquals(long[] actual, long... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  static void assertEquals(StringSeries actual, String... expected) {
    assertEquals(actual.getStrings().values(), expected);
  }

  static void assertEquals(String[] actual, String... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  static void assertEquals(BooleanSeries actual, byte... expected) {
    assertEquals(actual.getBooleans().values(), expected);
  }

  static void assertEquals(BooleanSeries actual, boolean... expected) {
    BooleanSeries s = actual.getBooleans();
    if(s.hasNull())
      Assert.fail("Encountered NULL when comparing against booleans");
    assertEquals(s.valuesBoolean(), expected);
  }

  static void assertEquals(byte[] actual, byte... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  static void assertEquals(boolean[] actual, boolean... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }
}
