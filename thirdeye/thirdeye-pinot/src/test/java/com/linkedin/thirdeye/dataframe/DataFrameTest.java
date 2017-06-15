package com.linkedin.thirdeye.dataframe;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DataFrameTest {
  private final static byte TRUE = BooleanSeries.TRUE;
  private final static byte FALSE = BooleanSeries.FALSE;
  private final static double DNULL = DoubleSeries.NULL;
  private final static long LNULL = LongSeries.NULL;
  private final static String SNULL = StringSeries.NULL;
  private final static byte BNULL = BooleanSeries.NULL;

  private final static double COMPARE_DOUBLE_DELTA = 0.001;

  private final static long[] INDEX = new long[] { -1, 1, -2, 4, 3 };
  private final static double[] VALUES_DOUBLE = new double[] { -2.1, -0.1, 0.0, 0.5, 1.3 };
  private final static long[] VALUES_LONG = new long[] { -2, 1, 0, 1, 2 };
  private final static String[] VALUES_STRING = new String[] { "-2.3", "-1", "0.0", "0.5", "0.13e1" };
  private final static byte[] VALUES_BOOLEAN = new byte[] { 1, 1, 0, 1, 1 };

  // TODO test double batch function
  // TODO test string batch function
  // TODO test boolean batch function

  // TODO string test head, tail, accessors
  // TODO boolean test head, tail, accessors

  // TODO shift double, long, boolean
  // TODO fill double, long, boolean

  private DataFrame df;

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
  public void testDataFrameBuilderDynamicTyping() {
    DataFrame.Builder builder = DataFrame.builder("double", "long", "string", "boolean");

    builder.append(4.0d, 1, null, "true");
    builder.append(null, 2, "2", "true");
    builder.append(2.3d, "", "hi", "false");
    builder.append(1.0d, 4, "4", "");

    DataFrame df = builder.build();
    Assert.assertEquals(df.get("double").type(), Series.SeriesType.DOUBLE);
    Assert.assertEquals(df.get("long").type(), Series.SeriesType.LONG);
    Assert.assertEquals(df.get("string").type(), Series.SeriesType.STRING);
    Assert.assertEquals(df.get("boolean").type(), Series.SeriesType.BOOLEAN);

    assertEquals(df.getDoubles("double"), 4, DNULL, 2.3, 1);
    assertEquals(df.getLongs("long"), 1, 2, LNULL, 4);
    assertEquals(df.getStrings("string"), SNULL, "2", "hi", "4");
    assertEquals(df.getBooleans("boolean"),TRUE, TRUE, FALSE, BNULL);
  }

  @Test
  public void testDataFrameBuilderStaticTyping() {
    DataFrame.Builder builder = DataFrame.builder("double:DOUBLE", "long:LONG", "string:STRING", "boolean:BOOLEAN");

    builder.append(4.0d, 1, null, "true");
    builder.append(null, 2.34, "2", "1");
    builder.append("2", "", "3", "false");
    builder.append(1.0d, 4, "4", "");

    DataFrame df = builder.build();
    Assert.assertEquals(df.get("double").type(), Series.SeriesType.DOUBLE);
    Assert.assertEquals(df.get("long").type(), Series.SeriesType.LONG);
    Assert.assertEquals(df.get("string").type(), Series.SeriesType.STRING);
    Assert.assertEquals(df.get("boolean").type(), Series.SeriesType.BOOLEAN);

    assertEquals(df.getDoubles("double"), 4, DNULL, 2, 1);
    assertEquals(df.getLongs("long"), 1, 2, LNULL, 4);
    assertEquals(df.getStrings("string"), SNULL, "2", "3", "4");
    assertEquals(df.getBooleans("boolean"),TRUE, TRUE, FALSE, BNULL);
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testDataFrameBuilderStaticTypingFailDouble() {
    DataFrame.builder("double:DOUBLE").append("true").build();
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testDataFrameBuilderStaticTypingFailLong() {
    DataFrame.builder("long:LONG").append("true").build();
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
    Grouping.SeriesGrouping grouping = in.groupByInterval(4);

    Assert.assertEquals(grouping.size(), 6);
    assertEquals(grouping.apply(0).getLongs(), 3);
    assertEquals(grouping.apply(1).getLongs(), 5);
    assertEmpty(grouping.apply(2).getLongs());
    assertEquals(grouping.apply(3).getLongs(), 15, 13);
    assertEquals(grouping.apply(4).getLongs(), 19);
    assertEquals(grouping.apply(5).getLongs(), 20);
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
    Grouping.SeriesGrouping grouping = in.groupByCount(3);

    Assert.assertEquals(grouping.size(), 2);
    assertEquals(grouping.apply(0).getLongs(), 3, 15, 13);
    assertEquals(grouping.apply(1).getLongs(), 5, 19, 20);
  }

  @Test
  public void testLongBucketsByCountUnaligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 11, 12, 9);
    Grouping.SeriesGrouping grouping = in.groupByCount(3);

    Assert.assertEquals(grouping.size(), 3);
    assertEquals(grouping.apply(0).getLongs(), 3, 15, 13);
    assertEquals(grouping.apply(1).getLongs(), 5, 19, 11);
    assertEquals(grouping.apply(2).getLongs(), 12, 9);
  }

  @Test
  public void testLongGroupByPartitionsEmpty() {
    Assert.assertFalse(DataFrame.toSeries(new long[0]).groupByPartitions(1).isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testLongGroupByPartitionsFailZero() {
    DataFrame.toSeries(-1).groupByPartitions(0);
  }

  @Test
  public void testLongGroupByPartitionsAligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20, 5, 5, 8, 1);
    Grouping.SeriesGrouping grouping = in.groupByPartitions(5);

    Assert.assertEquals(grouping.size(), 5);
    assertEquals(grouping.apply(0).getLongs(), 3, 15);
    assertEquals(grouping.apply(1).getLongs(), 13, 5);
    assertEquals(grouping.apply(2).getLongs(), 19, 20);
    assertEquals(grouping.apply(3).getLongs(), 5, 5);
    assertEquals(grouping.apply(4).getLongs(), 8, 1);
  }

  @Test
  public void testLongGroupByPartitionsUnaligned() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19, 20, 5, 5, 8, 1);
    Grouping.SeriesGrouping grouping = in.groupByPartitions(3);

    Assert.assertEquals(grouping.size(), 3);
    assertEquals(grouping.apply(0).getLongs(), 3, 15, 13);
    assertEquals(grouping.apply(1).getLongs(), 5, 19, 20, 5);
    assertEquals(grouping.apply(2).getLongs(),5, 8, 1);
  }

  @Test
  public void testLongGroupByPartitionsUnalignedSmall() {
    LongSeries in = DataFrame.toSeries(3, 15, 1);
    Grouping.SeriesGrouping grouping = in.groupByPartitions(7);

    Assert.assertEquals(grouping.size(), 7);
    assertEmpty(grouping.apply(0).getLongs());
    assertEquals(grouping.apply(1).getLongs(), 3);
    assertEmpty(grouping.apply(2).getLongs());
    assertEquals(grouping.apply(3).getLongs(), 15);
    assertEmpty(grouping.apply(4).getLongs());
    assertEquals(grouping.apply(5).getLongs(), 1);
    assertEmpty(grouping.apply(6).getLongs());
  }

  @Test
  public void testLongGroupByValueEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new long[0]).groupByValue().isEmpty());
  }

  @Test
  public void testLongGroupByValue() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5);
    Grouping.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 4);
    assertEquals(grouping.apply(0).getLongs(), 1);
    assertEquals(grouping.apply(1).getLongs(), 3, 3);
    assertEquals(grouping.apply(2).getLongs(), 4);
    assertEquals(grouping.apply(3).getLongs(), 5, 5, 5);
  }

  @Test
  public void testLongGroupByMovingWindow() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);

    Assert.assertEquals(grouping.size(), 5);
    assertEquals(grouping.apply(0).getLongs(), 3, 4, 5);
    assertEquals(grouping.apply(1).getLongs(), 4, 5, 5);
    assertEquals(grouping.apply(2).getLongs(), 5, 5, 3);
    assertEquals(grouping.apply(3).getLongs(), 5, 3, 1);
    assertEquals(grouping.apply(4).getLongs(), 3, 1, 5);
  }

  @Test
  public void testLongGroupByMovingWindowTooLarge() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(8);
    Assert.assertEquals(grouping.size(), 0);
  }

  @Test
  public void testLongGroupByMovingWindowAggregation() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);
    DataFrame out = grouping.aggregate(LongSeries.SUM);

    Assert.assertEquals(out.size(), 5);
    assertEquals(out.getLongs(Grouping.GROUP_KEY), 0, 1, 2, 3, 4);
    assertEquals(out.getLongs(Grouping.GROUP_VALUE), 12, 14, 13, 9, 9);
  }

  @Test
  public void testLongGroupByExpandingWindow() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();

    Assert.assertEquals(grouping.size(), 4);
    assertEquals(grouping.apply(0).getLongs(), 3);
    assertEquals(grouping.apply(1).getLongs(), 3, 4);
    assertEquals(grouping.apply(2).getLongs(), 3, 4, 5);
    assertEquals(grouping.apply(3).getLongs(), 3, 4, 5, 5);
  }

  @Test
  public void testLongGroupByExpandingWindowAggregation() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();
    DataFrame out = grouping.aggregate(LongSeries.SUM);

    Assert.assertEquals(out.size(), 4);
    assertEquals(out.getLongs(Grouping.GROUP_KEY), 0, 1, 2, 3);
    assertEquals(out.getLongs(Grouping.GROUP_VALUE), 3, 7, 12, 17);
  }

  @Test
  public void testBooleanGroupByValueEmpty() {
    Assert.assertTrue(DataFrame.toSeries(new boolean[0]).groupByValue().isEmpty());
  }

  @Test
  public void testBooleanGroupByValue() {
    BooleanSeries in = DataFrame.toSeries(true, false, false, true, false, true, false);
    Grouping.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 2);
    assertEquals(grouping.apply(0).getBooleans(), false, false, false, false);
    assertEquals(grouping.apply(1).getBooleans(), true, true, true);
  }

  @Test
  public void testBooleanGroupByValueTrueOnly() {
    BooleanSeries in = DataFrame.toSeries(true, true, true);
    Grouping.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 1);
    assertEquals(grouping.apply(0).getBooleans(), true, true, true);
  }

  @Test
  public void testBooleanGroupByValueFalseOnly() {
    BooleanSeries in = DataFrame.toSeries(false, false, false);
    Grouping.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 1);
    assertEquals(grouping.apply(0).getBooleans(), false, false, false);
  }

  @Test
  public void testLongAggregateSum() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19);
    Grouping grouping = Grouping.GroupingStatic.from(
        LongSeries.buildFrom(3, 5, 7),
        new int[] { 1, 3, 4 }, new int[] {}, new int[] { 0, 2 });

    DataFrame out = grouping.aggregate(in, LongSeries.SUM);
    assertEquals(out.getLongs("key"), 3, 5, 7);
    assertEquals(out.getLongs("value"), 39, LNULL, 16);
  }

  @Test
  public void testLongAggregateLast() {
    LongSeries in = DataFrame.toSeries(3, 15, 13, 5, 19);
    Grouping grouping = Grouping.GroupingStatic.from(
        LongSeries.buildFrom(3, 5, 7),
        new int[] { 1, 3, 4 }, new int[] {}, new int[] { 0, 2 });

    DataFrame out = grouping.aggregate(in, LongSeries.LAST);
    assertEquals(out.getLongs("key"), 3, 5, 7);
    assertEquals(out.getLongs("value"), 19, LNULL, 13);
  }

  @Test
  public void testLongGroupByAggregateEndToEnd() {
    LongSeries in = DataFrame.toSeries(0, 3, 12, 2, 4, 8, 5, 1, 7, 9, 6, 10, 11);
    Grouping.SeriesGrouping grouping = in.groupByInterval(4);
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
    Assert.assertEquals(s.sum(), 7.0);
    Assert.assertEquals(s.fillNull().sum(), 7.0);
    Assert.assertEquals(s.dropNull().sum(), 7.0);
  }

  @Test
  public void testLongAggregateWithNull() {
    LongSeries s = DataFrame.toSeries(1, 2, LNULL, 4);
    Assert.assertEquals(s.sum(), 7);
    Assert.assertEquals(s.fillNull().sum(), 7);
    Assert.assertEquals(s.dropNull().sum(), 7);
  }

  @Test
  public void testStringAggregateWithNull() {
    StringSeries s = DataFrame.toSeries("a", "b", SNULL, "d");
    Assert.assertEquals(s.join(), "abd");
    Assert.assertEquals(s.fillNull().join(), "abd");
    Assert.assertEquals(s.dropNull().join(), "abd");
  }

  @Test
  public void testBooleanAggregateWithNull() {
    BooleanSeries s = DataFrame.toSeries(TRUE, FALSE, BNULL, TRUE);
    Assert.assertEquals(s.aggregate(BooleanSeries.HAS_TRUE).value(), TRUE);
    Assert.assertEquals(s.fillNull().aggregate(BooleanSeries.HAS_TRUE).value(), 1);
    Assert.assertEquals(s.dropNull().aggregate(BooleanSeries.HAS_TRUE).value(), 1);
  }

  @Test
  public void testDataFrameGroupBy() {
    Grouping.DataFrameGrouping grouping = df.groupByValue("boolean");
    DoubleSeries ds = grouping.aggregate("double", new DoubleSeries.DoubleSum()).getDoubles("double");
    assertEquals(ds, 0.0, -0.4);

    LongSeries ls = grouping.aggregate("long", new LongSeries.LongSum()).get("long").getLongs();
    assertEquals(ls, 0, 2);

    StringSeries ss = grouping.aggregate("string", new StringSeries.StringConcat("|")).get("string").getStrings();
    assertEquals(ss, "0.0", "-2.3|-1|0.5|0.13e1");
  }

  @Test
  public void testResampleEndToEnd() {
    df = df.resample("index", 2, new DataFrame.ResampleLast());

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

    Assert.assertEquals(df.size(), 5);
    df = df.dropNull();

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
    Assert.assertEquals(df.dropNull().size(), 5);
  }

  @Test
  public void testFilterNone() {
    df = df.filter(DataFrame.toSeries(false, false, false, false, false));
    Assert.assertEquals(df.size(), 5);
    Assert.assertEquals(df.dropNull().size(), 0);
  }

  @Test
  public void testFilterNull() {
    df = df.filter(DataFrame.toSeries(BNULL, FALSE, TRUE, BNULL, FALSE));
    Assert.assertEquals(df.size(), 5);
    Assert.assertEquals(df.dropNull().size(), 1);
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
    assertEmpty(s.head(0));
    assertEquals(s.head(3), Arrays.copyOfRange(VALUES_DOUBLE, 0, 3));
    assertEquals(s.head(6), Arrays.copyOfRange(VALUES_DOUBLE, 0, 5));
  }

  @Test
  public void testDoubleTail() {
    DoubleSeries s = DataFrame.toSeries(VALUES_DOUBLE);
    assertEmpty(s.tail(0));
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
    } catch(IllegalStateException expected) {
      // left blank
    }

    try {
      s.last();
      Assert.fail();
    } catch(IllegalStateException expected) {
      // left blank
    }

    try {
      s.value();
      Assert.fail();
    } catch(IllegalStateException expected) {
      // left blank
    }
  }

  @Test
  public void testLongHead() {
    LongSeries s = DataFrame.toSeries(VALUES_LONG);
    assertEmpty(s.head(0));
    assertEquals(s.head(3), Arrays.copyOfRange(VALUES_LONG, 0, 3));
    assertEquals(s.head(6), Arrays.copyOfRange(VALUES_LONG, 0, 5));
  }

  @Test
  public void testLongTail() {
    LongSeries s = DataFrame.toSeries(VALUES_LONG);
    assertEmpty(s.tail(0));
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
    } catch(IllegalStateException expected) {
      // left blank
    }

    try {
      s.last();
      Assert.fail();
    } catch(IllegalStateException expected) {
      // left blank
    }

    try {
      s.value();
      Assert.fail();
    } catch(IllegalStateException expected) {
      // left blank
    }
  }

  @Test
  public void testLongUnique() {
    LongSeries s1 = DataFrame.toSeries(new long[0]);
    assertEmpty(s1.unique());

    LongSeries s2 = DataFrame.toSeries(4, 5, 2, 1);
    assertEquals(s2.unique(), 1, 2, 4, 5);

    LongSeries s3 = DataFrame.toSeries(9, 1, 2, 3, 6, 1, 2, 9, 2, 7);
    assertEquals(s3.unique(), 1, 2, 3, 6, 7, 9);
  }

  @Test
  public void testDoubleUnique() {
    DoubleSeries s1 = DataFrame.toSeries(new double[] {});
    assertEmpty(s1.unique());

    DoubleSeries s2 = DataFrame.toSeries(4.1, 5.2, 2.3, 1.4);
    assertEquals(s2.unique(), 1.4, 2.3, 4.1, 5.2);

    DoubleSeries s3 = DataFrame.toSeries(9.0, 1.1, 2.2, 3.0, 6.0, 1.1, 2.3, 9.0, 2.3, 7.0);
    assertEquals(s3.unique(), 1.1, 2.2, 2.3, 3.0, 6.0, 7.0, 9.0);
  }

  @Test
  public void testStringUnique() {
    StringSeries s1 = DataFrame.toSeries(new String[] {});
    assertEmpty(s1.unique());

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
  public void testJoinSameName() {
    DataFrame left = new DataFrame()
        .addSeries("name", 1, 2, 3, 4)
        .addSeries("value", 1, 2, 3, 4)
        .addSeries("left", 1, 2, 3, 4);

    DataFrame right = new DataFrame()
        .addSeries("name", 3, 4, 5, 6)
        .addSeries("value", 3, 4, 5, 6)
        .addSeries("right", 1, 2, 3, 4);

    DataFrame df = left.joinInner(right, "name", "name");

    Assert.assertEquals(df.getSeriesNames().size(), 5);

    Assert.assertTrue(df.contains("name"));
    Assert.assertFalse(df.contains("name" + DataFrame.COLUMN_JOIN_LEFT));
    Assert.assertFalse(df.contains("name" + DataFrame.COLUMN_JOIN_RIGHT));

    Assert.assertFalse(df.contains("value"));
    Assert.assertTrue(df.contains("value" + DataFrame.COLUMN_JOIN_LEFT));
    Assert.assertTrue(df.contains("value" + DataFrame.COLUMN_JOIN_RIGHT));

    Assert.assertTrue(df.contains("left"));
    Assert.assertFalse(df.contains("left" + DataFrame.COLUMN_JOIN_LEFT));
    Assert.assertFalse(df.contains("left" + DataFrame.COLUMN_JOIN_RIGHT));

    Assert.assertTrue(df.contains("right"));
    Assert.assertFalse(df.contains("right" + DataFrame.COLUMN_JOIN_LEFT));
    Assert.assertFalse(df.contains("right" + DataFrame.COLUMN_JOIN_RIGHT));
  }

  @Test
  public void testJoinDifferentName() {
    DataFrame left = new DataFrame()
        .addSeries("name", 1, 2, 3, 4);

    DataFrame right = new DataFrame()
        .addSeries("key", 3, 4, 5, 6);

    DataFrame df = left.joinInner(right, "name", "key");

    Assert.assertEquals(df.getSeriesNames().size(), 2);

    Assert.assertTrue(df.contains("name"));
    Assert.assertTrue(df.contains("key"));
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
  public void testAddSeriesFromDataFrame() {
    DataFrame dfLeft = new DataFrame(6)
        .addSeries("one", 5, 4, 3, 2, 1, 0);
    DataFrame dfRight = new DataFrame(5)
        .addSeries("two", 11, 12, 13, 14, 15)
        .addSeries("three", 22, 23, 24, 25, 26)
        .addSeries("four", 1, 1, 1, 1, 1);

    dfLeft.addSeries(dfRight, "two", "three");

    assertEquals(dfLeft.getLongs("two"), 11, 12, 13, 14, 15, LNULL);
    assertEquals(dfLeft.getLongs("three"), 22, 23, 24, 25, 26, LNULL);
    Assert.assertTrue(dfLeft.contains("one"));
    Assert.assertTrue(!dfLeft.contains("four"));
  }

  @Test
  public void testAddSeriesFromDataFrameFast() {
    DataFrame dfLeft = new DataFrame(5)
        .addSeries("one", 5, 4, 3, 2, 1);
    DataFrame dfRight = new DataFrame(5)
        .addSeries("two", 11, 12, 13, 14, 15);

    Assert.assertEquals(dfLeft.getIndex(), dfRight.getIndex());

    dfLeft.addSeries(dfRight);

    assertEquals(dfLeft.getLongs("two"), 11, 12, 13, 14, 15);
    Assert.assertTrue(dfLeft.contains("one"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddSeriesFromDataFrameFailIndexSource() {
    DataFrame dfLeft = new DataFrame(5);
    DataFrame dfRight = new DataFrame();
    dfLeft.addSeries(dfRight);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddSeriesFromDataFrameFailIndexDestination() {
    DataFrame dfLeft = new DataFrame();
    DataFrame dfRight = new DataFrame(5);
    dfLeft.addSeries(dfRight);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddSeriesFromDataFrameFailMissingSeries() {
    DataFrame dfLeft = new DataFrame(5);
    DataFrame dfRight = new DataFrame(5);
    dfLeft.addSeries(dfRight, "missing");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddSeriesFromDataFrameFailNonUniqueMapping() {
    DataFrame dfLeft = new DataFrame(3)
        .addSeries("one", 1, 2, 3);
    DataFrame dfRight = new DataFrame(1, 1, 3)
        .addSeries("two", 10, 11, 12);

    dfLeft.addSeries(dfRight);
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
    Assert.assertTrue(s5.hasFalse());
    Assert.assertTrue(s5.hasTrue());
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
    Assert.assertTrue(s5.allTrue());

    BooleanSeries s6 = DataFrame.toSeries(FALSE, FALSE, BNULL);
    Assert.assertTrue(s6.allFalse());
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
      public byte apply(byte... values) {
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
    assertEquals(base.pow(mod), DNULL, 0, 1, 1, DNULL);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);

    try {
      base.divide(mod);
      Assert.fail();
    } catch(ArithmeticException expected) {
      // left blank
    }
  }

  @Test
  public void testDoubleOperationsSeriesMisaligned() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    DoubleSeries mod = DataFrame.toSeries(1, 1, 1, DNULL);

    try {
      base.add(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.subtract(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.multiply(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.divide(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.eq(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
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
    } catch(ArithmeticException expected) {
      // left blank
    }
  }

  @Test
  public void testDoubleOperationPowConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.pow(1), DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.pow(0), DNULL, 1, 1, 1, 1);
    assertEquals(base.pow(-1), DNULL, DoubleSeries.INFINITY, 1, 1 / 1.5, 1 / 0.003);
    assertEquals(base.pow(DNULL), DoubleSeries.nulls(5));
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
  public void testDoubleCount() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 1, 1.5, 0.003);
    Assert.assertEquals(base.count(1), 2);
    Assert.assertEquals(base.count(2), 0);
    Assert.assertEquals(base.count(DNULL), 1);
  }

  @Test
  public void testDoubleContains() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 1, 1.5, 0.003);
    Assert.assertTrue(base.contains(1));
    Assert.assertFalse(base.contains(2));
    Assert.assertTrue(base.contains(DNULL));
  }

  @Test
  public void testDoubleReplace() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 1, 1.5, 0.003);
    assertEquals(base.replace(1, 2), DNULL, 2, 2, 1.5, 0.003);
    assertEquals(base.replace(1, DNULL), DNULL, DNULL, DNULL, 1.5, 0.003);
    assertEquals(base.replace(2, 1), DNULL, 1, 1, 1.5, 0.003);
    assertEquals(base.replace(1.5, DNULL), DNULL, 1, 1, DNULL, 0.003);
    assertEquals(base.replace(DNULL, 1), 1, 1, 1, 1.5, 0.003);
  }

  @Test
  public void testDoubleFilterSeries() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 1, 1.5, 0.003);
    BooleanSeries mod = DataFrame.toSeries(TRUE, TRUE, TRUE, FALSE, BNULL);
    assertEquals(base.filter(mod), DNULL, 1, 1, DNULL, DNULL);
  }

  @Test
  public void testDoubleFilterConditional() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 1, 1.5, 0.003);
    assertEquals(base.filter(new Series.DoubleConditional() {
      @Override
      public boolean apply(double... values) {
        return (values[0] >= 1 && values[0] < 1.5) || values[0] == 0.003;
      }
    }), DNULL, 1, 1, DNULL, 0.003);
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
    } catch(ArithmeticException expected) {
      // left blank
    }
  }

  @Test
  public void testLongOperationsSeriesMisaligned() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    LongSeries mod = DataFrame.toSeries(1, 1, 1, LNULL);

    try {
      base.add(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.subtract(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.multiply(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.divide(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.eq(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
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
    } catch(ArithmeticException expected) {
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
  public void testLongCount() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 0, 5, 10);
    Assert.assertEquals(base.count(0), 2);
    Assert.assertEquals(base.count(2), 0);
    Assert.assertEquals(base.count(LNULL), 1);
  }

  @Test
  public void testLongContains() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 0, 5, 10);
    Assert.assertTrue(base.contains(0));
    Assert.assertFalse(base.contains(2));
    Assert.assertTrue(base.contains(LNULL));
  }

  @Test
  public void testLongReplace() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 0, 5, 10);
    assertEquals(base.replace(0, 1), LNULL, 1, 1, 5, 10);
    assertEquals(base.replace(0, LNULL), LNULL, LNULL, LNULL, 5, 10);
    assertEquals(base.replace(2, 1), LNULL, 0, 0, 5, 10);
    assertEquals(base.replace(5, LNULL), LNULL, 0, 0, LNULL, 10);
    assertEquals(base.replace(LNULL, 1), 1, 0, 0, 5, 10);
  }

  @Test
  public void testLongFilterSeries() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 0, 5, 10);
    BooleanSeries mod = DataFrame.toSeries(TRUE, TRUE, TRUE, FALSE, BNULL);
    assertEquals(base.filter(mod), LNULL, 0, 0, LNULL, LNULL);
  }

  @Test
  public void testLongFilterConditional() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 0, 5, 10);
    assertEquals(base.filter(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= 0 && values[0] <= 5;
      }
    }), LNULL, 0, 0, 5, LNULL);
  }

  @Test
  public void testStringOperationsSeries() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b", "c", "d");
    StringSeries mod = DataFrame.toSeries("A", "A", "b", "B", SNULL);

    assertEquals(base.concat(mod), SNULL, "aA", "bb", "cB", SNULL);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);
  }

  @Test
  public void testStringOperationsSeriesMisaligned() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b", "c", "d");
    StringSeries mod = DataFrame.toSeries("A", "A", "b", SNULL);

    try {
      base.concat(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.eq(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }
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
  public void testStringCount() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "a", "b", "A");
    Assert.assertEquals(base.count("a"), 2);
    Assert.assertEquals(base.count("d"), 0);
    Assert.assertEquals(base.count(SNULL), 1);
  }

  @Test
  public void testStringContains() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "a", "b", "A");
    Assert.assertTrue(base.contains("a"));
    Assert.assertFalse(base.contains(""));
    Assert.assertTrue(base.contains(SNULL));
  }

  @Test
  public void testStringReplace() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "a", "b", "A");
    assertEquals(base.replace("a", "AA"), SNULL, "AA", "AA", "b", "A");
    assertEquals(base.replace("a", SNULL), SNULL, SNULL, SNULL, "b", "A");
    assertEquals(base.replace("b", "B"), SNULL, "a", "a", "B", "A");
    assertEquals(base.replace("", "X"), SNULL, "a", "a", "b", "A");
    assertEquals(base.replace(SNULL, "N"), "N", "a", "a", "b", "A");
  }

  @Test
  public void testStringFilterSeries() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "a", "b", "A");
    BooleanSeries mod = DataFrame.toSeries(TRUE, TRUE, TRUE, FALSE, BNULL);
    assertEquals(base.filter(mod), SNULL, "a", "a", SNULL, SNULL);
  }

  @Test
  public void testStringFilterConditional() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "a", "b", "A");
    assertEquals(base.filter(new Series.StringConditional() {
      @Override
      public boolean apply(String... values) {
        return values[0].equals("a") || values[0].equals("A");
      }
    }), SNULL, "a", "a", SNULL, "A");
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
  public void testBooleanOperationsSeriesMisaligned() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    BooleanSeries mod = DataFrame.toSeries(BNULL, TRUE, FALSE, BNULL);

    try {
      base.and(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.or(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.xor(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.implies(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.eq(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }
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
  public void testBooleanCount() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    Assert.assertEquals(base.count(TRUE), 2);
    Assert.assertEquals(base.count(FALSE), 2);
    Assert.assertEquals(base.count(BNULL), 1);
  }

  @Test
  public void testBooleanContains() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    Assert.assertTrue(base.contains(TRUE));
    Assert.assertTrue(base.contains(FALSE));
    Assert.assertTrue(base.contains(BNULL));
  }

  @Test
  public void testBooleanReplace() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.replace(TRUE, FALSE), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.replace(TRUE, BNULL), BNULL, BNULL, FALSE, BNULL, FALSE);
    assertEquals(base.replace(FALSE, TRUE), BNULL, TRUE, TRUE, TRUE, TRUE);
    assertEquals(base.replace(FALSE, BNULL), BNULL, TRUE, BNULL, TRUE, BNULL);
    assertEquals(base.replace(BNULL, TRUE), TRUE, TRUE, FALSE, TRUE, FALSE);
  }

  @Test
  public void testBooleanFilterSeries() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    BooleanSeries mod = DataFrame.toSeries(TRUE, TRUE, TRUE, FALSE, BNULL);
    assertEquals(base.filter(mod), BNULL, TRUE, FALSE, BNULL, BNULL);
  }

  @Test
  public void testBooleanFilterConditional() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.filter(new Series.BooleanConditional() {
      @Override
      public boolean apply(boolean... values) {
        return values[0];
      }
    }), BNULL, TRUE, BNULL, TRUE, BNULL);
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

  private static void assertEquals(Series actual, Series expected) {
    Assert.assertEquals(actual, expected);
  }

  private static void assertEquals(DoubleSeries actual, double... expected) {
    assertEquals(actual.getDoubles().values(), expected);
  }

  private static void assertEquals(double[] actual, double... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      if(Double.isNaN(actual[i]) && Double.isNaN(expected[i]))
        continue;
      Assert.assertEquals(actual[i], expected[i], COMPARE_DOUBLE_DELTA, "index=" + i);
    }
  }

  private static void assertEquals(LongSeries actual, long... expected) {
    assertEquals(actual.getLongs().values(), expected);
  }

  private static void assertEquals(long[] actual, long... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEquals(StringSeries actual, String... expected) {
    assertEquals(actual.getStrings().values(), expected);
  }

  private static void assertEquals(String[] actual, String... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEquals(BooleanSeries actual, byte... expected) {
    assertEquals(actual.getBooleans().values(), expected);
  }

  private static void assertEquals(BooleanSeries actual, boolean... expected) {
    BooleanSeries s = actual.getBooleans();
    if(s.hasNull())
      Assert.fail("Encountered NULL when comparing against booleans");
    assertEquals(s.valuesBoolean(), expected);
  }

  private static void assertEquals(byte[] actual, byte... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEquals(boolean[] actual, boolean... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", actual.length, expected.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEmpty(Series series) {
    if(!series.isEmpty())
      Assert.fail("expected series to be empty, but wasn't");
  }
}
