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

package org.apache.pinot.thirdeye.dataframe;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
  private final static Object ONULL = ObjectSeries.NULL;

  private final static double COMPARE_DOUBLE_DELTA = 0.001;

  private final static long[] INDEX = new long[] { -1, 1, -2, 4, 3 };
  private final static double[] VALUES_DOUBLE = new double[] { -2.1, -0.1, 0.0, 0.5, 1.3 };
  private final static long[] VALUES_LONG = new long[] { -2, 1, 0, 1, 2 };
  private final static String[] VALUES_STRING = new String[] { "-2.3", "-1", "0.0", "0.5", "0.13e1" };
  private final static byte[] VALUES_BOOLEAN = new byte[] { 1, 1, 0, 1, 1 };
  private final static Object[] VALUES_OBJECT = new Object[] { "-2.3", 1L, 0L, 0.5d, true };

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
        .addSeries("boolean", VALUES_BOOLEAN)
        .addSeriesObjects("object", VALUES_OBJECT);
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

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSeriesNameNullFail() {
    df.addSeries(null, VALUES_DOUBLE);
  }

  @Test(dataProvider = "testSeriesNameExceptionalProvider")
  public void testSeriesNameExceptional(String name) {
    df.addSeries(name, VALUES_DOUBLE);
  }

  @Test(dataProvider = "testSeriesNameExceptionalProvider")
  public void testSeriesNameExceptionalExpression(String name) {
    df.addSeries(name, 1, 2, 3, 4, 5);
    DoubleSeries s = df.map(String.format("${%s}", name));
    assertEquals(s, 1, 2, 3, 4, 5);
  }

  @DataProvider(name = "testSeriesNameExceptionalProvider")
  public Object[][] testSeriesNameFailProvider() {
    return new Object[][] { { "" }, { "1a" }, { "a,b" }, { "a-b" }, { "a+b" }, { "a*b" }, { "a/b" }, { "a=b" }, { "a>b" } };
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
  public void testDoubleToObject() {
    assertEquals(DataFrame.toSeries(VALUES_DOUBLE).getObjects(), -2.1d, -0.1d, 0.0d, 0.5d, 1.3d);
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
  public void testLongToObject() {
    assertEquals(DataFrame.toSeries(VALUES_LONG).getObjects(), -2L, 1L, 0L, 1L, 2L);
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
  public void testBooleanToObject() {
    assertEquals(DataFrame.toSeries(VALUES_BOOLEAN).getObjects(), true, true, false, true, true);
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
  public void testStringToObject() {
    assertEquals(DataFrame.toSeries(VALUES_STRING).getObjects(), "-2.3", "-1", "0.0", "0.5", "0.13e1");
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
  public void testObjectBuilderNull() {
    assertEquals(ObjectSeries.builder().addValues((Object)null).build(), ONULL);
  }

  @Test
  public void testDataFrameBuilderDynamicTyping() {
    DataFrame.Builder builder = DataFrame.builder("double", "long", "string", "boolean", "object");

    builder.append(4.0d, 1, null, "true", 1);
    builder.append(null, 2, "2", "true", null);
    builder.append(2.3d, "", "hi", "false", true);
    builder.append(1.0d, 4, "4", "", new DataFrame());

    DataFrame df = builder.build();
    Assert.assertEquals(df.get("double").type(), Series.SeriesType.DOUBLE);
    Assert.assertEquals(df.get("long").type(), Series.SeriesType.LONG);
    Assert.assertEquals(df.get("string").type(), Series.SeriesType.STRING);
    Assert.assertEquals(df.get("boolean").type(), Series.SeriesType.BOOLEAN);
    Assert.assertEquals(df.get("object").type(), Series.SeriesType.OBJECT);

    assertEquals(df.getDoubles("double"), 4, DNULL, 2.3, 1);
    assertEquals(df.getLongs("long"), 1, 2, LNULL, 4);
    assertEquals(df.getStrings("string"), SNULL, "2", "hi", "4");
    assertEquals(df.getBooleans("boolean"), TRUE, TRUE, FALSE, BNULL);
    assertEquals(df.getObjects("object"), 1, null, true, new DataFrame());
  }

  @Test
  public void testDataFrameBuilderStaticTyping() {
    DataFrame.Builder builder = DataFrame.builder("double:DOUBLE", "long:LONG", "string:STRING", "boolean:BOOLEAN", "object:OBJECT");

    builder.append(4.0d, 1, null, "true", 1);
    builder.append(null, 2.34, "2", "1", 2);
    builder.append("2", "", "3", "false", 3);
    builder.append(1.0d, 4, "4", "", 4);

    DataFrame df = builder.build();
    Assert.assertEquals(df.get("double").type(), Series.SeriesType.DOUBLE);
    Assert.assertEquals(df.get("long").type(), Series.SeriesType.LONG);
    Assert.assertEquals(df.get("string").type(), Series.SeriesType.STRING);
    Assert.assertEquals(df.get("boolean").type(), Series.SeriesType.BOOLEAN);
    Assert.assertEquals(df.get("object").type(), Series.SeriesType.OBJECT);

    assertEquals(df.getDoubles("double"), 4, DNULL, 2, 1);
    assertEquals(df.getLongs("long"), 1, 2, LNULL, 4);
    assertEquals(df.getStrings("string"), SNULL, "2", "3", "4");
    assertEquals(df.getBooleans("boolean"), TRUE, TRUE, FALSE, BNULL);
    assertEquals(df.getObjects("object"), 1, 2, 3, 4);
  }

  @Test
  public void testDataFrameBuilderStaticTypingMultiple() {
    DataFrame df = DataFrame.builder("double:string:LONG").append(2.5d).build();
    Assert.assertTrue(df.contains("double:string"));
    Assert.assertEquals(df.get("double:string").type(), Series.SeriesType.LONG);
  }

  @Test
  public void testDataFrameBuilderStaticTypingUnknown() {
    DataFrame df = DataFrame.builder("double:1:2:string").append(1.1d).build();
    Assert.assertTrue(df.contains("double:1:2:string"));
    Assert.assertEquals(df.get("double:1:2:string").type(), Series.SeriesType.DOUBLE);
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
    assertEquals(s.getObjects(), 1.0d, ONULL, 2.0d);
  }

  @Test
  public void testLongNull() {
    Series s = DataFrame.toSeries(1, LNULL, 2);
    assertEquals(s.getDoubles(), 1.0, DNULL, 2.0);
    assertEquals(s.getLongs(), 1, LNULL, 2);
    assertEquals(s.getBooleans(), TRUE, BNULL, TRUE);
    assertEquals(s.getStrings(), "1", SNULL, "2");
    assertEquals(s.getObjects(), 1L, ONULL, 2L);
  }

  @Test
  public void testBooleanNull() {
    Series s = DataFrame.toSeries(TRUE, BNULL, FALSE);
    assertEquals(s.getDoubles(), 1.0, DNULL, 0.0);
    assertEquals(s.getLongs(), 1, LNULL, 0);
    assertEquals(s.getBooleans(), TRUE, BNULL, FALSE);
    assertEquals(s.getStrings(), "true", SNULL, "false");
    assertEquals(s.getObjects(), true, ONULL, false);
  }

  @Test
  public void testStringNull() {
    Series s = DataFrame.toSeries("1.0", SNULL, "2.0");
    assertEquals(s.getDoubles(), 1.0, DNULL, 2.0);
    assertEquals(s.getLongs(), 1, LNULL, 2);
    assertEquals(s.getBooleans(), TRUE, BNULL, TRUE);
    assertEquals(s.getStrings(), "1.0", SNULL, "2.0");
    assertEquals(s.getObjects(), "1.0", ONULL, "2.0");
  }

  @Test
  public void testObjectNull() {
    Series s = DataFrame.toSeriesObjects(1.0d, ONULL, "2.0");
    assertEquals(s.getDoubles(), 1.0, DNULL, 2.0);
    assertEquals(s.getLongs(), 1, LNULL, 2);
    assertEquals(s.getBooleans(), TRUE, BNULL, TRUE);
    assertEquals(s.getStrings(), "1.0", SNULL, "2.0");
    assertEquals(s.getObjects(), 1.0d, ONULL, "2.0");
  }

  @Test
  public void testEmpty() {
    Assert.assertEquals(DoubleSeries.empty(), DataFrame.toSeries(new double[0]));
    Assert.assertEquals(LongSeries.empty(), DataFrame.toSeries(new long[0]));
    Assert.assertEquals(StringSeries.empty(), DataFrame.toSeries(new String[0]));
    Assert.assertEquals(BooleanSeries.empty(), DataFrame.toSeries(new byte[0]));
    Assert.assertEquals(ObjectSeries.empty(), DataFrame.toSeriesObjects());
  }

  @Test
  public void testDoubleInfinity() {
    Series s = DataFrame.toSeries(DoubleSeries.POSITIVE_INFINITY, DoubleSeries.NEGATIVE_INFINITY);
    assertEquals(s.getDoubles(), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    assertEquals(s.getLongs(), LongSeries.MAX_VALUE, LongSeries.MIN_VALUE);
    assertEquals(s.getBooleans(), BooleanSeries.TRUE, BooleanSeries.TRUE);
    assertEquals(s.getStrings(), "Infinity", "-Infinity");
    assertEquals(s.getObjects(), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

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
  public void testSortObjects() {
    ObjectSeries in = DataFrame.toSeriesObjects("b", "a", "ba", "ab", "aa", ONULL);
    assertEquals(in.sorted(), ONULL, "a", "aa", "ab", "b", "ba");
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
    assertEquals(ndf.getObjects("object"), 1L, ONULL, true, "-2.3");
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
    assertEquals(df.getObjects("object"), 0L, "-2.3", 1L, true, 0.5d);
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
  public void testSortByObject() {
    df = df.addSeriesObjects("myseries", "b", "aa", "bb", "c", "a" );
    df = df.sortedBy("myseries");
    assertEquals(df.getLongs("index"), 3, 1, -1, -2, 4);
    assertEquals(df.getLongs("long"), 2, 1, -2, 0, 1);
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
    assertEquals(df.getObjects("object"), true, 0.5d, 0L, 1L, "-2.3");
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
  public void testAppendObjectLong() {
    Series s = df.get("object").append(df.get("long"));
    Assert.assertEquals(s.type(), Series.SeriesType.OBJECT);
    assertEquals(s.getObjects(), "-2.3", 1L, 0L, 0.5d, true, -2L, 1L, 0L, 1L, 2L);
  }

  @Test
  public void testLongGroupByIntervalEmpty() {
    Assert.assertTrue(LongSeries.empty().groupByInterval(1).isEmpty());
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
    Assert.assertTrue(LongSeries.empty().groupByCount(1).isEmpty());
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
    Assert.assertFalse(LongSeries.empty().groupByPartitions(1).isEmpty());
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
    Assert.assertTrue(LongSeries.empty().groupByValue().isEmpty());
  }

  @Test
  public void testLongGroupByValue() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 5);
    assertEquals(grouping.apply(0).getLongs(), LNULL);
    assertEquals(grouping.apply(1).getLongs(), 1);
    assertEquals(grouping.apply(2).getLongs(), 3, 3);
    assertEquals(grouping.apply(3).getLongs(), 4);
    assertEquals(grouping.apply(4).getLongs(), 5, 5, 5);
  }

  @Test
  public void testObjectGroupByValue() {
    ObjectSeries in = DataFrame.toSeriesObjects(3, new TestTuple(1, 2), "1.0", 3L, 3, "1.0", new TestTuple(1, 0), "1.0", new TestTuple(1, 2), ONULL);
    Grouping.SeriesGrouping grouping = in.groupByValue();

    Assert.assertEquals(grouping.size(), 6);
    assertEquals(grouping.apply(0).getObjects(), 3, 3);
    assertEquals(grouping.apply(1).getObjects(), new TestTuple(1, 2), new TestTuple(1, 2));
    assertEquals(grouping.apply(2).getObjects(), "1.0", "1.0", "1.0");
    assertEquals(grouping.apply(3).getObjects(), 3L);
    assertEquals(grouping.apply(4).getObjects(), new TestTuple(1, 0));
    assertEquals(grouping.apply(5).getObjects(), ONULL);
  }

  @Test
  public void testMultipleGroupByValue() {
    DataFrame df = new DataFrame();
    df.addSeries("a", DataFrame.toSeries(1, 1, 1, 2, 2));
    df.addSeries("b", DataFrame.toSeries(1.0, 2.0, 1.0, 2.0, 2.0));
    df.addSeries("c", DataFrame.toSeries("a", "b", "c", "d", "e"));

    Grouping.DataFrameGrouping grouping = df.groupByValue("b", "a");

    final Series keys = grouping.grouping.keys;
    Assert.assertEquals(keys.size(), 3);
    Assert.assertEquals(keys.getObject(0), DataFrame.Tuple.buildFrom(1.0d, 1L));
    Assert.assertEquals(keys.getObject(1), DataFrame.Tuple.buildFrom(2.0d, 1L));
    Assert.assertEquals(keys.getObject(2), DataFrame.Tuple.buildFrom(2.0d, 2L));

    Assert.assertEquals(grouping.size(), 3);
    assertEquals(grouping.apply("a", 0).getObjects(), 1L, 1L);
    assertEquals(grouping.apply("a", 1).getObjects(), 1L);
    assertEquals(grouping.apply("a", 2).getObjects(), 2L, 2L);

    Assert.assertEquals(grouping.size(), 3);
    assertEquals(grouping.apply("b", 0).getObjects(), 1.0d, 1.0d);
    assertEquals(grouping.apply("b", 1).getObjects(), 2.0d);
    assertEquals(grouping.apply("b", 2).getObjects(), 2.0d, 2.0d);
  }

  @Test
  public void testLongGroupByMovingWindow() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);

    Assert.assertEquals(grouping.size(), 8);
    assertEmpty(grouping.apply(0).getLongs());
    assertEmpty(grouping.apply(1).getLongs());
    assertEquals(grouping.apply(2).getLongs(), 3, 4, 5);
    assertEquals(grouping.apply(3).getLongs(), 4, 5, 5);
    assertEquals(grouping.apply(4).getLongs(), 5, 5, 3);
    assertEquals(grouping.apply(5).getLongs(), 5, 3, 1);
    assertEquals(grouping.apply(6).getLongs(), 3, 1, 5);
    assertEquals(grouping.apply(7).getLongs(), 1, 5, LNULL);
  }

  @Test
  public void testLongGroupByMovingWindowTooLarge() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(9);
    Assert.assertEquals(grouping.size(), 8);
    assertEmpty(grouping.apply(0).getLongs());
    assertEmpty(grouping.apply(1).getLongs());
    assertEmpty(grouping.apply(2).getLongs());
    assertEmpty(grouping.apply(3).getLongs());
    assertEmpty(grouping.apply(4).getLongs());
    assertEmpty(grouping.apply(5).getLongs());
    assertEmpty(grouping.apply(6).getLongs());
    assertEmpty(grouping.apply(7).getLongs());
  }

  @Test
  public void testLongGroupByMovingWindowAggregation() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);
    DataFrame out = grouping.aggregate(LongSeries.SUM);

    Assert.assertEquals(out.size(), 8);
    assertEquals(out.getLongs(Grouping.GROUP_KEY), 0, 1, 2, 3, 4, 5, 6, 7);
    assertEquals(out.getLongs(Grouping.GROUP_VALUE), LNULL, LNULL, 12, 14, 13, 9, 9, 6);
  }

  @Test
  public void testLongGroupByMovingWindowSum() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, 3, 1, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByMovingWindowEmptySum() {
    Grouping.SeriesGrouping grouping = LongSeries.empty().groupByMovingWindow(3);
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByMovingWindowNullSum() {
    LongSeries in = LongSeries.fillValues(3, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByMovingWindowLongNullSequenceSum() {
    LongSeries in = LongSeries.builder().fillValues(5, LNULL).addValues(1, 2, 3).build();
    Grouping.SeriesGrouping grouping = in.groupByMovingWindow(3);
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByExpandingWindow() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();

    Assert.assertEquals(grouping.size(), 5);
    assertEquals(grouping.apply(0).getLongs(), 3);
    assertEquals(grouping.apply(1).getLongs(), 3, 4);
    assertEquals(grouping.apply(2).getLongs(), 3, 4, 5);
    assertEquals(grouping.apply(3).getLongs(), 3, 4, 5, 5);
    assertEquals(grouping.apply(4).getLongs(), 3, 4, 5, 5, LNULL);
  }

  @Test
  public void testLongGroupByExpandingWindowAggregation() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();
    DataFrame out = grouping.aggregate(LongSeries.SUM);

    Assert.assertEquals(out.size(), 5);
    assertEquals(out.getLongs(Grouping.GROUP_KEY), 0, 1, 2, 3, 4);
    assertEquals(out.getLongs(Grouping.GROUP_VALUE), 3, 7, 12, 17, 17);
  }

  @Test
  public void testLongGroupByExpandingWindowSum() {
    LongSeries in = DataFrame.toSeries(3, 4, 5, 5, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByExpandingWindowEmptySum() {
    Grouping.SeriesGrouping grouping = LongSeries.empty().groupByExpandingWindow();
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByExpandingWindowNullSum() {
    LongSeries in = LongSeries.fillValues(3, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();
    Assert.assertEquals(grouping.sum(), grouping.aggregate(LongSeries.SUM));
  }

  @Test
  public void testLongGroupByExpandingWindowMin() {
    LongSeries in = DataFrame.toSeries(LNULL, 3, 4, 5, 2, 5, LNULL, LNULL, LNULL, 0);

    Grouping.SeriesGrouping gBoolean = in.getBooleans().groupByExpandingWindow();
    Assert.assertEquals(gBoolean.min(), gBoolean.aggregate(BooleanSeries.MIN));

    Grouping.SeriesGrouping gLong = in.getLongs().groupByExpandingWindow();
    Assert.assertEquals(gLong.min(), gLong.aggregate(LongSeries.MIN));

    Grouping.SeriesGrouping gDouble = in.getDoubles().groupByExpandingWindow();
    Assert.assertEquals(gDouble.min(), gDouble.aggregate(DoubleSeries.MIN));

    Grouping.SeriesGrouping gString = in.getStrings().groupByExpandingWindow();
    Assert.assertEquals(gString.min(), gString.aggregate(StringSeries.MIN));

    Grouping.SeriesGrouping gObject = in.getObjects().groupByExpandingWindow();
    Assert.assertEquals(gObject.min(), gObject.aggregate(ObjectSeries.MIN));
  }

  @Test
  public void testLongGroupByExpandingWindowEmptyMin() {
    Grouping.SeriesGrouping grouping = LongSeries.empty().groupByExpandingWindow();
    Assert.assertEquals(grouping.min(), grouping.aggregate(LongSeries.MIN));
  }

  @Test
  public void testLongGroupByExpandingWindowNullMin() {
    LongSeries in = LongSeries.fillValues(3, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();
    Assert.assertEquals(grouping.min(), grouping.aggregate(LongSeries.MIN));
  }

  @Test
  public void testLongGroupByExpandingWindowMax() {
    LongSeries in = DataFrame.toSeries(LNULL, 3, 4, 5, 2, 5, LNULL, LNULL, LNULL, 0);

    Grouping.SeriesGrouping gBoolean = in.getBooleans().groupByExpandingWindow();
    Assert.assertEquals(gBoolean.max(), gBoolean.aggregate(BooleanSeries.MAX));

    Grouping.SeriesGrouping gLong = in.getLongs().groupByExpandingWindow();
    Assert.assertEquals(gLong.max(), gLong.aggregate(LongSeries.MAX));

    Grouping.SeriesGrouping gDouble = in.getDoubles().groupByExpandingWindow();
    Assert.assertEquals(gDouble.max(), gDouble.aggregate(DoubleSeries.MAX));

    Grouping.SeriesGrouping gString = in.getStrings().groupByExpandingWindow();
    Assert.assertEquals(gString.max(), gString.aggregate(StringSeries.MAX));

    Grouping.SeriesGrouping gObject = in.getObjects().groupByExpandingWindow();
    Assert.assertEquals(gObject.max(), gObject.aggregate(ObjectSeries.MAX));
  }

  @Test
  public void testLongGroupByExpandingWindowEmptyMax() {
    Grouping.SeriesGrouping grouping = LongSeries.empty().groupByExpandingWindow();
    Assert.assertEquals(grouping.max(), grouping.aggregate(LongSeries.MAX));
  }

  @Test
  public void testLongGroupByExpandingWindowNullMax() {
    LongSeries in = LongSeries.fillValues(3, LNULL);
    Grouping.SeriesGrouping grouping = in.groupByExpandingWindow();
    Assert.assertEquals(grouping.max(), grouping.aggregate(LongSeries.MAX));
  }

  @Test
  public void testLongGroupByPeriod() {
    LongSeries in = LongSeries.buildFrom(
        parseDateMillis("2018-01-01 00:00:00 PST"),
        parseDateMillis("2018-01-01 03:00:00 PST"),
        parseDateMillis("2018-01-01 06:00:00 PST"),
        parseDateMillis("2018-01-01 09:00:00 PST"),
        parseDateMillis("2018-01-01 12:00:00 PST"),
        parseDateMillis("2018-01-01 15:00:00 PST"),
        parseDateMillis("2018-01-01 18:00:00 PST"),
        parseDateMillis("2018-01-01 21:00:00 PST"),
        parseDateMillis("2018-01-02 00:00:00 PST")
    );

    // timestamps with timezone
    Grouping.SeriesGrouping grouping = in.groupByPeriod(DateTimeZone.forID("America/Los_Angeles"), Period.hours(6));
    Assert.assertEquals(grouping.size(), 5);
    assertEquals(grouping.count().getValues().getLongs(), 2, 2, 2, 2, 1);

    // timestamps from (truncating) origin
    Grouping.SeriesGrouping groupingOffset = in.groupByPeriod(parseDate("2018-01-01 02:00:00 PST"), Period.hours(6));
    Assert.assertEquals(groupingOffset.size(), 4);
    assertEquals(groupingOffset.count().getValues().getLongs(), 2, 2, 2, 2);
  }

  @Test
  public void testLongGroupByPeriodDaylightSavingsTime() {
    LongSeries in = LongSeries.buildFrom(
        parseDateMillis("2018-03-11 01:30:00 PST"),
        parseDateMillis("2018-03-11 03:00:00 PDT"),
        parseDateMillis("2018-03-11 04:00:00 PDT"),
        parseDateMillis("2018-03-12 01:30:00 PDT"), // 1 day later
        parseDateMillis("2018-03-12 02:00:00 PDT"), // 23.5 hours later
        parseDateMillis("2018-03-12 03:00:00 PDT")
    );

    // 24 hours
    Grouping.SeriesGrouping groupingHours = in.groupByPeriod(parseDate("2018-03-11 01:30:00 PST"), Period.hours(24));
    Assert.assertEquals(groupingHours.size(), 2);
    assertEquals(groupingHours.count().getValues().getLongs(), 5, 1);

    // 1 day
    Grouping.SeriesGrouping groupingDays = in.groupByPeriod(parseDate("2018-03-11 01:30:00 PST"), Period.days(1));
    Assert.assertEquals(groupingDays.size(), 2);
    assertEquals(groupingDays.count().getValues().getLongs(), 3, 3);
  }

  @Test
  public void testBooleanGroupByValueEmpty() {
    Assert.assertTrue(BooleanSeries.empty().groupByValue().isEmpty());
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
    assertEquals(DoubleSeries.empty().sum(), DNULL);
  }

  @Test
  public void testDoubleAggregateWithNull() {
    DoubleSeries s = DataFrame.toSeries(1.0, 2.0, DNULL, 4.0);
    assertEquals(s.sum(), 7.0);
    assertEquals(s.fillNull().sum(), 7.0);
    assertEquals(s.dropNull().sum(), 7.0);
  }

  @Test
  public void testLongAggregateWithNull() {
    LongSeries s = DataFrame.toSeries(1, 2, LNULL, 4);
    assertEquals(s.sum(), 7);
    assertEquals(s.fillNull().sum(), 7);
    assertEquals(s.dropNull().sum(), 7);
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
  public void testObjectAggregateWithNull() {
    ObjectSeries s = DataFrame.toSeriesObjects("a", "b", ONULL, 1L, true);
    assertEquals(s.dropNull(), "a", "b", 1L, true);
    Assert.assertEquals(s.aggregate(ObjectSeries.TOSTRING).value(), "[a, b, 1, true]");
  }

  @Test
  public void testDataFrameGroupBy() {
    Grouping.DataFrameGrouping grouping = df.groupByValue("boolean");

    DoubleSeries ds = grouping.aggregate("double", DoubleSeries.SUM).getValues().getDoubles();
    assertEquals(ds, 0.0, -0.4);

    LongSeries ls = grouping.aggregate("long", LongSeries.SUM).getValues().getLongs();
    assertEquals(ls, 0, 2);

    StringSeries ss = grouping.aggregate("string", StringSeries.CONCAT).getValues().getStrings();
    assertEquals(ss, "0.0", "-2.3-10.50.13e1");

    ObjectSeries os = grouping.aggregate("object", ObjectSeries.TOSTRING).getValues().getObjects();
    assertEquals(os, "[0]", "[-2.3, 1, 0.5, true]");
  }

  @Test
  public void testGroupingMultiColumn() {
    DataFrame out = df.groupByInterval("index", 2)
        .aggregate(new String[] { "double", "boolean", "long", "string", "object" },
            new Series.Function[] { DoubleSeries.SUM, LongSeries.MIN, LongSeries.MAX, StringSeries.LAST, StringSeries.CONCAT });

    Assert.assertEquals(out.getSeriesNames().size(), 6);
    Assert.assertEquals(out.size(), 4);
    assertEquals(out.getDoubles("double"), -2.1, -0.1, 1.3, 0.5);
    assertEquals(out.getLongs("boolean"), 0, 1, 1, 1);
    assertEquals(out.getLongs("long"), 0, 1, 2, 1);
    assertEquals(out.getStrings("string"), "0.0", "-1", "0.13e1", "0.5");
    assertEquals(out.getStrings("object"), "-2.30", "1", "true", "0.5");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGroupingMultiColumnFailReuse() {
    df.groupByInterval("index", 2)
        .aggregate(new String[] { "double", "double" },
            new Series.Function[] { DoubleSeries.SUM, LongSeries.MIN });
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGroupingMultiColumnFailReuseIndex() {
    df.groupByInterval("index", 2)
        .aggregate(new String[] { "index" }, new Series.Function[] { DoubleSeries.SUM });
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGroupingMultiColumnFailMisaligned() {
    df.groupByInterval("index", 2)
        .aggregate(new String[] { "double", "long" },
            new Series.Function[] { DoubleSeries.SUM, LongSeries.MIN, LongSeries.MAX });
  }

  @Test
  public void testGroupingMultiColumnExpression() {
    DataFrame out = df.groupByInterval("index", 2)
        .aggregate("double:sum", "boolean:min", "long:max", "string:last", "object:first");

    Assert.assertEquals(out.getSeriesNames().size(), 6);
    Assert.assertEquals(out.size(), 4);
    assertEquals(out.getDoubles("double"), -2.1, -0.1, 1.3, 0.5);
    assertEquals(out.getBooleans("boolean"), FALSE, TRUE, TRUE, TRUE);
    assertEquals(out.getLongs("long"), 0, 1, 2, 1);
    assertEquals(out.getStrings("string"), "0.0", "-1", "0.13e1", "0.5");
    assertEquals(out.getObjects("object"), "-2.3", 1L, true, 0.5d);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGroupingMultiColumnExpressionFailReuse() {
    df.groupByInterval("index", 2)
        .aggregate("double:sum", "double:min");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGroupingMultiColumnExpressionFailReuseIndex() {
    df.groupByInterval("index", 2).aggregate("index:sum");
  }

  @Test
  public void testGroupingMultiColumnExpressionRename() {
    DataFrame out = df.groupByInterval("index", 2)
        .aggregate("double:sum:a", "double:max:b");

    Assert.assertEquals(out.getSeriesNames().size(), 3);
    Assert.assertEquals(out.size(), 4);
    assertEquals(out.getDoubles("a"), -2.1, -0.1, 1.3, 0.5);
    assertEquals(out.getDoubles("b"), 0.0, -0.1, 1.3, 0.5);
  }

  @Test
  public void testGroupingExpressions() {
    DataFrame out = new DataFrame()
        .addSeries("a", 1, 2, 3, 4, 5)
        .groupByCount(3)
        .aggregate("a:sum:sum", "a:product:product",
            "a:min:min", "a:max:max", "a:first:first", "a:last:last",
            "a:mean:mean", "a:median:median", "a:std:std");

    Assert.assertEquals(out.getSeriesNames().size(), 10);
    Assert.assertEquals(out.size(), 2);
    assertEquals(out.getIndexSingleton().getLongs(), 0, 1);
    assertEquals(out.getLongs("sum"), 6, 9);
    assertEquals(out.getLongs("product"), 6, 20);
    assertEquals(out.getLongs("min"), 1, 4);
    assertEquals(out.getLongs("max"), 3, 5);
    assertEquals(out.getLongs("first"), 1, 4);
    assertEquals(out.getLongs("last"), 3, 5);
    assertEquals(out.getDoubles("mean"), 2, 4.5);
    assertEquals(out.getDoubles("median"), 2, 4.5);
    assertEquals(out.getDoubles("std"), 1, 0.70710);
  }

  @Test
  public void testGroupingExpressionDefaultOperation() {
    DataFrame out = new DataFrame()
        .addSeries("a", 1, 1, 2, 2, 2)
        .addSeries("b", "1", "1", "2", "2", "2")
        .groupByValue("a", "b")
        .aggregate("a", "a:first:first", "b");

    Assert.assertEquals(out.getSeriesNames().size(), 4);
    Assert.assertEquals(out.size(), 2);
    Assert.assertEquals(out.getIndexSingleton().type(), Series.SeriesType.OBJECT);
    assertEquals(out.getLongs("a"), 1, 2);
    assertEquals(out.getLongs("first"), 1, 2);
    assertEquals(out.getStrings("b"), "1", "2");
  }

  @Test
  public void testStableMultiSortDoubleLong() {
    DataFrame mydf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7, 8 })
        .addSeries("double", 1.0, 1.0, 2.0, 2.0, 1.0, 1.0, 2.0, 2.0)
        .addSeries("long", 2, 2, 2, 2, 1, 1, 1, 1);

    DataFrame sdfa = mydf.sortedBy("double", "long");
    assertEquals(sdfa.getLongs("index"), 5, 6, 1, 2, 7, 8, 3, 4);

    DataFrame sdfb = mydf.sortedBy("long", "double");
    assertEquals(sdfb.getLongs("index"), 5, 6, 7, 8, 1, 2, 3, 4);
  }

  @Test
  public void testStableMultiSortStringBoolean() {
    DataFrame mydf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7, 8 })
        .addSeries("string", "a", "a", "b", "b", "a", "a", "b", "b")
        .addSeries("boolean", true, true, true, true, false, false, false, false);

    DataFrame sdfa = mydf.sortedBy("string", "boolean");
    assertEquals(sdfa.getLongs("index"), 5, 6, 1, 2, 7, 8, 3, 4);

    DataFrame sdfb = mydf.sortedBy("boolean", "string");
    assertEquals(sdfb.getLongs("index"), 5, 6, 7, 8, 1, 2, 3, 4);
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
    assertEquals(df.getObjects("object"), "-2.3", 0L, 0.5d);
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

    Assert.assertNotSame(df.getDoubles("double"), ndf.getDoubles("double"));
    Assert.assertNotSame(df.getLongs("long"), ndf.getLongs("long"));
    Assert.assertNotSame(df.getStrings("string"), ndf.getStrings("string"));
    Assert.assertNotSame(df.getBooleans("boolean"), ndf.getBooleans("boolean"));
    Assert.assertNotSame(df.getObjects("object"), ndf.getObjects("object"));
  }

  @Test
  public void testSingleValueAccessor() {
    Series s = LongSeries.buildFrom(1);
    Assert.assertEquals(s.doubleValue(), 1.0d);
    Assert.assertEquals(s.longValue(), 1);
    Assert.assertEquals(s.stringValue(), "1");
    Assert.assertEquals(s.booleanValue(), true);
    Assert.assertEquals(s.objectValue(), 1L);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDoubleValueFailNull() {
    LongSeries.buildFrom(LNULL).doubleValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testLongValueFailNull() {
    LongSeries.buildFrom(LNULL).longValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testStringValueFailNull() {
    LongSeries.buildFrom(LNULL).stringValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testBooleanValueFailNull() {
    LongSeries.buildFrom(LNULL).booleanValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testObjectValueFailNull() {
    LongSeries.buildFrom(LNULL).objectValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDoubleValueFailMultiple() {
    LongSeries.buildFrom(1, 2, 3).doubleValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testLongValueFailMultiple() {
    LongSeries.buildFrom(1, 2, 3).longValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testStringValueFailMultiple() {
    LongSeries.buildFrom(1, 2, 3).stringValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testBooleanValueFailMultiple() {
    LongSeries.buildFrom(1, 2, 3).booleanValue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testObjectValueFailMultiple() {
    LongSeries.buildFrom(1, 2, 3).objectValue();
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
    Assert.assertTrue(DoubleSeries.isNull(s.sum().value()));
    Assert.assertTrue(DoubleSeries.isNull(s.min().value()));
    Assert.assertTrue(DoubleSeries.isNull(s.max().value()));
    Assert.assertTrue(DoubleSeries.isNull(s.mean().value()));
    Assert.assertTrue(DoubleSeries.isNull(s.std().value()));

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
    Assert.assertTrue(LongSeries.isNull(s.sum().value()));
    Assert.assertTrue(LongSeries.isNull(s.min().value()));
    Assert.assertTrue(LongSeries.isNull(s.max().value()));

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
  public void testObjectUnique() {
    ObjectSeries s1 = DataFrame.toSeriesObjects();
    assertEmpty(s1.unique());

    ObjectSeries s2 = DataFrame.toSeriesObjects("1", 1, 1, 1L, "1");
    assertEquals(s2.unique(), "1", 1, 1L);

    ObjectSeries s3 = DataFrame.toSeriesObjects(1, tup(1, 2), 2, 1, tup(1, 2), tup(3, 3));
    assertEquals(s3.unique(), 1, tup(1, 2), 2, tup(3, 3));
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
    DataFrame mdf = new DataFrame(new long[] { 1, 2, 3, 4, 5, 6, 7 })
        .addSeries("double", 1.0, 2.0, DNULL, 4.0, 5.0, 6.0, 7.0)
        .addSeries("long", LNULL, 2, 3, 4, 5, 6, 7)
        .addSeries("string", "1.0", "2", "bbb", "true", SNULL, "aaa", "aab")
        .addSeries("boolean", true, true, false, false, false, false, true)
        .addSeriesObjects("object", true, 2L, 3.0d, 4.0d, "5", "six", ONULL);

    DataFrame ddf = mdf.dropNull();
    Assert.assertEquals(ddf.size(), 3);
    assertEquals(ddf.getLongs("index"), 2, 4, 6);
    assertEquals(ddf.getDoubles("double"), 2.0, 4.0, 6.0);
    assertEquals(ddf.getLongs("long"), 2, 4, 6);
    assertEquals(ddf.getStrings("string"), "2", "true", "aaa");
    assertEquals(ddf.getBooleans("boolean"), TRUE, FALSE, FALSE);
    assertEquals(ddf.getObjects("object"), 2L, 4.0d, "six");
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
        .addSeries("boolean", TRUE, TRUE, FALSE)
        .addSeries("boolean_null", TRUE, FALSE, BNULL)
        .addSeriesObjects("object", true, 1L, "a")
        .addSeriesObjects("object_null", 2L, ONULL, false);

    DataFrame ddf = mdf.dropNullColumns();
    Assert.assertEquals(ddf.size(), 3);
    Assert.assertEquals(new HashSet<>(ddf.getSeriesNames()), new HashSet<>(Arrays.asList("double", "long", "string", "boolean", "object")));
  }

  @Test
  public void testMapExpression() {
    DoubleSeries s = df.map("(${double} * 2 + ${long} + ${boolean}) / 2");
    assertEquals(s, -2.6, 0.9, 0.0, 1.5, 2.8);
  }

  @Test
  public void testMapExpressionNull() {
    DataFrame mdf = new DataFrame(VALUES_LONG)
        .addSeries("null", 1.0, 1.0, DNULL, 1.0, 1.0);
    DoubleSeries out = mdf.map("${null} + 1");
    assertEquals(out, 2.0, 2.0, DNULL, 2.0, 2.0);
  }

  @Test
  public void testMapExpressionOtherNullPass() {
    DataFrame mdf = new DataFrame(VALUES_LONG)
        .addSeries("null", 1.0, 1.0, DNULL, 1.0, 1.0)
        .addSeries("notnull", 1.0, 1.0, 1.0, 1.0, 1.0);
    mdf.map("${notnull} + 1");
  }

  @Test
  public void testMapExpressionWithNull() {
    DataFrame mdf = new DataFrame(VALUES_LONG)
        .addSeries("null", 1.0, 1.0, DNULL, 1.0, 1.0);
    DoubleSeries s = mdf.map("${null} + 1");
    assertEquals(s, 2.0, 2.0, DNULL, 2.0, 2.0);
  }

  @Test
  public void testMapFunctionGeneric() {
    DataFrame df = new DataFrame()
        .addSeries("a", 0, 1, 2, 3, 4)
        .addSeries("b", 1, 2, 3, 4, 5)
        .addSeries("c", 2, 3, 4, 5, 6)
        .addSeries("d", 3, 4, 5, 6, 7);

    String[] names = new String[] { "a", "b", "c", "d" };
    assertEquals(df.map(DoubleSeries.SUM, names), 6, 10, 14, 18, 22);
    assertEquals(df.map(LongSeries.SUM, names), 6, 10, 14, 18, 22);
    assertEquals(df.map(StringSeries.CONCAT, names), "0123", "1234", "2345", "3456", "4567");
    assertEquals(df.map(BooleanSeries.ALL_TRUE, names), FALSE, TRUE, TRUE, TRUE, TRUE);
    assertEquals(df.map(ObjectSeries.TOSTRING, names), "[0, 1, 2, 3]", "[1, 2, 3, 4]", "[2, 3, 4, 5]", "[3, 4, 5, 6]", "[4, 5, 6, 7]");
  }

  @Test
  public void testSeriesEquals() {
    Assert.assertTrue(DataFrame.toSeries(0.0, 3.0, 4.0).equals(DataFrame.toSeries(0.0, 3.0, 4.0)));
    Assert.assertTrue(DataFrame.toSeries(0, 3, 4).equals(DataFrame.toSeries(0, 3, 4)));
    Assert.assertTrue(DataFrame.toSeries(false, true, true).equals(DataFrame.toSeries(false, true, true)));
    Assert.assertTrue(DataFrame.toSeries("1", "3", "4").equals(DataFrame.toSeries("1", "3", "4")));
    Assert.assertTrue(DataFrame.toSeriesObjects("1", 3L, true).equals(DataFrame.toSeriesObjects("1", 3L, true)));

    Assert.assertFalse(DataFrame.toSeries(0.0, 3.0, 4.0).equals(DataFrame.toSeries(0, 3, 4)));
    Assert.assertFalse(DataFrame.toSeries(0, 3, 4).equals(DataFrame.toSeries(0.0, 3.0, 4.0)));
    Assert.assertFalse(DataFrame.toSeries(false, true, true).equals(DataFrame.toSeries("0", "1", "1")));
    Assert.assertFalse(DataFrame.toSeries("1", "3", "4").equals(DataFrame.toSeries(1, 3, 4)));
    Assert.assertFalse(DataFrame.toSeriesObjects("1", 3L, false).equals(DataFrame.toSeries(1, 3, 0)));

    Assert.assertTrue(DataFrame.toSeries(0.0, 3.0, 4.0).equals(DataFrame.toSeries(0, 3, 4).getDoubles()));
    Assert.assertTrue(DataFrame.toSeries(0, 3, 4).equals(DataFrame.toSeries(0.0, 3.0, 4.0).getLongs()));
    Assert.assertTrue(DataFrame.toSeries(false, true, true).equals(DataFrame.toSeries("0", "1", "1").getBooleans()));
    Assert.assertTrue(DataFrame.toSeries("1", "3", "4").equals(DataFrame.toSeries(1, 3, 4).getStrings()));
    Assert.assertTrue(DataFrame.toSeriesObjects("1", "3", "4").equals(DataFrame.toSeries("1", "3", "4").getObjects()));
  }

  @Test
  public void testLongHashJoinOuter() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5, 4, 3, 3, 0);

    Series.JoinPairs pairs = Series.hashJoinOuter(new Series[] { sLeft }, new Series[] { sRight });

    Assert.assertEquals(pairs.size(), 7);
    Assert.assertEquals(pairs.get(0), 0x0000000000000001L);
    Assert.assertEquals(pairs.get(1), 0x0000000100000002L);
    Assert.assertEquals(pairs.get(2), 0x0000000100000003L);
    Assert.assertEquals(pairs.get(3), 0x00000002FFFFFFFFL);
    Assert.assertEquals(pairs.get(4), 0x00000003FFFFFFFFL);
    Assert.assertEquals(pairs.get(5), 0xFFFFFFFF00000000L);
    Assert.assertEquals(pairs.get(6), 0xFFFFFFFF00000004L);
  }

  @Test
  public void testLongHashJoinOuterMultiple() {
    Series sLeft1 = DataFrame.toSeries(1, 1, 1, 2, 2);
    Series sLeft2 = DataFrame.toSeries("a", "b", "a", "b", "a");
    Series sRight1 = DataFrame.toSeries(1.0, 1.0, 1.0, 2.0, 2.3);
    Series sRight2 = DataFrame.toSeries("a", "a", "a", "b", "b");

    Series.JoinPairs pairs = Series.hashJoinOuter(new Series[] { sLeft1, sLeft2 }, new Series[] { sRight1, sRight2 });

    Assert.assertEquals(pairs.size(), 10);
    Assert.assertEquals(pairs.get(0), 0x0000000000000000L);
    Assert.assertEquals(pairs.get(1), 0x0000000000000001L);
    Assert.assertEquals(pairs.get(2), 0x0000000000000002L);
    Assert.assertEquals(pairs.get(3), 0x00000001FFFFFFFFL);
    Assert.assertEquals(pairs.get(4), 0x0000000200000000L);
    Assert.assertEquals(pairs.get(5), 0x0000000200000001L);
    Assert.assertEquals(pairs.get(6), 0x0000000200000002L);
    Assert.assertEquals(pairs.get(7), 0x0000000300000003L);
    Assert.assertEquals(pairs.get(8), 0x0000000300000004L); // (!) 2.3 -> 2
    Assert.assertEquals(pairs.get(9), 0x00000004FFFFFFFFL);
  }

  @Test
  public void testLongDoubleJoin() {
    Series sLeft = DataFrame.toSeries(4, 3, 1, 2);
    Series sRight = DataFrame.toSeries(5.0, 4.0, 3.0, 3.0, 0.0);

    Series.JoinPairs pairs = Series.hashJoinOuter(new Series[] { sLeft }, new Series[] { sRight });

    Assert.assertEquals(pairs.size(), 7);
    Assert.assertEquals(pairs.get(0), 0x0000000000000001L);
    Assert.assertEquals(pairs.get(1), 0x0000000100000002L);
    Assert.assertEquals(pairs.get(2), 0x0000000100000003L);
    Assert.assertEquals(pairs.get(3), 0x00000002FFFFFFFFL);
    Assert.assertEquals(pairs.get(4), 0x00000003FFFFFFFFL);
    Assert.assertEquals(pairs.get(5), 0xFFFFFFFF00000000L);
    Assert.assertEquals(pairs.get(6), 0xFFFFFFFF00000004L);
  }

  @Test
  public void testStringJoin() {
    Series sLeft = DataFrame.toSeries("4", "3", "1", "2");
    Series sRight = DataFrame.toSeries("5", "4", "3", "3", "0");

    Series.JoinPairs pairs = Series.hashJoinOuter(new Series[] { sLeft }, new Series[] { sRight });

    Assert.assertEquals(pairs.size(), 7);
    Assert.assertEquals(pairs.get(0), 0x0000000000000001L);
    Assert.assertEquals(pairs.get(1), 0x0000000100000002L);
    Assert.assertEquals(pairs.get(2), 0x0000000100000003L);
    Assert.assertEquals(pairs.get(3), 0x00000002FFFFFFFFL);
    Assert.assertEquals(pairs.get(4), 0x00000003FFFFFFFFL);
    Assert.assertEquals(pairs.get(5), 0xFFFFFFFF00000000L);
    Assert.assertEquals(pairs.get(6), 0xFFFFFFFF00000004L);
  }

  @Test
  public void testBooleanJoin() {
    Series sLeft = DataFrame.toSeries(true, false, false, true);
    Series sRight = DataFrame.toSeries(false, true, true, false);

    Series.JoinPairs pairs = Series.hashJoinOuter(new Series[] { sLeft }, new Series[] { sRight });

    Assert.assertEquals(pairs.size(), 8);
    Assert.assertEquals(pairs.get(0), 0x0000000000000001L);
    Assert.assertEquals(pairs.get(1), 0x0000000000000002L);
    Assert.assertEquals(pairs.get(2), 0x0000000100000000L);
    Assert.assertEquals(pairs.get(3), 0x0000000100000003L);
    Assert.assertEquals(pairs.get(4), 0x0000000200000000L);
    Assert.assertEquals(pairs.get(5), 0x0000000200000003L);
    Assert.assertEquals(pairs.get(6), 0x0000000300000001L);
    Assert.assertEquals(pairs.get(7), 0x0000000300000002L);
  }

  @Test
  public void testJoinInner() {
    DataFrame left = new DataFrame()
        .addSeries("leftKey", 4, 2, 1, 3)
        .addSeries("leftValue", "a", "d", "c", "b");

    DataFrame right = new DataFrame()
        .addSeries("rightKey", 5.0, 2.0, 1.0, 3.0, 1.0, 0.0)
        .addSeries("rightValue", "v", "z", "w", "x", "y", "u");

    DataFrame joined = left.joinInner(right, new String[] { "leftKey" }, new String[] { "rightKey" });

    Assert.assertEquals(joined.getSeriesNames().size(), 3);
    Assert.assertEquals(joined.size(), 4);
    Assert.assertEquals(joined.get("leftKey").type(), Series.SeriesType.LONG);
    Assert.assertEquals(joined.get("leftValue").type(), Series.SeriesType.STRING);
    Assert.assertEquals(joined.get("rightValue").type(), Series.SeriesType.STRING);
    assertEquals(joined.getLongs("leftKey"), 2, 1, 1, 3);
    assertEquals(joined.getStrings("leftValue"), "d", "c", "c", "b");
    assertEquals(joined.getStrings("rightValue"), "z", "w", "y", "x");
  }

  @Test
  public void testJoinOuter() {
    DataFrame left = new DataFrame()
        .addSeries("leftKey", 4, 2, 1, 3)
        .addSeries("leftValue", "a", "d", "c", "b");

    DataFrame right = new DataFrame()
        .addSeries("rightKey", 5.0, 2.0, 1.0, 3.0, 1.0, 0.0)
        .addSeries("rightValue", "v", "z", "w", "x", "y", "u");

    DataFrame joined = left.joinOuter(right, new String[] { "leftKey" }, new String[] { "rightKey" });

    Assert.assertEquals(joined.getSeriesNames().size(), 3);
    Assert.assertEquals(joined.size(), 7);
    Assert.assertEquals(joined.get("leftKey").type(), Series.SeriesType.LONG);
    Assert.assertEquals(joined.get("leftValue").type(), Series.SeriesType.STRING);
    Assert.assertEquals(joined.get("rightValue").type(), Series.SeriesType.STRING);
    assertEquals(joined.getLongs("leftKey"), 4, 2, 1, 1, 3, 5, 0);
    assertEquals(joined.getStrings("leftValue"), "a", "d", "c", "c", "b", SNULL, SNULL);
    assertEquals(joined.getStrings("rightValue"), SNULL, "z", "w", "y", "x", "v", "u");
  }

  @Test
  public void testJoinOuterEmpty() {
    DataFrame left = new DataFrame()
        .addSeries("leftKey", 4, 2, 1, 3)
        .addSeries("leftValue", "a", "d", "c", "b");

    DataFrame right = new DataFrame()
        .addSeries("rightKey", DoubleSeries.empty())
        .addSeries("rightValue", ObjectSeries.empty());

    DataFrame joined = left.joinOuter(right, new String[] { "leftKey" }, new String[] { "rightKey" });

    Assert.assertEquals(joined.getSeriesNames().size(), 3);
    Assert.assertEquals(joined.size(), 4);
    Assert.assertEquals(joined.get("leftKey").type(), Series.SeriesType.LONG);
    Assert.assertEquals(joined.get("leftValue").type(), Series.SeriesType.STRING);
    Assert.assertEquals(joined.get("rightValue").type(), Series.SeriesType.OBJECT);
    assertEquals(joined.getLongs("leftKey"), 4, 2, 1, 3);
    assertEquals(joined.getStrings("leftValue"), "a", "d", "c", "b");
    assertEquals(joined.getObjects("rightValue"), ONULL, ONULL, ONULL, ONULL);
  }

  @Test
  public void testJoinOuterObject() {
    DataFrame left = new DataFrame()
        .addSeriesObjects("key", DataFrame.Tuple.buildFrom("a", "a"), DataFrame.Tuple.buildFrom("a", "b"))
        .addSeries("leftValue", 1, 2);

    DataFrame right = new DataFrame()
        .addSeriesObjects("key", DataFrame.Tuple.buildFrom("b", "b"), DataFrame.Tuple.buildFrom("a", "b"))
        .addSeries("rightValue", "3", "4");

    DataFrame joined = left.joinOuter(right, "key");

    Assert.assertEquals(joined.size(), 3);
    Assert.assertEquals(joined.get("key").type(), Series.SeriesType.OBJECT);
    Assert.assertEquals(joined.get("leftValue").type(), Series.SeriesType.LONG);
    Assert.assertEquals(joined.get("rightValue").type(), Series.SeriesType.STRING);
    assertEquals(joined.getObjects("key"),DataFrame.Tuple.buildFrom("a", "a"),
        DataFrame.Tuple.buildFrom("a", "b"), DataFrame.Tuple.buildFrom("b", "b"));
    assertEquals(joined.getLongs("leftValue"), 1, 2, LNULL);
    assertEquals(joined.getStrings("rightValue"), SNULL, "4","3");
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

    DataFrame df = left.joinInner(right, new String[] { "name" }, new String[] { "key" });

    Assert.assertEquals(df.getSeriesNames().size(), 1);
    Assert.assertTrue(df.contains("name"));
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

    Assert.assertEquals(dfLeft.getIndexSingleton(), dfRight.getIndexSingleton());

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
  public void testJoinMultiIndex() {
    DataFrame dfLeft = new DataFrame()
        .addSeries("A", 1, 1, 1, 2, 2)
        .addSeries("B", "a", "b", "c", "d", "e")
        .addSeries("V", 0, 1, 2, 3, 4)
        .setIndex("A", "B");

    DataFrame dfRight = new DataFrame()
        .addSeries("A", 1, 1, 2, 2, 2)
        .addSeries("B", "a", "b", "c", "d", "e")
        .addSeries("W", "a", "aa", "aaa", "aaaa", "aaaaa")
        .setIndex("A", "B");

    DataFrame joined = dfLeft.joinOuter(dfRight).sortedByIndex();

    Assert.assertEquals(joined.size(), 6);
    Assert.assertEquals(joined.getSeriesNames().size(), 4);
    Assert.assertEquals(joined.getIndexNames().size(), 2);
    assertEquals(joined.getLongs("A"), 1, 1, 1, 2, 2, 2);
    assertEquals(joined.getStrings("B"), "a", "b", "c", "c", "d", "e");
    assertEquals(joined.getLongs("V"), 0, 1, 2, LNULL, 3, 4);
    assertEquals(joined.getStrings("W"), "a", "aa", SNULL, "aaa", "aaaa", "aaaaa");
  }

  @Test
  public void testAddSeriesMultiIndex() {
    DataFrame dfLeft = new DataFrame()
        .addSeries("A", 1, 1, 1, 2, 2)
        .addSeries("B", "a", "b", "c", "d", "e")
        .addSeries("V", 0, 1, 2, 3, 4)
        .setIndex("A", "B");

    DataFrame dfRight = new DataFrame()
        .addSeries("A", 1, 1, 2, 2, 2)
        .addSeries("B", "a", "b", "c", "d", "e")
        .addSeries("W", "a", "aa", "aaa", "aaaa", "aaaaa")
        .setIndex("A", "B");

    dfLeft.addSeries(dfRight, "W");

    Assert.assertEquals(dfLeft.size(), 5);
    Assert.assertEquals(dfLeft.getSeriesNames().size(), 4);
    Assert.assertEquals(dfLeft.getIndexNames().size(), 2);
    assertEquals(dfLeft.getStrings("W"), "a", "aa", SNULL, "aaaa", "aaaaa");
  }

  @Test
  public void testJoinIndexRetention() {
    DataFrame dfLeft = new DataFrame()
        .addSeries("a", 0)
        .addSeries("b", 0)
        .setIndex("a");

    DataFrame dfRight = new DataFrame()
        .addSeries("a", 0)
        .addSeries("b", 0)
        .setIndex("a", "b");

    DataFrame joined = dfLeft.joinOuter(dfRight, Arrays.asList("a", "b"));

    Assert.assertEquals(joined.getSeriesNames().size(), 2);
    Assert.assertEquals(joined.getIndexNames().size(), 1);
    Assert.assertEquals(joined.getIndexNames().get(0), "a");
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
  public void testObjectFunctionConversion() {
    Series out = df.map(new Series.ObjectFunction() {
      @Override
      public Object apply(Object... values) {
        return values[0];
      }
    }, "long");
    Assert.assertEquals(out.type(), Series.SeriesType.OBJECT);
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
  public void testObjectConditionalConversion() {
    Series out = df.map(new Series.ObjectConditional() {
      @Override
      public boolean apply(Object... values) {
        return values[0] != null;
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
    Assert.assertEquals(df.getIndexNames().size(), 0);
    Assert.assertEquals(df.getIndexSeries().size(), 0);
    df.getIndexSingleton();
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
    Assert.assertTrue(df.copy().hasIndex());
    Assert.assertEquals(df.copy().getIndexSeries().size(), 1);
    Assert.assertEquals(df.copy().getIndexNames().size(), 1);
    Assert.assertEquals(df.copy().getIndexNames().get(0), "test");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIndexSetInvalid() {
    DataFrame df = new DataFrame(0);
    df.setIndex("test");
  }

  @Test
  public void testIndexRename() {
    DataFrame df = new DataFrame(0);
    Series index = df.getIndexSeries().get(0);
    df.renameSeries(df.getIndexNames().get(0), "test");
    df.addSeries(DataFrame.COLUMN_INDEX_DEFAULT, DataFrame.toSeries(new double[0]));
    Assert.assertEquals(df.getIndexNames().get(0), "test");
    Assert.assertEquals(df.getIndexSeries().get(0), index);
  }

  @Test
  public void testDoubleNormalize() {
    DoubleSeries s = DataFrame.toSeries(1.5, 2.0, 3.5).normalize();
    assertEquals(s, 0, 0.25, 1.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDoubleNormalizeFailInvalid() {
    DataFrame.toSeries(1.5, 1.5, 1.5).normalize();
  }

  @Test
  public void testDoubleNormalizeSum() {
    DoubleSeries s = DataFrame.toSeries(1.5, 1.0, 2.5).normalizeSum();
    assertEquals(s, 0.3, 0.2, 0.5);
  }

  @Test(expectedExceptions = ArithmeticException.class)
  public void testDoubleNormalizeSumFailInvalid() {
    DataFrame.toSeries(3.0, 0.0, -3.0).normalizeSum();
  }

  @Test
  public void testDoubleZScore() {
    DoubleSeries s = DataFrame.toSeries(0.0, 1.0, 2.0).zscore();
    assertEquals(s, -1, 0.0, 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDoubleZScoreFailInvalid() {
    DataFrame.toSeries(1.5, 1.5, 1.5).zscore();
  }

  @Test
  public void testDoubleRound() {
    DoubleSeries s = DataFrame.toSeries(0.1235, 0.193, 2.4, 71);
    assertEquals(s.round(5), 0.1235, 0.193, 2.4, 71);
    assertEquals(s.round(4), 0.1235, 0.193, 2.4, 71);
    assertEquals(s.round(3), 0.124, 0.193, 2.4, 71);
    assertEquals(s.round(2), 0.12, 0.19, 2.4, 71);
    assertEquals(s.round(1), 0.1, 0.2, 2.4, 71);
    assertEquals(s.round(0), 0, 0, 2, 71);
    assertEquals(s.round(-1), 0, 0, 0, 70);
    assertEquals(s.round(-2), 0, 0, 0, 100);
    assertEquals(s.round(-3), 0, 0, 0, 0);
  }

  @Test
  public void testDoubleLog() {
    DoubleSeries s = DataFrame.toSeries(-1, 0, Math.exp(1), Math.exp(2), Math.exp(3));
    assertEquals(s.log(), DataFrame.toSeries(DNULL, Double.NEGATIVE_INFINITY, 1.0, 2.0, 3.0));
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
  public void testDoubleOperationsSeriesSingleton() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0.0, 2.0);
    DoubleSeries mod = DataFrame.toSeries(2.0);

    assertEquals(base.add(mod), DNULL, 2, 4);
    assertEquals(base.subtract(mod), DNULL, -2, 0);
    assertEquals(base.multiply(mod), DNULL, 0, 4);
    assertEquals(base.divide(mod), DNULL, 0, 1);
    assertEquals(base.pow(mod), DNULL, 0, 4);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE);
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

    try {
      base.gt(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.gte(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.lt(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.lte(mod);
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
  public void testDoubleOperationGtConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.gt(1), BNULL, FALSE, FALSE, TRUE, FALSE);
    assertEquals(base.gt(0), BNULL, FALSE, TRUE, TRUE, TRUE);
    assertEquals(base.gt(DNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationGteConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.gte(1), BNULL, FALSE, TRUE, TRUE, FALSE);
    assertEquals(base.gte(0), BNULL, TRUE, TRUE, TRUE, TRUE);
    assertEquals(base.gte(DNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationLtConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.lt(1), BNULL, TRUE, FALSE, FALSE, TRUE);
    assertEquals(base.lt(0), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.lt(DNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationLteConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.lte(1), BNULL, TRUE, TRUE, FALSE, TRUE);
    assertEquals(base.lte(0), BNULL, TRUE, FALSE, FALSE, FALSE);
    assertEquals(base.lte(DNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testDoubleOperationBetweenConstant() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 0, 1, 1.5, 0.003);
    assertEquals(base.between(1, 1.5), BNULL, FALSE, TRUE, FALSE, FALSE);
    assertEquals(base.between(1, 1.8), BNULL, FALSE, TRUE, TRUE, FALSE);
    assertEquals(base.between(0, DNULL), BooleanSeries.nulls(5));
    assertEquals(base.between(DNULL, 1), BooleanSeries.nulls(5));
  }

  @Test
  public void testDoubleAbs() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, -1, 1.5, -0.003, 0.0d, -0.0d);
    assertEquals(base.abs(), DNULL, 1, 1, 1.5, 0.003, 0, 0);
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
  public void testDoubleFind() {
    DoubleSeries base = DataFrame.toSeries(0.5, DNULL, 3.1, 2.5, 1.8, DNULL, 3.2);
    Assert.assertEquals(base.find(0.4, 0.1), 0);
    Assert.assertEquals(base.find(DNULL, DNULL), 1);
    Assert.assertEquals(base.find(3, 0.2), 2);
    Assert.assertEquals(base.find(DNULL, 10.0, 2), 5);
    Assert.assertEquals(base.find(3, 0.2, 3), 6);
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
  public void testDoubleAggregation() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 1, 1.5, 0.003);
    assertEquals(base.sum(), 3.503);
    assertEquals(base.product(), 0.0045);
    assertEquals(base.min(), 0.003);
    assertEquals(base.max(), 1.5);
    assertEquals(base.first(), DNULL);
    assertEquals(base.last(), 0.003);
    assertEquals(base.mean(), 0.87575);
    assertEquals(base.median(), 1);
    assertEquals(base.std(), 0.62776236215094);
  }

  @Test
  public void testDoubleQuantile() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 2, 3, 4, 5);
    assertEquals(base.quantile(0.00), 1d);
    assertEquals(base.quantile(0.25), 2d);
    assertEquals(base.quantile(0.50), 3d);
    assertEquals(base.quantile(0.75), 4d);
    assertEquals(base.quantile(1.00), 5d);
  }

  @Test
  public void testDoubleQuantileInterpolation() {
    DoubleSeries base = DataFrame.toSeries(DNULL, 1, 2, 3, 4, 5);
    assertEquals(base.quantile(0.0000), 1.00);
    assertEquals(base.quantile(0.0625), 1.25);
    assertEquals(base.quantile(0.1250), 1.50);
    assertEquals(base.quantile(0.1875), 1.75);
    assertEquals(base.quantile(0.2500), 2.00);
  }

  @Test
  public void testDoubleQuantileNull() {
    DoubleSeries base = DataFrame.toSeries(DNULL);
    assertEquals(base.quantile(0.00), DNULL);
    assertEquals(base.quantile(1.00), DNULL);
  }

  @Test
  public void testDoubleQuantileSideEffectFree() {
    DoubleSeries base = DataFrame.toSeries(1.00, 3.00, 2.00, 4.00);
    base.quantile(0.5);
    assertEquals(base, 1.00, 3.00, 2.00, 4.00);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDoubleQuantileFailLowQ() {
    DoubleSeries base = DataFrame.toSeries(DNULL);
    assertEquals(base.quantile(-0.01), DNULL);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDoubleQuantileFailHighQ() {
    DoubleSeries base = DataFrame.toSeries(DNULL);
    assertEquals(base.quantile(1.01), DNULL);
  }

  @Test
  public void testDoubleMedianSideEffectFree() {
    DoubleSeries base = DataFrame.toSeries(1.00, 3.00, 2.00, 4.00);
    base.median();
    assertEquals(base, 1.00, 3.00, 2.00, 4.00);
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
  public void testLongOperationsSeriesSingleton() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 2);
    LongSeries mod = DataFrame.toSeries(2);

    assertEquals(base.add(mod), LNULL, 2, 4);
    assertEquals(base.subtract(mod), LNULL, -2, 0);
    assertEquals(base.multiply(mod), LNULL, 0, 4);
    assertEquals(base.divide(mod), LNULL, 0, 1);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE);
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

    try {
      base.lt(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.lte(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.gt(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }

    try {
      base.gte(mod);
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
  public void testLongOperationGtConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.gt(1), BNULL, FALSE, FALSE, TRUE, TRUE);
    assertEquals(base.gt(0), BNULL, FALSE, TRUE, TRUE, TRUE);
    assertEquals(base.gt(LNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testLongOperationGteConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.gte(1), BNULL, FALSE, TRUE, TRUE, TRUE);
    assertEquals(base.gte(0), BNULL, TRUE, TRUE, TRUE, TRUE);
    assertEquals(base.gte(LNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testLongOperationLtConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.lt(1), BNULL, TRUE, FALSE, FALSE, FALSE);
    assertEquals(base.lt(0), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.lt(LNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testLongOperationLteConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.lte(1), BNULL, TRUE, TRUE, FALSE, FALSE);
    assertEquals(base.lte(0), BNULL, TRUE, FALSE, FALSE, FALSE);
    assertEquals(base.lte(LNULL), BooleanSeries.nulls(5));
  }

  @Test
  public void testLongOperationBetweenConstant() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 1, 5, 10);
    assertEquals(base.between(1, 5), BNULL, FALSE, TRUE, FALSE, FALSE);
    assertEquals(base.between(1, 10), BNULL, FALSE, TRUE, TRUE, FALSE);
    assertEquals(base.between(0, LNULL), BooleanSeries.nulls(5));
    assertEquals(base.between(LNULL, 1), BooleanSeries.nulls(5));
  }

  @Test
  public void testLongAbs() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, -3, 5, -10);
    assertEquals(base.abs(), LNULL, 0, 3, 5, 10);
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
  public void testLongFind() {
    LongSeries base = DataFrame.toSeries(0, LNULL, 3, 2, 1, LNULL, 3);
    Assert.assertEquals(base.find(0), 0);
    Assert.assertEquals(base.find(LNULL), 1);
    Assert.assertEquals(base.find(3, 2), 2);
    Assert.assertEquals(base.find(LNULL, 2), 5);
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
  public void testLongAggregation() {
    LongSeries base = DataFrame.toSeries(LNULL, 0, 0, 5, 10);
    assertEquals(base.sum(), 15);
    assertEquals(base.product(), 0);
    assertEquals(base.min(), 0);
    assertEquals(base.max(), 10);
    assertEquals(base.first(), LNULL);
    assertEquals(base.last(), 10);
    assertEquals(base.mean(), 3.75);
    assertEquals(base.median(), 2.5);
    assertEquals(base.std(), 4.7871355387817);
  }

  @Test
  public void testStringOperationsSeries() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b", "c", "d");
    StringSeries mod = DataFrame.toSeries("A", "A", "b", "B", SNULL);

    assertEquals(base.concat(mod), SNULL, "aA", "bb", "cB", SNULL);
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);
  }

  @Test
  public void testStringOperationsSeriesSingleton() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "b");
    StringSeries mod = DataFrame.toSeries("b");

    assertEquals(base.concat(mod), SNULL, "ab", "bb");
    assertEquals(base.eq(mod), BNULL, FALSE, TRUE);
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
  public void testStringAggregation() {
    StringSeries base = DataFrame.toSeries(SNULL, "a", "1", "A", "0.003");
    assertEquals(base.sum(), "a1A0.003");
    assertEquals(base.min(), "0.003");
    assertEquals(base.max(), "a");
    assertEquals(base.first(), SNULL);
    assertEquals(base.last(), "0.003");

    try {
      base.product();
      Assert.fail();
    } catch(NumberFormatException ignore) {
      // left  blank
    }

    try {
      base.mean();
      Assert.fail();
    } catch(NumberFormatException ignore) {
      // left  blank
    }

    try {
      base.median();
      Assert.fail();
    } catch(NumberFormatException ignore) {
      // left  blank
    }

    try {
      base.std();
      Assert.fail();
    } catch(NumberFormatException ignore) {
      // left  blank
    }
  }

  @Test
  public void testStringInferEmpty() {
    Assert.assertEquals(StringSeries.empty().inferType(), Series.SeriesType.STRING);
  }

  @Test
  public void testStringUncompressed() {
    String[] values = new String[4];
    for (int i = 0; i < values.length; i++) {
      values[i] = "myString" + (i % 2);
    }
    StringSeries s = StringSeries.buildFrom(values);

    Assert.assertNotSame(s.getString(0), s.getString(2));
    Assert.assertNotSame(s.getString(1), s.getString(3));
  }

  @Test
  public void testStringCompressed() {
    String[] values = new String[4];
    for (int i = 0; i < values.length; i++) {
      values[i] = "myString" + (i % 2);
    }
    StringSeries s = StringSeries.buildFrom(values).compress();

    Assert.assertSame(s.getString(0), s.getString(2));
    Assert.assertSame(s.getString(1), s.getString(3));
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
  public void testBooleanOperationsSeriesSingleton() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE);
    BooleanSeries mod = DataFrame.toSeries(FALSE);

    assertEquals(base.and(mod), BNULL, FALSE, FALSE);
    assertEquals(base.or(mod), BNULL, TRUE, FALSE);
    assertEquals(base.xor(mod), BNULL, TRUE, FALSE);
    assertEquals(base.implies(mod), BNULL, FALSE, TRUE);
    assertEquals(base.eq(mod), BNULL,FALSE, TRUE);
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
  public void testBooleanAggregation() {
    BooleanSeries base = DataFrame.toSeries(BNULL, TRUE, FALSE, TRUE, FALSE);
    assertEquals(base.sum(), 2);
    assertEquals(base.product(), FALSE);
    assertEquals(base.min(), FALSE);
    assertEquals(base.max(), TRUE);
    assertEquals(base.first(), BNULL);
    assertEquals(base.last(), FALSE);
    assertEquals(base.mean(), 0.5);
    assertEquals(base.median(), 0.5);
    assertEquals(base.std(), 0.57735026918963);
  }

  @Test
  public void testObjectOperationsSeries() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    ObjectSeries mod = DataFrame.toSeriesObjects(1L, false, "b", 1, ONULL);

    assertEquals(base.eq(mod), BNULL, FALSE, TRUE, FALSE, BNULL);
  }

  @Test
  public void testObjectOperationsSeriesSingleton() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L);
    ObjectSeries mod = DataFrame.toSeriesObjects(true);

    assertEquals(base.eq(mod), BNULL, TRUE, FALSE, FALSE);
  }

  @Test
  public void testObjectOperationsSeriesMisaligned() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    ObjectSeries mod = DataFrame.toSeriesObjects(false, "b", 1, ONULL);

    try {
      base.eq(mod);
      Assert.fail();
    } catch(IllegalArgumentException expected) {
      // left blank
    }
  }

  @Test
  public void testObjectOperationEqConstant() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    assertEquals(base.eq(1L), BNULL, FALSE, FALSE, TRUE, FALSE);
    assertEquals(base.eq("b"), BNULL, FALSE, TRUE, FALSE, FALSE);
    assertEquals(base.eq(1), BNULL, FALSE, FALSE, FALSE, FALSE);
    assertEquals(base.eq(ONULL), BooleanSeries.nulls(5));
  }


  @Test
  public void testObjectCount() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    Assert.assertEquals(base.count("b"), 1);
    Assert.assertEquals(base.count(1), 0);
    Assert.assertEquals(base.count(ONULL), 1);
  }

  @Test
  public void testObjectContains() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    Assert.assertTrue(base.contains("b"));
    Assert.assertFalse(base.contains(1));
    Assert.assertTrue(base.contains(ONULL));
  }

  @Test
  public void testObjectReplace() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    assertEquals(base.replace("b", "AA"), ONULL, true, "AA", 1L, 0.5d);
    assertEquals(base.replace(1, ONULL), ONULL, true, "b", 1L, 0.5d);
    assertEquals(base.replace(ONULL, "N"), "N", true, "b", 1L, 0.5d);
  }

  @Test
  public void testObjectFilterSeries() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 0.5d);
    BooleanSeries mod = DataFrame.toSeries(TRUE, TRUE, TRUE, FALSE, BNULL);
    assertEquals(base.filter(mod), ONULL, true, "b", ONULL, ONULL);
  }

  @Test
  public void testObjectFilterConditional() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, true, "b", 1L, 1);
    assertEquals(base.filter(new Series.ObjectConditional() {
      @Override
      public boolean apply(Object... values) {
        return values[0].equals("b") || values[0].equals(1L);
      }
    }), ONULL, ONULL, "b", 1L, ONULL);
  }

  @Test
  public void testObjectAggregation() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, 1, 3, 0, 2);
    assertEquals(base.first(), ONULL);
    assertEquals(base.last(), 2);
    assertEquals(base.min(), 0);
    assertEquals(base.max(), 3);

    try {
      base.sum();
      Assert.fail();
    } catch(RuntimeException ignore) {
      // left  blank
    }

    try {
      base.product();
      Assert.fail();
    } catch(RuntimeException ignore) {
      // left  blank
    }

    try {
      base.mean();
      Assert.fail();
    } catch(RuntimeException ignore) {
      // left  blank
    }

    try {
      base.median();
      Assert.fail();
    } catch(RuntimeException ignore) {
      // left  blank
    }

    try {
      base.std();
      Assert.fail();
    } catch(RuntimeException ignore) {
      // left  blank
    }
  }

  @Test
  public void testObjectListTyped() {
    ObjectSeries base = DataFrame.toSeriesObjects(ONULL, 1, 3, 0, 2);

    List<Integer> list = base.toListTyped();
    Assert.assertEquals(list.get(0), null);
    Assert.assertEquals(list.get(1), (Integer)1);
    Assert.assertEquals(list.get(2), (Integer)3);
    Assert.assertEquals(list.get(3), (Integer)0);
    Assert.assertEquals(list.get(4), (Integer)2);
  }

  @Test
  public void testObjectSortMap() {
    ObjectSeries base = DataFrame.toSeriesObjects(tup(1, 1), tup(2, 3), tup(0, 6), tup(1, 0));
    StringSeries out = base.sorted().map(new Series.StringFunction() {
      @Override
      public String apply(String... values) {
        return values[0] + "!";
      }
    });
    Assert.assertEquals(out.join(), "(0,6)!(1,0)!(1,1)!(2,3)!");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testObjectSortedFail() {
    ObjectSeries base = DataFrame.toSeriesObjects("A", tup(1, 1));
    base.sorted();
  }

  @Test
  public void testObjectInferEmpty() {
    Assert.assertEquals(ObjectSeries.empty().inferType(), Series.SeriesType.OBJECT);
  }

  @Test
  public void testObjectInferTypeLong() {
    ObjectSeries base = DataFrame.toSeriesObjects(1L, 2, 3.0, 0b100, "5", ONULL);
    Assert.assertEquals(base.inferType(), Series.SeriesType.LONG);

    ObjectSeries base2 = DataFrame.toSeriesObjects("1", "2", "3", "100", "5", ONULL);
    Assert.assertEquals(base2.inferType(), Series.SeriesType.LONG);
  }

  @Test
  public void testObjectInferTypeDouble() {
    ObjectSeries base = DataFrame.toSeriesObjects(1L, 2, 3.1, 0b100, "5", ONULL);
    Assert.assertEquals(base.inferType(), Series.SeriesType.DOUBLE);

    ObjectSeries base2 = DataFrame.toSeriesObjects("1", "2", "3.1", "100", "5", ONULL);
    Assert.assertEquals(base2.inferType(), Series.SeriesType.DOUBLE);
  }

  @Test
  public void testObjectInferTypeString() {
    ObjectSeries base = DataFrame.toSeriesObjects(1L, "true", false, "0b100", "5", ONULL);
    Assert.assertEquals(base.inferType(), Series.SeriesType.STRING);
  }

  @Test
  public void testObjectInferTypeBoolean() {
    ObjectSeries base = DataFrame.toSeriesObjects("false", "true", false, true, ONULL);
    Assert.assertEquals(base.inferType(), Series.SeriesType.BOOLEAN);
  }

  @Test
  public void testObjectInferTypeObject() {
    ObjectSeries base = DataFrame.toSeriesObjects("false", "true", tup(1, 2), true, ONULL);
    Assert.assertEquals(base.inferType(), Series.SeriesType.OBJECT);
  }

  @Test
  public void testObjectMapExpression() {
    ObjectSeries base = DataFrame.toSeriesObjects(tup(1, 2), tup(0, 3), tup(3, -2), ONULL);
    assertEquals(base.map("this.myself.a"), 1, 0, 3, ONULL);
    assertEquals(base.map("this.myself.b"), 2, 3, -2, ONULL);
    assertEquals(base.map("this.myself.a"), base.map("myself.getA()"));
    assertEquals(base.map("this.myself.b"), base.map("getB()"));
  }

  @Test
  public void testObjectLongConversion() {
    ObjectSeries base = DataFrame.toSeriesObjects(tup(1, 2), tup(0, 3), tup(3, -2), ONULL);
    assertEquals(base.getLongs(), 102, 3, 298, LNULL);
  }

  @Test
  public void testObjectDoubleConversion() {
    ObjectSeries base = DataFrame.toSeriesObjects(tup(1, 2), tup(0, 3), tup(3, -2), ONULL);
    assertEquals(base.getDoubles(), 12.0, 3.0, 28.0, DNULL);
  }

  @Test
  public void testObjectBooleanConversion() {
    ObjectSeries base = DataFrame.toSeriesObjects(tup(1, 2), tup(0, 3), tup(3, -2), ONULL);
    assertEquals(base.getBooleans(), TRUE, TRUE, FALSE, BNULL);
  }

  @Test
  public void testObjectStringConversion() {
    ObjectSeries base = DataFrame.toSeriesObjects(tup(1, 2), tup(0, 3), tup(3, -2), ONULL);
    assertEquals(base.getStrings(), "(1,2)", "(0,3)", "(3,-2)", SNULL);
  }

  @Test
  public void testSetSingleValue() {
    BooleanSeries mask = DataFrame.toSeries(FALSE, TRUE, TRUE, BNULL);

    Series values = DataFrame.toSeries(TRUE);

    assertEquals(DataFrame.toSeries(BNULL, FALSE, FALSE, FALSE).set(mask, values), BNULL, TRUE, TRUE, FALSE);
    assertEquals(DataFrame.toSeries(LNULL, 2, 3, 4).set(mask, values), LNULL, 1, 1, 4);
    assertEquals(DataFrame.toSeries(DNULL, 2.0, 3.0, 4.0).set(mask, values), DNULL, 1.0, 1.0, 4.0);
    assertEquals(DataFrame.toSeries(SNULL, "a", "b", "c").set(mask, values), SNULL, "true", "true", "c");
    assertEquals(DataFrame.toSeriesObjects(ONULL, false, 3, "a").set(mask, values), ONULL, true, true, "a");
  }

  @Test
  public void testSetSeries() {
    BooleanSeries mask = DataFrame.toSeries(FALSE, TRUE, TRUE, BNULL);

    Series values = DataFrame.toSeries(TRUE, BNULL, TRUE, FALSE);

    assertEquals(DataFrame.toSeries(BNULL, FALSE, FALSE, FALSE).set(mask, values), BNULL, BNULL, TRUE, FALSE);
    assertEquals(DataFrame.toSeries(LNULL, 2, 3, 4).set(mask, values), LNULL, LNULL, 1, 4);
    assertEquals(DataFrame.toSeries(DNULL, 2.0, 3.0, 4.0).set(mask, values), DNULL, DNULL, 1.0, 4.0);
    assertEquals(DataFrame.toSeries(SNULL, "a", "b", "c").set(mask, values), SNULL, SNULL, "true", "c");
    assertEquals(DataFrame.toSeriesObjects(ONULL, false, 3, "a").set(mask, values), ONULL, ONULL, true, "a");
  }

  @Test
  public void testSetDataFrame() {
    DataFrame df = new DataFrame().addSeries("boolean", TRUE, BNULL, TRUE, FALSE);

    df.set("boolean", df.getBooleans("boolean"), df.getBooleans("boolean").not());

    assertEquals(df.getBooleans("boolean"), FALSE, BNULL, FALSE, FALSE);
  }

  @Test
  public void testSetFailMaskMisaligned() {
    BooleanSeries mask = DataFrame.toSeries(FALSE, TRUE, TRUE);
    Series values = DataFrame.toSeries(TRUE, BNULL, TRUE, FALSE);

    try {
      DataFrame.toSeries(BNULL, FALSE, FALSE, FALSE).set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeries(LNULL, 2, 3, 4).set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeries(DNULL, 2.0, 3.0, 4.0).set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeries(SNULL, "a", "b", "c").set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeriesObjects(ONULL, false, 3, "a").set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testSetFailValuesMisaligned() {
    BooleanSeries mask = DataFrame.toSeries(FALSE, TRUE, TRUE, BNULL);
    Series values = DataFrame.toSeries(TRUE, BNULL, FALSE);

    try {
      DataFrame.toSeries(BNULL, FALSE, FALSE, FALSE).set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeries(LNULL, 2, 3, 4).set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeries(DNULL, 2.0, 3.0, 4.0).set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeries(SNULL, "a", "b", "c").set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      DataFrame.toSeriesObjects(ONULL, false, 3, "a").set(mask, values);
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
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

  @Test
  public void testToString() {
    DataFrame df = new DataFrame();
    df.addSeries("one", DNULL, 1.0, 2.0, 3.0, 4.0);
    df.addSeries("two", LNULL, 1, 2, 3, 4);
    df.addSeries("three", SNULL, "1", "2", "3", "4");
    df.addSeries("four", BNULL, FALSE, TRUE, FALSE, TRUE);
    df.addSeriesObjects("five", ONULL, tup(1, 1), tup(1, 2), tup(1, 3), tup(1, 4));

    String expected =
          " one   two  three   four   five\n"
        + "null  null  null    null  null \n"
        + " 1.0     1  1      false  (1,1)\n"
        + " 2.0     2  2       true  (1,2)\n"
        + " 3.0     3  3      false  (1,3)\n"
        + " 4.0     4  4       true  (1,4)\n";

    Assert.assertEquals(df.toString(), expected);
  }

  @Test
  public void testToStringSelective() {
    DataFrame df = new DataFrame();
    df.addSeries("one", DNULL, 1.0, 2.0, 3.0, 4.0);
    df.addSeries("two", LNULL, 1, 2, 3, 4);
    df.addSeries("three", SNULL, "1", "2", "3", "4");
    df.addSeries("four", BNULL, FALSE, TRUE, FALSE, TRUE);
    df.addSeriesObjects("five", ONULL, tup(1, 1), tup(1, 2), tup(1, 3), tup(1, 4));

    String expected =
          "three   one\n"
        + "null   null\n"
        + "1       1.0\n"
        + "2       2.0\n"
        + "3       3.0\n"
        + "4       4.0\n";

    Assert.assertEquals(df.toString("three", "one"), expected);
  }

  @Test
  public void testToStringWithIndex() {
    DataFrame df = new DataFrame();
    df.addSeries("one", DNULL, 1.0, 2.0, 3.0, 4.0);
    df.addSeries("two", LNULL, 1, 2, 3, 4);
    df.addSeries("three", SNULL, "1", "2", "3", "4");
    df.addSeries("four", BNULL, FALSE, TRUE, FALSE, TRUE);
    df.addSeriesObjects("five", ONULL, tup(1, 1), tup(1, 2), tup(1, 3), tup(1, 4));
    df.setIndex("three");

    String expected =
          "three   one   two   four   five\n"
        + "null   null  null   null  null \n"
        + "1       1.0     1  false  (1,1)\n"
        + "2       2.0     2   true  (1,2)\n"
        + "3       3.0     3  false  (1,3)\n"
        + "4       4.0     4   true  (1,4)\n";

    Assert.assertEquals(df.toString(), expected);
  }

  @Test
  public void testToStringNoSeries() {
    DataFrame df = new DataFrame();
    df.addSeries("one", DNULL, 1.0, 2.0, 3.0, 4.0);

    Assert.assertEquals(df.toString(new String[0]), "");
  }

  @Test
  public void testToStringNoData() {
    DataFrame df = new DataFrame();
    df.addSeries("one", LongSeries.empty());
    df.addSeries("two", LongSeries.empty());

    String expected = "one  two\n";

    Assert.assertEquals(df.toString(), expected);
  }

  @Test
  public void testSliceRows() {
    DataFrame df = new DataFrame();
    df.addSeries("one", 1, 2, 3, 4);
    df.addSeries("two", 1.0, 2.0, 3.0, 4.0);
    df.addSeries("three", "1", "2", "3", "4");
    df.addSeries("four", false, true, false, true);
    df.addSeriesObjects("five", true, 2.0, 3L, 4);

    DataFrame out = df.slice(1, 3);

    Assert.assertEquals(out.size(), 2);
    Assert.assertEquals(out.getSeriesNames().size(), 5);
    assertEquals(out.getLongs("one"), 2, 3);
    assertEquals(out.getDoubles("two"), 2.0, 3.0);
    assertEquals(out.getStrings("three"), "2", "3");
    assertEquals(out.getBooleans("four"), true, false);
    assertEquals(out.getObjects("five"), 2.0, 3L);
  }

  @Test
  public void testSliceRowsTooLow() {
    DataFrame df = new DataFrame();
    df.addSeries("one", 1, 2, 3, 4);
    df.addSeries("two", 1.0, 2.0, 3.0, 4.0);
    df.addSeries("three", "1", "2", "3", "4");
    df.addSeries("four", false, true, false, true);
    df.addSeriesObjects("five", true, 2.0, 3L, 4);

    DataFrame out = df.slice(-2, 3);

    Assert.assertEquals(out.size(), 3);
    Assert.assertEquals(out.getSeriesNames().size(), 5);
    assertEquals(out.getLongs("one"), 1, 2, 3);
    assertEquals(out.getDoubles("two"), 1.0, 2.0, 3.0);
    assertEquals(out.getStrings("three"), "1", "2", "3");
    assertEquals(out.getBooleans("four"), false, true, false);
    assertEquals(out.getObjects("five"), true, 2.0, 3L);
  }

  @Test
  public void testSliceRowsTooHigh() {
    DataFrame df = new DataFrame();
    df.addSeries("one", 1, 2, 3, 4);
    df.addSeries("two", 1.0, 2.0, 3.0, 4.0);
    df.addSeries("three", "1", "2", "3", "4");
    df.addSeries("four", false, true, false, true);
    df.addSeriesObjects("five", true, 2.0, 3L, 4);

    DataFrame out = df.slice(1, 7);

    Assert.assertEquals(out.size(), 3);
    Assert.assertEquals(out.getSeriesNames().size(), 5);
    assertEquals(out.getLongs("one"), 2, 3, 4);
    assertEquals(out.getDoubles("two"), 2.0, 3.0, 4.0);
    assertEquals(out.getStrings("three"), "2", "3", "4");
    assertEquals(out.getBooleans("four"), true, false, true);
    assertEquals(out.getObjects("five"), 2.0, 3L, 4);
  }

  @Test
  public void testSliceRowsEmpty() {
    DataFrame df = new DataFrame();
    df.addSeries("one", 1, 2, 3, 4);
    df.addSeries("two", 1.0, 2.0, 3.0, 4.0);
    df.addSeries("three", "1", "2", "3", "4");
    df.addSeries("four", false, true, false, true);
    df.addSeriesObjects("five", true, 2.0, 3L, 4);

    DataFrame out = df.slice(4, 4);

    Assert.assertEquals(out.size(), 0);
    Assert.assertEquals(out.getSeriesNames().size(), 5);
    assertEmpty(out.getLongs("one"));
    assertEmpty(out.getDoubles("two"));
    assertEmpty(out.getStrings("three"));
    assertEmpty(out.getBooleans("four"));
    assertEmpty(out.getObjects("five"));
  }

  @Test
  public void testSliceColumns() {
    DataFrame df = new DataFrame();
    df.addSeries("one", 1, 2, 3, 4);
    df.addSeries("two", 1.0, 2.0, 3.0, 4.0);
    df.addSeries("three", "1", "2", "3", "4");
    df.addSeries("four", false, true, false, true);
    df.addSeriesObjects("five", true, 2.0, 3L, 4);
    df.setIndex("one");

    DataFrame out = df.slice("three", "one");

    Assert.assertEquals(out.size(), 4);
    Assert.assertEquals(out.getSeriesNames().size(), 2);
    Assert.assertEquals(out.getIndexNames().get(0), "one");
    assertEquals(out.getStrings("three"), "1", "2", "3", "4");
    assertEquals(out.getLongs("one"), 1, 2, 3, 4);
  }

  @Test
  public void testSliceColumnsNoIndex() {
    DataFrame df = new DataFrame();
    df.addSeries("one", 1, 2, 3, 4);
    df.addSeries("two", 1.0, 2.0, 3.0, 4.0);
    df.setIndex("two");

    DataFrame out = df.slice("one");

    Assert.assertEquals(out.size(), 4);
    Assert.assertEquals(out.getSeriesNames().size(), 1);
    Assert.assertFalse(out.hasIndex());
    assertEquals(out.getLongs("one"), 1, 2, 3, 4);
  }

  @Test
  public void testSeriesHasNull() {
    Assert.assertTrue(DataFrame.toSeries(1, 2, 3, LNULL, 5).hasNull());
  }

  @Test
  public void testSeriesHasNullFail() {
    Assert.assertFalse(DataFrame.toSeries(1, 2, 3, 4, 5).hasNull());
  }

  @Test
  public void testSeriesHasNullEmpty() {
    Assert.assertFalse(LongSeries.empty().hasNull());
  }

  @Test
  public void testSeriesAllNull() {
    Assert.assertTrue(DataFrame.toSeries(LNULL, LNULL, LNULL, LNULL, LNULL).allNull());
  }

  @Test
  public void testSeriesAllNullFail() {
    Assert.assertFalse(DataFrame.toSeries(LNULL, LNULL, 3, LNULL, LNULL).allNull());
  }

  @Test
  public void testSeriesAllNullEmpty() {
    Assert.assertTrue(LongSeries.empty().allNull());
  }

  @Test
  public void testDropAllNull() {
    DataFrame df = new DataFrame()
        .addSeries("a", LNULL, 1, LNULL)
        .addSeries("b", LNULL, LNULL, LNULL)
        .addSeries("c", DNULL, 1.1, DNULL)
        .addSeries("d", 1.2, 1.3, 1.4);

    DataFrame dfOut = df.dropAllNullColumns();

    Assert.assertEquals(df.getSeriesNames(), new HashSet<>(Arrays.asList("a", "b", "c", "d")));
    Assert.assertEquals(dfOut.getSeriesNames(), new HashSet<>(Arrays.asList("a", "c", "d")));
  }

  @Test
  public void testDropSeries() {
    Assert.assertEquals(this.df.dropSeries("long", "boolean").getSeriesNames(),
        new HashSet<>(Arrays.asList("double", "string", "object", "index")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDropSeriesFail() {
    new DataFrame("a", LongSeries.empty()).dropSeries("b");
  }

  @Test
  public void testRetainSeries() {
    Assert.assertEquals(this.df.retainSeries("long", "boolean").getSeriesNames(),
        new HashSet<>(Arrays.asList("long", "boolean")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRetainSeriesFail() {
    new DataFrame("a", LongSeries.empty()).retainSeries("b");
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
      Assert.fail(String.format("expected array length [%d] but found [%d]", expected.length, actual.length));
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
      Assert.fail(String.format("expected array length [%d] but found [%d]", expected.length, actual.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEquals(StringSeries actual, String... expected) {
    assertEquals(actual.getStrings().values(), expected);
  }

  private static void assertEquals(String[] actual, String... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", expected.length, actual.length));
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
      Assert.fail(String.format("expected array length [%d] but found [%d]", expected.length, actual.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEquals(boolean[] actual, boolean... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", expected.length, actual.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEquals(ObjectSeries actual, Object... expected) {
    assertEqualsObjects(actual.getObjects().values(), expected);
  }

  private static void assertEqualsObjects(Object[] actual, Object... expected) {
    if(actual.length != expected.length)
      Assert.fail(String.format("expected array length [%d] but found [%d]", expected.length, actual.length));
    for(int i=0; i<actual.length; i++) {
      Assert.assertEquals(actual[i], expected[i], "index=" + i);
    }
  }

  private static void assertEmpty(Series series) {
    if(!series.isEmpty())
      Assert.fail("expected series to be empty, but wasn't");
  }

  /* **************************************************************************
   * Test classes
   ***************************************************************************/

  private static class TestTuple {
    final int a;
    final int b;
    final TestTuple myself;

    TestTuple(int a, int b) {
      this.a = a;
      this.b = b;
      this.myself = this;
    }

    @SuppressWarnings("unused")
    public int getA() {
      return a;
    }

    @SuppressWarnings("unused")
    public int getB() {
      return b;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestTuple tuple = (TestTuple) o;
      return a == tuple.a && b == tuple.b;
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b);
    }

    @Override
    public String toString() {
      return "(" + a + "," + b + ')';
    }

    @SuppressWarnings("unused")
    public double doubleValue() {
      return a * 10 + b;
    }

    @SuppressWarnings("unused")
    public long longValue() {
      return a * 100 + b;
    }

    @SuppressWarnings("unused")
    public boolean booleanValue() {
      return a <= b;
    }
  }

  private static class CompTestTuple extends TestTuple implements Comparable<CompTestTuple> {
    CompTestTuple(int a, int b) {
      super(a, b);
    }

    @Override
    public int compareTo(CompTestTuple o) {
      if(o == null)
        return 1;
      if(this.a == o.a)
        return Integer.compare(this.b, o.b);
      return Integer.compare(this.a, o.a);
    }
  }

  private static CompTestTuple tup(int a, int b) {
    return new CompTestTuple(a, b);
  }

  private static DateTime parseDate(String date) {
    return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z").parseDateTime(date);
  }

  private static long parseDateMillis(String date) {
    return parseDate(date).getMillis();
  }
}
