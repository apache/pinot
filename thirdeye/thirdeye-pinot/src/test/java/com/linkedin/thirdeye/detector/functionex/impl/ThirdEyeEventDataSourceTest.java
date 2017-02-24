package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MockManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThirdEyeEventDataSourceTest {
  static class MockEventManager extends MockManager<EventDTO> implements EventManager {
    String type;
    long start;
    long end;

    @Override
    public List<EventDTO> findByEventType(String eventType) {
      Assert.fail();
      return null;
    }

    @Override
    public List<EventDTO> findEventsBetweenTimeRange(String eventType, long startTime, long endTime) {
      this.type = eventType;
      this.start = startTime;
      this.end = endTime;

      return Arrays.asList(
          makeEvent("HOLIDAY", "New Years 2016", 1451606400000L, 1451606400000L + 86400000),
          makeEvent("HOLIDAY", "New Years 2017", 1483228800000L, 1483228800000L + 86400000),
          makeEvent("holiday", "New Years 2018", 1514764800000L, 1514764800000L + 86400000)
          );
    }

    @Override
    public List<EventDTO> findEventsBetweenTimeRangeByName(String eventType, String name, long startTime,
        long endTime) {
      Assert.fail();
      return null;
    }
  }

  ThirdEyeEventDataSource ds;
  MockEventManager manager;
  AnomalyFunctionExContext context;

  @BeforeMethod
  public void before() {
    manager = new MockEventManager();
    ds = new ThirdEyeEventDataSource(manager);
    context = new AnomalyFunctionExContext();
  }

  @Test
  public void testParseQuery() throws Exception {
    ds.query("type=abc,start=0123,end=444", context);

    Assert.assertEquals(manager.type, "abc");
    Assert.assertEquals(manager.start, 123000);
    Assert.assertEquals(manager.end, 444000);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseQueryFail() throws Exception {
    ds.query("type=abc,start=0123,end=", context);
  }

  @Test
  public void testQueryDataFrame() throws Exception {
    DataFrame df = ds.query("type=IGNORED,start=000,end=000", context);

    Assert.assertEquals(df.getIndex().size(), 3);
    Assert.assertEquals(df.getSeriesNames(), new HashSet<>(Arrays.asList("type", "name", "start", "end")));
    Assert.assertEquals(df.toStrings("type").values(), new String[] { "HOLIDAY", "HOLIDAY", "holiday" });
    Assert.assertEquals(df.toStrings("name").values(), new String[] { "New Years 2016", "New Years 2017", "New Years 2018" });
    Assert.assertEquals(df.toLongs("start").values(), new long[] { 1451606400, 1483228800, 1514764800 });
    Assert.assertEquals(df.toLongs("end").values(), new long[] { 1451606400 + 86400, 1483228800 + 86400, 1514764800 + 86400 });
  }

  private static EventDTO makeEvent(String type, String name, long start, long end) {
    EventDTO e = new EventDTO();
    e.setEventType(type);
    e.setName(name);
    e.setStartTime(start);
    e.setEndTime(end);
    return e;
  }

}
