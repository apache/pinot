package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ThirdEyeEventDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {

  private static final String PATTERN = "type=(\\w+),start=(\\d+),end=(\\d+)";
  private static final Pattern pattern = Pattern.compile(PATTERN);

  private final EventManager manager;

  public ThirdEyeEventDataSource(EventManager manager) {
    this.manager = manager;
  }

  @Override
  public DataFrame query(String query, AnomalyFunctionExContext context) throws Exception {
    List<EventDTO> events = parseQuery(query);
    DataFrame df = eventsToDataFrame(events);
    return df;
  }

  public static class Query {
    final long start;
    final long end;
    final String type;

    public Query(long start, long end, String type) {
      this.start = start;
      this.end = end;
      this.type = type;
    }
  }

  private DataFrame eventsToDataFrame(List<EventDTO> events) {
    String[] type = new String[events.size()];
    String[] name = new String[events.size()];
    long[] start = new long[events.size()];
    long[] end = new long[events.size()];

    for(int i=0; i<events.size(); i++) {
      EventDTO e = events.get(i);
      type[i] = e.getEventType();
      name[i] = e.getName();
      start[i] = e.getStartTime();
      end[i] = e.getEndTime();
    }

    DataFrame df = new DataFrame(events.size());
    df.addSeries("type", type);
    df.addSeries("name", name);
    df.addSeries("start", start);
    df.addSeries("end", end);

    return df;
  }

  private List<EventDTO> parseQuery(String query) {
    Matcher m = pattern.matcher(query);

    if(!m.find())
      throw new IllegalArgumentException(String.format("query '%s' does not match pattern '%s'", query, PATTERN));

    String type = m.group(1);
    long start = Long.parseLong(m.group(2));
    long end = Long.parseLong(m.group(3));

    return manager.findEventsBetweenTimeRange(type, start, end);
  }
}
