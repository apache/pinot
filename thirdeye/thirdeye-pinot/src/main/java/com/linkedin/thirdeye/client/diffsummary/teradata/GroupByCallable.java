package com.linkedin.thirdeye.client.diffsummary.teradata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.joda.time.DateTime;



public class GroupByCallable implements Callable<Map<List<String>, Double>> {
  private List<String> _dimensions;
  private String _tableName;
  private DateTime _start;
  private DateTime _end;
  private QueryTera _queryTera;

  public GroupByCallable(QueryTera queryTera, List<String> dimensions, String tableName, DateTime start, DateTime end) {
    this._dimensions = dimensions;
    this._tableName = tableName;
    this._start = start;
    this._end = end;
    this._queryTera = queryTera;
  }

  @Override
  public Map<List<String>, Double> call() throws Exception {
    return _queryTera.groupBy(_dimensions, _tableName, _start, _end);
  }
}
