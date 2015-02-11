package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeUtils;

import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryUtils
{
  public static Map<DimensionKey, MetricTimeSeries> doQuery(StarTree starTree, long start, long end, UriInfo uriInfo)
  {
    // Expand queries
    List<StarTreeQuery> queries
            = StarTreeUtils.expandQueries(starTree,
                                          UriUtils.createQueryBuilder(starTree, uriInfo)
                                                  .setTimeRange(new TimeRange(start, end)).build(starTree.getConfig()));

    // Filter queries
    queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

    // Do queries
    Map<DimensionKey, MetricTimeSeries> result = new HashMap<DimensionKey, MetricTimeSeries>(queries.size());
    for (StarTreeQuery query : queries)
    {
      result.put(query.getDimensionKey(), starTree.getTimeSeries(query));
    }

    return result;
  }

  public static Map<String, String> convertDimensionKey(List<DimensionSpec> dimensions, DimensionKey dimensionKey)
  {
    Map<String, String> result = new HashMap<String, String>(dimensions.size());

    for (int i = 0; i < dimensions.size(); i++)
    {
      result.put(dimensions.get(i).getName(), dimensionKey.getDimensionValues()[i]);
    }

    return result;
  }

  public static DimensionKey convertDimensionMap(List<DimensionSpec> dimensions, Map<String, String> values)
  {
    String[] keyValues = new String[dimensions.size()];

    for (int i = 0; i < dimensions.size(); i++)
    {
      String value = values.get(dimensions.get(i).getName());
      if (value == null)
      {
        throw new IllegalArgumentException("Must provide value for every dimension " + values);
      }
      keyValues[i] = value;
    }

    return new DimensionKey(keyValues);
  }

  public static Map<String, Number> convertMetricValues(List<MetricSpec> metrics, MetricTimeSeries timeSeries)
  {
    Map<String, Number> result = new HashMap<String, Number>();

    Number[] aggregates = timeSeries.getMetricSums();

    for (int i = 0; i < metrics.size(); i++)
    {
      result.put(metrics.get(i).getName(), aggregates[i]);
    }

    return result;
  }

  public static MetricTimeSeries convertMetricMap(List<MetricSpec> metrics, Map<String, Number> values, Long time)
  {
    MetricTimeSeries timeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(metrics));

    for (int i = 0; i < metrics.size(); i++)
    {
      Number value = values.get(metrics.get(i).getName());
      if (value == null)
      {
        value = 0; // just won't affect other metrics
      }
      timeSeries.increment(time, metrics.get(i).getName(), value);
    }

    return timeSeries;
  }
}
