package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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

  public static Map<String, MetricTimeSeries> groupByQuery(
          ExecutorService parallelQueryExecutor,
          final StarTree starTree,
          String dimensionName,
          TimeRange timeRange,
          UriInfo uriInfo) throws InterruptedException, ExecutionException
  {
    StarTreeQuery baseQuery = UriUtils.createQueryBuilder(starTree, uriInfo).setTimeRange(timeRange).build(starTree.getConfig());

    // Set target dimension to all
    int dimensionIndex = -1;
    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
    {
      if (starTree.getConfig().getDimensions().get(i).getName().equals(dimensionName))
      {
        baseQuery.getDimensionKey().getDimensionValues()[i] = StarTreeConstants.ALL;
        dimensionIndex = i;
        break;
      }
    }
    if (dimensionIndex < 0)
    {
      throw new NotFoundException("No dimension " + dimensionName);
    }

    // Generate all queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);
    queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

    // Do queries
    Map<StarTreeQuery, Future<MetricTimeSeries>> futures
            = new HashMap<StarTreeQuery, Future<MetricTimeSeries>>(queries.size());
    for (final StarTreeQuery query : queries)
    {
      futures.put(query, parallelQueryExecutor.submit(new Callable<MetricTimeSeries>()
      {
        @Override
        public MetricTimeSeries call() throws Exception
        {
          return starTree.getTimeSeries(query);
        }
      }));
    }

    // Compose result
    // n.b. all dimension values in results will be distinct because "!" used for query
    Map<String, MetricTimeSeries> result = new HashMap<String, MetricTimeSeries>();
    for (Map.Entry<StarTreeQuery, Future<MetricTimeSeries>> entry : futures.entrySet())
    {
      result.put(entry.getKey().getDimensionKey().getDimensionValues()[dimensionIndex], entry.getValue().get());
    }

    return result;
  }
}
