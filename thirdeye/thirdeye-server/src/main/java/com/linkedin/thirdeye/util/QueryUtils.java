package com.linkedin.thirdeye.util;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.sun.jersey.api.NotFoundException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueryUtils
{
  public static Map<DimensionKey, MetricTimeSeries> doQuery(Collection<StarTree> starTrees, long start, long end, UriInfo uriInfo)
  {
    Map<DimensionKey, MetricTimeSeries> result = new HashMap<DimensionKey, MetricTimeSeries>();

    for (StarTree starTree : starTrees)
    {
      // Expand queries
      List<StarTreeQuery> queries
              = StarTreeUtils.expandQueries(starTree,
              UriUtils.createQueryBuilder(starTree, uriInfo)
                      .setTimeRange(new TimeRange(start, end)).build(starTree.getConfig()));

      // Filter queries
      queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

      // Do queries
      for (StarTreeQuery query : queries)
      {
        MetricTimeSeries timeSeries = starTree.getTimeSeries(query);
        MetricTimeSeries existingTimeSeries = result.get(query.getDimensionKey());
        if (existingTimeSeries == null)
        {
          result.put(query.getDimensionKey(), timeSeries);
        }
        else
        {
          existingTimeSeries.aggregate(timeSeries);
        }
      }
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
          final Collection<StarTree> starTrees,
          StarTreeConfig config,
          String dimensionName,
          TimeRange timeRange,
          UriInfo uriInfo) throws InterruptedException, ExecutionException
  {
    Map<String, MetricTimeSeries> result = new HashMap<String, MetricTimeSeries>();

    for (final StarTree starTree : starTrees)
    {
      StarTreeQuery baseQuery = UriUtils.createQueryBuilder(starTree, uriInfo).setTimeRange(timeRange).build(config);

      // Set target dimension to all
      int dimensionIndex = -1;
      for (int i = 0; i < config.getDimensions().size(); i++)
      {
        if (config.getDimensions().get(i).getName().equals(dimensionName))
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
      queries = StarTreeUtils.filterQueries(config, queries, uriInfo.getQueryParameters());

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
      for (Map.Entry<StarTreeQuery, Future<MetricTimeSeries>> entry : futures.entrySet())
      {
        String dimensionValue = entry.getKey().getDimensionKey().getDimensionValues()[dimensionIndex];
        MetricTimeSeries existingTimeSeries = result.get(dimensionValue);
        if (existingTimeSeries == null)
        {
          result.put(dimensionValue, entry.getValue().get());
        }
        else
        {
          existingTimeSeries.aggregate(entry.getValue().get());
        }
      }
    }

    return result;
  }

  public static DateTime getDateTime(long time, long bucketSize, TimeUnit bucketUnit)
  {
    return new DateTime(TimeUnit.MILLISECONDS.convert(time * bucketSize, bucketUnit), DateTimeZone.UTC);
  }

  public static String checkDimensions(StarTreeConfig config, UriInfo uriInfo)
  {
    List<String> allDimensions = new ArrayList<String>();

    for (DimensionSpec dimensionSpec : config.getDimensions())
    {
      allDimensions.add(dimensionSpec.getName());
    }

    String query = uriInfo.getRequestUri().getQuery();

    if (query != null)
    {
      String[] dimensionTokens = query.split("&");

      for (String dimensionToken : dimensionTokens)
      {
        String dimensionName = dimensionToken.split("=")[0];
        if (!allDimensions.contains(dimensionName))
        {
          return dimensionName;
        }
      }
    }

    return null;
  }

  public static Range<DateTime> getDataTimeRange(Collection<StarTree> starTrees)
  {
    long globalMinTimeMillis = -1;
    long globalMaxTimeMillis = -1;

    if (starTrees != null)
    {
      for (StarTree starTree : starTrees)
      {
        TimeGranularity bucket = starTree.getConfig().getTime().getBucket();
        StarTreeStats stats = starTree.getStats();
        long minTimeMillis = TimeUnit.MILLISECONDS.convert(stats.getMinTime() * bucket.getSize(), bucket.getUnit());
        long maxTimeMillis = TimeUnit.MILLISECONDS.convert(stats.getMaxTime() * bucket.getSize(), bucket.getUnit());

        if (globalMinTimeMillis == -1 || minTimeMillis < globalMaxTimeMillis)
        {
          globalMinTimeMillis = minTimeMillis;
        }

        if (globalMaxTimeMillis == -1 || maxTimeMillis > globalMaxTimeMillis)
        {
          globalMaxTimeMillis = maxTimeMillis;
        }
      }
    }

    return Range.closed(new DateTime(globalMinTimeMillis), new DateTime(globalMaxTimeMillis));
  }
}
