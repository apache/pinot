package com.linkedin.thirdeye.resource;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.heatmap.HeatMapCell;
import com.linkedin.thirdeye.heatmap.VolumeHeatMap;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.UriUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/heatMap")
@Produces(MediaType.APPLICATION_JSON)
public class HeatMapResource
{
  private final StarTreeManager starTreeManager;

  public HeatMapResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/volume/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}")
  public List<HeatMapCell> getHeatMap(@PathParam("collection") String collection,
                                      @PathParam("metricName") String metricName,
                                      @PathParam("dimensionName") String dimensionName,
                                      @PathParam("baselineStart") Long baselineStart,
                                      @PathParam("baselineEnd") Long baselineEnd,
                                      @PathParam("currentStart") Long currentStart,
                                      @PathParam("currentEnd") Long currentEnd,
                                      @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    List<Map<String, Number>> result
            = doQuery(starTree,
                      metricName,
                      dimensionName,
                      baselineStart,
                      baselineEnd,
                      currentStart,
                      currentEnd,
                      uriInfo);

    return VolumeHeatMap.instance().generateHeatMap(result.get(0), result.get(1));
  }

  private static List<Map<String, Number>> doQuery(StarTree starTree,
                                                   String metricName,
                                                   String dimensionName,
                                                   Long baselineStart,
                                                   Long baselineEnd,
                                                   Long currentStart,
                                                   Long currentEnd,
                                                   UriInfo uriInfo)
  {
    StarTreeQuery baselineQuery
            = UriUtils.createQueryBuilder(starTree, uriInfo)
                              .setTimeRange(new TimeRange(baselineStart, baselineEnd))
                              .build(starTree.getConfig());

    StarTreeQuery currentQuery
            = UriUtils.createQueryBuilder(starTree, uriInfo)
                              .setTimeRange(new TimeRange(currentStart, currentEnd))
                              .build(starTree.getConfig());

    // Set target dimension to all
    boolean foundDimension = false;
    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
    {
      if (starTree.getConfig().getDimensions().get(i).getName().equals(dimensionName))
      {
        baselineQuery.getDimensionKey().getDimensionValues()[i] = StarTreeConstants.ALL;
        currentQuery.getDimensionKey().getDimensionValues()[i] = StarTreeConstants.ALL;
        foundDimension = true;
        break;
      }
    }
    if (!foundDimension)
    {
      throw new NotFoundException("No dimension " + dimensionName);
    }

    Map<String, Number> baseline
            = getMetricsByDimensionValue(starTree, baselineQuery, uriInfo, metricName, dimensionName);

    Map<String, Number> current
            = getMetricsByDimensionValue(starTree, currentQuery, uriInfo, metricName, dimensionName);

    return ImmutableList.of(baseline, current);
  }

  private static Map<String, Number> getMetricsByDimensionValue(StarTree starTree,
                                                                StarTreeQuery baseQuery,
                                                                UriInfo uriInfo,
                                                                String metric,
                                                                String dimension)
  {
    // Get metric index
    int metricIdx = -1;
    for (int i = 0; i < starTree.getConfig().getMetrics().size(); i++)
    {
      if (starTree.getConfig().getMetrics().get(i).getName().equals(metric))
      {
        metricIdx = i;
        break;
      }
    }
    if (metricIdx == -1)
    {
      throw new IllegalArgumentException("No metric for " + starTree.getConfig().getCollection() + ": " + metric);
    }

    // Get dimension index
    int dimensionIdx = -1;
    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
    {
      if (starTree.getConfig().getDimensions().get(i).getName().equals(dimension))
      {
        dimensionIdx = i;
        break;
      }
    }
    if (dimensionIdx == -1)
    {
      throw new IllegalArgumentException("No dimension for " + starTree.getConfig().getCollection() + ": " + dimension);
    }

    // Generate all queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);
    queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

    // Do queries and compose result
    // n.b. all dimension values in results will be distinct because "!" used for query
    Map<String, Number> result = new HashMap<String, Number>();
    for (StarTreeQuery query : queries)
    {
      StarTreeRecord record = starTree.getAggregate(query);
      Number[] metrics = record.getMetricTimeSeries().getMetricSums();
      String[] dimensions = record.getDimensionKey().getDimensionValues();
      result.put(dimensions[dimensionIdx], metrics[metricIdx]);
    }

    return result;
  }


}
