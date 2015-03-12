package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.views.DefaultDashboardView;
import com.linkedin.thirdeye.views.DefaultLandingView;
import com.linkedin.thirdeye.views.DefaultSelectionView;
import com.linkedin.thirdeye.views.FunnelComponentView;
import com.linkedin.thirdeye.views.HeatMapComponentView;
import com.linkedin.thirdeye.views.TimeSeriesComponentView;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Path("/dashboard")
@Produces(MediaType.TEXT_HTML)
public class DashboardResource
{
  private final StarTreeManager starTreeManager;
  private final TimeSeriesResource timeSeriesResource;
  private final FunnelResource funnelResource;
  private final HeatMapResource heatMapResource;
  private final String feedbackAddress;

  public DashboardResource(StarTreeManager starTreeManager,
                           TimeSeriesResource timeSeriesResource,
                           FunnelResource funnelResource,
                           HeatMapResource heatMapResource,
                           String feedbackAddress)
  {
    this.starTreeManager = starTreeManager;
    this.timeSeriesResource = timeSeriesResource;
    this.funnelResource = funnelResource;
    this.heatMapResource = heatMapResource;
    this.feedbackAddress = feedbackAddress;
  }

  @GET
  @Timed
  public DefaultSelectionView getDefaultSelectionView()
  {
    List<String> collections = new ArrayList<String>(starTreeManager.getCollections());

    if (collections.isEmpty())
    {
      throw new NotFoundException("No collections loaded!");
    }

    Collections.sort(collections);

    return new DefaultSelectionView(collections);
  }

  @GET
  @Path("/{collection}")
  @Timed
  public DefaultLandingView getDefaultLandingView(@PathParam("collection") String collection)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    return new DefaultLandingView(starTree.getConfig(), feedbackAddress);
  }

  @GET
  @Path("/{collection}/{heatMapType}/{metric}/{startMillis}/{endMillis}{funnel:(/funnel/[^/]+?)?}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}{normalized:(/normalized/[^/]+?)?}")
  @Timed
  public DefaultDashboardView getDefaultDashboardView(
          @PathParam("collection") String collection,
          @PathParam("heatMapType") String heatMapType,
          @PathParam("metric") String metric,
          @PathParam("funnel") String funnel,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @PathParam("aggregate") String aggregate,
          @PathParam("movingAverage") String movingAverage,
          @PathParam("normalized") String normalized,
          @Context UriInfo uriInfo) throws Exception
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    TimeSeriesComponentView timeSeriesComponentView
            = timeSeriesResource.getTimeSeriesComponentView(collection,
                                                            "*",
                                                            startMillis,
                                                            endMillis,
                                                            aggregate,
                                                            movingAverage,
                                                            normalized,
                                                            uriInfo);

    HeatMapComponentView heatMapComponentView
            = heatMapResource.getHeatMapComponentView(heatMapType,
                                                      collection,
                                                      metric,
                                                      startMillis,
                                                      endMillis,
                                                      aggregate,
                                                      movingAverage,
                                                      uriInfo);

    // Funnel string: {type}:{m1},{m2},...
    FunnelComponentView funnelComponentView = null;
    if (!"".equals(funnel))
    {
      String[] funnelTokens = funnel.split(":");
      funnelComponentView
              = funnelResource.getFunnelView(funnelTokens[0].substring("/funnel/".length()),
                                             collection,
                                             funnelTokens[1],
                                             startMillis,
                                             endMillis,
                                             aggregate,
                                             movingAverage,
                                             uriInfo);
    }

    List<List<String>> disabledDimensions = new ArrayList<List<String>>();
    List<String> activeDimension = null;
    String queryString = uriInfo.getRequestUri().getQuery();
    if (queryString != null && queryString.length() > 0)
    {
      String[] queryComponents = queryString.split("&");

      for (int i = 0; i < queryComponents.length - 1; i++)
      {
        disabledDimensions.add(Arrays.asList(queryComponents[i].split("=")));
      }

      if (queryComponents.length != 0)
      {
        String[] tokens = queryComponents[queryComponents.length - 1].split("=");
        if (tokens.length==1)
        {
          activeDimension = new ArrayList<String>();
          activeDimension.add(tokens[0]);
          activeDimension.add("");
        }
        else
        {
          activeDimension = Arrays.asList(tokens);
        }

      }
    }

    return new DefaultDashboardView(starTree.getConfig(),
                                    metric,
                                    endMillis,
                                    disabledDimensions,
                                    activeDimension,
                                    timeSeriesComponentView,
                                    funnelComponentView,
                                    heatMapComponentView,
                                    feedbackAddress);
  }
}
