package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.views.CollectionsView;
import com.linkedin.thirdeye.views.HeatMapView;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Path("/dashboard")
@Produces(MediaType.TEXT_HTML)
public class DashboardResource
{
  private final StarTreeManager manager;

  public DashboardResource(StarTreeManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Timed
  public CollectionsView getCollectionsView()
  {
    List<String> collections = new ArrayList<String>(manager.getCollections());

    if (collections.isEmpty())
    {
      throw new NotFoundException("No collections loaded!");
    }

    Collections.sort(collections);

    return new CollectionsView(collections);
  }

  @GET
  @Path("/{collection}")
  @Timed
  public HeatMapView getHeatMapView(@PathParam("collection") String collection)
  {
    if (!manager.getCollections().contains(collection))
    {
      throw new NotFoundException(collection + " is not loaded");
    }

    return new HeatMapView(collection);
  }
}
