package com.linkedin.thirdeye.resource;

import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.views.HeatMapView;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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
  @Path("/{collection}")
  public HeatMapView getHeatMapView(@PathParam("collection") String collection)
  {
    if (!manager.getCollections().contains(collection))
    {
      throw new NotFoundException("No data for " + collection);
    }

    return new HeatMapView(collection);
  }
}
