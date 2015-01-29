package com.linkedin.thirdeye.resource;

import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.views.SnapshotHeatMapView;
import com.linkedin.thirdeye.views.VolumeHeatMapView;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
  public VolumeHeatMapView getVolumeHeatMapView()
  {
    List<String> collections = new ArrayList<String>(manager.getCollections());
    Collections.sort(collections);
    return new VolumeHeatMapView(collections);
  }

  @GET
  @Path("/snapshot")
  public SnapshotHeatMapView getSnapshotHeatMapView()
  {
    List<String> collections = new ArrayList<String>(manager.getCollections());
    Collections.sort(collections);
    return new SnapshotHeatMapView(collections);
  }
}
