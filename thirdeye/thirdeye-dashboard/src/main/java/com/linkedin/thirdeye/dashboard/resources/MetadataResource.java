package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.dashboard.api.SegmentDescriptor;
import com.linkedin.thirdeye.dashboard.util.DataCache;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class MetadataResource {
  private final String serverUri;
  private final DataCache dataCache;

  public MetadataResource(String serverUri, DataCache dataCache) {
    this.serverUri = serverUri;
    this.dataCache = dataCache;
  }

  @GET
  @Path("/{collection}/segments")
  public List<SegmentDescriptor> getSegments(@PathParam("collection") String collection) throws Exception {
    return dataCache.getSegmentDescriptors(serverUri, collection);
  }
}
