package com.linkedin.thirdeye.dashboard.resources;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.dashboard.util.DataCache;

@Path("/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class MetadataResource {
  private final DataCache dataCache;

  public MetadataResource(DataCache dataCache) {
    this.dataCache = dataCache;
  }

  @GET
  @Path("/{collection}/segments")
  public List<SegmentDescriptor> getSegments(@PathParam("collection") String collection)
      throws Exception {
    return dataCache.getSegmentDescriptors(collection);
  }
}
