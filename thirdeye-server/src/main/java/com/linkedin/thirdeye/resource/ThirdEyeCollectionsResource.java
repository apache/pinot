package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.sun.jersey.api.NotFoundException;
import org.apache.helix.HelixManager;
import org.apache.helix.api.id.StateModelDefId;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeCollectionsResource
{
  private final StarTreeManager manager;
  private final HelixManager helixManager;

  public ThirdEyeCollectionsResource(StarTreeManager manager, HelixManager helixManager)
  {
    this.manager = manager;
    this.helixManager = helixManager;
  }

  @POST
  @Path("/{collection}/{partitions}")
  @Timed
  public Response addCollection(@PathParam("collection") String collection,
                                @PathParam("partitions") Integer partitions)
  {
    if (helixManager == null)
    {
      throw new NotFoundException();
    }

    helixManager.getClusterManagmentTool()
                .addResource(helixManager.getClusterName(),
                             collection,
                             partitions,
                             StateModelDefId.OnlineOffline.stringify());

    return Response.ok().build();
  }

  @DELETE
  @Path("/{collection}")
  @Timed
  public Response dropCollection(@PathParam("collection") String collection)
  {
    if (helixManager == null)
    {
      throw new NotFoundException();
    }

    helixManager.getClusterManagmentTool().dropResource(helixManager.getClusterName(), collection);

    return Response.ok().build();
  }

  @GET
  @Timed
  public List<String> getCollections()
  {
    List<String> collections = new ArrayList<String>(manager.getCollections());
    Collections.sort(collections);
    return collections;
  }

  @GET
  @Path("/{collection}")
  @Timed
  public StarTreeStats getStats(@PathParam("collection") String collection)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No tree for collection " + collection);
    }

    return starTree.getStats();
  }

  @GET
  @Path("/{collection}/starTree")
  @Timed
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response getStarTree(@PathParam("collection") String collection) throws IOException
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No tree for collection " + collection);
    }

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(starTree.getRoot());
    oos.flush();

    return Response.ok(baos.toByteArray(), MediaType.APPLICATION_OCTET_STREAM).build();
  }

  @GET
  @Path("/{collection}/config")
  @Timed
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response getConfig(@PathParam("collection") String collection) throws IOException
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No tree for collection " + collection);
    }
    byte[] bytes = starTree.getConfig().encode().getBytes();
    return Response.ok(bytes, MediaType.APPLICATION_OCTET_STREAM).build();
  }
}
