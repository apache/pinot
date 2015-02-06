package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.linkedin.thirdeye.impl.TarUtils;
import com.sun.jersey.api.NotFoundException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class CollectionsResource
{
  private static final Logger LOG = LoggerFactory.getLogger(CollectionsResource.class);

  private final StarTreeManager manager;
  private final File rootDir;

  public CollectionsResource(StarTreeManager manager, File rootDir)
  {
    this.manager = manager;
    this.rootDir = rootDir;
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
  public StarTreeConfig getConfig(@PathParam("collection") String collection)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No tree for collection " + collection);
    }
    return starTree.getConfig();
  }

  @DELETE
  @Path("/{collection}")
  @Timed
  public Response deleteCollection(@PathParam("collection") String collection) throws IOException
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No tree for collection " + collection);
    }

    manager.close(collection);

    File collectionDir = new File(rootDir, collection);

    if (!collectionDir.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    FileUtils.forceDelete(collectionDir);

    return Response.noContent().build();
  }

  @POST
  @Path("/{collection}")
  @Timed
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response postConfig(@PathParam("collection") String collection, byte[] configBytes) throws IOException
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }

    File configFile = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);

    FileUtils.copyInputStreamToFile(new ByteArrayInputStream(configBytes), configFile);

    return Response.ok().build();
  }

  @GET
  @Path("/{collection}/stats")
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

  @POST
  @Path("/{collection}/starTree")
  @Timed
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response postStarTree(@PathParam("collection") String collection, byte[] starTreeBytes) throws IOException
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }

    File configFile = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);

    FileUtils.copyInputStreamToFile(new ByteArrayInputStream(starTreeBytes), configFile);

    return Response.ok().build();
  }

  @POST
  @Path("/{collection}/data")
  @Timed
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response postData(@PathParam("collection") String collection,
                           @QueryParam("includeDimensions") boolean includeDimensions,
                           byte[] dataBytes) throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }

    File dataDir = new File(collectionDir, StarTreeConstants.DATA_DIR_NAME);
    if (!dataDir.exists())
    {
      FileUtils.forceMkdir(dataDir);
    }

    // TODO: This only works for StarTreeRecordStoreFixedImpl - if we want to be generic, record store should do following logic

    // n.b. for partial updates, we will not include dimensions
    Set<String> blacklist = includeDimensions ? null : ImmutableSet.of("dimensionStore");

    // Extract into data dir, stripping first two path components
    TarUtils.extractGzippedTarArchive(new ByteArrayInputStream(dataBytes), dataDir, 2, blacklist);

    return Response.ok().build();
  }
}
