package com.linkedin.thirdeye.resource;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.linkedin.thirdeye.impl.TarUtils;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.core.spi.factory.ResponseBuilderImpl;

import org.apache.commons.io.FileUtils;

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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class CollectionsResource
{
  private static final String LAST_POST_DATA_MILLIS = "lastPostDataMillis";

  private final StarTreeManager manager;
  private final File rootDir;
  private final AtomicLong lastPostDataMillis;

  public CollectionsResource(StarTreeManager manager, MetricRegistry metricRegistry, File rootDir)
  {
    this.manager = manager;
    this.rootDir = rootDir;
    this.lastPostDataMillis = new AtomicLong(-1);

    // Metric for time we last received a POST to update collection's data
    metricRegistry.register(MetricRegistry.name(CollectionsResource.class, LAST_POST_DATA_MILLIS),
                            new Gauge<Long>() {
                              @Override
                              public Long getValue()
                              {
                                return lastPostDataMillis.get();
                              }
                            });
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

    manager.remove(collection);

    File collectionDir = new File(rootDir, collection);

    if (!collectionDir.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    try
    {
      FileUtils.forceDelete(collectionDir);
    }
    catch (FileNotFoundException fe)
    {
      ResponseBuilderImpl builder = new ResponseBuilderImpl();
      builder.status(Response.Status.NOT_FOUND);
      builder.entity("Collection "+collection+" not found");
      throw new WebApplicationException(builder.build());
    }

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

    if (!configFile.exists())
    {
      FileUtils.copyInputStreamToFile(new ByteArrayInputStream(configBytes), configFile);
    }
    else
    {
      ResponseBuilderImpl builder = new ResponseBuilderImpl();
      builder.status(Response.Status.CONFLICT);
      builder.entity(configFile.getPath()+" already exists. A DELETE of /collections/{collection} is required first");
      throw new WebApplicationException(builder.build());
    }
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

    File starTreeFile = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);

    if (!starTreeFile.exists())
    {
      FileUtils.copyInputStreamToFile(new ByteArrayInputStream(starTreeBytes), starTreeFile);
    }
    else
    {
      ResponseBuilderImpl builder = new ResponseBuilderImpl();
      builder.status(Response.Status.CONFLICT);
      builder.entity(starTreeFile.getPath()+" already exists. A DELETE of /collections/{collection} is required first");
      throw new WebApplicationException(builder.build());
    }

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
    Set<String> blacklist = includeDimensions ? null : ImmutableSet.of(StarTreeConstants.DIMENSION_STORE);

    // Extract into data dir, stripping first two path components
    TarUtils.extractGzippedTarArchive(new ByteArrayInputStream(dataBytes), dataDir, 2, blacklist);

    lastPostDataMillis.set(System.currentTimeMillis());

    return Response.ok().build();
  }

  @GET
  @Path("/{collection}/schema")
  @Timed
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public byte[] getSchema(@PathParam("collection") String collection) throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      throw new NotFoundException();
    }

    File schemaFile = new File(collectionDir, StarTreeConstants.SCHEMA_FILE_NAME);
    if (!schemaFile.exists())
    {
      throw new NotFoundException();
    }

    return FileUtils.readFileToByteArray(schemaFile);
  }

  @POST
  @Path("/{collection}/schema")
  @Timed
  public Response postSchema(@PathParam("collection") String collection, byte[] schemaBytes) throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    File schemaFile = new File(collectionDir, StarTreeConstants.SCHEMA_FILE_NAME);

    if (!schemaFile.exists())
    {
      FileUtils.writeByteArrayToFile(schemaFile, schemaBytes);
    }
    else
    {
      ResponseBuilderImpl builder = new ResponseBuilderImpl();
      builder.status(Response.Status.CONFLICT);
      builder.entity(schemaFile.getPath()+" already exists. A DELETE of /collections/{collection} is required first");
      throw new WebApplicationException(builder.build());
    }

    return Response.ok().build();
  }

  @DELETE
  @Path("/{collection}/schema")
  @Timed
  public Response deleteSchema(@PathParam("collection") String collection) throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    File schemaFile = new File(collectionDir, StarTreeConstants.SCHEMA_FILE_NAME);
    if (!schemaFile.exists())
    {
      throw new NotFoundException();
    }

    FileUtils.forceDelete(schemaFile);

    return Response.noContent().build();
  }
}
