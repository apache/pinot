package com.linkedin.thirdeye.resource;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.NotFoundException;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class CollectionsResource
{
  private static final String LAST_POST_DATA_MILLIS = "lastPostDataMillis";

  private final StarTreeManager manager;
  private final File rootDir;
  private final AtomicLong lastPostDataMillis;
  private final DataUpdateManager dataUpdateManager;

  public CollectionsResource(StarTreeManager manager,
                             MetricRegistry metricRegistry,
                             DataUpdateManager dataUpdateManager,
                             File rootDir)
  {
    this.manager = manager;
    this.rootDir = rootDir;
    this.lastPostDataMillis = new AtomicLong(-1);
    this.dataUpdateManager = dataUpdateManager;

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
  public List<String> getCollections()
  {
    List<String> collections = new ArrayList<String>(manager.getCollections());
    Collections.sort(collections);
    return collections;
  }

  @GET
  @Path("/{collection}")
  public StarTreeConfig getConfig(@PathParam("collection") String collection)
  {
    StarTreeConfig config = manager.getConfig(collection);
    if (config == null)
    {
      throw new NotFoundException("No collection " + collection);
    }
    return config;
  }

  @DELETE
  @Path("/{collection}")
  public Response deleteCollection(@PathParam("collection") String collection) throws Exception
  {
    StarTreeConfig config = manager.getConfig(collection);
    if (config == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    manager.close(collection);

    try
    {
      dataUpdateManager.deleteCollection(collection);
    }
    catch (FileNotFoundException e)
    {
      throw new NotFoundException(e.getMessage());
    }

    return Response.noContent().build();
  }

  @POST
  @Path("/{collection}")
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
      throw new ConflictException(configFile.getPath()+" already exists. A DELETE of /collections/{collection} is required first");
    }
    return Response.ok().build();
  }

  @GET
  @Path("/{collection}/kafkaConfig")
  public byte[] getKafkaConfig(@PathParam("collection") String collection) throws Exception
  {
    File kafkaConfigFile = new File(new File(rootDir, collection), StarTreeConstants.KAFKA_CONFIG_FILE_NAME);
    if (!kafkaConfigFile.exists())
    {
      throw new NotFoundException();
    }
    if (!kafkaConfigFile.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    return FileUtils.readFileToByteArray(kafkaConfigFile);
  }

  @POST
  @Path("/{collection}/kafkaConfig")
  public Response postKafkaConfig(@PathParam("collection") String collection, byte[] kafkaConfigBytes) throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      FileUtils.forceMkdir(collectionDir);
    }
    if (!collectionDir.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    File configFile = new File(collectionDir, StarTreeConstants.KAFKA_CONFIG_FILE_NAME);

    FileUtils.copyInputStreamToFile(new ByteArrayInputStream(kafkaConfigBytes), configFile);

    return Response.ok().build();
  }

  @DELETE
  @Path("/{collection}/kafkaConfig")
  public Response deleteKafkaConfig(@PathParam("collection") String collection) throws Exception
  {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.isAbsolute())
    {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    File kafkaConfigFile = new File(collectionDir, StarTreeConstants.KAFKA_CONFIG_FILE_NAME);
    if (!kafkaConfigFile.exists())
    {
      throw new NotFoundException();
    }

    FileUtils.forceDelete(kafkaConfigFile);

    return Response.noContent().build();
  }

  @POST
  @Path("/{collection}/data/{minTime}/{maxTime}")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Timed
  public Response postData(@PathParam("collection") String collection,
                           @PathParam("minTime") long minTimeMillis,
                           @PathParam("maxTime") long maxTimeMillis,
                           @QueryParam("schedule") @DefaultValue("UNKNOWN") String schedule,
                           byte[] dataBytes) throws Exception
  {
    dataUpdateManager.updateData(
        collection,
        schedule,
        new DateTime(minTimeMillis),
        new DateTime(maxTimeMillis),
        dataBytes);

    return Response.ok().build();
  }
}
