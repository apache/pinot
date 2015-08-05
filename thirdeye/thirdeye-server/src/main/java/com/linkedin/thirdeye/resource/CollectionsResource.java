  package com.linkedin.thirdeye.resource;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.NotFoundException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import io.dropwizard.lifecycle.Managed;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Path("/collections")
@Produces(MediaType.APPLICATION_JSON)
public class CollectionsResource implements Managed
{
  private static final String LAST_POST_DATA_MILLIS = "lastPostDataMillis";
  private static final String DATA_TIME_LAG_MILLIS = "dataTimeLagMillis";
  private static final Logger LOG = LoggerFactory.getLogger(CollectionsResource.class);

  private final StarTreeManager manager;
  private final File rootDir;
  private final DataUpdateManager dataUpdateManager;
  private final MetricRegistry metricRegistry;
  private final ConcurrentMap<String, AtomicLong> lastPostDataMillis;

  public CollectionsResource(StarTreeManager manager,
                             MetricRegistry metricRegistry,
                             DataUpdateManager dataUpdateManager,
                             File rootDir)
  {
    this.manager = manager;
    this.rootDir = rootDir;
    this.dataUpdateManager = dataUpdateManager;
    this.metricRegistry = metricRegistry;
    lastPostDataMillis = new ConcurrentHashMap<String, AtomicLong>();
  }

  @Override
  public void start() throws Exception {

    for (String collection : manager.getCollections())
    {
      final String collectionName = collection;
      // Metric for time we last received a POST to update collection's data
      lastPostDataMillis.putIfAbsent(collection, new AtomicLong(System.currentTimeMillis()));
      metricRegistry.register(MetricRegistry.name(CollectionsResource.class, collection, LAST_POST_DATA_MILLIS),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return lastPostDataMillis.get(collectionName).get();
            }
          });
    }
  }


  @Override
  public void stop() throws Exception {

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
      IOUtils.copy(new ByteArrayInputStream(configBytes), new FileOutputStream(configFile));
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

    return IOUtils.toByteArray(new FileInputStream(kafkaConfigFile));
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

    IOUtils.copy(new ByteArrayInputStream(kafkaConfigBytes), new FileOutputStream(configFile));

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
                           InputStream dataBytes) throws Exception
  {
    DateTime minTime = new DateTime(minTimeMillis, DateTimeZone.UTC);
    DateTime maxTime = new DateTime(maxTimeMillis, DateTimeZone.UTC);

    LOG.info("Received data for {} in {} to {}", collection, minTime, maxTime);

    dataUpdateManager.updateData(
        collection,
        schedule,
        minTime,
        maxTime,
        dataBytes);

    final String collectionName = collection;
    AtomicLong value = lastPostDataMillis.putIfAbsent(collectionName, new AtomicLong(System.currentTimeMillis()));
    if (value == null)
    {
      metricRegistry.register(MetricRegistry.name(CollectionsResource.class, collectionName, LAST_POST_DATA_MILLIS),
          new Gauge<Long>() {

            @Override
            public Long getValue() {
             return lastPostDataMillis.get(collectionName).get();
            }
          });
    }
    else
    {
      value.set((System.currentTimeMillis()));
    }

    if (!metricRegistry.getGauges().containsKey(MetricRegistry.name(CollectionsResource.class, collection, DATA_TIME_LAG_MILLIS))) {
      metricRegistry.register(MetricRegistry.name(CollectionsResource.class, collection, DATA_TIME_LAG_MILLIS),
          new Gauge<Long>() {

            @Override
            public Long getValue() {
              Long maxDataTime = manager.getMaxDataTime(collectionName);
              if (maxDataTime == null) {
                maxDataTime = 0L;
              }
              return System.currentTimeMillis() - maxDataTime;
            }
          });
    }

    return Response.ok().build();
  }

  @GET
  @Path("/{collection}/segments")
  public List<SegmentDescriptor> getSegments(@PathParam("collection") String collection) throws Exception {
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists()) {
      throw new NotFoundException("No collection " + collection);
    }

    StarTreeConfig config = manager.getConfig(collection);
    if (config == null) {
      throw new NotFoundException("No config for " + collection);
    }

    File[] dataDirs = collectionDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(StorageUtils.getDataDirPrefix());
      }
    });

    List<SegmentDescriptor> descriptors = new ArrayList<>();
    if (dataDirs != null) {
      for (File dataDir : dataDirs) {
        // Segment metadata
        File metadataFile = new File(dataDir, StarTreeConstants.METADATA_FILE_NAME);
        Properties properties = new Properties();
        InputStream inputStream = new FileInputStream(metadataFile);
        try {
          properties.load(inputStream);
        } finally {
          inputStream.close();
        }

        IndexMetadata indexMetadata = IndexMetadata.fromProperties(properties);
        DateTime startWallTime= new DateTime(indexMetadata.getStartTimeMillis());
        DateTime endWallTime= new DateTime(indexMetadata.getEndTimeMillis());
        descriptors.add(new SegmentDescriptor(dataDir, startWallTime, endWallTime, new DateTime(
            indexMetadata.getMinDataTimeMillis()), new DateTime(indexMetadata.getMaxDataTimeMillis())));
      }
    }

    // A "virtual" segment descriptor for the in-memory tree
    StarTree mutableTree = manager.getMutableStarTree(collection);
    if (mutableTree != null) {
      StarTreeStats stats = mutableTree.getStats();
      DateTime minTime = collectionTimeToDateTime(mutableTree.getConfig(), stats.getMinTime());
      DateTime maxTime = collectionTimeToDateTime(mutableTree.getConfig(), stats.getMaxTime());
      SegmentDescriptor descriptor = new SegmentDescriptor(null, null, null, minTime, maxTime);
      descriptors.add(descriptor);
    }

    return descriptors;
  }

  private static DateTime collectionTimeToDateTime(StarTreeConfig config, long collectionTime) {
    if (collectionTime == -1) {
      return null;
    }
    TimeGranularity bucket = config.getTime().getBucket();
    long millis = TimeUnit.MILLISECONDS.convert(collectionTime * bucket.getSize(), bucket.getUnit());
    return new DateTime(millis);
  }
}

