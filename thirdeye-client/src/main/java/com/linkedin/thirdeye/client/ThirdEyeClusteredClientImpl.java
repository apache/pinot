package com.linkedin.thirdeye.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.ThirdEyeMetrics;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.State;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThirdEyeClusteredClientImpl implements ThirdEyeClient, IdealStateChangeListener
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference RESULT_LIST_REF = new TypeReference<List<ThirdEyeMetrics>>(){};
  private static final TypeReference TIME_SERIES_LIST_REF = new TypeReference<List<ThirdEyeTimeSeries>>(){};
  private static final TypeReference DIMENSION_VALUES_REF = new TypeReference<Map<String, List<String>>>(){};

  private static final String INSTANCE_NAME = "THIRDEYE_CLIENT";
  private static final State ONLINE = State.from("ONLINE");
  private static final Joiner EQUALS_JOINER = Joiner.on("=");
  private static final Joiner AND_JOINER = Joiner.on("&");
  private static final String QUERY_SEPARATOR = "?";

  private final ClientConfig clientConfig;
  private final AtomicBoolean isConnected;
  private final Map<String, Integer> collections;
  private final Map<String, StarTree> starTrees;
  private final Map<String, Map<String, List<String>>> dimensionValues;
  private final ReadWriteLock lock;
  private final RoutingTableProvider routingTable;

  private HelixManager helixManager;
  private CloseableHttpAsyncClient httpAsyncClient;

  public ThirdEyeClusteredClientImpl(ClientConfig clientConfig)
  {
    this.clientConfig = clientConfig.validate();
    this.isConnected = new AtomicBoolean();
    this.collections = new HashMap<String, Integer>();
    this.starTrees = new HashMap<String, StarTree>();
    this.dimensionValues = new HashMap<String, Map<String, List<String>>>();
    this.lock = new ReentrantReadWriteLock();
    this.routingTable = new RoutingTableProvider();
  }

  public static class ClientConfig
  {
    private String zkAddress;
    private String clusterName;
    private int requestTimeoutMillis = 5000;

    public String getZkAddress()
    {
      return zkAddress;
    }

    public ClientConfig setZkAddress(String zkAddress)
    {
      this.zkAddress = zkAddress;
      return this;
    }

    public String getClusterName()
    {
      return clusterName;
    }

    public ClientConfig setClusterName(String clusterName)
    {
      this.clusterName = clusterName;
      return this;
    }

    public int getRequestTimeoutMillis()
    {
      return requestTimeoutMillis;
    }

    public ClientConfig setRequestTimeoutMillis(int requestTimeoutMillis)
    {
      this.requestTimeoutMillis = requestTimeoutMillis;
      return this;
    }

    public ClientConfig validate()
    {
      if (zkAddress == null)
      {
        throw new IllegalStateException("Must specify zkAddress");
      }

      if(clusterName == null)
      {
        throw new IllegalStateException("Must specify clusterName");
      }

      return this;
    }
  }

  @Override
  public void connect() throws Exception
  {
    if (!isConnected.getAndSet(true))
    {
      httpAsyncClient = HttpAsyncClients.createDefault();
      httpAsyncClient.start();

      helixManager
              = HelixManagerFactory.getZKHelixManager(clientConfig.getClusterName(),
                                                      INSTANCE_NAME,
                                                      InstanceType.SPECTATOR,
                                                      clientConfig.getZkAddress());
      helixManager.connect();
      helixManager.addIdealStateChangeListener(this);
      helixManager.addExternalViewChangeListener(routingTable);

      List<String> collections = helixManager.getClusterManagmentTool().getResourcesInCluster(clientConfig.getClusterName());
      if (collections != null)
      {
        List<IdealState> idealStates = new ArrayList<IdealState>(collections.size());
        for (String collection : collections)
        {
          IdealState idealState = helixManager.getClusterManagmentTool().getResourceIdealState(clientConfig.getClusterName(), collection);
          if (idealState != null)
          {
            idealStates.add(idealState);
          }
        }
        onIdealStateChange(idealStates, null);
      }
    }
  }

  @Override
  public void disconnect() throws Exception
  {
    if (isConnected.getAndSet(false))
    {
      httpAsyncClient.close();
      helixManager.disconnect();

      lock.writeLock().lock();
      try
      {
        collections.clear();
      }
      finally
      {
        lock.writeLock().unlock();
      }
    }
  }

  @Override
  public Set<String> getCollections()
  {
    lock.readLock().lock();
    try
    {
      return ImmutableSet.copyOf(collections.keySet());
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<ThirdEyeMetrics> getAggregates(String collection) throws IOException
  {
    return getAggregates(collection,
                         getBuilder(collection, null)
                                 .build(getStarTree(collection).getConfig()));
  }

  @Override
  public List<ThirdEyeMetrics> getAggregates(String collection,
                                               Map<String, String> dimensionValues) throws IOException
  {
    return getAggregates(collection,
                         getBuilder(collection, dimensionValues)
                                 .build(getStarTree(collection).getConfig()));
  }

  @Override
  public List<ThirdEyeMetrics> getAggregates(String collection,
                                             Map<String, String> dimensionValues,
                                             Long start,
                                             Long end) throws IOException
  {
    return getAggregates(collection,
                         getBuilder(collection, dimensionValues)
                                 .setTimeRange(new TimeRange(start, end))
                                 .build(getStarTree(collection).getConfig()));
  }

  private StarTreeQueryImpl.Builder getBuilder(String collection, Map<String, String> dimensionValues) throws IOException
  {
    StarTreeQueryImpl.Builder builder = new StarTreeQueryImpl.Builder();

    StarTree starTree = getStarTree(collection);

    builder.setDimensionKey(convertToDimensionKey(starTree.getConfig(), dimensionValues));

    return builder;
  }

  private List<ThirdEyeMetrics> getAggregates(final String collection,
                                                final StarTreeQuery query) throws IOException
  {
    try
    {
      StarTree starTree = getStarTree(collection);

      // Expand queries
      List<StarTreeQuery> queries = expandQueries(collection, starTree, query);

      // The explicitly specified dimension values are used as filter on expanded queries
      queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, getFilter(starTree.getConfig(), query));

      // Find query -> node mapping
      Map<StarTreeQuery, UUID> queryToNodeId = getQueryToNodeId(starTree, queries);

      // Find node -> replica mapping
      Map<UUID, HttpHost> nodeIdToHost = getNodeIdToHost(collection, collections.get(collection), queryToNodeId.values());

      // Execute in parallel
      Set<Future<HttpResponse>> responses = new HashSet<Future<HttpResponse>>(nodeIdToHost.size());
      for (Map.Entry<StarTreeQuery, UUID> entry : queryToNodeId.entrySet())
      {
        HttpHost host = nodeIdToHost.get(entry.getValue());
        HttpGet req = new HttpGet(getMetricsUri(collection, starTree.getConfig(), entry.getKey()));
        responses.add(httpAsyncClient.execute(host, req, null));
      }

      // Group responses
      List<ThirdEyeMetrics> allResults = new ArrayList<ThirdEyeMetrics>();
      for (Future<HttpResponse> entry : responses)
      {
        HttpResponse response = entry.get(clientConfig.getRequestTimeoutMillis(), TimeUnit.MILLISECONDS);

        if (response.getStatusLine().getStatusCode() != 200)
        {
          throw new IOException(response.getStatusLine().getStatusCode() + ": " + response.getStatusLine().getReasonPhrase());
        }

        // Parse
        List<ThirdEyeMetrics> results = OBJECT_MAPPER.readValue(response.getEntity().getContent(), RESULT_LIST_REF);
        EntityUtils.consume(response.getEntity());
        allResults.addAll(results);
      }

      return allResults;
    }
    catch (IOException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public List<ThirdEyeTimeSeries> getTimeSeries(final String collection,
                                                final String metricName,
                                                final Long start,
                                                final Long end,
                                                final Map<String, String> dimensionValues) throws IOException
  {
    try
    {
      StarTree starTree = getStarTree(collection);

      // Convert to query
      StarTreeQuery query = new StarTreeQueryImpl.Builder()
              .setDimensionKey(convertToDimensionKey(starTree.getConfig(), dimensionValues))
              .setTimeRange(new TimeRange(start, end))
              .build(starTree.getConfig());

      // Expand queries
      List<StarTreeQuery> queries = expandQueries(collection, starTree, query);

      // The explicitly specified dimension values are used as filter on expanded queries
      queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, getFilter(starTree.getConfig(), query));

      // Find query -> node mapping
      Map<StarTreeQuery, UUID> queryToNodeId = getQueryToNodeId(starTree, queries);

      // Find node -> replica mapping
      Map<UUID, HttpHost> nodeIdToHost = getNodeIdToHost(collection, collections.get(collection), queryToNodeId.values());

      // Execute in parallel
      Set<Future<HttpResponse>> responses = new HashSet<Future<HttpResponse>>(nodeIdToHost.size());
      for (Map.Entry<StarTreeQuery, UUID> entry : queryToNodeId.entrySet())
      {
        HttpHost host = nodeIdToHost.get(entry.getValue());
        HttpGet req = new HttpGet(getTimeSeriesUri(collection, metricName, starTree.getConfig(), entry.getKey()));
        responses.add(httpAsyncClient.execute(host, req, null));
      }

      // Group responses
      List<ThirdEyeTimeSeries> allResults = new ArrayList<ThirdEyeTimeSeries>();
      for (Future<HttpResponse> entry : responses)
      {
        HttpResponse response = entry.get(clientConfig.getRequestTimeoutMillis(), TimeUnit.MILLISECONDS);

        if (response.getStatusLine().getStatusCode() != 200)
        {
          throw new IOException(response.getStatusLine().getStatusCode() + ": " + response.getStatusLine().getReasonPhrase());
        }

        // Parse
        List<ThirdEyeTimeSeries> results = OBJECT_MAPPER.readValue(response.getEntity().getContent(), TIME_SERIES_LIST_REF);
        EntityUtils.consume(response.getEntity());
        allResults.addAll(results);
      }

      return allResults;
    }
    catch (IOException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext)
  {
    lock.writeLock().lock();
    try
    {
      collections.clear();

      for (IdealState idealState : idealStates)
      {
        collections.put(idealState.getResourceName(), idealState.getNumPartitions());
      }

      for (String collection : starTrees.keySet())
      {
        if (!collections.containsKey(collection))
        {
          starTrees.remove(collection);
        }
      }
    }
    finally
    {
      lock.writeLock().unlock();
    }
  }

  /** Gets and lazily instantiates star tree structure (i.e. StarTreeNode) */
  private StarTree getStarTree(String collection) throws IOException
  {
    StarTree starTree = null;

    lock.readLock().lock();
    try
    {
      starTree = starTrees.get(collection);
    }
    finally
    {
      lock.readLock().unlock();
    }

    // Lazily instantiate
    if (starTree == null)
    {
      lock.writeLock().lock();
      try
      {
        starTree = starTrees.get(collection);
        if (starTree == null)
        {
          // Find an online partition
          Set<InstanceConfig> instances = routingTable.getInstances(collection, ONLINE.toString());
          if (instances.isEmpty())
          {
            throw new IllegalStateException("No ONLINE partitions for " + collection);
          }
          InstanceConfig someInstance = instances.iterator().next();
          String[] hostPort = someInstance.getInstanceName().split("_");
          HttpHost host = new HttpHost(hostPort[0], Integer.valueOf(hostPort[1]));

          // Get config
          HttpGet req = new HttpGet(getCollectionUri(collection, "config"));
          Future<HttpResponse> res = httpAsyncClient.execute(host, req, null);
          if (res.get().getStatusLine().getStatusCode() != 200)
          {
            throw new IllegalStateException("Could not retrieve star tree for collection " + collection);
          }
          StarTreeConfig config = StarTreeConfig.decode(res.get().getEntity().getContent());
          EntityUtils.consume(res.get().getEntity());

          // Get tree structure
          req = new HttpGet(getCollectionUri(collection, "starTree"));
          res = httpAsyncClient.execute(host, req, null);
          if (res.get().getStatusLine().getStatusCode() != 200)
          {
            throw new IllegalStateException("Could not retrieve star tree for collection " + collection);
          }
          ObjectInputStream ois = new ObjectInputStream(res.get().getEntity().getContent());
          StarTreeNode root = (StarTreeNode) ois.readObject();
          EntityUtils.consume(res.get().getEntity());

          // Get dimension values
          req = new HttpGet(getDimensionsUri(collection));
          res = httpAsyncClient.execute(host, req, null);
          if (res.get().getStatusLine().getStatusCode() != 200)
          {
            throw new IllegalStateException("Could not retrieve dimension values for collection " + collection);
          }
          Map<String, List<String>> values = OBJECT_MAPPER.readValue(res.get().getEntity().getContent(), DIMENSION_VALUES_REF);
          dimensionValues.put(collection, values);

          // Register star tree
          starTree = new StarTreeImpl(config, null, root);
          starTrees.put(collection, starTree);
        }
      }
      catch (IOException e)
      {
        throw e;
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
      finally
      {
        lock.writeLock().unlock();
      }
    }

    return starTree;
  }

  /** Creates the resource URI for the collection's star tree */
  private String getCollectionUri(String collection, String subResource) throws IOException
  {
    return "/collections/" + URLEncoder.encode(collection, "UTF-8") + "/" + subResource;
  }

  private String getDimensionsUri(String collection) throws IOException
  {
    return "/dimensions/" + URLEncoder.encode(collection, "UTF-8");
  }

  private String getMetricsUri(String collection, StarTreeConfig config, StarTreeQuery query) throws IOException
  {
    StringBuilder sb = new StringBuilder();

    sb.append("/metrics/").append(URLEncoder.encode(collection, "UTF-8"));

    if (query.getTimeRange() != null)
    {
      sb.append("/").append(query.getTimeRange().getStart())
        .append("/").append(query.getTimeRange().getEnd());
    }

    return appendDimensions(sb, config, query).toString();
  }

  private String getTimeSeriesUri(String collection, String metricName, StarTreeConfig config, StarTreeQuery query) throws IOException
  {
    if (query.getTimeRange() == null)
    {
      throw new IllegalArgumentException("Query must have time range");
    }

    StringBuilder sb = new StringBuilder();

    sb.append("/timeSeries")
      .append("/").append(URLEncoder.encode(collection, "UTF-8"))
      .append("/").append(URLEncoder.encode(metricName, "UTF-8"))
      .append("/").append(query.getTimeRange().getStart())
      .append("/").append(query.getTimeRange().getEnd());

    return appendDimensions(sb, config, query).toString();
  }

  private StringBuilder appendDimensions(StringBuilder sb, StarTreeConfig config, StarTreeQuery query) throws IOException
  {
    List<String> queryParts = new ArrayList<String>(query.getDimensionKey().getDimensionValues().length);

    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();
      String dimensionValue = query.getDimensionKey().getDimensionValues()[i];

      if (!StarTreeConstants.STAR.equals(dimensionValue))
      {
        queryParts.add(EQUALS_JOINER.join(URLEncoder.encode(dimensionName, "UTF-8"), URLEncoder.encode(dimensionValue, "UTF-8")));
      }
    }

    if (!queryParts.isEmpty())
    {
      sb.append(QUERY_SEPARATOR).append(AND_JOINER.join(queryParts));
    }

    return sb;
  }

  /** Gets a mapping of queries to all the node ids */
  private Map<StarTreeQuery, UUID> getQueryToNodeId(StarTree starTree, Collection<StarTreeQuery> queries)
  {
    Map<StarTreeQuery, UUID> queryToNodeId = new HashMap<StarTreeQuery, UUID>(queries.size());

    for (StarTreeQuery query : queries)
    {
      StarTreeNode node = starTree.find(query);
      if (node == null)
      {
        throw new IllegalArgumentException("No node for query " + query);
      }
      queryToNodeId.put(query, node.getId());
    }

    return queryToNodeId;
  }

  /** Uses routing table to determine which participants host which leaves */
  private Map<UUID, HttpHost> getNodeIdToHost(String collection, int numPartitions, Collection<UUID> nodeIds)
  {
    Map<UUID, HttpHost> nodeIdToHost = new HashMap<UUID, HttpHost>(nodeIds.size());

    for (UUID nodeId : nodeIds)
    {
      String partitionName = String.format("%s_%d", collection, StarTreeUtils.getPartitionId(nodeId, numPartitions));
      List<InstanceConfig> instances = routingTable.getInstances(collection, partitionName, ONLINE.toString());
      if (instances == null || instances.isEmpty())
      {
        throw new IllegalStateException("No ONLINE replica of " + partitionName + " for " + nodeId);
      }
      InstanceConfig instance = instances.iterator().next();
      String[] hostPort = instance.getInstanceName().split("_");
      nodeIdToHost.put(nodeId, new HttpHost(hostPort[0], Integer.valueOf(hostPort[1])));
    }

    return nodeIdToHost;
  }

  private List<StarTreeQuery> expandQueries(String collection, StarTree starTree, StarTreeQuery baseQuery)
  {
    Set<String> dimensionsToExpand = new HashSet<String>();
    List<DimensionSpec> dimensionSpecs = starTree.getConfig().getDimensions();

    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      if (StarTreeConstants.ALL.equals(baseQuery.getDimensionKey().getDimensionValues()[i]))
      {
        dimensionsToExpand.add(dimensionSpecs.get(i).getName());
      }
    }

    List<StarTreeQuery> queries = new LinkedList<StarTreeQuery>();
    queries.add(baseQuery);

    // Expand "!" (all) dimension values into multiple queries
    for (String dimensionName : dimensionsToExpand)
    {
      // For each existing getAggregate, add a new one with these
      List<StarTreeQuery> expandedQueries = new ArrayList<StarTreeQuery>();
      for (StarTreeQuery query : queries)
      {
        Map<String, List<String>> allValues = dimensionValues.get(collection);
        if (allValues == null)
        {
          throw new IllegalStateException("No dimension values for collection " + collection);
        }
        List<String> values = allValues.get(dimensionName);

        for (String value : values)
        {
          // Create new key
          String[] newValues = new String[dimensionSpecs.size()];
          for (int i = 0; i < dimensionSpecs.size(); i++)
          {
            newValues[i] = dimensionSpecs.get(i).getName().equals(dimensionName)
                    ? value
                    : query.getDimensionKey().getDimensionValues()[i];
          }

          // Copy original getAggregate with new value
          expandedQueries.add(new StarTreeQueryImpl.Builder()
                                      .setDimensionKey(new DimensionKey(newValues))
                                      .setTimeRange(query.getTimeRange())
                                      .build(starTree.getConfig()));
        }
      }

      // Reset list of queries
      queries = expandedQueries;
    }

    return queries;
  }

  /**
   * Converts dimension values to a dimension key, replacing unspecified values with STAR
   */
  private DimensionKey convertToDimensionKey(StarTreeConfig config, Map<String, String> dimensionValues)
  {
    String[] key = new String[config.getDimensions().size()];

    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();

      key[i] = StarTreeConstants.STAR;

      if (dimensionValues != null)
      {
        key[i] = dimensionValues.containsKey(dimensionName)
                ? dimensionValues.get(dimensionName)
                : StarTreeConstants.STAR;
      }
    }

    return new DimensionKey(key);
  }

  private Map<String, List<String>> getFilter(StarTreeConfig config, StarTreeQuery query)
  {
    Map<String, List<String>> filter = new HashMap<String, List<String>>(config.getDimensions().size());

    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();
      String dimensionValue = query.getDimensionKey().getDimensionValues()[i];

      if (!StarTreeConstants.ALL.equals(dimensionValue) || StarTreeConstants.STAR.equals(dimensionValue))
      {
        filter.put(dimensionName, Arrays.asList(dimensionValue));
      }
    }

    return filter;
  }
}
