package com.linkedin.thirdeye.client;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Objects;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.util.ThirdEyeClientConfig;

/**
 * Map used to create {@link com.linkedin.thirdeye.client.ThirdEyeClient} instances for each
 * provided {@link ThirdEyeClientConfig} and retrieve them via their associated collections. Because
 * clients may have duplicate collection names, the specific keys are prefixed with the
 * configuration name.
 * <br/>
 * While methods that directly insert or remove clients are provided, they
 * should be used with caution as any changes made via these methods may be lost if the map is
 * reloaded from a file.
 * @author jteoh
 */
public class CollectionMapThirdEyeClient implements ThirdEyeClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionMapThirdEyeClient.class);
  private static ObjectMapper ymlReader = new ObjectMapper(new YAMLFactory());

  private List<ThirdEyeClientConfig> configs;
  private boolean needToInitializeClients = true;
  private final Map<CollectionKey, ThirdEyeClient> clientMap = new HashMap<>();
  private final List<ThirdEyeClient> clients = new ArrayList<>();
  private final Set<String> collections = new LinkedHashSet<>();

  public CollectionMapThirdEyeClient() throws Exception {
    this(new ArrayList<ThirdEyeClientConfig>());
  }

  public CollectionMapThirdEyeClient(List<ThirdEyeClientConfig> configs) throws Exception {
    setConfigs(configs);
  }

  public static CollectionMapThirdEyeClient fromFolder(String folderPath) throws Exception {
    List<ThirdEyeClientConfig> configs = readFolder(folderPath);
    return new CollectionMapThirdEyeClient(configs);
  }

  public void reloadFromFolder(String folderPath) throws Exception {
    List<ThirdEyeClientConfig> configs = readFolder(folderPath);
    this.setConfigs(configs);
  }

  /** Loads all client configurations in the provided folder. */
  public static List<ThirdEyeClientConfig> readFolder(String folderPath) {
    List<ThirdEyeClientConfig> configs = new ArrayList<ThirdEyeClientConfig>();
    File folder = new File(folderPath);
    if (!folder.exists() || !folder.isDirectory()) {
      LOGGER.error("{} does not exist or is not a directory!", folderPath);
      return Collections.emptyList();
    }
    for (File file : folder.listFiles()) {
      List<ThirdEyeClientConfig> fileClients = readFile(file.getAbsolutePath());
      configs.addAll(fileClients);
    }
    return configs;
  }

  public static List<ThirdEyeClientConfig> readFile(String filePath) {
    File file = new File(filePath);
    if (!file.exists()) {
      LOGGER.error("ThirdEyeClient configuration file does not exist: {}", file);
      return Collections.emptyList();
    }
    try {
      List<ThirdEyeClientConfig> fileClients = ymlReader.readValue(file, ymlReader.getTypeFactory()
          .constructCollectionType(List.class, ThirdEyeClientConfig.class));
      LOGGER.info("Successfully read client configs from {}: {}", file, fileClients);
      return fileClients;
    } catch (Exception e) {
      LOGGER.error("Unable to load ThirdEyeClient configuration file: {}", file);
      return Collections.emptyList();
    }

  }

  public void setConfigs(List<ThirdEyeClientConfig> configs) throws Exception {
    // n.b. this class delays instantiating clients and to reduce startup failures.
    this.needToInitializeClients = true;
    this.configs = configs;
  }

  /**
   * Initializes clients if not already initialized. This is provided so that the clients are only
   * loaded when required and the normal deployment process will not fail in the event of a broken
   * configuration.
   */
  private void initializeClients() throws Exception {
    if (this.needToInitializeClients) {
      loadConfigs();
      this.needToInitializeClients = false;
    }
  }

  private void loadConfigs() throws Exception {
    clear();
    if (configs == null || configs.isEmpty()) {
      LOGGER.warn("No configurations provided, using empty map.");
      return;
    }
    for (ThirdEyeClientConfig config : configs) {
      try {
        LOGGER.info("Building client: {}", config);
        createClient(config);
      } catch (Exception e) {
        LOGGER.error("Unable to create client and retrieve collections for config: {}", config);
        LOGGER.error("Client exception was: ", e);
      }
    }
  }

  public List<ThirdEyeClient> getClients() throws Exception {
    initializeClients();
    return Collections.unmodifiableList(this.clients);
  }

  public List<ThirdEyeClientConfig> getClientConfigs() throws Exception {
    return Collections.unmodifiableList(this.configs);
  }

  public ThirdEyeClient get(String collection) throws Exception {
    initializeClients();
    CollectionKey collectionKey = new CollectionKey(collection);
    if (!clientMap.containsKey(collectionKey)) {
      throw new IllegalArgumentException(
          "No ThirdEyeClient exists for collection: " + collectionKey);
    }
    return clientMap.get(collectionKey);
  }

  /** Retrieves the client associated with the request's collection. */
  public ThirdEyeClient get(ThirdEyeRequest request) throws Exception {
    return get(request.getCollection());
  }

  /**
   * Use with caution: any changes made via these methods may be lost if the map is reloaded from
   * client configurations.
   * @return the previous client associated with the collection, or null if not applicable.
   */
  public ThirdEyeClient put(String collection, ThirdEyeClient client) {
    this.needToInitializeClients = false;
    CollectionKey collectionKey = new CollectionKey(collection);
    ThirdEyeClient prev = clientMap.put(collectionKey, client);
    if (prev != null) {
      LOGGER.info("Replacing existing client for collection {}: {}", collectionKey, client);
    }
    return prev;
  }

  /** Builds client from config and adds it to the existing map. */
  public Map<String, ThirdEyeClient> createClient(ThirdEyeClientConfig config) throws Exception {
    ThirdEyeClient client = config.buildClient();
    return add(client, config.getName());
  }

  /**
   * Use with caution: any changes made via these methods may be lost if the map is reloaded from
   * client configurations.
   * @return A map containing previous clients associated with any of the collections returned by
   *         this client
   * @throws Exception
   */
  public Map<String, ThirdEyeClient> add(ThirdEyeClient client, String collectionPrefix)
      throws Exception {
    this.needToInitializeClients = false;
    clients.add(client);
    HashMap<String, ThirdEyeClient> replacedMap = new HashMap<String, ThirdEyeClient>();
    for (String collection : client.getCollections()) {
      String collectionKey = CollectionKey.getFullKey(collectionPrefix, collection);
      ThirdEyeClient replaced = this.put(collectionKey, client);
      this.collections.add(collectionKey);
      if (replaced != null) {
        replacedMap.put(collectionKey, replaced);
      }
    }
    return replacedMap;
  }

  /**
   * Use with caution: any changes made via these methods may be lost if the map is reloaded from
   * client configurations.
   */
  public ThirdEyeClient remove(String collection) {
    this.needToInitializeClients = false;
    return clientMap.remove(collection);
  }

  @Override
  public Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception {
    initializeClients();
    return get(request).execute(getBaseRequest(request));
  }

  @Override
  public ThirdEyeRawResponse getRawResponse(ThirdEyeRequest request) throws Exception {
    initializeClients();
    return get(request).getRawResponse(getBaseRequest(request));
  }

  @Override
  public StarTreeConfig getStarTreeConfig(String collection) throws Exception {
    initializeClients();
    return get(collection).getStarTreeConfig(CollectionKey.getBaseCollection(collection));
  }

  /** Returns collections in order returned by the clients added to this map. */
  @Override
  public List<String> getCollections() throws Exception {
    initializeClients();
    return Collections.unmodifiableList(new ArrayList<>(this.collections));
  }

  @Override
  public List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception {
    initializeClients();
    return get(collection).getSegmentDescriptors(CollectionKey.getBaseCollection(collection));
  }

  @Override
  public long getExpectedTimeBuckets(ThirdEyeRequest request) throws Exception {
    return get(request).getExpectedTimeBuckets(getBaseRequest(request));
  }

  @Override
  public List<String> getExpectedTimestamps(ThirdEyeRequest request) throws Exception {
    return get(request).getExpectedTimestamps(getBaseRequest(request));
  }

  private ThirdEyeRequest getBaseRequest(ThirdEyeRequest request) {
    ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder(request);
    builder.setCollection(CollectionKey.getBaseCollection(request.getCollection()));
    return builder.build();
  }

  @Override
  public void clear() throws Exception {
    LOGGER.info("Clearing ThirdEyeClients...");
    for (ThirdEyeClient client : clients) {
      client.clear();
    }
    close();
    clientMap.clear();
    clients.clear();
    collections.clear();
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing ThirdEyeClients...");
    for (ThirdEyeClient client : clients) {
      client.close();
    }
  }

  @Override
  public String toString() {
    return clientMap.toString();
  }

  /**
   * Key used to represent a collection within {@link CollectionMapThirdEyeClient}. It consists of
   * two parts, a prefix (config name) and the base collection as viewed by the client.
   */
  static class CollectionKey {
    public final static String SEPARATOR = ":";
    public final String fullKey;
    public final String prefix;
    public final String baseCollection;

    /** Input should consist of two strings separated by {@value #SEPARATOR} */
    CollectionKey(String key) {
      String[] split = splitKey(key);
      this.prefix = split[0];
      this.baseCollection = split[1];

      this.fullKey = key;
    }

    CollectionKey(String prefix, String collection) {
      this.prefix = prefix;
      this.baseCollection = collection;
      this.fullKey = getFullKey(prefix, collection);
    }

    public static String getFullKey(String prefix, String collection) {
      if (prefix == null || prefix.contains(SEPARATOR)) {
        throw new IllegalArgumentException("Prefix cannot be null or contain '" + SEPARATOR + "'");
      }
      if (collection == null || collection.contains(SEPARATOR)) {
        throw new IllegalArgumentException(
            "Collection name cannot be null or contain '" + SEPARATOR + "'");
      }
      return prefix + SEPARATOR + collection;

    }

    /** Input should consist of two strings separated by {@value #SEPARATOR} */
    public static String getPrefix(String fullKey) {
      return splitKey(fullKey)[0];
    }

    /** Input should consist of two strings separated by {@value #SEPARATOR} */
    public static String getBaseCollection(String fullKey) {
      return splitKey(fullKey)[1];
    }

    /**
     * Splits input key into 2-D String array of [prefix,collection]
     */
    public static String[] splitKey(String key) throws IllegalArgumentException {
      if (key == null || StringUtils.countMatches(key, SEPARATOR) != 1) {
        throw new IllegalArgumentException("Key cannot be null and must be of the form  "
            + "'prefix" + SEPARATOR + "collection' with exactly one '" + SEPARATOR + "'");
      }
      String[] split = key.split(SEPARATOR);
      return split;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(fullKey);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CollectionKey)) {
        return false;
      }
      CollectionKey other = (CollectionKey) obj;
      return Objects.equal(fullKey, other.fullKey);
    }

    @Override
    public String toString() {
      return fullKey;
    }

  }

}
