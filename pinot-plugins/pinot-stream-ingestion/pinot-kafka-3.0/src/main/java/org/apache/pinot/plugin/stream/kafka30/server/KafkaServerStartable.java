/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.stream.kafka30.server;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kafka 3.x implementation of {@link StreamDataServerStartable}.
 *
 * This class can either connect to an external broker or start a managed single-node KRaft
 * container for local quickstart usage. It is not thread-safe.
 */
public class KafkaServerStartable implements StreamDataServerStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServerStartable.class);

  private static final String KAFKA_IMAGE = "apache/kafka:3.9.1";
  private static final int KAFKA_STARTUP_TIMEOUT_SECONDS = 60;

  private static final int DEFAULT_BROKER_ID = 0;
  private static final int DEFAULT_KAFKA_PORT = 19092;
  private static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;

  private static final String KAFKA_SERVER_BOOTSTRAP_SERVERS = "kafka.server.bootstrap.servers";
  private static final String KAFKA_SERVER_PORT = "kafka.server.port";
  private static final String KAFKA_SERVER_BROKER_ID = "kafka.server.broker.id";
  private static final String KAFKA_SERVER_OWNER_NAME = "kafka.server.owner.name";
  private static final String KAFKA_SERVER_ALLOW_MANAGED_FOR_CONFIGURED_BROKER =
      "kafka.server.allow.managed.for.configured.broker";
  private static final String KAFKA_SERVER_CONTAINER_NAME = "kafka.server.container.name";
  private static final String KAFKA_SERVER_NETWORK_NAME = "kafka.server.network.name";
  private static final String KAFKA_SERVER_CLUSTER_ID = "kafka.server.cluster.id";
  private static final String KAFKA_SERVER_CLUSTER_SIZE = "kafka.server.cluster.size";
  private static final String KAFKA_SERVER_CONTROLLER_QUORUM_VOTERS = "kafka.server.controller.quorum.voters";
  private static final String KAFKA_SERVER_INTERNAL_HOST = "kafka.server.internal.host";
  private static final String KAFKA_SERVER_SKIP_READINESS_CHECK = "kafka.server.skip.readiness.check";

  private static final String KAFKA_EXTERNAL_LISTENER_PORT = "9094";
  private static final String KAFKA_BROKER_LISTENER_PORT = "9092";
  private static final String KAFKA_CONTROLLER_LISTENER_PORT = "9093";
  private static final int TOPIC_MUTATION_RETRIES = 5;
  private static final int DOCKER_NETWORK_READY_RETRIES = 10;
  private static final long DOCKER_NETWORK_READY_WAIT_MS = 500L;
  private static final int DOCKER_NETWORK_REMOVE_RETRIES = 5;
  private static final long DOCKER_NETWORK_REMOVE_RETRY_WAIT_MS = 500L;
  private static final int DOCKER_PULL_RETRIES = 3;
  private static final long DOCKER_PULL_RETRY_WAIT_MS = 2_000L;
  private static final int CONTAINER_STOP_TIMEOUT_SECONDS = 30;
  private static final int PORT_RELEASE_TIMEOUT_SECONDS = 30;
  private static final long STOP_POLL_INTERVAL_MS = 200L;
  private static final int PROCESS_TIMEOUT_SECONDS = 120;

  private static final String LOCALHOST = "localhost";
  private static final String LOOPBACK = "127.0.0.1";
  private static final Map<String, AtomicInteger> MANAGED_NETWORK_USAGE_COUNTS = new ConcurrentHashMap<>();
  private static final Set<String> MANAGED_NETWORKS_CREATED = ConcurrentHashMap.newKeySet();

  private String _ownerName = KafkaServerStartable.class.getSimpleName();
  private String _kafkaBrokerList = DEFAULT_KAFKA_BROKER;
  private int _kafkaPort = DEFAULT_KAFKA_PORT;
  private int _kafkaBrokerId = DEFAULT_BROKER_ID;
  private boolean _allowManagedKafkaForConfiguredBroker;
  private String _containerName;
  private String _dockerNetworkName;
  private String _clusterId;
  private String _controllerQuorumVoters;
  private String _internalHost = LOCALHOST;
  private int _clusterSize = 1;
  private boolean _skipReadinessCheck;

  private boolean _started;
  private String _resolvedKafkaBrokerList;
  private String _managedKafkaContainerName;
  private boolean _registeredManagedNetworkUsage;
  private Thread _shutdownHookThread;

  @Override
  public void init(Properties props) {
    _ownerName = props.getProperty(KAFKA_SERVER_OWNER_NAME, _ownerName);
    _kafkaBrokerList = props.getProperty(KAFKA_SERVER_BOOTSTRAP_SERVERS, _kafkaBrokerList);
    _kafkaPort = parseInt(props.getProperty(KAFKA_SERVER_PORT), parsePort(_kafkaBrokerList, DEFAULT_KAFKA_PORT));
    _kafkaBrokerId = parseInt(props.getProperty(KAFKA_SERVER_BROKER_ID), DEFAULT_BROKER_ID);
    _allowManagedKafkaForConfiguredBroker =
        Boolean.parseBoolean(props.getProperty(KAFKA_SERVER_ALLOW_MANAGED_FOR_CONFIGURED_BROKER, "false"));
    _containerName = props.getProperty(KAFKA_SERVER_CONTAINER_NAME);
    _dockerNetworkName = props.getProperty(KAFKA_SERVER_NETWORK_NAME);
    _clusterId = props.getProperty(KAFKA_SERVER_CLUSTER_ID);
    _controllerQuorumVoters = props.getProperty(KAFKA_SERVER_CONTROLLER_QUORUM_VOTERS);
    _internalHost = props.getProperty(KAFKA_SERVER_INTERNAL_HOST, LOCALHOST);
    _clusterSize = Math.max(1, parseInt(props.getProperty(KAFKA_SERVER_CLUSTER_SIZE), 1));
    _skipReadinessCheck = Boolean.parseBoolean(props.getProperty(KAFKA_SERVER_SKIP_READINESS_CHECK, "false"));
  }

  @Override
  public void start() {
    if (_started) {
      return;
    }

    try {
      boolean startManagedKafka = shouldStartManagedKafka(_kafkaBrokerList) && !isKafkaAvailable(_kafkaBrokerList);
      if (startManagedKafka) {
        ensureDockerDaemonRunning();
        startManagedKafkaContainer();
      }
      if (startManagedKafka && _skipReadinessCheck) {
        _resolvedKafkaBrokerList = _kafkaBrokerList;
      } else {
        _resolvedKafkaBrokerList = resolveKafkaBrokerList(_kafkaBrokerList);
      }
      LOGGER.info("Using Kafka at {}", _resolvedKafkaBrokerList);
      _started = true;
    } catch (Exception e) {
      cleanupManagedKafkaResources();
      throw new RuntimeException("Failed to initialize Kafka for " + _ownerName, e);
    }
  }

  @Override
  public void stop() {
    unregisterShutdownHook();
    cleanupManagedKafkaResources();
    _started = false;
  }

  @Override
  public void createTopic(String topic, Properties topicProps) {
    ensureStarted();
    int numPartitions = parseInt(String.valueOf(topicProps.getOrDefault("partition", 1)), 1);
    int requestedReplicationFactor =
        parseInt(String.valueOf(topicProps.getOrDefault("replicationFactor", 1)), 1);
    short replicationFactor = (short) Math.max(1, Math.min(_clusterSize, requestedReplicationFactor));
    try (AdminClient adminClient = createKafkaAdminClient()) {
      NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
      runAdminWithRetry(() -> adminClient.createTopics(Collections.singletonList(newTopic)).all().get(),
          "create topic: " + topic);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create topic: " + topic, e);
    }
  }

  @Override
  public void deleteTopic(String topic) {
    ensureStarted();
    try (AdminClient adminClient = createKafkaAdminClient()) {
      runAdminWithRetry(() -> adminClient.deleteTopics(Collections.singletonList(topic)).all().get(),
          "delete topic: " + topic);
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete topic: " + topic, e);
    }
  }

  @Override
  public void createPartitions(String topic, int numPartitions) {
    ensureStarted();
    try (AdminClient adminClient = createKafkaAdminClient()) {
      runAdminWithRetry(() -> {
        adminClient.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(numPartitions))).all()
            .get();
        return null;
      }, "create partitions for topic: " + topic);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create partitions for topic: " + topic, e);
    }
  }

  @Override
  public void deleteRecordsBeforeOffset(String topic, int partition, long offset) {
    ensureStarted();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    try (AdminClient adminClient = createKafkaAdminClient()) {
      runAdminWithRetry(() -> {
        adminClient.deleteRecords(Collections.singletonMap(topicPartition, RecordsToDelete.beforeOffset(offset))).all()
            .get();
        return null;
      }, "delete records before offset for topic: " + topic + ", partition: " + partition);
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete records before offset for topic: " + topic + ", partition: "
          + partition + ", offset: " + offset, e);
    }
  }

  @Override
  public int getPort() {
    return _kafkaPort;
  }

  private AdminClient createKafkaAdminClient() {
    Properties props = new Properties();
    props.put("bootstrap.servers", _resolvedKafkaBrokerList);
    return AdminClient.create(props);
  }

  String resolveKafkaBrokerList(String configuredBrokerList) {
    if (isKafkaAvailable(configuredBrokerList)) {
      return configuredBrokerList;
    }

    String fallback = "localhost:9092";
    if (isKafkaAvailable(fallback)) {
      LOGGER.warn("Configured Kafka broker list {} is not reachable; falling back to default {}", configuredBrokerList,
          fallback);
      return fallback;
    }

    throw new IllegalStateException("Kafka broker list is not reachable: " + configuredBrokerList
        + ". Please start Kafka or pass -kafkaBrokerList.");
  }

  boolean shouldStartManagedKafka(String brokerList) {
    if (_allowManagedKafkaForConfiguredBroker) {
      return isLocalBroker(brokerList);
    }
    return managedKafkaBroker().equals(brokerList);
  }

  static boolean isLocalBroker(String brokerList) {
    int separator = brokerList.lastIndexOf(':');
    if (separator <= 0 || separator == brokerList.length() - 1) {
      return false;
    }
    String host = brokerList.substring(0, separator);
    return LOCALHOST.equalsIgnoreCase(host) || LOOPBACK.equals(host);
  }

  private static int parsePort(String brokerList, int defaultPort) {
    int separator = brokerList.lastIndexOf(':');
    if (separator <= 0 || separator == brokerList.length() - 1) {
      return defaultPort;
    }
    return parseInt(brokerList.substring(separator + 1), defaultPort);
  }

  private static int parseInt(String value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  boolean isKafkaAvailable(String brokerList) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("request.timeout.ms", "2000");
    props.put("default.api.timeout.ms", "2000");
    try (AdminClient adminClient = AdminClient.create(props)) {
      adminClient.describeCluster().nodes().get(2, TimeUnit.SECONDS);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void ensureDockerDaemonRunning()
      throws Exception {
    LOGGER.info("Checking Docker daemon availability");
    try {
      runProcess(List.of("docker", "info"), false);
    } catch (Exception e) {
      throw new IllegalStateException("Docker daemon is not running or not reachable. Quickstart starts Kafka in "
          + "Docker on " + managedKafkaBroker()
          + ". Start Docker, or pass -kafkaBrokerList <host:port> to use an external Kafka broker.", e);
    }
  }

  private void pullKafkaDockerImage()
      throws Exception {
    if (isKafkaDockerImagePresent()) {
      LOGGER.info("Kafka Docker image {} already exists locally", KAFKA_IMAGE);
      return;
    }
    LOGGER.info("Pulling Kafka Docker image {}", KAFKA_IMAGE);
    Exception lastException = null;
    for (int attempt = 1; attempt <= DOCKER_PULL_RETRIES; attempt++) {
      try {
        runProcess(List.of("docker", "pull", KAFKA_IMAGE), false);
        LOGGER.info("Kafka Docker image {} is ready", KAFKA_IMAGE);
        return;
      } catch (Exception e) {
        lastException = e;
        if (!isTransientDockerPullError(e) || attempt == DOCKER_PULL_RETRIES) {
          throw e;
        }
        LOGGER.warn("Failed to pull Kafka Docker image on attempt {}/{}; retrying in {}ms", attempt,
            DOCKER_PULL_RETRIES, DOCKER_PULL_RETRY_WAIT_MS);
        Thread.sleep(DOCKER_PULL_RETRY_WAIT_MS);
      }
    }
    throw lastException;
  }

  private void startManagedKafkaContainer()
      throws Exception {
    _managedKafkaContainerName =
        hasText(_containerName) ? _containerName : ("pinot-qs-kafka-" + System.currentTimeMillis());
    pullKafkaDockerImage();
    ensureDockerNetworkReady();

    boolean singleNodeCluster = _clusterSize <= 1;
    int topicReplicationFactor = Math.max(1, Math.min(3, _clusterSize));
    String quorumVoters = hasText(_controllerQuorumVoters)
        ? _controllerQuorumVoters
        : (_kafkaBrokerId + "@"
        + (singleNodeCluster ? LOCALHOST : _internalHost) + ":" + KAFKA_CONTROLLER_LISTENER_PORT);

    List<String> runCommand = new ArrayList<>();
    runCommand.add("docker");
    runCommand.add("run");
    runCommand.add("-d");
    runCommand.add("--name");
    runCommand.add(_managedKafkaContainerName);
    if (hasText(_dockerNetworkName)) {
      runCommand.add("--network");
      runCommand.add(_dockerNetworkName);
    }
    runCommand.add("-p");
    runCommand.add(_kafkaPort + ":" + (singleNodeCluster ? KAFKA_BROKER_LISTENER_PORT : KAFKA_EXTERNAL_LISTENER_PORT));
    runCommand.add("-e");
    runCommand.add("KAFKA_NODE_ID=" + _kafkaBrokerId);
    runCommand.add("-e");
    runCommand.add("KAFKA_PROCESS_ROLES=broker,controller");
    if (singleNodeCluster) {
      runCommand.add("-e");
      runCommand.add(
          "KAFKA_LISTENERS=PLAINTEXT://:" + KAFKA_BROKER_LISTENER_PORT
              + ",CONTROLLER://:" + KAFKA_CONTROLLER_LISTENER_PORT);
      runCommand.add("-e");
      runCommand.add("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + managedKafkaBroker());
      runCommand.add("-e");
      runCommand.add("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
      runCommand.add("-e");
      runCommand.add("KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT");
    } else {
      runCommand.add("-e");
      runCommand.add("KAFKA_LISTENERS=INTERNAL://:" + KAFKA_BROKER_LISTENER_PORT + ",EXTERNAL://:"
          + KAFKA_EXTERNAL_LISTENER_PORT + ",CONTROLLER://:" + KAFKA_CONTROLLER_LISTENER_PORT);
      runCommand.add("-e");
      runCommand.add("KAFKA_ADVERTISED_LISTENERS=INTERNAL://" + _internalHost + ":" + KAFKA_BROKER_LISTENER_PORT
          + ",EXTERNAL://" + managedKafkaBroker());
      runCommand.add("-e");
      runCommand.add(
          "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT");
      runCommand.add("-e");
      runCommand.add("KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL");
    }
    runCommand.add("-e");
    runCommand.add("KAFKA_CONTROLLER_QUORUM_VOTERS=" + quorumVoters);
    runCommand.add("-e");
    runCommand.add("KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER");
    runCommand.add("-e");
    runCommand.add("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=" + topicReplicationFactor);
    runCommand.add("-e");
    runCommand.add("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=" + topicReplicationFactor);
    runCommand.add("-e");
    runCommand.add("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2");
    if (hasText(_clusterId)) {
      runCommand.add("-e");
      runCommand.add("CLUSTER_ID=" + _clusterId);
    }
    runCommand.add(KAFKA_IMAGE);

    runProcess(List.of("docker", "rm", "-f", _managedKafkaContainerName), true);
    LOGGER.info("Starting Kafka container {}", _managedKafkaContainerName);
    runKafkaContainerWithNetworkRetry(runCommand);
    if (!_skipReadinessCheck) {
      waitForKafkaReady();
    }

    registerShutdownHook();
  }

  private void waitForKafkaReady()
      throws Exception {
    String managedBroker = managedKafkaBroker();
    LOGGER.info("Waiting for Kafka broker to become available on {}", managedBroker);
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(KAFKA_STARTUP_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadline) {
      if (isKafkaAvailable(managedBroker)) {
        LOGGER.info("Kafka broker is ready on {}", managedBroker);
        return;
      }
      Thread.sleep(1000L);
    }
    throw new IllegalStateException("Kafka container did not become ready on " + managedBroker);
  }

  private void ensureDockerNetworkReady()
      throws Exception {
    if (!hasText(_dockerNetworkName)) {
      return;
    }
    boolean existedBeforeCreate = isDockerNetworkReady();
    String createOutput = "";
    if (!existedBeforeCreate) {
      createOutput = runProcess(List.of("docker", "network", "create", _dockerNetworkName), true);
      if (createOutput.contains("all predefined address pools have been fully subnetted")) {
        LOGGER.warn("Docker network address pool is exhausted. Cleaning stale Pinot networks and retrying.");
        cleanupStalePinotDockerNetworks();
        createOutput = runProcess(List.of("docker", "network", "create", _dockerNetworkName), true);
      }
    }
    for (int attempt = 1; attempt <= DOCKER_NETWORK_READY_RETRIES; attempt++) {
      if (isDockerNetworkReady()) {
        if (!existedBeforeCreate) {
          MANAGED_NETWORKS_CREATED.add(_dockerNetworkName);
        }
        registerManagedNetworkUsage();
        return;
      }
      Thread.sleep(DOCKER_NETWORK_READY_WAIT_MS);
    }
    throw new IllegalStateException("Docker network '" + _dockerNetworkName + "' is not ready"
        + (hasText(createOutput) ? ". docker network create output: " + createOutput : ""));
  }

  private boolean isDockerNetworkReady() {
    return isDockerNetworkReady(_dockerNetworkName);
  }

  private static boolean isDockerNetworkReady(String networkName) {
    try {
      runProcess(List.of("docker", "network", "inspect", networkName), false);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void registerManagedNetworkUsage() {
    if (_registeredManagedNetworkUsage || !hasText(_dockerNetworkName)) {
      return;
    }
    MANAGED_NETWORK_USAGE_COUNTS.computeIfAbsent(_dockerNetworkName, ignored -> new AtomicInteger()).incrementAndGet();
    _registeredManagedNetworkUsage = true;
  }

  private void runKafkaContainerWithNetworkRetry(List<String> runCommand)
      throws Exception {
    try {
      runProcess(runCommand, false);
    } catch (Exception e) {
      if (!hasText(_dockerNetworkName) || !isMissingDockerNetworkError(e)) {
        throw e;
      }
      LOGGER.warn("Docker network {} was not found during Kafka container start; recreating and retrying once",
          _dockerNetworkName);
      ensureDockerNetworkReady();
      runProcess(runCommand, false);
    }
  }

  private boolean isMissingDockerNetworkError(Exception e) {
    String message = e.getMessage();
    if (message == null) {
      return false;
    }
    return message.contains("network " + _dockerNetworkName + " not found")
        || message.contains("No such network");
  }

  private boolean isKafkaDockerImagePresent() {
    try {
      runProcess(List.of("docker", "image", "inspect", KAFKA_IMAGE), false);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isTransientDockerPullError(Exception e) {
    String message = e.getMessage();
    if (message == null) {
      return false;
    }
    return message.contains("TLS handshake timeout")
        || message.contains("Client.Timeout exceeded")
        || message.contains("i/o timeout");
  }

  private void cleanupStalePinotDockerNetworks()
      throws Exception {
    String networks = runProcess(List.of("docker", "network", "ls", "--format", "{{.Name}}"), true);
    if (!hasText(networks)) {
      return;
    }
    for (String network : networks.split("\\R")) {
      if (network.startsWith("pinot-it-kafka-") || network.startsWith("pinot-test-network")) {
        runProcess(List.of("docker", "network", "rm", network), true);
      }
    }
  }

  private void cleanupManagedKafkaResources() {
    String managedKafkaContainerName = _managedKafkaContainerName;
    if (managedKafkaContainerName != null) {
      try {
        LOGGER.info("Stopping managed Kafka container {}", managedKafkaContainerName);
        runProcess(List.of("docker", "rm", "-f", managedKafkaContainerName), true);
        waitForContainerToStop(managedKafkaContainerName);
        waitForPortRelease(_kafkaPort);
      } catch (Exception e) {
        LOGGER.warn("Failed to stop managed Kafka container {}", managedKafkaContainerName, e);
      } finally {
        _managedKafkaContainerName = null;
      }
    }
    if (_registeredManagedNetworkUsage && hasText(_dockerNetworkName)) {
      cleanupManagedKafkaNetwork();
    }
  }

  private void registerShutdownHook() {
    if (_shutdownHookThread != null) {
      return;
    }
    _shutdownHookThread = new Thread(this::cleanupManagedKafkaResources, _ownerName + "-kafka-cleanup");
    Runtime.getRuntime().addShutdownHook(_shutdownHookThread);
  }

  private void unregisterShutdownHook() {
    if (_shutdownHookThread == null) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(_shutdownHookThread);
    } catch (IllegalStateException e) {
      // JVM is shutting down; cleanup hook is already running or about to run.
    } finally {
      _shutdownHookThread = null;
    }
  }

  private void waitForContainerToStop(String containerName)
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(CONTAINER_STOP_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadlineMs) {
      if (!isContainerPresent(containerName)) {
        LOGGER.info("Managed Kafka container {} is fully stopped", containerName);
        return;
      }
      Thread.sleep(STOP_POLL_INTERVAL_MS);
    }
    throw new IllegalStateException("Managed Kafka container is still present after stop timeout: " + containerName);
  }

  private boolean isContainerPresent(String containerName) {
    try {
      runProcess(List.of("docker", "container", "inspect", containerName), false);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void waitForPortRelease(int port)
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(PORT_RELEASE_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadlineMs) {
      if (NetUtils.available(port)) {
        LOGGER.info("Kafka port {} is released", port);
        return;
      }
      Thread.sleep(STOP_POLL_INTERVAL_MS);
    }
    throw new IllegalStateException("Kafka port is still in use after stop timeout: " + port);
  }

  private void cleanupManagedKafkaNetwork() {
    _registeredManagedNetworkUsage = false;

    AtomicInteger usage = MANAGED_NETWORK_USAGE_COUNTS.get(_dockerNetworkName);
    if (usage == null) {
      return;
    }

    int remainingUsage = usage.decrementAndGet();
    if (remainingUsage > 0) {
      return;
    }
    MANAGED_NETWORK_USAGE_COUNTS.remove(_dockerNetworkName, usage);

    if (!MANAGED_NETWORKS_CREATED.remove(_dockerNetworkName)) {
      return;
    }
    try {
      removeManagedDockerNetworkWithRetry(_dockerNetworkName);
    } catch (Exception e) {
      LOGGER.warn("Failed to remove managed Kafka Docker network {}", _dockerNetworkName, e);
    }
  }

  private void removeManagedDockerNetworkWithRetry(String networkName)
      throws Exception {
    for (int attempt = 1; attempt <= DOCKER_NETWORK_REMOVE_RETRIES; attempt++) {
      runProcess(List.of("docker", "network", "rm", networkName), true);
      if (!isDockerNetworkReady(networkName)) {
        return;
      }
      if (attempt < DOCKER_NETWORK_REMOVE_RETRIES) {
        Thread.sleep(DOCKER_NETWORK_REMOVE_RETRY_WAIT_MS);
      }
    }
    throw new IllegalStateException("Managed Kafka Docker network is still present: " + networkName);
  }

  private static String runProcess(List<String> command, boolean ignoreFailure)
      throws Exception {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    if (!process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      String timeoutOutput = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      throw new IllegalStateException("Command timed out after " + PROCESS_TIMEOUT_SECONDS + "s: "
          + String.join(" ", command) + (timeoutOutput.isEmpty() ? "" : "\n" + timeoutOutput));
    }
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
    int code = process.exitValue();
    if (code != 0 && !ignoreFailure) {
      throw new IllegalStateException("Command failed (" + code + "): " + String.join(" ", command)
          + (output.isEmpty() ? "" : "\n" + output));
    }
    return output;
  }

  private void ensureStarted() {
    if (!_started) {
      throw new IllegalStateException("Kafka is not started for " + _ownerName
          + ". Call start() explicitly before using Kafka operations.");
    }
  }

  private <T> T runAdminWithRetry(AdminOperation<T> operation, String action)
      throws Exception {
    for (int attempt = 1; attempt <= TOPIC_MUTATION_RETRIES; attempt++) {
      try {
        return operation.execute();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException
            && attempt < TOPIC_MUTATION_RETRIES) {
          Thread.sleep(TimeUnit.SECONDS.toMillis(1));
          continue;
        }
        throw e;
      }
    }
    throw new IllegalStateException("Failed to " + action + " after retries");
  }

  private static boolean hasText(String value) {
    return value != null && !value.isEmpty();
  }

  private String managedKafkaBroker() {
    return LOCALHOST + ":" + _kafkaPort;
  }

  @FunctionalInterface
  private interface AdminOperation<T> {
    T execute()
        throws Exception;
  }
}
