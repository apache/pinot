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
package org.apache.pinot.plugin.stream.kafka09;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides the kafka connection after ensuring that we're connected to the appropriate broker for consumption.
 */
public class KafkaConnectionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectionHandler.class);

  enum ConsumerState {
    CONNECTING_TO_BOOTSTRAP_NODE,
    CONNECTED_TO_BOOTSTRAP_NODE,
    FETCHING_LEADER_INFORMATION,
    CONNECTING_TO_PARTITION_LEADER,
    CONNECTED_TO_PARTITION_LEADER
  }

  State _currentState;

  final String _clientId;
  final String _topic;
  final int _partition;
  final long _connectTimeoutMillis;

  String[] _bootstrapHosts;
  int[] _bootstrapPorts;
  KafkaBrokerWrapper _leader;
  String _currentHost;
  int _currentPort;
  int _bufferSize;
  int _socketTimeout;

  final KafkaSimpleConsumerFactory _simpleConsumerFactory;
  SimpleConsumer _simpleConsumer;

  final Random _random = new Random();

  boolean isPartitionProvided;

  private final KafkaLowLevelStreamConfig _kafkaLowLevelStreamConfig;

  @VisibleForTesting
  public SimpleConsumer getSimpleConsumer() {
    return _simpleConsumer;
  }

  /**
   * A Kafka protocol error that indicates a situation that is not likely to clear up by retrying the request (for
   * example, no such topic).
   */
  private class KafkaPermanentConsumerException extends RuntimeException {
    public KafkaPermanentConsumerException(Errors error) {
      super(error.exception());
    }
  }

  /**
   * A Kafka protocol error that indicates a situation that is likely to be transient (for example, network error or
   * broker not available).
   */
  private class KafkaTransientConsumerException extends RuntimeException {
    public KafkaTransientConsumerException(Errors error) {
      super(error.exception());
    }
  }

  /**
   * Creates a kafka connection given the stream metadata
   * @param streamConfig
   * @param simpleConsumerFactory
   */
  public KafkaConnectionHandler(String clientId, StreamConfig streamConfig,
      KafkaSimpleConsumerFactory simpleConsumerFactory) {
    _kafkaLowLevelStreamConfig = new KafkaLowLevelStreamConfig(streamConfig);
    _simpleConsumerFactory = simpleConsumerFactory;
    _clientId = clientId;
    _topic = _kafkaLowLevelStreamConfig.getKafkaTopicName();
    _connectTimeoutMillis = streamConfig.getConnectionTimeoutMillis();
    _simpleConsumer = null;

    isPartitionProvided = false;
    _partition = Integer.MIN_VALUE;

    _bufferSize = _kafkaLowLevelStreamConfig.getKafkaBufferSize();
    _socketTimeout = _kafkaLowLevelStreamConfig.getKafkaSocketTimeout();
    initializeBootstrapNodeList(_kafkaLowLevelStreamConfig.getBootstrapHosts());
    setCurrentState(new ConnectingToBootstrapNode());
  }

  /**
   * Creates a kafka connection given the stream metadata and partition
   * @param streamConfig
   * @param partition
   * @param simpleConsumerFactory
   */
  public KafkaConnectionHandler(String clientId, StreamConfig streamConfig, int partition,
      KafkaSimpleConsumerFactory simpleConsumerFactory) {

    _kafkaLowLevelStreamConfig = new KafkaLowLevelStreamConfig(streamConfig);
    _simpleConsumerFactory = simpleConsumerFactory;
    _clientId = clientId;
    _topic = _kafkaLowLevelStreamConfig.getKafkaTopicName();
    _connectTimeoutMillis = streamConfig.getConnectionTimeoutMillis();
    _simpleConsumer = null;

    isPartitionProvided = true;
    _partition = partition;

    _bufferSize = _kafkaLowLevelStreamConfig.getKafkaBufferSize();
    _socketTimeout = _kafkaLowLevelStreamConfig.getKafkaSocketTimeout();
    initializeBootstrapNodeList(_kafkaLowLevelStreamConfig.getBootstrapHosts());
    setCurrentState(new ConnectingToBootstrapNode());
  }

  void initializeBootstrapNodeList(String bootstrapNodes) {

    if (StringUtils.isBlank(bootstrapNodes)) {
      throw new IllegalArgumentException("Need at least one bootstrap host");
    }

    ArrayList<String> hostsAndPorts =
        Lists.newArrayList(Splitter.on(',').trimResults().omitEmptyStrings().split(bootstrapNodes));
    final int bootstrapHostCount = hostsAndPorts.size();
    _bootstrapHosts = new String[bootstrapHostCount];
    _bootstrapPorts = new int[bootstrapHostCount];

    for (int i = 0; i < bootstrapHostCount; i++) {
      String hostAndPort = hostsAndPorts.get(i);
      String[] splitHostAndPort = hostAndPort.split(":");
      if (splitHostAndPort.length != 2) {
        throw new IllegalArgumentException("Unable to parse host:port combination for " + hostAndPort);
      }

      _bootstrapHosts[i] = splitHostAndPort[0];

      try {
        _bootstrapPorts[i] = Integer.parseInt(splitHostAndPort[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Could not parse port number " + splitHostAndPort[1] + " for host:port combination " + hostAndPort);
      }
    }
  }

  protected KafkaLowLevelStreamConfig getkafkaLowLevelStreamConfig() {
    return _kafkaLowLevelStreamConfig;
  }

  abstract class State {
    private ConsumerState stateValue;

    protected State(ConsumerState stateValue) {
      this.stateValue = stateValue;
    }

    abstract void process();

    abstract boolean isConnectedToKafkaBroker();

    void handleConsumerException(Exception e) {
      // By default, just log the exception and switch back to CONNECTING_TO_BOOTSTRAP_NODE (which will take care of
      // closing the connection if it exists)
      LOGGER.warn("Caught Kafka consumer exception while in state {}, disconnecting and trying again for topic {}",
          _currentState.getStateValue(), _topic, e);

      Uninterruptibles.sleepUninterruptibly(250, TimeUnit.MILLISECONDS);

      setCurrentState(new ConnectingToBootstrapNode());
    }

    ConsumerState getStateValue() {
      return stateValue;
    }
  }

  private class ConnectingToBootstrapNode extends State {
    public ConnectingToBootstrapNode() {
      super(ConsumerState.CONNECTING_TO_BOOTSTRAP_NODE);
    }

    @Override
    public void process() {
      // Connect to a random bootstrap node
      if (_simpleConsumer != null) {
        try {
          _simpleConsumer.close();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while closing consumer for topic {}, ignoring", _topic, e);
        }
      }

      int randomHostIndex = _random.nextInt(_bootstrapHosts.length);
      _currentHost = _bootstrapHosts[randomHostIndex];
      _currentPort = _bootstrapPorts[randomHostIndex];

      try {
        LOGGER.info("Connecting to bootstrap host {}:{} for topic {}", _currentHost, _currentPort, _topic);
        _simpleConsumer = _simpleConsumerFactory.buildSimpleConsumer(_currentHost, _currentPort, _socketTimeout,
            _bufferSize, _clientId);
        setCurrentState(new ConnectedToBootstrapNode());
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return false;
    }
  }

  private class ConnectedToBootstrapNode extends State {
    protected ConnectedToBootstrapNode() {
      super(ConsumerState.CONNECTED_TO_BOOTSTRAP_NODE);
    }

    @Override
    void process() {
      if (isPartitionProvided) {
        // If we're consuming from a partition, we need to find the leader so that we can consume from it. By design,
        // Kafka only allows consumption from the leader and not one of the in-sync replicas.
        setCurrentState(new FetchingLeaderInformation());
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private class FetchingLeaderInformation extends State {
    public FetchingLeaderInformation() {
      super(ConsumerState.FETCHING_LEADER_INFORMATION);
    }

    @Override
    void process() {
      // Fetch leader information
      try {
        TopicMetadataResponse response =
            _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(_topic)));
        try {
          _leader = null;
          List<PartitionMetadata> pMetaList = response.topicsMetadata().get(0).partitionsMetadata();
          for (PartitionMetadata pMeta : pMetaList) {
            if (pMeta.partitionId() == _partition) {
              _leader = new KafkaBrokerWrapper(pMeta.leader());
              break;
            }
          }

          // If we've located a broker
          if (_leader != null) {
            LOGGER.info("Located leader broker {} for topic {}, connecting to it.", _leader, _topic);
            setCurrentState(new ConnectingToPartitionLeader());
          } else {
            // Failed to get the leader broker. There could be a leader election at the moment, so retry after a little
            // bit.
            LOGGER.warn("Leader broker is null for topic {}, retrying leader fetch in 100ms", _topic);
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          }
        } catch (Exception e) {
          // Failed to get the leader broker. There could be a leader election at the moment, so retry after a little
          // bit.
          LOGGER.warn("Failed to get the leader broker for topic {} due to exception, retrying in 100ms", _topic, e);
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private class ConnectingToPartitionLeader extends State {
    public ConnectingToPartitionLeader() {
      super(ConsumerState.CONNECTING_TO_PARTITION_LEADER);
    }

    @Override
    void process() {
      // If we're already connected to the leader broker, don't disconnect and reconnect
      LOGGER.info("Trying to fetch leader host and port: {}:{} for topic {}", _leader.host(), _leader.port(), _topic);
      if (_leader.host().equals(_currentHost) && _leader.port() == _currentPort) {
        setCurrentState(new ConnectedToPartitionLeader());
        return;
      }

      // Disconnect from current broker
      if (_simpleConsumer != null) {
        try {
          _simpleConsumer.close();
          _simpleConsumer = null;
        } catch (Exception e) {
          handleConsumerException(e);
          return;
        }
      }

      // Connect to the partition leader
      try {
        _simpleConsumer = _simpleConsumerFactory.buildSimpleConsumer(_leader.host(), _leader.port(), _socketTimeout,
            _bufferSize, _clientId);

        setCurrentState(new ConnectedToPartitionLeader());
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return false;
    }
  }

  private class ConnectedToPartitionLeader extends State {
    public ConnectedToPartitionLeader() {
      super(ConsumerState.CONNECTED_TO_PARTITION_LEADER);
    }

    @Override
    void process() {
      // Nothing to do
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private void setCurrentState(State newState) {
    if (_currentState != null) {
      LOGGER.info("Switching from state {} to state {} for topic {}", _currentState.getStateValue(),
          newState.getStateValue(), _topic);
    }

    _currentState = newState;
  }

  RuntimeException exceptionForKafkaErrorCode(short kafkaErrorCode) {
    final Errors kafkaError = Errors.forCode(kafkaErrorCode);
    switch (kafkaError) {
      case UNKNOWN:
      case CORRUPT_MESSAGE:
      case MESSAGE_TOO_LARGE:
      case OFFSET_METADATA_TOO_LARGE:
      case INVALID_TOPIC_EXCEPTION:
      case RECORD_LIST_TOO_LARGE:
      case INVALID_REQUIRED_ACKS:
      case ILLEGAL_GENERATION:
      case INCONSISTENT_GROUP_PROTOCOL:
      case INVALID_GROUP_ID:
      case UNKNOWN_MEMBER_ID:
      case INVALID_SESSION_TIMEOUT:
      case INVALID_COMMIT_OFFSET_SIZE:
        return new PermanentConsumerException(new KafkaPermanentConsumerException(kafkaError));
      case OFFSET_OUT_OF_RANGE:
      case UNKNOWN_TOPIC_OR_PARTITION:
      case LEADER_NOT_AVAILABLE:
      case NOT_LEADER_FOR_PARTITION:
      case REQUEST_TIMED_OUT:
      case BROKER_NOT_AVAILABLE:
      case REPLICA_NOT_AVAILABLE:
      case STALE_CONTROLLER_EPOCH:
      case NETWORK_EXCEPTION:
      case GROUP_LOAD_IN_PROGRESS:
      case GROUP_COORDINATOR_NOT_AVAILABLE:
      case NOT_COORDINATOR_FOR_GROUP:
      case NOT_ENOUGH_REPLICAS:
      case NOT_ENOUGH_REPLICAS_AFTER_APPEND:
      case REBALANCE_IN_PROGRESS:
      case TOPIC_AUTHORIZATION_FAILED:
      case GROUP_AUTHORIZATION_FAILED:
      case CLUSTER_AUTHORIZATION_FAILED:
        return new TransientConsumerException(new KafkaTransientConsumerException(kafkaError));
      case NONE:
      default:
        return new RuntimeException("Unhandled error " + kafkaError);
    }
  }

  void close() throws IOException {
    boolean needToCloseConsumer = _currentState.isConnectedToKafkaBroker() && _simpleConsumer != null;

    // Reset the state machine
    setCurrentState(new ConnectingToBootstrapNode());

    // Close the consumer if needed
    if (needToCloseConsumer) {
      _simpleConsumer.close();
      _simpleConsumer = null;
    }
  }
}
