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
package org.apache.pinot.plugin.inputformat.json.confluent.kafka.schemaregistry;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;


public class SchemaRegistryStarter {
  public static final int DEFAULT_PORT = 8081;
  private static final String CONFLUENT_PLATFORM_VERSION = "7.2.0";
  private static final DockerImageName KAFKA_DOCKER_IMAGE_NAME =
      DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION);
  private static final DockerImageName SCHEMA_REGISTRY_DOCKER_IMAGE_NAME =
      DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION);
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryStarter.class);

  private SchemaRegistryStarter() {
  }

  public static KafkaSchemaRegistryInstance startLocalInstance(int port) {
    KafkaSchemaRegistryInstance kafkaSchemaRegistry = new KafkaSchemaRegistryInstance(port);
    kafkaSchemaRegistry.start();
    return kafkaSchemaRegistry;
  }

  public static class KafkaSchemaRegistryInstance {
    private final int _port;
    public KafkaContainer _kafkaContainer;
    private Network _network;
    private GenericContainer _schemaRegistryContainer;

    private KafkaSchemaRegistryInstance(int port) {
      _port = port;
    }

    public String getUrl() {
      return "http://" + _schemaRegistryContainer.getHost() + ":" + _schemaRegistryContainer.getMappedPort(_port);
    }

    public void start() {
      LOGGER.info("Starting schema registry");
      if (_kafkaContainer != null || _schemaRegistryContainer != null) {
        throw new IllegalStateException("Schema registry is already running");
      }

      _network = Network.newNetwork();

      _kafkaContainer = new KafkaContainer(KAFKA_DOCKER_IMAGE_NAME).withNetwork(_network).withNetworkAliases("kafka")
          .withCreateContainerCmdModifier(it -> it.withHostName("kafka")).waitingFor(Wait.forListeningPort());
      _kafkaContainer.start();

      Map<String, String> schemaRegistryProps = new HashMap<>();
      schemaRegistryProps.put("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092");
      schemaRegistryProps.put("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry");
      schemaRegistryProps.put("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + _port);
      schemaRegistryProps.put("SCHEMA_REGISTRY_DEBUG", "true");
      _schemaRegistryContainer =
          new GenericContainer(SCHEMA_REGISTRY_DOCKER_IMAGE_NAME).dependsOn(_kafkaContainer).withNetwork(_network)
              .withNetworkAliases("schemaregistry").withEnv(schemaRegistryProps).withExposedPorts(_port)
              .waitingFor(Wait.forListeningPort());
      _schemaRegistryContainer.start();
    }

    public void stop() {
      LOGGER.info("Stopping schema registry");
      if (_schemaRegistryContainer != null) {
        _schemaRegistryContainer.stop();
        _schemaRegistryContainer = null;
      }

      if (_kafkaContainer != null) {
        _kafkaContainer.stop();
        _kafkaContainer = null;
      }

      if (_network != null) {
        _network.close();
      }
    }
  }
}
