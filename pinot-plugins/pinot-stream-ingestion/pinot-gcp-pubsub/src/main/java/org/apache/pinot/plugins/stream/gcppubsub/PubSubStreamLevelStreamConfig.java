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
package org.apache.pinot.plugins.stream.gcppubsub;

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PubSubStreamLevelStreamConfig {
	private static final Map<String, String> defaultProps;
	private final Map<String, String> pubsubConsumerProps;

	private final String projectId;
	private final String subscriptionId;

	static {
		defaultProps = new HashMap<>();
	}

	public PubSubStreamLevelStreamConfig(StreamConfig streamConfig, String tableName, String groupId) {
		Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

		// Check mandatory configuration
		String projectIdKey = PubSubStreamConfigProperties.constructStreamProperty(PubSubStreamConfigProperties.HighLevelConsumer.PUBSUB_PROJECT_ID);
		projectId = streamConfigMap.get(projectIdKey);
		Preconditions.checkNotNull(projectId, "GCP project ID key must be specified in {} high level properties", projectIdKey);

		String subscriptionIdKey = PubSubStreamConfigProperties.constructStreamProperty(PubSubStreamConfigProperties.HighLevelConsumer.PUBSUB_SUBSCRIPTION_ID);
		subscriptionId = streamConfigMap.get(subscriptionIdKey);
		Preconditions.checkNotNull(projectId, "GCP subscription ID key must be specified in {} high level properties", subscriptionIdKey);

		// Build Pub/Sub custom properties hashmap
		pubsubConsumerProps = new HashMap<>();
		String pubsubConsumerPropsPrefix = PubSubStreamConfigProperties.constructStreamProperty(
				PubSubStreamConfigProperties.PUBSUB_CONSUMER_PROP_PREFIX);
		for (String key : streamConfigMap.keySet()) {
			if (key.startsWith(pubsubConsumerPropsPrefix)) {
				pubsubConsumerProps.put(
					StreamConfigProperties.getPropertySuffix(key, pubsubConsumerPropsPrefix),
					streamConfigMap.get(key)
				);
			}
		}
	}

	public Properties getPubsubConsumerProperties() {
		Properties pros = new Properties();

		// Default stream properties
		for (String key : defaultProps.keySet()) {
			pros.put(key, defaultProps.get(key));
		}

		// Custom Pub/Sub properties
		for (String key : pubsubConsumerProps.keySet()) {
			pros.put(key, pubsubConsumerProps.get(key));
		}

		return pros;
	}

	public String getProjectId() {
		return projectId;
	}

	public String getSubscriptionId() {
		return subscriptionId;
	}
}
