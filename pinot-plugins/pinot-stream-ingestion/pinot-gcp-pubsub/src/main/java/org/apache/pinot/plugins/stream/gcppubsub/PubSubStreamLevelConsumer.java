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

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PubSubStreamLevelConsumer implements StreamLevelConsumer {
	private final static Logger LOGGER = LoggerFactory.getLogger(PubSubStreamLevelConsumer.class);
	private final static Integer MAX_MESSAGES_PULLED_PER_BATCH = 10;

	private final StreamMessageDecoder messageDecoder;
	private PubSubStreamLevelStreamConfig pubSubStreamLevelStreamConfig;

	private List<String> ackIds = new ArrayList<>();
	private Iterator<ReceivedMessage> pubsubMessageIterator;
	private PullRequest pubsubPullRequest;
	private SubscriberStub pubsubSubscriber;
	private final String pubsubSubscriptionName;

	// Keeps information for logging
	private long lastLogTime = 0;
	private long lastCount = 0;
	private long currentCount = 0L;


	public PubSubStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig, Set<String> sourceFields, String groupId) {
		pubSubStreamLevelStreamConfig = new PubSubStreamLevelStreamConfig(streamConfig, tableName, groupId);
		messageDecoder = StreamDecoderProvider.create(streamConfig, sourceFields);

		// Build Pub/Sub subscription path based on user's config
		pubsubSubscriptionName = ProjectSubscriptionName.format(
			pubSubStreamLevelStreamConfig.getProjectId(),
			pubSubStreamLevelStreamConfig.getSubscriptionId()
		);

		LOGGER.info("Pub/Sub subscription {}", pubsubSubscriptionName);
	}

	@Override
	public void start() throws Exception {
		SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
			.setTransportChannelProvider(
				SubscriberStubSettings
					.defaultGrpcTransportProviderBuilder()
					.setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
					.build()
			).build();

		// Try to connect to Pub/Sub
		pubsubSubscriber = GrpcSubscriberStub.create(subscriberStubSettings);
	}

	@Override
	public GenericRow next(GenericRow destination) {
		if (pubsubMessageIterator == null || !pubsubMessageIterator.hasNext()) {
			if (!ackIds.isEmpty()) {
				// Time to ack messages pulled from iterator
				AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
						.setSubscription(pubsubSubscriptionName)
						.addAllAckIds(ackIds)
						.build();

				// Use acknowledgeCallable().futureCall to asynchronously perform this operation.
				pubsubSubscriber.acknowledgeCallable().call(acknowledgeRequest);
				ackIds.clear();
			}

			// Pull new batch of messages
			updatePubsubMessageIterator();
		}

		if (pubsubMessageIterator.hasNext()) {
			try {
				ReceivedMessage message = pubsubMessageIterator.next();

				// Decode Pub/Sub message to Pinot's GenericRow format.
				destination = messageDecoder.decode(message.getMessage(), destination);

				// Log every minutes or 100k events
				currentCount = currentCount + 1;
				final long now = System.currentTimeMillis();
				if (now - lastLogTime > 60000 || currentCount - lastCount >= 100000) {
					if (lastCount == 0) {
						LOGGER.info("Consumed {} events from Pub/Sub {}", currentCount, pubsubSubscriptionName);
					} else {
						LOGGER.info(
							"Consumed {} events from from Pub/Sub {} (rate:{}/s)",
								currentCount - lastCount,
								pubsubSubscriptionName,
								(float) (currentCount - lastCount) * 1000 / (now - lastLogTime)
						);
					}
					lastCount = currentCount;
					lastLogTime = now;
				}

				// Add message ID to list of messages to be ACK
				ackIds.add(message.getAckId());
				return destination;
			} catch (Exception exc) {
				LOGGER.warn("Caught exception while consuming Pub/Sub message", exc);
				throw exc;
			}
		}

		return null;
	}

	@Override
	public void commit() {}

	@Override
	public void shutdown() throws Exception {
		if (pubsubSubscriber != null) {
			pubsubSubscriber.close();
		}
	}

	private void updatePubsubMessageIterator() {
		if (pubsubPullRequest == null) {
			pubsubPullRequest = PullRequest.newBuilder()
				.setMaxMessages(MAX_MESSAGES_PULLED_PER_BATCH)
				.setSubscription(pubsubSubscriptionName)
				.build();
		}

		// Pull messages from Pub/Sub subscription and update message iterator.
		PullResponse pullResponse = pubsubSubscriber.pullCallable().call(pubsubPullRequest);
		pubsubMessageIterator = pullResponse.getReceivedMessagesList().listIterator();
	}

}
