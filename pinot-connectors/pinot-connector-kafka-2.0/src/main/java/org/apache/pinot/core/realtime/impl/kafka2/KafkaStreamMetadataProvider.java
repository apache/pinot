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
package org.apache.pinot.core.realtime.impl.kafka2;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.pinot.core.realtime.stream.OffsetCriteria;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamMetadataProvider;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class KafkaStreamMetadataProvider extends KafkaConnectionHandler implements StreamMetadataProvider {

    private AdminClient _adminClient;

    public KafkaStreamMetadataProvider(StreamConfig streamConfig, int partition) {
        super(streamConfig, partition);
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _config.getBootstrapHosts());
        _adminClient = AdminClient.create(props);
    }

    @Override
    public int fetchPartitionCount(long timeoutMillis) {
        DescribeTopicsResult result = _adminClient.describeTopics(Collections.singletonList(_config.getKafkaTopicName()));
        Map<String, KafkaFuture<TopicDescription>> values = result.values();
        KafkaFuture<TopicDescription> topicDescription = values.get(_config.getKafkaTopicName());
        try {
            return topicDescription.get().partitions().size();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis) throws TimeoutException {

        Preconditions.checkNotNull(offsetCriteria);
        if (offsetCriteria.isLargest()) {
            return _consumer.endOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis)).get(_topicPartition);
        } else if (offsetCriteria.isSmallest()) {
            return _consumer.beginningOffsets(Collections.singletonList(_topicPartition), Duration.ofMillis(timeoutMillis)).get(_topicPartition);
        } else {
            throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria.toString());
        }

    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
