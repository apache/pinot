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
package org.apache.pinot.tools.streams;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class MeetupRsvpStream {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MeetupRsvpStream.class);
  private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .append(DateTimeFormatter.ISO_LOCAL_TIME)
      .toFormatter();
  private static final String DEFAULT_TOPIC_NAME = "meetupRSVPEvents";
  protected String _topicName = DEFAULT_TOPIC_NAME;

  protected final boolean _partitionByKey;
  protected final StreamDataProducer _producer;
  private final Source _source;

  public MeetupRsvpStream()
      throws Exception {
    this(false);
  }

  public MeetupRsvpStream(boolean partitionByKey)
      throws Exception {
    _partitionByKey = partitionByKey;

    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    _source = new Source(createConsumer());
  }

  public void run()
      throws Exception {
    _source.start();
  }

  public void stopPublishing() {
    _producer.close();
    _source.close();
  }

  protected Consumer<RSVP> createConsumer() {
    return message -> {
      try {
        if (_partitionByKey) {
          _producer.produce(_topicName, message.getEventId().getBytes(UTF_8),
              message.getPayload().toString().getBytes(UTF_8));
        } else {
          _producer.produce(_topicName, message.getPayload().toString().getBytes(UTF_8));
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing the message: {}", message, e);
      }
    };
  }

  private static class Source implements AutoCloseable, Runnable {

    private final Consumer<RSVP> _consumer;

    private final ExecutorService _executorService = Executors.newSingleThreadExecutor();
    private volatile Future<?> _future;

    private Source(Consumer<RSVP> consumer) {
      _consumer = consumer;
    }

    @Override
    public void close() {
      if (_future != null) {
        _future.cancel(true);
      }
      _executorService.shutdownNow();
    }

    public void start() {
      _future = _executorService.submit(this);
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          RSVP rsvp = createMessage();
          _consumer.accept(rsvp);
          int delay = (int) (Math.log(ThreadLocalRandom.current().nextDouble()) / Math.log(0.999)) + 1;
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private RSVP createMessage() {
      String eventId = Math.abs(ThreadLocalRandom.current().nextLong()) + "";
      ObjectNode json = JsonUtils.newObjectNode();
      json.put("venue_name", "venue_name" + ThreadLocalRandom.current().nextInt());
      json.put("event_name", "event_name" + ThreadLocalRandom.current().nextInt());
      json.put("event_id", eventId);
      json.put("event_time", DATE_TIME_FORMATTER.format(LocalDateTime.now().plusDays(10)));
      json.put("group_city", "group_city" + ThreadLocalRandom.current().nextInt());
      json.put("group_country", "group_country" + ThreadLocalRandom.current().nextInt());
      json.put("group_id", Math.abs(ThreadLocalRandom.current().nextLong()));
      json.put("group_name", "group_name" + ThreadLocalRandom.current().nextInt());
      json.put("group_lat", ThreadLocalRandom.current().nextFloat());
      json.put("group_lon", ThreadLocalRandom.current().nextFloat());
      json.put("mtime", DATE_TIME_FORMATTER.format(LocalDateTime.now()));
      json.put("rsvp_count", 1);
      return new RSVP(eventId, eventId, json);
    }
  }
}
