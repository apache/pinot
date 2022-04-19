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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents one Pinot Real Time Source that is capable of
 * 1. Keep running forever
 * 2. Pull from generator and write into StreamDataProducer
 * The Source has a thread that is looping forever.
 */
public class PinotRealtimeSource implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRealtimeSource.class);
  public static final String KEY_OF_MAX_MESSAGE_PER_SECOND = "pinot.stream.max.message.per.second";
  public static final String KEY_OF_TOPIC_NAME = "pinot.topic.name";
  public static final long DEFAULT_MAX_MESSAGE_PER_SECOND = Long.MAX_VALUE;
  public static final long DEFAULT_EMPTY_SOURCE_SLEEP_MS = 10;
  protected final StreamDataProducer _producer;
  protected final PinotSourceGenerator _generator;
  protected final String _topicName;
  protected final ExecutorService _executor;
  protected RateLimiter _rateLimiter;
  protected volatile boolean _shutdown;

  /**
   * Constructs a source by passing in a Properties file, a generator, and a producer
   * @param settings the settings for all components passed in
   * @param generator the generator that can create data
   * @param producer the producer to write the generator's data into
   */
  public PinotRealtimeSource(Properties settings, PinotSourceGenerator generator, StreamDataProducer producer) {
    this(settings, generator, producer, Executors.newSingleThreadExecutor());
  }

  /**
   * Constructs a source by passing in properties file, a generator, a producer and an executor service
   * @param settings the settings for all components passed in
   * @param generator the generator that can create data
   * @param producer the producer to write the generator's data into
   * @param executor the preferred executor instead of creating a thread pool.
   */
  public PinotRealtimeSource(Properties settings, PinotSourceGenerator generator, StreamDataProducer producer,
      ExecutorService executor) {
    _producer = producer;
    Preconditions.checkNotNull(_producer, "Producer of a stream cannot be null");
    _generator = generator;
    Preconditions.checkNotNull(_generator, "Generator of a stream cannot be null");
    _executor = executor;
    _topicName = settings.getProperty(KEY_OF_TOPIC_NAME);
    Preconditions.checkNotNull(_topicName, "Topic name needs to be set via " + KEY_OF_TOPIC_NAME);
    String qpsStr = settings.getProperty(KEY_OF_MAX_MESSAGE_PER_SECOND, String.valueOf(DEFAULT_MAX_MESSAGE_PER_SECOND));
    long maxQps = DEFAULT_MAX_MESSAGE_PER_SECOND;
    try {
      maxQps = Long.parseLong(qpsStr);
    } catch (NumberFormatException ex) {
      LOGGER.warn("Cannot parse {} as max qps setting, using default {}", qpsStr, DEFAULT_MAX_MESSAGE_PER_SECOND);
    }
    _rateLimiter = RateLimiter.create(maxQps);
  }

  public void run() {
    _executor.execute(this::mainLoop);
  }

  public void mainLoop() {
    while (!_shutdown) {
      List<byte[]> rows = _generator.generateRows();
      if (rows.isEmpty()) {
        try {
          Thread.sleep(DEFAULT_EMPTY_SOURCE_SLEEP_MS);
        } catch (InterruptedException ex) {
          LOGGER.info("Interrupted but will continue");
        }
      } else {
        _rateLimiter.acquire(rows.size());
        if (!_shutdown) {
          _producer.produceBatch(_topicName, rows);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    _generator.close();
    _shutdown = true;
    _producer.close();
    _executor.shutdownNow();
  }

  public void stop() throws Exception {
    close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String _topic;
    private long _maxMessagePerSecond;
    private PinotSourceGenerator _generator;
    private StreamDataProducer _producer;
    private ExecutorService _executor;
    public Builder setTopic(String topic) {
      _topic = topic;
      return this;
    }

    public Builder setMaxMessagePerSecond(long maxMessagePerSecond) {
      _maxMessagePerSecond = maxMessagePerSecond;
      return this;
    }

    public Builder setGenerator(PinotSourceGenerator generator) {
      _generator = generator;
      return this;
    }

    public Builder setProducer(StreamDataProducer producer) {
      _producer = producer;
      return this;
    }

    public Builder setExecutor(ExecutorService executor) {
      _executor = executor;
      return this;
    }
    public PinotRealtimeSource build() {
      Properties properties = new Properties();
      properties.setProperty(KEY_OF_MAX_MESSAGE_PER_SECOND, String.valueOf(_maxMessagePerSecond));
      properties.setProperty(KEY_OF_TOPIC_NAME, _topic);
      return new PinotRealtimeSource(properties, _generator, _producer, _executor);
    }
  }
}
