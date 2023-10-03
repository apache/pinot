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
package org.apache.pinot.spi.stream;

import com.google.common.base.Joiner;


/**
 * Defines the keys for the stream config properties map
 */
public class StreamConfigProperties {
  private StreamConfigProperties() {
  }

  public static final String DOT_SEPARATOR = ".";
  public static final String STREAM_PREFIX = "stream";
  // TODO: this can be removed, check all properties before doing so
  public static final String LLC_SUFFIX = ".llc";

  /**
   * Generic properties
   */
  public static final String STREAM_TYPE = "streamType";
  public static final String STREAM_TOPIC_NAME = "topic.name";
  @Deprecated
  public static final String STREAM_CONSUMER_TYPES = "consumer.type";
  public static final String STREAM_CONSUMER_FACTORY_CLASS = "consumer.factory.class.name";
  public static final String STREAM_CONSUMER_OFFSET_CRITERIA = "consumer.prop.auto.offset.reset";
  public static final String STREAM_FETCH_TIMEOUT_MILLIS = "fetch.timeout.millis";
  public static final String STREAM_CONNECTION_TIMEOUT_MILLIS = "connection.timeout.millis";
  public static final String STREAM_IDLE_TIMEOUT_MILLIS = "idle.timeout.millis";
  public static final String STREAM_DECODER_CLASS = "decoder.class.name";
  public static final String DECODER_PROPS_PREFIX = "decoder.prop";
  public static final String GROUP_ID = "hlc.group.id";
  public static final String PARTITION_MSG_OFFSET_FACTORY_CLASS = "partition.offset.factory.class.name";
  public static final String TOPIC_CONSUMPTION_RATE_LIMIT = "topic.consumption.rate.limit";
  public static final String METADATA_POPULATE = "metadata.populate";

  /**
   * Time threshold that will keep the realtime segment open for before we complete the segment
   */
  public static final String SEGMENT_FLUSH_THRESHOLD_TIME = "realtime.segment.flush.threshold.time";

  /**
   * @deprecated because the property key is confusing (says size but is actually rows). Use
   * {@link StreamConfigProperties#SEGMENT_FLUSH_THRESHOLD_ROWS}
   *
   * Row count flush threshold for realtime segments. This behaves in a similar way for HLC and LLC. For HLC,
   * since there is only one consumer per server, this size is used as the size of the consumption buffer and
   * determines after how many rows we flush to disk. For example, if this threshold is set to two million rows,
   * then a high level consumer would have a buffer size of two million.
   *
   * For LLC, this size is divided across all the segments assigned to a given server and is set on a per segment
   * basis. Assuming a low level consumer server is assigned four stream partitions to consume from and a flush
   * size of two million, then each consuming segment would have a flush size of five hundred thousand rows, for a
   * total of two million rows in memory.
   *
   * Keep in mind that this NOT a hard threshold, as other tables can also be assigned to this server, and that in
   * certain conditions (eg. if the number of servers, replicas of partitions changes) where partition
   * to server assignment changes, it's possible to end up with more (or less) than this number of rows in memory.
   *
   * If this value is set to 0, then the consumers adjust the number of rows consumed by a partition such that
   * the size of the completed segment is the desired size (see REALTIME_DESIRED_SEGMENT_SIZE), unless
   * REALTIME_SEGMENT_FLUSH_TIME is reached first)
   */
  public static final String DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS = "realtime.segment.flush.threshold.size";
  public static final String SEGMENT_FLUSH_THRESHOLD_ROWS = "realtime.segment.flush.threshold.rows";
  public static final String SEGMENT_FLUSH_ENABLE_COLUMN_MAJOR = "realtime.segment.flush.enable_column_major";

  /**
   * @deprecated because the property key is confusing (desired size is not indicative of segment size).
   * Use {@link StreamConfigProperties#SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE}
   *
   * The desired size of a completed realtime segment.
   * This config is used only if REALTIME_SEGMENT_FLUSH_SIZE is set
   * to 0. Default value of REALTIME_SEGMENT_FLUSH_SIZE is "200M". Values are parsed using DataSize class.
   *
   * The value for this configuration should be chosen based on the amount of memory available on consuming
   * machines, the number of completed segments that are expected to be resident on the machine and the amount
   * of memory used by consuming machines. In other words:
   *
   *    numPartitionsInMachine * (consumingPartitionMemory + numPartitionsRetained * REALTIME_DESIRED_SEGMENT_SIZE)
   *
   * must be less than or equal to the total memory available to store pinot data.
   *
   * Note that consumingPartitionMemory will vary depending on the rows that are consumed.
   *
   * Not included here is any heap memory used (currently inverted index uses heap memory for consuming partitions).
   */
  public static final String DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE = "realtime.segment.flush.desired.size";
  public static final String SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE = "realtime.segment.flush.threshold.segment.size";

  /**
   * The initial num rows to use for segment size auto tuning. By default 100_000 is used.
   */
  public static final String SEGMENT_FLUSH_AUTOTUNE_INITIAL_ROWS = "realtime.segment.flush.autotune.initialRows";
  // Time threshold that controller will wait for the segment to be built by the server
  public static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "realtime.segment.commit.timeoutSeconds";

  /**
   * Config used to indicate whether server should by-pass controller and directly upload the segment to the deep store
   */
  public static final String SERVER_UPLOAD_TO_DEEPSTORE = "realtime.segment.serverUploadToDeepStore";

  /**
   * Helper method to create a stream specific property
   */
  public static String constructStreamProperty(String streamType, String property) {
    return Joiner.on(DOT_SEPARATOR).join(STREAM_PREFIX, streamType, property);
  }

  public static String getPropertySuffix(String incoming, String propertyPrefix) {
    String prefix = propertyPrefix + DOT_SEPARATOR;
    return incoming.substring(prefix.length());
  }
}
