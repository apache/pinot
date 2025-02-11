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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.utils.JsonUtils;
import org.joda.time.DateTime;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * A simple random generator that fakes RSVP
 */
public class RsvpSourceGenerator implements PinotSourceDataGenerator {
  private final int _nullProbability;
  private final KeyColumn _keyColumn;
  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ')
          .append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter();

  public RsvpSourceGenerator(KeyColumn keyColumn, int nullProbability) {
    _keyColumn = keyColumn;
    _nullProbability = nullProbability;
  }

  public RSVP createMessage() {
    String eventId = Math.abs(ThreadLocalRandom.current().nextLong(100)) + "";
    ObjectNode json = JsonUtils.newObjectNode();
    ObjectNode eventJson = JsonUtils.newObjectNode();
    json.set("event", eventJson);
    ObjectNode groupJson = JsonUtils.newObjectNode();
    json.set("group", groupJson);
    ObjectNode venueJson = JsonUtils.newObjectNode();
    json.set("venue", venueJson);

    String venueName = "venue_name" + ThreadLocalRandom.current().nextInt();
    venueJson.put("venue_name", venueName);
    json.put("venue_name", venueName);

    String eventName = "event_name" + ThreadLocalRandom.current().nextInt();
    eventJson.put("event_name", eventName);
    json.put("event_name", eventName);

    json.put("event_id", eventId);
    boolean isNull = ThreadLocalRandom.current().nextInt(100) < _nullProbability;
    json.put("event_time", isNull ? null : DATE_TIME_FORMATTER.format(LocalDateTime.now().plusDays(10)));

    ArrayNode groupTopicsJson = JsonUtils.newArrayNode();
    groupJson.set("group_topics", groupTopicsJson);
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(5) + 1; i++) {
      ObjectNode groupTopicJson = JsonUtils.newObjectNode();
      groupTopicJson.put("topic_name", "topic_name" + ThreadLocalRandom.current().nextInt(10));
      groupTopicJson.put("urlkey", "http://group-url-" + ThreadLocalRandom.current().nextInt(1000));
      groupTopicsJson.add(groupTopicJson);
    }

    String groupCity = "group_city" + ThreadLocalRandom.current().nextInt(1000);
    groupJson.put("group_city", groupCity);
    json.put("group_city", groupCity);

    String groupCountry = "group_country" + ThreadLocalRandom.current().nextInt(100);
    groupJson.put("group_country", groupCountry);
    json.put("group_country", groupCountry);

    long groupId = Math.abs(ThreadLocalRandom.current().nextLong());
    groupJson.put("group_id", groupId);
    json.put("group_id", groupId);

    String groupName = "group_name" + ThreadLocalRandom.current().nextInt();
    groupJson.put("group_name", groupName);
    json.put("group_name", groupName);

    double groupLat = ThreadLocalRandom.current().nextDouble(-90.0, 90.0);
    groupJson.put("group_lat", groupLat);
    json.put("group_lat", groupLat);

    double groupLon = ThreadLocalRandom.current().nextDouble(-90.0, 90.0);
    groupJson.put("group_lon", groupLon);
    json.put("group_lon", groupLon);

    json.put("mtime", DateTime.now().getMillis());

    json.put("rsvp_id", ThreadLocalRandom.current().nextLong(100));
    json.put("guests", ThreadLocalRandom.current().nextInt(100));

    json.put("rsvp_count", ThreadLocalRandom.current().nextInt(10) + 1);
    return new RSVP(eventId, eventId, json);
  }

  @Override
  public void init(Properties properties) {
  }

  @Override
  public List<StreamDataProducer.RowWithKey> generateRows() {
    RSVP msg = createMessage();
    byte[] key;
    switch (_keyColumn) {
      case EVENT_ID:
        key = msg.getEventId().getBytes(UTF_8);
        break;
      case RSVP_ID:
        key = msg.getRsvpId().getBytes(UTF_8);
        break;
      default:
        key = null;
        break;
    }
    return ImmutableList.of(new StreamDataProducer.RowWithKey(key, msg.getPayload().toString().getBytes(UTF_8)));
  }

  @Override
  public void close()
      throws Exception {
  }

  public enum KeyColumn {
    NONE,
    EVENT_ID,
    RSVP_ID
  }
}
