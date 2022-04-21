package org.apache.pinot.tools.streams;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.spi.stream.RowWithKey;
import org.apache.pinot.spi.utils.JsonUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * A simple random generator that fakes RSVP
 */
public class RsvpSourceGenerator implements PinotSourceGenerator {
  private final KeyColumn _keyColumn;
  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ')
          .append(DateTimeFormatter.ISO_LOCAL_TIME).toFormatter();

  public RsvpSourceGenerator(KeyColumn keyColumn) {
    _keyColumn = keyColumn;
  }

  public RSVP createMessage() {
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

  @Override
  public void init(Properties properties) {
  }

  @Override
  public List<RowWithKey> generateRows() {
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
    return ImmutableList.of(new RowWithKey(key, msg.getPayload().toString().getBytes(UTF_8)));
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
