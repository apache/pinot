package com.linkedin.thirdeye.anomaly.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalApiQueryUtil {
  private final static Logger LOG = LoggerFactory.getLogger(ExternalApiQueryUtil.class);
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // 1000ms: the smallest time unit is 1 second. 1ms: smallest unit is 1 ms.
  // The value depends on the backend database (e.g., MySQL < 5.7 is 1000ms).
  public static int TIME_PRECISION_IN_MILLIS = 1000;

  private final static String INFORMED_QUERY = "?topics=%s";

  private String queryPrefix;

  public ExternalApiQueryUtil(String informedUrl) {
    this.queryPrefix = informedUrl + INFORMED_QUERY;
  }

  public InFormedPosts retrieveInformedEvents(String informedEventName, DateTime startTimeInclusive,
      DateTime endTimeExclusive) {
    long startMillis = startTimeInclusive.getMillis() / TIME_PRECISION_IN_MILLIS;
    long endMillis = endTimeExclusive.getMillis() / TIME_PRECISION_IN_MILLIS;
    String fetchUrl =
        new StringBuilder(String.format(queryPrefix, informedEventName)).append("&start=")
            .append(startMillis).append("&end=").append(endMillis).toString();
    LOG.info("Fetching {} events between {} and {} from {}", informedEventName, startMillis,
        endMillis, fetchUrl);

    long start = System.currentTimeMillis();
    InFormedPosts informedPosts = null;

    try {
      URL website = new URL(fetchUrl);
      URLConnection connection = website.openConnection();
      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

      StringBuilder response = new StringBuilder();
      String inputLine;

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
      informedPosts = OBJECT_MAPPER.readValue(response.toString(), InFormedPosts.class);
    } catch (Exception e) {
      LOG.error("Error reading from informed API", e);
    }

    LOG.info("Fetching Time: {} ms", System.currentTimeMillis() - start);
      LOG.info("Message Count: {}", informedPosts.data.size());
      if (informedPosts.status.equals("success")) {
        if (LOG.isTraceEnabled()) {
          for (InFormedPost post : informedPosts.data) {
            LOG.trace(post.toString());
          }
        }
        //        informedPosts.sortInAscendingOrder();
        return informedPosts;
    }
   return new InFormedPosts();
  }

  public static class InFormedPosts {
    public List<InFormedPost> data = new ArrayList<>();
    public String status = "failed";

    public List<InFormedPost> getData() {
      return data;
    }

    public void setData(List<InFormedPost> data) {
      this.data = data;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class InFormedPost {
    public long id;
    public String topic;
    public String content;
    public String post_time;
    public String source;
    public String timestamp;
    public String user;

    public long getId() {
      return id;
    }

    public void setId(long id) {
      this.id = id;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public String getContent() {
      return content;
    }

    public void setContent(String content) {
      this.content = content;
    }

    public String getPost_time() {
      return post_time;
    }

    public void setPost_time(String post_time) {
      this.post_time = post_time;
    }

    public String getSource() {
      return source;
    }

    public void setSource(String source) {
      this.source = source;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(String timestamp) {
      this.timestamp = timestamp;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }
}
