package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.query.ThirdEyeQueryExecutor;
import com.linkedin.thirdeye.query.ThirdEyeQueryResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/query")
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryResource.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ThirdEyeQueryExecutor queryExecutor;

  public QueryResource(ThirdEyeQueryExecutor queryExecutor) {
    this.queryExecutor = queryExecutor;
  }

  @GET
  @Path("/{sql}")
  @Timed
  public QueryResponse get(@PathParam("sql") String sql,
                           @QueryParam("iso8601") boolean iso8601,
                           @QueryParam("timeZone") String timeZoneString) throws Exception {
    sql = URLDecoder.decode(sql, "UTF-8");

    ThirdEyeQueryResult result;
    try {
      result = queryExecutor.executeQuery(sql);
    } catch (IllegalArgumentException|IllegalStateException e) {
      LOGGER.error("Malformed SQL {}", sql, e);
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      LOGGER.error("Exception executing SQL {}", sql);
      throw e;
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("{}", sql);
    }

    DateTimeZone timeZone = null;
    if (timeZoneString != null) {
      timeZone = DateTimeZone.forID(timeZoneString);
    }

    QueryResponse response = new QueryResponse(iso8601, timeZone);
    response.setDimensions(result.getDimensions());
    response.setMetrics(result.getMetrics());

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.getData().entrySet()) {
      response.addDatum(entry.getKey(), entry.getValue());
    }

    return response;
  }

  public static class QueryResponse {
    private final boolean iso8601;
    private final Map<String, Map<String, Number[]>> data = new HashMap<>();
    private final DateTimeZone timeZone;

    private List<String> dimensions;
    private List<String> metrics;

    public QueryResponse(boolean iso8601, DateTimeZone timeZone) {
      this.iso8601 = iso8601;
      this.timeZone = timeZone;
    }

    public void addDatum(DimensionKey dimensionKey, MetricTimeSeries timeSeries) throws Exception {
      if (metrics == null) {
        throw new IllegalStateException("Must add metrics to query response first");
      }

      String key = OBJECT_MAPPER.writeValueAsString(dimensionKey.getDimensionValues());
      data.put(key, new HashMap<String, Number[]>());

      for (Long time : timeSeries.getTimeWindowSet()) {
        Number[] values = new Number[timeSeries.getSchema().getNumMetrics()];
        for (int i = 0; i < timeSeries.getSchema().getNumMetrics(); i++) {
          values[i] = timeSeries.get(time, timeSeries.getSchema().getMetricName(i));
        }

        DateTime dateTime = new DateTime(time);
        if (timeZone != null) {
          dateTime = dateTime.toDateTime(timeZone);
        }

        if (iso8601) {
          data.get(key).put(dateTime.toString(ISODateTimeFormat.dateTime()), values);
        } else {
          data.get(key).put(Long.toString(dateTime.getMillis()), values);
        }
      }
    }

    public Map<String, Map<String, Number[]>> getData() {
      return data;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
      this.dimensions = dimensions;
    }

    public List<String> getMetrics() {
      return metrics;
    }

    public void setMetrics(List<String> metrics) {
      this.metrics = metrics;
    }
  }
}
