package com.linkedin.thirdeye.detector.resources;

import io.dropwizard.hibernate.UnitOfWork;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path("/anomaly-results")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResultResource {
  private final AnomalyResultDAO dao;

  public AnomalyResultResource(AnomalyResultDAO dao) {
    this.dao = dao;
  }

  @POST
  @Timed
  @UnitOfWork
  public Response create(AnomalyResult anomalyResult) {
    Long id = dao.create(anomalyResult);
    return Response.ok(id).build();
  }

  @DELETE
  @Timed
  @UnitOfWork
  @Path("/id/{id}")
  public Response delete(@PathParam("id") Long id) {
    dao.delete(id);
    return Response.noContent().build();
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/id/{id}")
  public AnomalyResult findById(@PathParam("id") Long id) {
    AnomalyResult result = dao.findById(id);
    if (result == null) {
      throw new NotFoundException();
    }
    return result;
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/collection/{collection}")
  public List<AnomalyResult> find(@PathParam("collection") String collection) {
    DateTime endTime = DateTime.now();
    DateTime startTime = endTime.minusDays(7);
    return dao.findAllByCollectionAndTime(collection, startTime, endTime);
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/collection/{collection}/{startIsoTime}")
  public List<AnomalyResult> find(@PathParam("collection") String collection,
      @PathParam("startIsoTime") String startIsoTime) {
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startIsoTime);
    DateTime endTime = DateTime.now();
    return dao.findAllByCollectionAndTime(collection, startTime, endTime);
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/collection/{collection}/{startIsoTime}/{endIsoTime}")
  public List<AnomalyResult> find(@PathParam("collection") String collection,
      @PathParam("startIsoTime") String startIsoTime, @PathParam("endIsoTime") String endIsoTime,
      @QueryParam("filters") String filterJson, @QueryParam("metrics") String metrics)
      throws UnsupportedEncodingException {
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startIsoTime);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endIsoTime);
    String filters = null;

    List<AnomalyResult> results = new ArrayList<>();

    if (StringUtils.isNotBlank(filterJson)) {
      String filtersDecoded = URLDecoder.decode(filterJson, "UTF-8");
      if (!filtersDecoded.equals("{}")) {
        filters = ThirdEyeUtils.getSortedFiltersFromJson(filtersDecoded);
      }
    }

    if (StringUtils.isBlank(metrics)) {
      if (StringUtils.isBlank(filters)) {
        results = dao.findAllByCollectionAndTime(collection, startTime, endTime);
      } else {
        results = dao.findAllByCollectionTimeAndFilters(collection, startTime, endTime, filters);
      }
    } else {
      String[] metricList = metrics.split(",");
      for (String metric : metricList) {
        List<AnomalyResult> metricResults;
        if (StringUtils.isBlank(filters)) {
          metricResults =
              dao.findAllByCollectionTimeAndMetric(collection, metric, startTime, endTime);
        } else {
          metricResults =
              dao.findAllByCollectionTimeMetricAndFilters(collection, metric, startTime, endTime,
                  filters);
        }
        results.addAll(metricResults);
      }
    }

    return results;
  }
}
