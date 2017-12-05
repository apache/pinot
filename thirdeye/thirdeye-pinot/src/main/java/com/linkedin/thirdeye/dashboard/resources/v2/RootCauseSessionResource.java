package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.datalayer.bao.RootcauseSessionManager;
import com.linkedin.thirdeye.datalayer.dto.RootcauseSessionDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


@Path(value = "/session")
@Produces(MediaType.APPLICATION_JSON)
public class RootCauseSessionResource {
  private final RootcauseSessionManager sessionDAO;
  private final ObjectMapper mapper;

  public RootCauseSessionResource(RootcauseSessionManager sessionDAO, ObjectMapper mapper) {
    this.sessionDAO = sessionDAO;
    this.mapper = mapper;
  }

  @GET
  @Path("/{sessionId}")
  public RootcauseSessionDTO getSession(@PathParam("sessionId") Long sessionId) {
    if (sessionId == null) {
      throw new IllegalArgumentException("Must provide sessionId");
    }

    RootcauseSessionDTO session = this.sessionDAO.findById(sessionId);

    if (session == null) {
      throw new IllegalArgumentException(String.format("Could not resolve session id %d", sessionId));
    }

    return session;
  }

  @POST
  @Path("/")
  public Long postSession(
      String jsonString) throws IOException {
    RootcauseSessionDTO session = this.mapper.readValue(jsonString, new TypeReference<RootcauseSessionDTO>() {});

    return this.sessionDAO.save(session);
  }

  @GET
  @Path("/query")
  public List<RootcauseSessionDTO> getSession(
      @QueryParam("id") String idsString,
      @QueryParam("name") String namesString,
      @QueryParam("owner") String ownersString,
      @QueryParam("previousId") String previousIdsString,
      @QueryParam("anomalyRangeStart") Long anomalyRangeStart,
      @QueryParam("anomalyRangeEnd") Long anomalyRangeEnd,
      @QueryParam("createdRangeStart") Long createdRangeStart,
      @QueryParam("createdRangeEnd") Long createdRangeEnd) {

    List<Predicate> predicates = new ArrayList<>();

    if (!StringUtils.isBlank(idsString)) {
      predicates.add(Predicate.IN("base_id", split(idsString)));
    }

    if (!StringUtils.isBlank(namesString)) {
      predicates.add(Predicate.IN("name", split(namesString)));
    }

    if (!StringUtils.isBlank(ownersString)) {
      predicates.add(Predicate.IN("owner", split(ownersString)));
    }

    if (!StringUtils.isBlank(previousIdsString)) {
      predicates.add(Predicate.IN("previousId", split(previousIdsString)));
    }

    if (anomalyRangeStart != null) {
      predicates.add(Predicate.GT("anomalyRangeEnd", anomalyRangeStart));
    }

    if (anomalyRangeEnd != null) {
      predicates.add(Predicate.LT("anomalyRangeStart", anomalyRangeEnd));
    }

    if (createdRangeStart != null) {
      predicates.add(Predicate.GE("created", createdRangeStart));
    }

    if (createdRangeEnd != null) {
      predicates.add(Predicate.LT("created", createdRangeEnd));
    }

    if (predicates.isEmpty()) {
      throw new IllegalArgumentException("Must provide at least one property");
    }

    return this.sessionDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[predicates.size()])));
  }

  private static String[] split(String str) {
    List<String> args = new ArrayList<>(Arrays.asList(str.split(",")));
    Iterator<String> itStr = args.iterator();
    while (itStr.hasNext()) {
      if (itStr.next().length() <= 0) {
        itStr.remove();
      }
    }
    return args.toArray(new String[args.size()]);
  }
}
