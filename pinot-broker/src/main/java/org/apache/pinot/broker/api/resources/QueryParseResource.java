package org.apache.pinot.broker.api.resources;

import java.util.Collections;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.pinot.sql.parsers.CalciteSqlParser;

@Path("/query/parse")
@Produces(MediaType.APPLICATION_JSON)
public class QueryParseResource {

    @POST
    public Response parseQuery(String query) {
        try {
            // Parse SQL only
            CalciteSqlParser.compileToSqlNodeAndOptions(query);
            Map<String, Object> response =
                    Collections.singletonMap("parsed", true);
            return Response.ok(response).build();
        } catch (Exception e) {
            Map<String, Object> error =
                    Collections.singletonMap("error", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(error)
                    .build();
        }
    }
}
