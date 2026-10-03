package org.apache.pinot.broker.api.resources;

import javax.ws.rs.core.Response;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class QueryParseResourceTest {

    @Test
    public void testValidQuery() {
        QueryParseResource resource = new QueryParseResource();

        Response response =
                resource.parseQuery("SELECT * FROM myTable LIMIT 10");

        assertEquals(response.getStatus(),
                Response.Status.OK.getStatusCode());
    }

    @Test
    public void testInvalidQuery() {
        QueryParseResource resource = new QueryParseResource();

        Response response =
                resource.parseQuery("SELECT FROM WHERE");

        assertEquals(response.getStatus(),
                Response.Status.BAD_REQUEST.getStatusCode());
    }
}
