package org.apache.pinot.core.auth;

import java.lang.reflect.Method;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FineGrainedAuthUtilsTest {

    @Test
    public void testValidateFineGrainedAuthAllowed() {
        FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
        Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(true);

        UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
        HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

        FineGrainedAuthUtils.validateFineGrainedAuth(getClusterMethod(), mockUriInfo, mockHttpHeaders, ac);
    }

    @Test
    public void testValidateFineGrainedAuthDenied() {
        FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
        Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(false);

        UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
        HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

        try {
            FineGrainedAuthUtils.validateFineGrainedAuth(getClusterMethod(), mockUriInfo, mockHttpHeaders, ac);
            Assert.fail("Expected WebApplicationException");
        } catch (WebApplicationException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied to testAction in the cluster"));
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
        }
    }

    @Test
    public void testValidateFineGrainedAuthWithNoSuchMethodError() {
        FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
        Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(new NoSuchMethodError("Method not found"));

        UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
        HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

        try {
            FineGrainedAuthUtils.validateFineGrainedAuth(getClusterMethod(), mockUriInfo, mockHttpHeaders, ac);
            Assert.fail("Expected WebApplicationException");
        } catch (WebApplicationException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to check for access"));
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    private Method getClusterMethod() {
        return new Object() {
            @Authorize(targetType = TargetType.CLUSTER, action = "testAction")
            void getClusterMethod() { }
        }.getClass().getDeclaredMethods()[0];
    }
}
