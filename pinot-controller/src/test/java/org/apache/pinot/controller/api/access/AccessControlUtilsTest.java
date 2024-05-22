package org.apache.pinot.controller.api.access;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AccessControlUtilsTest {
    @Test
    public void testValidatePermissionAllowed() {
        AccessControl ac = Mockito.mock(AccessControl.class);
        Mockito.when(ac.hasAccess(Mockito.anyString(), Mockito.any(AccessType.class),
                Mockito.any(HttpHeaders.class), Mockito.anyString()))
                .thenReturn(true);

        HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

        AccessControlUtils.validatePermission("testTable", AccessType.READ, mockHttpHeaders,
                "/testEndpoint", ac);
    }

    @Test
    public void testValidatePermissionDenied() {
        AccessControl ac = Mockito.mock(AccessControl.class);
        Mockito.when(ac.hasAccess(Mockito.anyString(), Mockito.any(AccessType.class),
                Mockito.any(HttpHeaders.class), Mockito.anyString()))
                .thenReturn(false);

        HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

        try {
            AccessControlUtils.validatePermission("testTable", AccessType.READ, mockHttpHeaders,
                    "/testEndpoint", ac);
            Assert.fail("Expected ControllerApplicationException");
        } catch (ControllerApplicationException e) {
            Assert.assertTrue(e.getMessage().contains("Permission is denied"));
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
        }
    }

    @Test
    public void testValidatePermissionWithNoSuchMethodError() {
        AccessControl ac = Mockito.mock(AccessControl.class);
        Mockito.when(ac.hasAccess(Mockito.anyString(), Mockito.any(AccessType.class),
                Mockito.any(HttpHeaders.class), Mockito.anyString()))
                .thenThrow(new NoSuchMethodError("Method not found"));

        HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

        try {
            AccessControlUtils.validatePermission("testTable", AccessType.READ, mockHttpHeaders,
                    "/testEndpoint", ac);
        } catch (ControllerApplicationException e) {
            Assert.assertTrue(e.getMessage().contains("Caught exception while validating permission for testTable"));
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
