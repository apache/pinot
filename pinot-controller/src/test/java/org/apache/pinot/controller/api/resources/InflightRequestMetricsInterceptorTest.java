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
package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import java.lang.reflect.Method;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.ext.WriterInterceptorContext;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class InflightRequestMetricsInterceptorTest {

  @Mock
  private ControllerMetrics _controllerMetrics;

  @Mock
  private ResourceInfo _resourceInfo;

  @Mock
  private ContainerRequestContext _containerRequestContext;

  @Mock
  private WriterInterceptorContext _writerInterceptorContext;

  private InflightRequestMetricsInterceptor _interceptor;

  @BeforeMethod
  public void setUp() {
    initMocks(this);
    _interceptor = new InflightRequestMetricsInterceptor(_controllerMetrics, _resourceInfo);
  }

  @Test
  public void testContainerRequestFilterIncrementsGauge()
      throws Exception {
    Method methodOne = TrackedClass.class.getDeclaredMethod("trackedMethod");
    when(_resourceInfo.getResourceMethod()).thenReturn(methodOne);
    _interceptor.filter(_containerRequestContext);
    verify(_controllerMetrics)
        .addValueToGlobalGauge(ControllerGauge.SEGMENT_DOWNLOADS_IN_PROGRESS, 1L);
  }

  @Test
  public void testWriterInterceptorDecrementsGauge()
      throws Exception {
    Method methodOne = TrackedClass.class.getDeclaredMethod("trackedMethod");
    when(_resourceInfo.getResourceMethod()).thenReturn(methodOne);
    _interceptor.aroundWriteTo(_writerInterceptorContext);
    verify(_controllerMetrics)
        .addValueToGlobalGauge(ControllerGauge.SEGMENT_DOWNLOADS_IN_PROGRESS, -1L);
  }

  @Test(expectedExceptions = IOException.class)
  public void testWriterInterceptorDecrementsGaugeWhenWriterThrowsException()
      throws Exception {
    Method methodOne = TrackedClass.class.getDeclaredMethod("trackedMethod");
    when(_resourceInfo.getResourceMethod()).thenReturn(methodOne);
    doThrow(new IOException()).when(_writerInterceptorContext).proceed();
    try {
      _interceptor.aroundWriteTo(_writerInterceptorContext);
    } finally {
      verify(_controllerMetrics).addValueToGlobalGauge(ControllerGauge.SEGMENT_DOWNLOADS_IN_PROGRESS, -1L);
    }
  }

  private static class TrackedClass {
    @TrackInflightRequestMetrics
    @TrackedByGauge(gauge = ControllerGauge.SEGMENT_DOWNLOADS_IN_PROGRESS)
    public void trackedMethod() {
    }
  }
}
