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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import org.apache.pinot.common.metrics.ControllerMetrics;


/**
 * A class that implements a JXRS request filter and writer interceptor to track the number of in progress requests
 * using the gauge specified by the {@link TrackedByGauge} method annotation.
 * The gauge specified by this annotation will be incremented when the request starts and decremented once the
 * request has completed processing.
 *
 * See {@link PinotSegmentUploadDownloadRestletResource#downloadSegment} for an example of its usage.
 */
@Singleton
@Provider
@TrackInflightRequestMetrics
public class InflightRequestMetricsInterceptor implements ContainerRequestFilter, WriterInterceptor {

  @Inject
  ControllerMetrics _controllerMetrics;

  @Context
  ResourceInfo _resourceInfo;

  public InflightRequestMetricsInterceptor() {
  }

  @VisibleForTesting
  public InflightRequestMetricsInterceptor(ControllerMetrics controllerMetrics, ResourceInfo resourceInfo) {
    _controllerMetrics = controllerMetrics;
    _resourceInfo = resourceInfo;
  }

  @Override
  public void filter(ContainerRequestContext req)
      throws IOException {
    TrackedByGauge trackedByGauge = _resourceInfo.getResourceMethod().getAnnotation(TrackedByGauge.class);
    if (trackedByGauge != null) {
      _controllerMetrics.addValueToGlobalGauge(trackedByGauge.gauge(), 1L);
    }
  }

  @Override
  public void aroundWriteTo(WriterInterceptorContext ctx)
      throws IOException, WebApplicationException {
    try {
      ctx.proceed();
    } finally {
      TrackedByGauge trackedByGauge = _resourceInfo.getResourceMethod().getAnnotation(TrackedByGauge.class);
      if (trackedByGauge != null) {
        _controllerMetrics.addValueToGlobalGauge(trackedByGauge.gauge(), -1L);
      }
    }
  }
}
