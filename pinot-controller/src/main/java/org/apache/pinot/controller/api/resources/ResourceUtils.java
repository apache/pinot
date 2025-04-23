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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;

public class ResourceUtils {

  private ResourceUtils() {
  }

  // Shared static variable
  private static AtomicLong _deepStoreSegmentUploadCount = new AtomicLong(0);
  private static AtomicLong _deepStoreSegmentBytesUploading = new AtomicLong(0);
  private static AtomicLong _deepStoreSegmentDownloadCount = new AtomicLong(0);
  private static AtomicLong _deepStoreSegmentBytesDownloading = new AtomicLong(0);

  public static List<String> getExistingTableNamesWithType(PinotHelixResourceManager pinotHelixResourceManager,
                                                           String tableName, @Nullable TableType tableType,
                                                           Logger logger) {
    try {
      return pinotHelixResourceManager.getExistingTableNamesWithType(tableName, tableType);
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(logger, e.getMessage(), Response.Status.NOT_FOUND);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(logger, e.getMessage(), Response.Status.FORBIDDEN);
    }
  }

  /**
   * Validates the permission and access for a specified type based on the incoming request and HTTP headers.
   * This method ensures that the current user has the necessary permissions to perform the specified action
   * on the given type. It leverages the {@link AccessControl} mechanism to assess access rights and
   * throws a {@link ControllerApplicationException} with a {@link Response.Status#FORBIDDEN} status code
   * if access is denied. This is crucial for enforcing security and access control within the application.
   *
   * @param typeName The name of the type for which permission and access are being verified.
   * @param request The {@link Request} object containing details about the current request, utilized
   *                to extract the endpoint URL for access validation.
   * @param httpHeaders The {@link HttpHeaders} associated with the request, used for authorization
   *                    and other header-based access control checks.
   * @param accessType The type of access being requested (e.g., CREATE, READ, UPDATE, DELETE).
   * @param action The specific action being checked against the access control policies.
   * @param accessControlFactory The {@link AccessControlFactory} used to create an {@link AccessControl} instance
   *                             for validating permissions.
   * @param logger The {@link Logger} used for logging any access control related messages.
   * @throws ControllerApplicationException if the user lacks the required permissions or access.
   */
  public static void checkPermissionAndAccess(String typeName, Request request, HttpHeaders httpHeaders,
      AccessType accessType, String action, AccessControlFactory accessControlFactory, Logger logger) {
    String endpointUrl = request.getRequestURL().toString();
    AccessControl accessControl = accessControlFactory.create();
    AccessControlUtils.validatePermission(typeName, accessType, httpHeaders, endpointUrl, accessControl);
    if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, typeName, action)) {
      throw new ControllerApplicationException(logger, "Permission denied", Response.Status.FORBIDDEN);
    }
  }

  public static void emitPreSegmentUploadMetrics(ControllerMetrics controllerMetrics, String rawTableName,
                                                 long segmentSizeInBytes) {
    // Active segment uploads
    long uploadCount = _deepStoreSegmentUploadCount.incrementAndGet();
    controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_UPLOAD_COUNT, uploadCount);
    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_UPLOAD_COUNT, uploadCount);
    // Bytes uploading to deep store
    long segmentBytesUploading = _deepStoreSegmentBytesUploading.addAndGet(segmentSizeInBytes);
    controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_SEGMENT_BYTES_UPLOADING,
            segmentBytesUploading);
    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_SEGMENT_BYTES_UPLOADING, segmentBytesUploading);
  }

  public static void emitPostSegmentUploadMetrics(ControllerMetrics controllerMetrics, String rawTableName,
                                                  long startTimeMs, long segmentSizeInBytes) {
    // Active segment uploads
    long uploadCount = _deepStoreSegmentUploadCount.decrementAndGet();
    controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_UPLOAD_COUNT, uploadCount);
    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_UPLOAD_COUNT, uploadCount);
    // Bytes uploading to deep store
    long segmentBytesUploading = _deepStoreSegmentBytesUploading.addAndGet(-segmentSizeInBytes);
    controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_SEGMENT_BYTES_UPLOADING,
            segmentBytesUploading);
    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_SEGMENT_BYTES_UPLOADING, segmentBytesUploading);
    // Segment upload time
    long durationMs = System.currentTimeMillis() - startTimeMs;
    controllerMetrics.addTimedTableValue(rawTableName, ControllerTimer.DEEP_STORE_SEGMENT_UPLOAD_TIME_MS, durationMs,
            TimeUnit.MILLISECONDS);
    controllerMetrics.addTimedValue(ControllerTimer.DEEP_STORE_SEGMENT_UPLOAD_TIME_MS, durationMs,
            TimeUnit.MILLISECONDS);
    // Bytes uploaded to deep store
    controllerMetrics.addMeteredTableValue(rawTableName, ControllerMeter.DEEP_STORE_SEGMENT_UPLOAD_BYTES,
            segmentSizeInBytes);
    controllerMetrics.addMeteredGlobalValue(ControllerMeter.DEEP_STORE_SEGMENT_UPLOAD_BYTES, segmentSizeInBytes);
  }

  public static void emitPreSegmentDownloadMetrics(ControllerMetrics controllerMetrics, String rawTableName,
                                                   long segmentSizeInBytes) {
    // Active segment downloads
    long downloadCount = _deepStoreSegmentDownloadCount.incrementAndGet();
    controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_DOWNLOAD_COUNT, downloadCount);
    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_DOWNLOAD_COUNT, downloadCount);
    // Bytes downloading from deep store
    long segmentBytesDownloading = _deepStoreSegmentBytesDownloading.addAndGet(segmentSizeInBytes);
    controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_SEGMENT_BYTES_DOWNLOADING,
            segmentBytesDownloading);
    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_SEGMENT_BYTES_DOWNLOADING,
            segmentBytesDownloading);
  }

    public static void emitPostSegmentDownloadMetrics(ControllerMetrics controllerMetrics, String rawTableName,
                                                        long startTimeMs, long segmentSizeInBytes) {
      // Active segment downloads
      long downloadCount = _deepStoreSegmentDownloadCount.decrementAndGet();
      controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_DOWNLOAD_COUNT, downloadCount);
      controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_DOWNLOAD_COUNT, downloadCount);
      // Bytes downloading from deep store
      long segmentBytesDownloading = _deepStoreSegmentBytesDownloading.addAndGet(-segmentSizeInBytes);
      controllerMetrics.setOrUpdateTableGauge(rawTableName, ControllerGauge.DEEP_STORE_SEGMENT_BYTES_DOWNLOADING,
              segmentBytesDownloading);
      controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DEEP_STORE_SEGMENT_BYTES_DOWNLOADING,
              segmentBytesDownloading);
      // Segment download time
      long durationMs = System.currentTimeMillis() - startTimeMs;
      controllerMetrics.addTimedTableValue(rawTableName, ControllerTimer.DEEP_STORE_SEGMENT_DOWNLOAD_TIME_MS,
              durationMs, TimeUnit.MILLISECONDS);
      controllerMetrics.addTimedValue(ControllerTimer.DEEP_STORE_SEGMENT_DOWNLOAD_TIME_MS, durationMs,
              TimeUnit.MILLISECONDS);
      // Bytes downloaded from deep store
      controllerMetrics.addMeteredTableValue(rawTableName, ControllerMeter.DEEP_STORE_SEGMENT_DOWNLOAD_BYTES,
              segmentSizeInBytes);
      controllerMetrics.addMeteredGlobalValue(ControllerMeter.DEEP_STORE_SEGMENT_DOWNLOAD_BYTES, segmentSizeInBytes);
    }
}
