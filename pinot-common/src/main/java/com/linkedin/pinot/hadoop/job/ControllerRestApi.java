/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.hadoop.job;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.HttpErrorStatusException;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.SimpleHttpResponse;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import com.linkedin.pinot.hadoop.utils.PushLocation;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRestApi.class);
  private final List<PushLocation> _pushLocations;
  private final String _tableName;
  private final FileUploadDownloadClient _fileUploadDownloadClient;

  public static final String HDR_CONTROLLER_HOST = "Pinot-Controller-Host";
  public static final String HDR_CONTROLLER_VERSION = "Pinot-Controller-Version";
  public static final String HDR_SEGMENT_NAME = "Pinot-Segment-Name";
  public static final String HDR_TABLE_NAME = "Pinot-Table-Name";
  public static final String NOT_IN_RESPONSE = "NotInResponse";
  private static final float DEFAULT_RETRY_SCALE_FACTOR = 2f;

  private static final int PUSH_TIMEOUT_SECONDS = 60;
  private static final int GET_TIMEOUT_SECONDS = 5;
  private static final int MAX_RETRIES = 5;

  private static final String OFFLINE = "OFFLINE";

  public ControllerRestApi(List<PushLocation> pushLocations, String tableName) {
    LOGGER.info("Push Locations are: " + pushLocations);
    _pushLocations = pushLocations;
    _tableName = tableName;
    _fileUploadDownloadClient = new FileUploadDownloadClient();
  }

  public TableConfig getTableConfig() {
    List<URI> tableConfigURIs = new ArrayList<URI>();
    try {
      for (PushLocation pushLocation : _pushLocations) {
        tableConfigURIs.add(FileUploadDownloadClient.getRetrieveTableConfigURI(pushLocation.getHost(), pushLocation.getPort(), _tableName));
      }
    } catch (URISyntaxException e) {
      LOGGER.error("Could not construct table config URI for table {}", _tableName);
      throw new RuntimeException(e);
    }

    // Return the first table config it can retrieve
    for (URI uri : tableConfigURIs) {
      try {
        SimpleHttpResponse response = _fileUploadDownloadClient.getTableConfig(uri);
        JSONObject queryResponse = new JSONObject(response.getResponse());
        JSONObject offlineTableConfig = queryResponse.getJSONObject(OFFLINE);
        LOGGER.info("Got table config {}", offlineTableConfig);
        if (!queryResponse.isNull(OFFLINE)) {
          return TableConfig.fromJSONConfig(offlineTableConfig);
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while trying to get table config for " + _tableName + " " + e);
      }
    }
    LOGGER.error("Could not get table configs from any push locations provided for " + _tableName);
    throw new RuntimeException("Could not get table config for table " + _tableName);
  }

  public String getSchema() {
    List<URI> schemaURIs = new ArrayList<URI>();
    try {
      for (PushLocation pushLocation : _pushLocations) {
        schemaURIs.add(FileUploadDownloadClient.getRetrieveSchemaHttpURI(pushLocation.getHost(), pushLocation.getPort(), _tableName));
      }
    } catch (URISyntaxException e) {
      LOGGER.error("Could not construct schema URI for table {}", _tableName);
      throw new RuntimeException(e);
    }

    for (URI schemaURI : schemaURIs) {
      try {
        SimpleHttpResponse response = _fileUploadDownloadClient.getSchema(schemaURI);
        if (response != null) {
          return response.getResponse();
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while trying to get schema for " + _tableName + " " + e);
      }
    }
    LOGGER.error("Could not get schema configs for any push locations provided for " + _tableName);
    throw new RuntimeException("Could not get schema for table " + _tableName);
  }

  public void sendSegments(List<Path> tarFilePathList, FileSystem fs) {
    int pushTimeoutMs = PUSH_TIMEOUT_SECONDS * 1000;
    int maxParallelPushesPerColo = 3;

    ArrayList<Future<?>> futuresPerColo = new ArrayList<>();
    ArrayList<ExecutorService> executorServices = new ArrayList<>();

    // Create a threadpool for each host of size max parallel pushes per colo
    try {
      for (PushLocation pushLocation : _pushLocations) {
        ExecutorService fixedPoolPerColo = Executors.newFixedThreadPool(maxParallelPushesPerColo);
        executorServices.add(fixedPoolPerColo);
        for (Path filePath : tarFilePathList) {
          LOGGER.info("Scheduling file to " + filePath + " to " + pushLocation.toString());

          futuresPerColo.add(fixedPoolPerColo.submit(
              new PushFileToHostCallable(filePath, fs, pushLocation.toString(), pushTimeoutMs, MAX_RETRIES)));
        }
      }
      for (Future<?> future : futuresPerColo) {
        future.get();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      for (ExecutorService executorService : executorServices) {
        executorService.shutdownNow();
      }
    }
  }

  /**
   * Callable class which pushes one .tar.gz file to one host.
   * Throws exception when push fails.
   */
  private class PushFileToHostCallable implements Callable<Void> {
    public static final String TARGZ = ".tar.gz";
    private final int FILE_EXTENSION_LENGTH = TARGZ.length();
    private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PushFileToHostCallable.class);

    private final Path _filePath;
    private final FileSystem _fs;
    private final String _uri;
    private final int _pushTimeoutMs;
    private final int _maxRetries;

    public PushFileToHostCallable(Path filePath, FileSystem fs, String uri, int pushTimeoutMs, int maxRetries) {
      _filePath = filePath;
      _fs = fs;
      _uri = uri;
      _pushTimeoutMs = pushTimeoutMs;
      _maxRetries = maxRetries;
    }

    @Override
    public Void call()
        throws Exception {
      // Set query parameters
      final List<NameValuePair> parameters = Collections.<NameValuePair>singletonList(
          new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION, "true"));

      RetryPolicy retryPolicy =
          RetryPolicies.exponentialBackoffRetryPolicy(_maxRetries, _pushTimeoutMs, DEFAULT_RETRY_SCALE_FACTOR);
      LOGGER.info("Push file retry policy configuration: maxNumAttempts {}, initialDelayMs: {}, delayScaleFactor: {}",
          _maxRetries, _pushTimeoutMs, DEFAULT_RETRY_SCALE_FACTOR);

      final String fileName = _filePath.getName();
      final String segmentName = fileName.substring(0, fileName.length() - FILE_EXTENSION_LENGTH);

      Header segmentNameHeader = new BasicHeader(HDR_SEGMENT_NAME, segmentName);
      Header tableNameHeader = new BasicHeader(HDR_TABLE_NAME, _tableName);
      final List<Header> httpHeaders = Arrays.asList(segmentNameHeader, tableNameHeader);

      retryPolicy.attempt(new Callable<Boolean>() {
        @Override
        public Boolean call()
            throws Exception {
          try {
            _fileUploadDownloadClient.uploadSegment(new URI(_uri), segmentName, _fs.open(_filePath), httpHeaders, parameters,
                FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
            int responseCode = 0; // getStatusCode();
            LOGGER.info("Response code {} received for file {} from uri {}", responseCode, fileName, _uri);
            return true;
          } catch (HttpErrorStatusException e) {
            int statusCode = e.getStatusCode();
            if (statusCode == HttpStatus.SC_CONFLICT || statusCode >= 500) {
              // Temporary exception
              LOGGER.warn("Caught temporary exception while uploading segment to {}: {}, will retry", _uri, segmentName, e);
              return false;
            } else {
              // Permanent exception
              LOGGER.error("Caught permanent exception while uploading segment to {}: {}, won't retry", _uri, segmentName, e);
              throw e;
            }
          } catch (Exception e) {
            LOGGER.warn("Caught temporary exception while uploading segment to {}: {}, will retry", _uri, segmentName, e);
            return false;
          }
        }
      });
      return null;
    }
  }
}
