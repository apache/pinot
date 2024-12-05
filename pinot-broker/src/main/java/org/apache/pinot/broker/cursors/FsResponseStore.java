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
package org.apache.pinot.broker.cursors;

import com.google.auto.service.AutoService;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stores responses in a file system. All storage schemes supported by PinotFS can be used.
 * Responses are stored in "data.dir" directory with the following structure:
 * - A directory is created for every request id.
 * - Response metadata is stored with filename "response"
 * - Results are stored with filename "resultTable"
 * The extension of the file is determined by the config "extension"
 *
 */
@AutoService(ResponseStore.class)
public class FsResponseStore extends AbstractResponseStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FsResponseStore.class);
  private static final String TYPE = "file";
  private static final String RESULT_TABLE_FILE_NAME_FORMAT = "resultTable.%s";
  private static final String RESPONSE_FILE_NAME_FORMAT = "response.%s";
  private static final String URI_SEPARATOR = "/";

  public static final String TEMP_DIR = "temp.dir";
  public static final String DATA_DIR = "data.dir";
  public static final String FILE_NAME_EXTENSION = "extension";
  public static final String DEFAULT_ROOT_DIR = System.getProperty("java.io.tmpdir") + File.separator + "broker"
      + File.separator + "result_store" + File.separator;
  public static final String DEFAULT_SCHEME = "file://";
  public static final String DEFAULT_TEMP_DIR = DEFAULT_ROOT_DIR + "tmp";
  public static final String DEFAULT_DATA_DIR = DEFAULT_SCHEME + DEFAULT_ROOT_DIR + "data";
  public static final String DEFAULT_FILE_NAME_EXTENSION = "json";

  private Path _localTempDir;
  private URI _dataDir;
  private BrokerMetrics _brokerMetrics;
  private JsonResponseSerde _responseSerde;
  private String _brokerHost;
  private int _brokerPort;
  private String _brokerId;
  private long _expirationIntervalInMs;
  private String _fileExtension;

  private static URI combinePath(URI baseUri, String path)
      throws URISyntaxException {
    String newPath =
        baseUri.getPath().endsWith(URI_SEPARATOR) ? baseUri.getPath() + path : baseUri.getPath() + URI_SEPARATOR + path;
    return new URI(baseUri.getScheme(), baseUri.getHost(), newPath, null);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public void init(@NotNull PinotConfiguration config, @NotNull String brokerHost, int brokerPort, String brokerId,
      @NotNull BrokerMetrics brokerMetrics, String expirationTime)
      throws Exception {
    _brokerMetrics = brokerMetrics;
    _responseSerde = new JsonResponseSerde();
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _brokerId = brokerId;
    _expirationIntervalInMs = TimeUtils.convertPeriodToMillis(expirationTime);

    _fileExtension = config.getProperty(FILE_NAME_EXTENSION, DEFAULT_FILE_NAME_EXTENSION);
    _localTempDir = Paths.get(config.getProperty(TEMP_DIR, DEFAULT_TEMP_DIR));
    Files.createDirectories(_localTempDir);

    _dataDir = new URI(config.getProperty(DATA_DIR, DEFAULT_DATA_DIR));
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    pinotFS.mkdir(_dataDir);
  }

  private Path getTempPath(String... nameParts) {
    StringBuilder filename = new StringBuilder();
    for (String part : nameParts) {
      filename.append(part).append("_");
    }
    filename.append(Thread.currentThread().getId());
    return _localTempDir.resolve(filename.toString());
  }

  @NotNull
  @Override
  protected BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  @Override
  protected String getBrokerHost() {
    return _brokerHost;
  }

  @Override
  protected int getBrokerPort() {
    return _brokerPort;
  }

  @Override
  protected long getExpirationIntervalInMs() {
    return _expirationIntervalInMs;
  }

  @Override
  public boolean exists(String requestId)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);
    return pinotFS.exists(queryDir);
  }

  @Override
  public Collection<String> getAllStoredRequestIds()
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    List<FileMetadata> queryPaths = pinotFS.listFilesWithMetadata(_dataDir, true);
    List<String> requestIdList = new ArrayList<>(queryPaths.size());

    LOGGER.debug("Found {} paths.", queryPaths.size());

    for (FileMetadata metadata : queryPaths) {
      LOGGER.debug("Processing query path: {}", metadata.toString());
      if (metadata.isDirectory()) {
        try {
          URI queryDir = new URI(metadata.getFilePath());
          URI metadataFile = combinePath(queryDir, String.format(RESPONSE_FILE_NAME_FORMAT, _fileExtension));
          boolean metadataFileExists = pinotFS.exists(metadataFile);
          LOGGER.debug("Checking for query dir {} & metadata file: {}. Metadata file exists: {}", queryDir,
              metadataFile, metadataFileExists);
          if (metadataFileExists) {
            BrokerResponse response =
                _responseSerde.deserialize(pinotFS.open(metadataFile), CursorResponseNative.class);
            if (response.getBrokerId().equals(_brokerId)) {
              requestIdList.add(response.getRequestId());
            }
            LOGGER.debug("Added response store {}", queryDir);
          }
        } catch (Exception e) {
          LOGGER.error("Error when processing {}", metadata, e);
        }
      }
    }

    return requestIdList;
  }

  @Override
  protected boolean deleteResponseImpl(String requestId)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);
    if (pinotFS.exists(queryDir)) {
      pinotFS.delete(queryDir, true);
      return true;
    }
    return false;
  }

  @Override
  protected void writeResponse(String requestId, CursorResponse response)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);

    // Create a directory for this query.
    pinotFS.mkdir(queryDir);

    Path tempResponseFile = getTempPath("response", requestId);
    URI metadataFile = combinePath(queryDir, String.format(RESPONSE_FILE_NAME_FORMAT, _fileExtension));

    try (OutputStream tempResponseFileOS = Files.newOutputStream(tempResponseFile)) {
      _responseSerde.serialize(response, tempResponseFileOS);
    }

    try {
      pinotFS.copyFromLocalFile(tempResponseFile.toFile(), metadataFile);
    } finally {
      Files.delete(tempResponseFile);
    }
  }

  @Override
  protected long writeResultTable(String requestId, ResultTable resultTable)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);

    // Create a directory for this query.
    pinotFS.mkdir(queryDir);

    Path tempResultTableFile = getTempPath("resultTable", requestId);
    URI dataFile = combinePath(queryDir, String.format(RESULT_TABLE_FILE_NAME_FORMAT, _fileExtension));

    try (OutputStream tempResultTableFileOS = Files.newOutputStream(tempResultTableFile)) {
      _responseSerde.serialize(resultTable, tempResultTableFileOS);
    }

    try {
      pinotFS.copyFromLocalFile(tempResultTableFile.toFile(), dataFile);
      return pinotFS.length(dataFile);
    } finally {
      Files.delete(tempResultTableFile);
    }
  }

  @Override
  public CursorResponse readResponse(String requestId)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);
    URI metadataFile = combinePath(queryDir, String.format(RESPONSE_FILE_NAME_FORMAT, _fileExtension));
    try (InputStream metadataIS = pinotFS.open(metadataFile)) {
      return _responseSerde.deserialize(metadataIS, CursorResponseNative.class);
    }
  }

  @Override
  protected ResultTable readResultTable(String requestId, int offset, int numRows)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);
    URI dataFile = combinePath(queryDir, String.format(RESULT_TABLE_FILE_NAME_FORMAT, _fileExtension));
    CursorResponse response = readResponse(requestId);
    int totalTableRows = response.getNumRowsResultSet();

    try (InputStream dataIS = pinotFS.open(dataFile)) {
      ResultTable resultTable = _responseSerde.deserialize(dataIS, ResultTable.class);

      int sliceEnd = offset + numRows;
      if (sliceEnd > totalTableRows) {
        sliceEnd = totalTableRows;
      }

      return new ResultTable(resultTable.getDataSchema(), resultTable.getRows().subList(offset, sliceEnd));
    }
  }
}
