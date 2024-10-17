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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.cursors.AbstractResultStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseSerde;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FsResultStore extends AbstractResultStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FsResultStore.class);
  private static final String RESULT_TABLE_FILE_NAME_FORMAT = "resultTable.%s";
  private static final String RESPONSE_FILE_NAME_FORMAT = "response.%s";
  private static final String URI_SEPARATOR = "/";

  public static final String TEMP_DIR = "temp.dir";
  public static final String DATA_DIR = "data.dir";
  public static final String FILE_NAME_EXTENSION = "extension";
  public static final String DEFAULT_TEMP_DIR = "/tmp/pinot/broker/result_store/tmp";
  public static final String DEFAULT_DATA_DIR = "/tmp/pinot/broker/result_store/data";
  public static final String DEFAULT_FILE_NAME_EXTENSION = "json";

  Path _localTempDir;
  URI _dataDir;
  ResponseSerde _responseSerde;
  String _fileExtension;

  public static Path getTempPath(Path localTempDir, String... nameParts) {
    StringBuilder filename = new StringBuilder();
    for (String part : nameParts) {
      filename.append(part).append("_");
    }
    filename.append(Thread.currentThread().getId());
    return localTempDir.resolve(filename.toString());
  }

  public static URI combinePath(URI baseUri, String path)
      throws URISyntaxException {
    String newPath =
        baseUri.getPath().endsWith(URI_SEPARATOR) ? baseUri.getPath() + path : baseUri.getPath() + URI_SEPARATOR + path;
    return new URI(baseUri.getScheme(), baseUri.getHost(), newPath, null);
  }

  @Override
  public void init(PinotConfiguration config, ResponseSerde responseSerde)
      throws Exception {
    _fileExtension = config.getProperty(FILE_NAME_EXTENSION, DEFAULT_FILE_NAME_EXTENSION);
    _localTempDir = Paths.get(config.getProperty(TEMP_DIR, DEFAULT_TEMP_DIR));
    Files.createDirectories(_localTempDir);

    _dataDir = new URI(config.getProperty(DATA_DIR, DEFAULT_DATA_DIR));
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    pinotFS.mkdir(_dataDir);
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

    LOGGER.debug(String.format("Found %d paths.", queryPaths.size()));

    for (FileMetadata metadata : queryPaths) {
      LOGGER.debug(String.format("Processing query path: %s", metadata.toString()));
      if (metadata.isDirectory()) {
        try {
          URI queryDir = new URI(metadata.getFilePath());
          URI metadataFile = combinePath(queryDir, RESPONSE_FILE_NAME_FORMAT);
          boolean metadataFileExists = pinotFS.exists(metadataFile);
          LOGGER.debug(
              String.format("Checking for query dir %s & metadata file: %s. Metadata file exists: %s", queryDir,
                  metadataFile, metadataFileExists));
          if (metadataFileExists) {
            BrokerResponse response = _responseSerde.deserialize(pinotFS.open(metadataFile), BrokerResponse.class);
            requestIdList.add(response.getRequestId());
            LOGGER.debug("Added query store {}", queryDir);
          }
        } catch (Exception e) {
          LOGGER.error("Error when processing {}", metadata, e);
        }
      }
    }

    return requestIdList;
  }

  @Override
  public boolean deleteResponse(String requestId)
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
  public void writeResponse(CursorResponse response)
      throws Exception {
    String requestId = response.getRequestId();
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);

    // Create a directory for this query.
    pinotFS.mkdir(queryDir);

    Path tempResultTableFile =
        getTempPath(_localTempDir, "resultTable", requestId);
    Path tempResponseFile = getTempPath(_localTempDir, "response", requestId);
    URI dataFile = combinePath(queryDir, String.format(RESULT_TABLE_FILE_NAME_FORMAT, _fileExtension));
    URI metadataFile = combinePath(queryDir, RESPONSE_FILE_NAME_FORMAT);
    try {
      _responseSerde.serialize(response.getResultTable(), Files.newOutputStream(tempResultTableFile));
      pinotFS.copyFromLocalFile(tempResultTableFile.toFile(), dataFile);

      // Remove the resultTable from the response as it is serialized in a data file.
      response.setResultTable(null);
      _responseSerde.serialize(response, Files.newOutputStream(tempResponseFile));
      pinotFS.copyFromLocalFile(tempResponseFile.toFile(), metadataFile);
    } finally {
      Files.delete(tempResultTableFile);
      Files.delete(tempResponseFile);
    }
  }

  @Override
  public CursorResponse readResponse(String requestId)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);
    URI metadataFile = combinePath(queryDir, String.format(RESPONSE_FILE_NAME_FORMAT, _fileExtension));
    return _responseSerde.deserialize(pinotFS.open(metadataFile), CursorResponse.class);
  }

  @Override
  protected ResultTable readResultTable(String requestId)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = combinePath(_dataDir, requestId);
    URI dataFile = combinePath(queryDir, RESULT_TABLE_FILE_NAME_FORMAT);
    return _responseSerde.deserialize(pinotFS.open(dataFile), ResultTable.class);
  }
}
