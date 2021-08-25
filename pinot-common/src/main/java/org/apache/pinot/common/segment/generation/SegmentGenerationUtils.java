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
package org.apache.pinot.common.segment.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.JsonUtils;


public class SegmentGenerationUtils {
  private SegmentGenerationUtils() {
  }

  private static final String OFFLINE = "OFFLINE";
  public static final String PINOT_PLUGINS_TAR_GZ = "pinot-plugins.tar.gz";
  public static final String PINOT_PLUGINS_DIR = "pinot-plugins-dir";

  public static String generateSchemaURI(String controllerUri, String table) {
    return String.format("%s/tables/%s/schema", controllerUri, table);
  }

  public static String generateTableConfigURI(String controllerUri, String table) {
    return String.format("%s/tables/%s", controllerUri, table);
  }

  public static Schema getSchema(String schemaURIString) {
    return getSchema(schemaURIString, null);
  }

  public static Schema getSchema(String schemaURIString, String authToken) {
    URI schemaURI;
    try {
      schemaURI = new URI(schemaURIString);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Schema URI is not valid - '" + schemaURIString + "'", e);
    }
    String scheme = schemaURI.getScheme();
    String schemaJson;
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      // Try to use PinotFS to read schema URI
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      InputStream schemaStream;
      try {
        schemaStream = pinotFS.open(schemaURI);
      } catch (IOException e) {
        throw new RuntimeException("Failed to fetch schema from PinotFS - '" + schemaURI + "'", e);
      }
      try {
        schemaJson = IOUtils.toString(schemaStream, StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read from schema file data stream on Pinot fs - '" + schemaURI + "'", e);
      }
    } else {
      // Try to directly read from URI.
      try {
        schemaJson = fetchUrl(schemaURI.toURL(), authToken);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read from Schema URI - '" + schemaURI + "'", e);
      }
    }
    try {
      return Schema.fromString(schemaJson);
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode Pinot schema from json string - '" + schemaJson + "'", e);
    }
  }

  @Deprecated
  public static TableConfig getTableConfig(String tableConfigURIStr) {
    return getTableConfig(tableConfigURIStr, null);
  }

  public static TableConfig getTableConfig(String tableConfigURIStr, String authToken) {
    URI tableConfigURI;
    try {
      tableConfigURI = new URI(tableConfigURIStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Table config URI is not valid - '" + tableConfigURIStr + "'", e);
    }
    String scheme = tableConfigURI.getScheme();
    String tableConfigJson;
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      // Try to use PinotFS to read table config URI
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      try {
        tableConfigJson = IOUtils.toString(pinotFS.open(tableConfigURI), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException("Failed to open table config file stream on Pinot fs - '" + tableConfigURI + "'", e);
      }
    } else {
      try {
        tableConfigJson = fetchUrl(tableConfigURI.toURL(), authToken);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to read from table config file data stream on Pinot fs - '" + tableConfigURI + "'", e);
      }
    }
    // Controller API returns a wrapper of table config.
    JsonNode tableJsonNode;
    try {
      tableJsonNode = new ObjectMapper().readTree(tableConfigJson);
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode table config into JSON from String - '" + tableConfigJson + "'", e);
    }
    if (tableJsonNode.has(OFFLINE)) {
      tableJsonNode = tableJsonNode.get(OFFLINE);
    }
    try {
      return JsonUtils.jsonNodeToObject(tableJsonNode, TableConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to decode table config from JSON - '" + tableJsonNode + "'", e);
    }
  }

  /**
   * Generate a relative output directory path when `useRelativePath` flag is on.
   * This method will compute the relative path based on `inputFile` and `baseInputDir`,
   * then apply only the directory part of relative path to `outputDir`.
   * E.g.
   *    baseInputDir = "/path/to/input"
   *    inputFile = "/path/to/input/a/b/c/d.avro"
   *    outputDir = "/path/to/output"
   *    getRelativeOutputPath(baseInputDir, inputFile, outputDir) = /path/to/output/a/b/c
   */
  public static URI getRelativeOutputPath(URI baseInputDir, URI inputFile, URI outputDir) {
    URI relativePath = baseInputDir.relativize(inputFile);
    Preconditions.checkState(relativePath.getPath().length() > 0 && !relativePath.equals(inputFile),
        "Unable to extract out the relative path for input file '" + inputFile + "', based on base input path: "
            + baseInputDir);
    String outputDirStr = outputDir.toString();
    outputDir = !outputDirStr.endsWith("/") ? URI.create(outputDirStr.concat("/")) : outputDir;
    URI relativeOutputURI = outputDir.resolve(relativePath).resolve(".");
    return relativeOutputURI;
  }

  /**
   * Extract file name from a given URI.
   *
   * @param inputFileURI
   * @return
   */
  public static String getFileName(URI inputFileURI) {
    String scheme = inputFileURI.getScheme();
    if (scheme != null && scheme.equalsIgnoreCase("file")) {
      return new File(inputFileURI).getName();
    }
    String[] pathSplits = inputFileURI.getPath().split("/");
    return pathSplits[pathSplits.length - 1];
  }

  /**
   * Convert a File URI String to URI Object, use parent URI scheme/userInfo/host/port if sheme is not specified.
   *
   * @param uriStr
   * @param fullUriForPathOnlyUriStr
   * @return
   * @throws URISyntaxException
   */
  public static URI getFileURI(String uriStr, URI fullUriForPathOnlyUriStr)
      throws URISyntaxException {
    URI fileURI = URI.create(uriStr);
    if (fileURI.getScheme() == null) {
      return new URI(fullUriForPathOnlyUriStr.getScheme(), fullUriForPathOnlyUriStr.getUserInfo(),
          fullUriForPathOnlyUriStr.getHost(), fullUriForPathOnlyUriStr.getPort(), fileURI.getPath(), fileURI.getQuery(),
          fileURI.getFragment());
    }

    return fileURI;
  }

  /**
   * Convert Directory URI String to URI Object, default to local file system scheme.
   *
   * @param uriStr
   * @return
   * @throws URISyntaxException
   */
  public static URI getDirectoryURI(String uriStr)
      throws URISyntaxException {
    URI uri = new URI(uriStr);
    if (uri.getScheme() == null) {
      uri = new File(uriStr).toURI();
    }
    return uri;
  }

  /**
   * Retrieve a URL via GET request, with an optional authorization token.
   *
   * @param url target url
   * @param authToken optional auth token, or null
   * @return fetched document
   * @throws IOException on connection problems
   */
  private static String fetchUrl(URL url, String authToken)
      throws IOException {
    URLConnection connection = url.openConnection();

    if (StringUtils.isNotBlank(authToken)) {
      connection.setRequestProperty("Authorization", authToken);
    }
    return IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);
  }
}
