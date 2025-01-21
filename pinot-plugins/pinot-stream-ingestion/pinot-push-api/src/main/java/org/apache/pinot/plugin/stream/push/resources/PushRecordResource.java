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
package org.apache.pinot.plugin.stream.push.resources;

import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.plugin.stream.push.BufferedRecord;
import org.apache.pinot.plugin.stream.push.PushBasedIngestionBufferManager;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This resource API can be used to retrieve instance level information like instance tags.
 */
@Path("/ingestion")
public class PushRecordResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PushRecordResource.class);

  @Inject
  private PushBasedIngestionBufferManager _bufferManager;

  private static final String REALTIME_SUFFIX = "_REALTIME";

  @POST
  @Path("/records/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response ingestRecords(@PathParam("tableName") String tableName, @Context Request request) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "'tableName' cannot be null or empty");
    LOGGER.info("Processing records for table {}", tableName);
    if (!tableName.endsWith(REALTIME_SUFFIX)) {
      tableName = tableName + REALTIME_SUFFIX;
    }
    try {
      _bufferManager.getBufferForTable(tableName).append(generateRecord(request));
      return Response.ok().build();
    } catch (IOException e) {
      LOGGER.error("Caught exception while processing records for table {}", tableName, e);
      throw new RuntimeException("Failed to read request body", e);
    }
  }

  private BufferedRecord generateRecord(Request request)
      throws IOException {
    // Get the input stream of the request to read the body
    InputStream inputStream = request.getInputStream();
    final byte[] bytes = drain(new BufferedInputStream(inputStream));
    return new BufferedRecord(bytes);
  }

  byte[] drain(InputStream inputStream)
      throws IOException {
    final byte[] buf = new byte[1024];
    int len;
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    while ((len = inputStream.read(buf)) > 0) {
      byteArrayOutputStream.write(buf, 0, len);
    }
    return byteArrayOutputStream.toByteArray();
  }
}
