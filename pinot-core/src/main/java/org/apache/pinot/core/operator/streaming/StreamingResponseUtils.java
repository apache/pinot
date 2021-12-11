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
package org.apache.pinot.core.operator.streaming;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response;


public class StreamingResponseUtils {
  private StreamingResponseUtils() {
  }

  public static Server.ServerResponse getDataResponse(DataTable dataTable)
      throws IOException {
    return getResponse(dataTable, Response.ResponseType.DATA);
  }

  public static Server.ServerResponse getMetadataResponse(DataTable dataTable)
      throws IOException {
    return getResponse(dataTable, Response.ResponseType.METADATA);
  }

  public static Server.ServerResponse getNonStreamingResponse(DataTable dataTable)
      throws IOException {
    return getResponse(dataTable, Response.ResponseType.NON_STREAMING);
  }

  private static Server.ServerResponse getResponse(DataTable dataTable, String responseType)
      throws IOException {
    return Server.ServerResponse.newBuilder().putMetadata(Response.MetadataKeys.RESPONSE_TYPE, responseType)
        .setPayload(ByteString.copyFrom(dataTable.toBytes())).build();
  }
}
