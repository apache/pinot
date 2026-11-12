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
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.segment.spi.memory.DataBuffer;
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
        .setPayload(toByteString(dataTable.toDataBuffer())).build();
  }

  /// Wraps the buffers of a serialized data table in one [ByteString] without copying them. The buffers must not change
  /// after this call, which holds for a serialized data table.
  private static ByteString toByteString(DataBuffer dataBuffer) {
    List<ByteBuffer> buffers = new ArrayList<>();
    dataBuffer.appendAsByteBuffers(buffers);
    List<ByteString> byteStrings = new ArrayList<>(buffers.size());
    for (ByteBuffer buffer : buffers) {
      byteStrings.add(UnsafeByteOperations.unsafeWrap(buffer));
    }
    // Despite its name, copyFrom(Iterable) concatenates the byte strings into a balanced rope without copying them.
    return ByteString.copyFrom(byteStrings);
  }
}
