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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.ServiceStatus;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidDocIdsBitmapResponse {
  private final String _segmentName;
  private final String _segmentCrc;
  private final ValidDocIdsType _validDocIdsType;
  private final byte[] _bitmap;
  private final String _instanceId;
  private final ServiceStatus.Status _serverStatus;
  // Server's data CRC (forward index + dictionary checksum); null when the server doesn't report it.
  @Nullable
  private final String _segmentDataCrc;

  public ValidDocIdsBitmapResponse(String segmentName, String crc, ValidDocIdsType validDocIdsType, byte[] bitmap,
      String instanceId, ServiceStatus.Status serverStatus) {
    this(segmentName, crc, validDocIdsType, bitmap, instanceId, serverStatus, null);
  }

  @JsonCreator
  public ValidDocIdsBitmapResponse(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("segmentCrc") String crc, @JsonProperty("validDocIdsType") ValidDocIdsType validDocIdsType,
      @JsonProperty("bitmap") byte[] bitmap, @JsonProperty("instanceId") String instanceId,
      @JsonProperty("serverStatus") ServiceStatus.Status serverStatus,
      @JsonProperty("segmentDataCrc") @Nullable String segmentDataCrc) {
    _segmentName = segmentName;
    _segmentCrc = crc;
    _validDocIdsType = validDocIdsType;
    _bitmap = bitmap;
    _instanceId = instanceId;
    _serverStatus = serverStatus;
    _segmentDataCrc = segmentDataCrc;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getSegmentCrc() {
    return _segmentCrc;
  }

  public ValidDocIdsType getValidDocIdsType() {
    return _validDocIdsType;
  }

  public byte[] getBitmap() {
    return _bitmap;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public ServiceStatus.Status getServerStatus() {
    return _serverStatus;
  }

  /** Server's data CRC, or null if not reported. Omitted from the payload when null. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getSegmentDataCrc() {
    return _segmentDataCrc;
  }
}
