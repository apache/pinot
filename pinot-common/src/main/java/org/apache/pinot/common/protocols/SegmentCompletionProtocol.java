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
package org.apache.pinot.common.protocols;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.JsonUtils;


/*
 * This class encapsulates the segment completion protocol used by the server and the controller for
 * low-level consumer realtime segments. The protocol has two requests: SegmentConsumedRequest
 * and SegmentCommitRequest.It has a response that may contain different status codes depending on the state machine
 * that the controller drives for that segment. All responses have two elements -- status and an offset.
 *
 * The overall idea is that when a server has completed consuming a segment until the "end criteria" that
 * is set (in the table configuration), it sends a SegmentConsumedRequest to the controller (leader).  The
 * controller may respond with a HOLD, asking the server to NOT consume any new messages, but get back
 * to the controller after a while.
 *
 * Meanwhile, the controller co-ordinates the SegmentConsumedRequest messages from replicas and selects a server
 * to commit the segment. The server uses SegmentCommitRequest message, in which it also posts the completed
 * segment to the controller.
 *
 * The controller may respond with a failure for this commit message, but on success, the controller changes the
 * segment state to ONLINE in idealstate, and adds new CONSUMING segments as well.
 *
 * For details see the design document
 * https://cwiki.apache.org/confluence/display/PINOT/Consuming+and+Indexing+rows+in+Realtime
 */
public class SegmentCompletionProtocol {

  /**
   * MAX_HOLD_TIME_MS is the maximum time (msecs) for which a server will be in HOLDING state, after which it will
   * send in a SegmentConsumedRequest with its current offset in the stream.
   */
  public static final long MAX_HOLD_TIME_MS = 3000;
  /**
   * MAX_SEGMENT_COMMIT_TIME_MS is the longest time (msecs) a server will take to complete building a segment and committing
   * it  (via a SegmentCommit message) after the server has been notified that it is the committer.
   */
  private static final int DEFAULT_MAX_SEGMENT_COMMIT_TIME_SEC = 120;
  private static long MAX_SEGMENT_COMMIT_TIME_MS =
      TimeUnit.MILLISECONDS.convert(DEFAULT_MAX_SEGMENT_COMMIT_TIME_SEC, TimeUnit.SECONDS);

  public enum ControllerResponseStatus {
    /** Never sent by the controller, but locally used by server when sending a request fails */
    NOT_SENT,

    /** Server should send back a SegmentCommitRequest after processing this response */
    COMMIT,

    /** Server should send SegmentConsumedRequest after waiting for less than MAX_HOLD_TIME_MS */
    HOLD,

    /** Server should consume stream events to catch up to the offset contained in this response */
    CATCH_UP,

    /** Server should discard the rows in memory */
    DISCARD,

    /** Server should build a segment out of the rows in memory, and replace in-memory rows with the segment built */
    KEEP,

    /** Server should locate the current controller leader and re-send the message */
    NOT_LEADER,

    /** Commit failed. Server should go back to HOLDING state and re-start with the SegmentConsumed message  */
    FAILED,

    /** Commit succeeded, behave exactly like KEEP */
    COMMIT_SUCCESS,

    /** Never sent by the controller, but locally used by the controller during the segmentCommit() processing */
    COMMIT_CONTINUE,

    /** Sent by controller as an acknowledgement to the SegmentStoppedConsuming message */
    PROCESSED,

    /** Sent by controller as an acknowledgement during split commit for successful segment upload */
    UPLOAD_SUCCESS,
  }

  public static final String STATUS_KEY = "status";
  public static final String OFFSET_KEY = "offset";
  public static final String BUILD_TIME_KEY = "buildTimeSec";  // Sent by controller in COMMIT message
  public static final String COMMIT_TYPE_KEY = "isSplitCommitType";
  public static final String SEGMENT_LOCATION_KEY = "segmentLocation";
  public static final String CONTROLLER_VIP_URL_KEY = "controllerVipUrl";

  public static final String MSG_TYPE_CONSUMED = "segmentConsumed";
  public static final String MSG_TYPE_COMMIT = "segmentCommit";
  public static final String MSG_TYPE_COMMIT_START = "segmentCommitStart";
  public static final String MSG_TYPE_SEGMENT_UPLOAD = "segmentUpload";
  public static final String MSG_TYPE_COMMIT_END = "segmentCommitEnd";
  public static final String MSG_TYPE_COMMIT_END_METADATA = "segmentCommitEndWithMetadata";
  public static final String MSG_TYPE_STOPPED_CONSUMING = "segmentStoppedConsuming";
  public static final String MSG_TYPE_EXTEND_BUILD_TIME = "extendBuildTime";

  public static final String PARAM_SEGMENT_LOCATION = "location";
  public static final String PARAM_SEGMENT_NAME = "name";
  public static final String PARAM_OFFSET = "offset";
  public static final String PARAM_INSTANCE_ID = "instance";
  public static final String PARAM_MEMORY_USED_BYTES = "memoryUsedBytes";
  public static final String PARAM_SEGMENT_SIZE_BYTES = "segmentSizeBytes";
  public static final String PARAM_REASON = "reason";
  // Sent by servers to request additional time to build
  public static final String PARAM_EXTRA_TIME_SEC = "extraTimeSec";
  public static final String PARAM_ROW_COUNT = "rowCount"; // Sent by servers to indicate the number of rows read so far
  public static final String PARAM_BUILD_TIME_MILLIS = "buildTimeMillis"; // Time taken to build segment
  public static final String PARAM_WAIT_TIME_MILLIS = "waitTimeMillis";   // Time taken to wait for build to start.

  public static final String REASON_ROW_LIMIT = "rowLimit";  // Stop reason sent by server as max num rows reached
  public static final String REASON_TIME_LIMIT = "timeLimit";  // Stop reason sent by server as max time reached

  // Canned responses
  public static final Response RESP_NOT_LEADER =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.NOT_LEADER));
  public static final Response RESP_FAILED =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.FAILED));
  public static final Response RESP_DISCARD =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.DISCARD));
  public static final Response RESP_COMMIT_SUCCESS =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.COMMIT_SUCCESS));
  public static final Response RESP_COMMIT_CONTINUE =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.COMMIT_CONTINUE));
  public static final Response RESP_PROCESSED =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.PROCESSED));
  public static final Response RESP_NOT_SENT =
      new Response(new Response.Params().withStatus(ControllerResponseStatus.NOT_SENT));

  private static final long MEMORY_USED_BYTES_DEFAULT = -1L;
  private static final int NUM_ROWS_DEFAULT = -1;
  private static final long SEGMENT_SIZE_BYTES_DEFAULT = -1L;
  private static final long BUILD_TIME_MILLIS_DEFAULT = -1L;
  private static final long WAIT_TIME_MILLIS_DEFAULT = -1L;

  public static long getMaxSegmentCommitTimeMs() {
    return MAX_SEGMENT_COMMIT_TIME_MS;
  }

  public static void setMaxSegmentCommitTimeMs(long commitTimeMs) {
    MAX_SEGMENT_COMMIT_TIME_MS = commitTimeMs;
  }

  public static int getDefaultMaxSegmentCommitTimeSeconds() {
    return DEFAULT_MAX_SEGMENT_COMMIT_TIME_SEC;
  }

  public static abstract class Request {
    final Params _params;
    final String _msgType;

    private Request(Params params, String msgType) {
      _params = params;
      _msgType = msgType;
    }

    public String getUrl(String hostPort, String protocol) {
      return protocol + "://" + hostPort + "/" + _msgType + "?" + PARAM_SEGMENT_NAME + "=" + _params.getSegmentName()
          + "&" + PARAM_OFFSET + "=" + _params.getOffset() + "&" + PARAM_INSTANCE_ID + "=" + _params.getInstanceId() + (
          _params.getReason() == null ? "" : ("&" + PARAM_REASON + "=" + _params.getReason())) + (
          _params.getBuildTimeMillis() <= 0 ? "" : ("&" + PARAM_BUILD_TIME_MILLIS + "=" + _params.getBuildTimeMillis()))
          + (_params.getWaitTimeMillis() <= 0 ? "" : ("&" + PARAM_WAIT_TIME_MILLIS + "=" + _params.getWaitTimeMillis()))
          + (_params.getExtraTimeSec() <= 0 ? "" : ("&" + PARAM_EXTRA_TIME_SEC + "=" + _params.getExtraTimeSec())) + (
          _params.getMemoryUsedBytes() <= 0 ? "" : ("&" + PARAM_MEMORY_USED_BYTES + "=" + _params.getMemoryUsedBytes()))
          + (_params.getSegmentSizeBytes() <= 0 ? ""
          : ("&" + PARAM_SEGMENT_SIZE_BYTES + "=" + _params.getSegmentSizeBytes())) + (_params.getNumRows() <= 0 ? ""
          : ("&" + PARAM_ROW_COUNT + "=" + _params.getNumRows())) + (_params.getSegmentLocation() == null ? ""
          : ("&" + PARAM_SEGMENT_LOCATION + "=" + _params.getSegmentLocation()));
    }

    public static class Params {
      private long _offset;
      private String _segmentName;
      private String _instanceId;
      private String _reason;
      private int _numRows;
      private long _buildTimeMillis;
      private long _waitTimeMillis;
      private int _extraTimeSec;
      private String _segmentLocation;
      private long _memoryUsedBytes;
      private long _segmentSizeBytes;

      public Params() {
        _offset = -1L;
        _segmentName = "UNKNOWN_SEGMENT";
        _instanceId = "UNKNOWN_INSTANCE";
        _numRows = NUM_ROWS_DEFAULT;
        _buildTimeMillis = BUILD_TIME_MILLIS_DEFAULT;
        _waitTimeMillis = WAIT_TIME_MILLIS_DEFAULT;
        _extraTimeSec = -1;
        _segmentLocation = null;
        _memoryUsedBytes = MEMORY_USED_BYTES_DEFAULT;
        _segmentSizeBytes = SEGMENT_SIZE_BYTES_DEFAULT;
      }

      public Params(Params params) {
        _offset = params.getOffset();
        _segmentName = params.getSegmentName();
        _instanceId = params.getInstanceId();
        _numRows = params.getNumRows();
        _buildTimeMillis = params.getBuildTimeMillis();
        _waitTimeMillis = params.getWaitTimeMillis();
        _extraTimeSec = params.getExtraTimeSec();
        _segmentLocation = params.getSegmentLocation();
        _memoryUsedBytes = params.getMemoryUsedBytes();
        _segmentSizeBytes = params.getSegmentSizeBytes();
      }

      public Params withOffset(long offset) {
        _offset = offset;
        return this;
      }

      public Params withSegmentName(String segmentName) {
        _segmentName = segmentName;
        return this;
      }

      public Params withInstanceId(String instanceId) {
        _instanceId = instanceId;
        return this;
      }

      public Params withReason(String reason) {
        _reason = reason;
        return this;
      }

      public Params withNumRows(int numRows) {
        _numRows = numRows;
        return this;
      }

      public Params withBuildTimeMillis(long buildTimeMillis) {
        _buildTimeMillis = buildTimeMillis;
        return this;
      }

      public Params withWaitTimeMillis(long waitTimeMillis) {
        _waitTimeMillis = waitTimeMillis;
        return this;
      }

      public Params withExtraTimeSec(int extraTimeSec) {
        _extraTimeSec = extraTimeSec;
        return this;
      }

      public Params withSegmentLocation(String segmentLocation) {
        _segmentLocation = segmentLocation;
        return this;
      }

      public Params withMemoryUsedBytes(long memoryUsedBytes) {
        _memoryUsedBytes = memoryUsedBytes;
        return this;
      }

      public Params withSegmentSizeBytes(long segmentSizeBytes) {
        _segmentSizeBytes = segmentSizeBytes;
        return this;
      }

      public String getSegmentName() {
        return _segmentName;
      }

      public long getOffset() {
        return _offset;
      }

      public String getReason() {
        return _reason;
      }

      public String getInstanceId() {
        return _instanceId;
      }

      public int getNumRows() {
        return _numRows;
      }

      public long getBuildTimeMillis() {
        return _buildTimeMillis;
      }

      public long getWaitTimeMillis() {
        return _waitTimeMillis;
      }

      public int getExtraTimeSec() {
        return _extraTimeSec;
      }

      public String getSegmentLocation() {
        return _segmentLocation;
      }

      public long getMemoryUsedBytes() {
        return _memoryUsedBytes;
      }

      public long getSegmentSizeBytes() {
        return _segmentSizeBytes;
      }

      public String toString() {
        return "Offset: " + _offset + ",Segment name: " + _segmentName + ",Instance Id: " + _instanceId + ",Reason: "
            + _reason + ",NumRows: " + _numRows + ",BuildTimeMillis: " + _buildTimeMillis + ",WaitTimeMillis: "
            + _waitTimeMillis + ",ExtraTimeSec: " + _extraTimeSec + ",SegmentLocation: " + _segmentLocation
            + ",MemoryUsedBytes: " + _memoryUsedBytes + ",SegmentSizeBytes: " + _segmentSizeBytes;
      }
    }
  }

  public static class ExtendBuildTimeRequest extends Request {
    public ExtendBuildTimeRequest(Params params) {
      super(params, MSG_TYPE_EXTEND_BUILD_TIME);
    }
  }

  public static class SegmentConsumedRequest extends Request {
    public SegmentConsumedRequest(Params params) {
      super(params, MSG_TYPE_CONSUMED);
    }
  }

  public static class SegmentCommitRequest extends Request {
    public SegmentCommitRequest(Params params) {
      super(params, MSG_TYPE_COMMIT);
    }
  }

  public static class SegmentCommitStartRequest extends Request {
    public SegmentCommitStartRequest(Params params) {
      super(params, MSG_TYPE_COMMIT_START);
    }
  }

  public static class SegmentCommitUploadRequest extends Request {
    public SegmentCommitUploadRequest(Params params) {
      super(params, MSG_TYPE_SEGMENT_UPLOAD);
    }
  }

  public static class SegmentCommitEndRequest extends Request {
    public SegmentCommitEndRequest(Params params) {
      super(params, MSG_TYPE_COMMIT_END);
    }
  }

  public static class SegmentCommitEndWithMetadataRequest extends Request {
    public SegmentCommitEndWithMetadataRequest(Params params) {
      super(params, MSG_TYPE_COMMIT_END_METADATA);
    }
  }

  public static class SegmentStoppedConsuming extends Request {
    public SegmentStoppedConsuming(Params params) {
      super(params, MSG_TYPE_STOPPED_CONSUMING);
    }
  }

  public static class Response {
    private ControllerResponseStatus _status;
    private long _offset = -1;
    private long _buildTimeSeconds = -1;
    private boolean _splitCommit;
    private String _segmentLocation;
    private String _controllerVipUrl;

    public Response() {
    }

    public Response(Params params) {
      _status = params.getStatus();
      _offset = params.getOffset();
      _buildTimeSeconds = params.getBuildTimeSeconds();
      _splitCommit = params.isSplitCommit();
      _segmentLocation = params.getSegmentLocation();
      _controllerVipUrl = params.getControllerVipUrl();
    }

    @JsonProperty(STATUS_KEY)
    public ControllerResponseStatus getStatus() {
      return _status;
    }

    @JsonProperty(STATUS_KEY)
    public void setStatus(ControllerResponseStatus status) {
      _status = status;
    }

    @JsonProperty(OFFSET_KEY)
    public long getOffset() {
      return _offset;
    }

    @JsonProperty(OFFSET_KEY)
    public void setOffset(long offset) {
      _offset = offset;
    }

    @JsonProperty(BUILD_TIME_KEY)
    public long getBuildTimeSeconds() {
      return _buildTimeSeconds;
    }

    @JsonProperty(BUILD_TIME_KEY)
    public void setBuildTimeSeconds(long buildTimeSeconds) {
      _buildTimeSeconds = buildTimeSeconds;
    }

    @JsonProperty(COMMIT_TYPE_KEY)
    public boolean isSplitCommit() {
      return _splitCommit;
    }

    @JsonProperty(COMMIT_TYPE_KEY)
    public void setSplitCommit(boolean splitCommit) {
      _splitCommit = splitCommit;
    }

    @JsonProperty(CONTROLLER_VIP_URL_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getControllerVipUrl() {
      return _controllerVipUrl;
    }

    @JsonProperty(CONTROLLER_VIP_URL_KEY)
    public void setControllerVipUrl(String controllerVipUrl) {
      _controllerVipUrl = controllerVipUrl;
    }

    @JsonProperty(SEGMENT_LOCATION_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getSegmentLocation() {
      return _segmentLocation;
    }

    @JsonProperty(SEGMENT_LOCATION_KEY)
    public void setSegmentLocation(String segmentLocation) {
      _segmentLocation = segmentLocation;
    }

    public String toJsonString() {
      try {
        return JsonUtils.objectToString(this);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public static Response fromJsonString(String jsonString) {
      try {
        return JsonUtils.stringToObject(jsonString, Response.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public static class Params {
      private ControllerResponseStatus _status;
      private long _offset;
      private long _buildTimeSeconds;
      private boolean _splitCommit;
      private String _segmentLocation;
      private String _controllerVipUrl;

      public Params() {
        _offset = -1L;
        _status = ControllerResponseStatus.FAILED;
        _buildTimeSeconds = -1;
        _splitCommit = false;
        _segmentLocation = null;
        _controllerVipUrl = null;
      }

      public Params withOffset(long offset) {
        _offset = offset;
        return this;
      }

      public Params withStatus(ControllerResponseStatus status) {
        _status = status;
        return this;
      }

      public Params withBuildTimeSeconds(long buildTimeSeconds) {
        _buildTimeSeconds = buildTimeSeconds;
        return this;
      }

      public Params withSplitCommit(boolean splitCommit) {
        _splitCommit = splitCommit;
        return this;
      }

      public Params withControllerVipUrl(String controllerVipUrl) {
        _controllerVipUrl = controllerVipUrl;
        return this;
      }

      public Params withSegmentLocation(String segmentLocation) {
        _segmentLocation = segmentLocation;
        return this;
      }

      public ControllerResponseStatus getStatus() {
        return _status;
      }

      public long getOffset() {
        return _offset;
      }

      public long getBuildTimeSeconds() {
        return _buildTimeSeconds;
      }

      public boolean isSplitCommit() {
        return _splitCommit;
      }

      public String getSegmentLocation() {
        return _segmentLocation;
      }

      public String getControllerVipUrl() {
        return _controllerVipUrl;
      }
    }
  }
}
