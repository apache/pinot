/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.protocols;

import java.util.concurrent.TimeUnit;
import com.alibaba.fastjson.JSONObject;

/*
 * TODO Add unit tests for this after we finalize the protocol elements.
 * TODO Finalize the protocol elements.
 *
 * This class encapsulates the segment completion protocol used by the server and the controller for
 * low-level kafka consumer realtime segments. The protocol has two requests: SegmentConsumedRequest
 * and SegmentCommitRequest.It has a response that may contain different status codes depending on the state machine
 * that the controller drives for that segment. All responses have two elements -- status and an offset.
 *
 * The overall idea is that when a server has completed consuming a segment until the "end criteria" that
 * is set (in the table configuration), it sends a SegmentConsumedRequest to the controller (leader).  The
 * controller may respond with a HOLD, asking the server to NOT consume any new kafka messages, but get back
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
 * https://github.com/linkedin/pinot/wiki/Low-level-kafka-consumers
 */
public class SegmentCompletionProtocol {

  /**
   * MAX_HOLD_TIME_MS is the maximum time (msecs) for which a server will be in HOLDING state, after which it will
   * send in a SegmentConsumedRequest with its current offset in kafka.
   */
  public static final long MAX_HOLD_TIME_MS = 3000;
  /**
   * MAX_SEGMENT_COMMIT_TIME_MS is the longest time (msecs) a server will take to complete building a segment and committing
   * it  (via a SegmentCommit message) after the server has been notified that it is the committer.
   */
  private static final int DEFAULT_MAX_SEGMENT_COMMIT_TIME_SEC = 120;
  private static long MAX_SEGMENT_COMMIT_TIME_MS = TimeUnit.MILLISECONDS.convert(DEFAULT_MAX_SEGMENT_COMMIT_TIME_SEC, TimeUnit.SECONDS);

  public enum ControllerResponseStatus {
    /** Never sent by the controller, but locally used by server when sending a request fails */
    NOT_SENT,

    /** Server should send back a SegmentCommitRequest after processing this response */
    COMMIT,

    /** Server should send SegmentConsumedRequest after waiting for less than MAX_HOLD_TIME_MS */
    HOLD,

    /** Server should consume kafka events to catch up to the offset contained in this response */
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
  }

  public static final String STATUS_KEY = "status";
  public static final String OFFSET_KEY = "offset";

  public static final String MSG_TYPE_CONSUMED = "segmentConsumed";
  public static final String MSG_TYPE_COMMMIT = "segmentCommit";
  public static final String MSG_TYPE_STOPPED_CONSUMING = "segmentStoppedConsuming";

  public static final String PARAM_SEGMENT_NAME = "name";
  public static final String PARAM_OFFSET = "offset";
  public static final String PARAM_INSTANCE_ID = "instance";
  public static final String PARAM_REASON = "reason";

  // Canned responses
  public static final Response RESP_NOT_LEADER = new Response(new Response.Params().setStatus(ControllerResponseStatus.NOT_LEADER));
  public static final Response RESP_FAILED = new Response(new Response.Params().setStatus(ControllerResponseStatus.FAILED));
  public static final Response RESP_DISCARD = new Response(new Response.Params().setStatus(ControllerResponseStatus.DISCARD));
  public static final Response RESP_COMMIT_SUCCESS = new Response(new Response.Params().setStatus(ControllerResponseStatus.COMMIT_SUCCESS));
  public static final Response RESP_COMMIT_CONTINUE = new Response(new Response.Params().setStatus(ControllerResponseStatus.COMMIT_CONTINUE));
  public static final Response RESP_PROCESSED = new Response(new Response.Params().setStatus(ControllerResponseStatus.PROCESSED));
  public static final Response RESP_NOT_SENT = new Response(new Response.Params().setStatus(ControllerResponseStatus.NOT_SENT));

  public static long getMaxSegmentCommitTimeMs() {
    return MAX_SEGMENT_COMMIT_TIME_MS;
  }

  public static void setMaxSegmentCommitTimeMs(long commitTimeMs) {
    MAX_SEGMENT_COMMIT_TIME_MS = commitTimeMs;
  }

  public static int getDefaultMaxSegmentCommitTimeSec() {
    return DEFAULT_MAX_SEGMENT_COMMIT_TIME_SEC;
  }

  public static abstract class Request {
    final String _segmentName;
    final long _offset;
    final String _instanceId;
    final String _reason;

    private Request(Params params) {
      _segmentName = params.getSegmentName();
      _instanceId = params.getInstanceId();
      _offset = params.getOffset();
      _reason = params.getReason();
    }

    public abstract String getUrl(String hostPort);

    protected String getUri(String msgType) {
      return "/" + msgType + "?" +
        PARAM_SEGMENT_NAME + "=" + _segmentName + "&" +
        PARAM_OFFSET + "=" + _offset + "&" +
        PARAM_INSTANCE_ID + "=" + _instanceId +
          (_reason == null ? "" : ("&" + PARAM_REASON + "=" + _reason));
    }

    public static class Params {
      private static final String UNKNOWN_SEGMENT = "UNKNOWN_SEGMENT";
      private static final long UNKNOWN_OFFSET = -1L;
      private static final String UNKNOWN_INSTANCE = "UNKNOWN_INSTANCE";

      long _offset;
      String _segmentName;
      String _instanceId;
      String _reason;

      public Params() {
        resetInternal();
      }
      public Params reset() {
        resetInternal();
        return this;
      }
      private void resetInternal() {
        _offset = UNKNOWN_OFFSET;
        _segmentName = UNKNOWN_SEGMENT;
        _instanceId = UNKNOWN_INSTANCE;
      }
      public Params setOffset(long offset) {
        _offset = offset;
        return this;
      }
      public Params setSegmentName(String segmentName) {
        _segmentName = segmentName;
        return this;
      }
      public Params setInstanceId(String instanceId) {
        _instanceId = instanceId;
        return this;
      }
      public Params setReason(String reason) {
        _reason = reason;
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
    }
  }


  public static class SegmentConsumedRequest extends Request {
    public SegmentConsumedRequest(Params params) {
      super(params);
    }
    @Override
      public String getUrl(final String hostPort) {
        return "http://" + hostPort + getUri(MSG_TYPE_CONSUMED);
      }
  }

  public static class SegmentCommitRequest extends Request {

    public SegmentCommitRequest(Params params) {
      super(params);
    }
    @Override
      public String getUrl(final String hostPort) {
        return "http://" + hostPort + getUri(MSG_TYPE_COMMMIT);
      }
  }

  public static class SegmentStoppedConsuming extends Request {

    public SegmentStoppedConsuming(Params params) {
      super(params);
    }

    @Override
    public String getUrl(final String hostPort) {
      return "http://" + hostPort + getUri(MSG_TYPE_STOPPED_CONSUMING);
    }
  }

  public static class Response {
    final ControllerResponseStatus _status;
    final long _offset;

    public Response(String jsonRespStr) {
      JSONObject jsonObject = JSONObject.parseObject(jsonRespStr);
      long offset = -1;
      if (jsonObject.containsKey(OFFSET_KEY)) {
        offset = jsonObject.getLong(OFFSET_KEY);
      }
      _offset = offset;

      String statusStr = jsonObject.getString(STATUS_KEY);
      ControllerResponseStatus status;
      try {
        status = ControllerResponseStatus.valueOf(statusStr);
      } catch (Exception e) {
        status = ControllerResponseStatus.FAILED;
      }
      _status = status;
    }

    public Response(Params params) {
      _status = params.getStatus();
      _offset = params.getOffset();
    }

    public ControllerResponseStatus getStatus() {
      return _status;
    }

    public long getOffset() {
      return _offset;
    }

    public String toJsonString() {
      StringBuilder builder = new StringBuilder();
      builder.append("{\"" + STATUS_KEY + "\":" + "\"" + _status.name() + "\"," + "\"" + OFFSET_KEY + "\":" + _offset);
      builder.append("}");
      return builder.toString();
    }

    public static class Params {
      private static ControllerResponseStatus DEFAULT_RESPONSE_STATUS = ControllerResponseStatus.FAILED;
      private static long DEFAULT_RESP_OFFSET = -1L;

      ControllerResponseStatus _status;
      long _offset;

      public Params() {
        resetInternal();
      }
      public Params reset() {
        resetInternal();
        return this;
      }
      private void resetInternal() {
        _offset = DEFAULT_RESP_OFFSET;
        _status = DEFAULT_RESPONSE_STATUS;
      }

      public Params setOffset(long offset) {
        _offset = offset;
        return this;
      }
      public Params setStatus(ControllerResponseStatus status) {
        _status = status;
        return this;
      }

      public ControllerResponseStatus getStatus() {
        return _status;
      }
      public long getOffset() {
        return _offset;
      }
    }
  }
}
