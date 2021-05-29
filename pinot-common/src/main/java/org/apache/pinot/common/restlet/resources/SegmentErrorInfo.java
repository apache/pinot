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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.commons.lang3.exception.ExceptionUtils;


/**
 * This class is used to represent errors related to a segment.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"timestamp", "errorMessage", "stackTrace"}) // For readability of JSON output
public class SegmentErrorInfo {

  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss z";
  private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT, Locale.getDefault());

  private final String _timestamp;
  private final String _errorMessage;
  private final String _stackTrace;

  static {
    SIMPLE_DATE_FORMAT.setTimeZone(TimeZone.getDefault());
  }

  /**
   * This constructor is specifically for JSON ser/de.
   *
   * @param timestamp Time stamp of the error in Simple Date Format.
   * @param errorMessage Error message
   * @param stackTrace Exception stack trace
   *
   */
  @JsonCreator
  public SegmentErrorInfo(@JsonProperty("timestamp") String timestamp,
      @JsonProperty("errorMessage") String errorMessage, @JsonProperty("stackTrace") String stackTrace) {
    _timestamp = timestamp;
    _errorMessage = errorMessage;
    _stackTrace = stackTrace;
  }

  /**
   * Constructor for the class
   * @param timestampMs Timestamp of error/exception
   * @param errorMessage Error message
   * @param exception Exception
   */
  public SegmentErrorInfo(long timestampMs, String errorMessage, Exception exception) {
    _timestamp = epochToSDF(timestampMs);
    _errorMessage = errorMessage;
    _stackTrace = (exception != null) ? ExceptionUtils.getStackTrace(exception) : null;
  }

  public String getStackTrace() {
    return _stackTrace;
  }

  public String getErrorMessage() {
    return _errorMessage;
  }

  public String getTimestamp() {
    return _timestamp;
  }

  /**
   * Utility function to convert epoch in millis to SDF of form "yyyy-MM-dd HH:mm:ss z".
   *
   * @param millisSinceEpoch Time in millis to convert
   * @return SDF equivalent
   */
  private static String epochToSDF(long millisSinceEpoch) {
    return SIMPLE_DATE_FORMAT.format(new Date(millisSinceEpoch));
  }
}
