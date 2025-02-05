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
package org.apache.pinot.common.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.spi.exception.QException;


// TODO: Clean up ProcessingException (thrift) because we don't send it through the wire
// TODO: Rename this class to QueryExceptionUtil because it doesn't extend Exception
public class QueryException {
  private QueryException() {
  }

  private static int _maxLinesOfStackTracePerFrame = 5;

  /**
   * The 2 regexp below must conform with JDK's internal implementation in {@link Throwable#printStackTrace()}.
   */
  private static final Pattern CAUSE_CAPTION_REGEXP = Pattern.compile("^([\\t]*)Caused by: ");
  private static final Pattern SUPPRESSED_CAPTION_REGEXP = Pattern.compile("^([\\t]*)Suppressed: ");

  private static final String OMITTED_SIGNAL = "...";

  // TODO: config max lines of stack trace if necessary. The config should be on instance level.
  public static void setMaxLinesOfStackTrace(int maxLinesOfStackTracePerFrame) {
    _maxLinesOfStackTracePerFrame = maxLinesOfStackTracePerFrame;
  }

  public static final int JSON_PARSING_ERROR_CODE = QException.JSON_PARSING_ERROR_CODE;
  public static final int SQL_PARSING_ERROR_CODE = QException.SQL_PARSING_ERROR_CODE;
  public static final int ACCESS_DENIED_ERROR_CODE = QException.ACCESS_DENIED_ERROR_CODE;
  public static final int TABLE_DOES_NOT_EXIST_ERROR_CODE = QException.TABLE_DOES_NOT_EXIST_ERROR_CODE;
  public static final int TABLE_IS_DISABLED_ERROR_CODE = QException.TABLE_IS_DISABLED_ERROR_CODE;
  public static final int QUERY_EXECUTION_ERROR_CODE = QException.QUERY_EXECUTION_ERROR_CODE;
  public static final int QUERY_CANCELLATION_ERROR_CODE = QException.QUERY_CANCELLATION_ERROR_CODE;
  // TODO: Handle these errors in broker
  public static final int SERVER_SHUTTING_DOWN_ERROR_CODE = QException.SERVER_SHUTTING_DOWN_ERROR_CODE;
  public static final int SERVER_OUT_OF_CAPACITY_ERROR_CODE = QException.SERVER_OUT_OF_CAPACITY_ERROR_CODE;
  public static final int SERVER_TABLE_MISSING_ERROR_CODE = QException.SERVER_TABLE_MISSING_ERROR_CODE;
  public static final int SERVER_SEGMENT_MISSING_ERROR_CODE = QException.SERVER_SEGMENT_MISSING_ERROR_CODE;
  public static final int QUERY_SCHEDULING_TIMEOUT_ERROR_CODE = QException.QUERY_SCHEDULING_TIMEOUT_ERROR_CODE;
  public static final int SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE =
      QException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE;
  public static final int EXECUTION_TIMEOUT_ERROR_CODE = QException.EXECUTION_TIMEOUT_ERROR_CODE;
  public static final int BROKER_SEGMENT_UNAVAILABLE_ERROR_CODE = QException.BROKER_SEGMENT_UNAVAILABLE_ERROR_CODE;
  public static final int BROKER_TIMEOUT_ERROR_CODE = QException.BROKER_TIMEOUT_ERROR_CODE;
  public static final int BROKER_RESOURCE_MISSING_ERROR_CODE = QException.BROKER_RESOURCE_MISSING_ERROR_CODE;
  public static final int BROKER_INSTANCE_MISSING_ERROR_CODE = QException.BROKER_INSTANCE_MISSING_ERROR_CODE;
  public static final int BROKER_REQUEST_SEND_ERROR_CODE = QException.BROKER_REQUEST_SEND_ERROR_CODE;
  public static final int SERVER_NOT_RESPONDING_ERROR_CODE = QException.SERVER_NOT_RESPONDING_ERROR_CODE;
  public static final int TOO_MANY_REQUESTS_ERROR_CODE = QException.TOO_MANY_REQUESTS_ERROR_CODE;
  public static final int INTERNAL_ERROR_CODE = QException.INTERNAL_ERROR_CODE;
  public static final int MERGE_RESPONSE_ERROR_CODE = QException.MERGE_RESPONSE_ERROR_CODE;
  public static final int QUERY_VALIDATION_ERROR_CODE = QException.QUERY_VALIDATION_ERROR_CODE;
  public static final int UNKNOWN_COLUMN_ERROR_CODE = QException.UNKNOWN_COLUMN_ERROR_CODE;
  public static final int QUERY_PLANNING_ERROR_CODE = QException.QUERY_PLANNING_ERROR_CODE;
  public static final int UNKNOWN_ERROR_CODE = QException.UNKNOWN_ERROR_CODE;
  // NOTE: update isClientError() method appropriately when new codes are added

  public static final ProcessingException JSON_PARSING_ERROR = new ProcessingException(JSON_PARSING_ERROR_CODE);
  public static final ProcessingException SQL_PARSING_ERROR = new ProcessingException(SQL_PARSING_ERROR_CODE);
  public static final ProcessingException QUERY_PLANNING_ERROR = new ProcessingException(QUERY_PLANNING_ERROR_CODE);
  public static final ProcessingException TABLE_DOES_NOT_EXIST_ERROR =
      new ProcessingException(TABLE_DOES_NOT_EXIST_ERROR_CODE);
  public static final ProcessingException TABLE_IS_DISABLED_ERROR =
      new ProcessingException(TABLE_IS_DISABLED_ERROR_CODE);
  public static final ProcessingException QUERY_EXECUTION_ERROR = new ProcessingException(QUERY_EXECUTION_ERROR_CODE);
  public static final ProcessingException QUERY_CANCELLATION_ERROR =
      new ProcessingException(QUERY_CANCELLATION_ERROR_CODE);
  public static final ProcessingException SERVER_SCHEDULER_DOWN_ERROR =
      new ProcessingException(SERVER_SHUTTING_DOWN_ERROR_CODE);
  public static final ProcessingException SERVER_OUT_OF_CAPACITY_ERROR =
      new ProcessingException(SERVER_OUT_OF_CAPACITY_ERROR_CODE);
  public static final ProcessingException SERVER_TABLE_MISSING_ERROR =
      new ProcessingException(SERVER_TABLE_MISSING_ERROR_CODE);
  public static final ProcessingException SERVER_SEGMENT_MISSING_ERROR =
      new ProcessingException(SERVER_SEGMENT_MISSING_ERROR_CODE);
  public static final ProcessingException QUERY_SCHEDULING_TIMEOUT_ERROR =
      new ProcessingException(QUERY_SCHEDULING_TIMEOUT_ERROR_CODE);
  public static final ProcessingException SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR =
      new ProcessingException(SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
  public static final ProcessingException EXECUTION_TIMEOUT_ERROR =
      new ProcessingException(EXECUTION_TIMEOUT_ERROR_CODE);
  public static final ProcessingException BROKER_SEGMENT_UNAVAILABLE_ERROR =
      new ProcessingException(BROKER_SEGMENT_UNAVAILABLE_ERROR_CODE);
  public static final ProcessingException BROKER_TIMEOUT_ERROR = new ProcessingException(BROKER_TIMEOUT_ERROR_CODE);
  public static final ProcessingException BROKER_RESOURCE_MISSING_ERROR =
      new ProcessingException(BROKER_RESOURCE_MISSING_ERROR_CODE);
  public static final ProcessingException BROKER_INSTANCE_MISSING_ERROR =
      new ProcessingException(BROKER_INSTANCE_MISSING_ERROR_CODE);
  public static final ProcessingException INTERNAL_ERROR = new ProcessingException(INTERNAL_ERROR_CODE);
  public static final ProcessingException MERGE_RESPONSE_ERROR = new ProcessingException(MERGE_RESPONSE_ERROR_CODE);
  public static final ProcessingException QUERY_VALIDATION_ERROR = new ProcessingException(QUERY_VALIDATION_ERROR_CODE);
  public static final ProcessingException UNKNOWN_COLUMN_ERROR = new ProcessingException(UNKNOWN_COLUMN_ERROR_CODE);
  public static final ProcessingException UNKNOWN_ERROR = new ProcessingException(UNKNOWN_ERROR_CODE);
  public static final ProcessingException QUOTA_EXCEEDED_ERROR = new ProcessingException(TOO_MANY_REQUESTS_ERROR_CODE);
  public static final ProcessingException BROKER_REQUEST_SEND_ERROR =
      new ProcessingException(BROKER_REQUEST_SEND_ERROR_CODE);

  static {
    JSON_PARSING_ERROR.setMessage("JsonParsingError");
    SQL_PARSING_ERROR.setMessage("SQLParsingError");
    QUERY_PLANNING_ERROR.setMessage("QueryPlanningError");
    TABLE_DOES_NOT_EXIST_ERROR.setMessage("TableDoesNotExistError");
    TABLE_IS_DISABLED_ERROR.setMessage("TableIsDisabledError");
    QUERY_EXECUTION_ERROR.setMessage("QueryExecutionError");
    QUERY_CANCELLATION_ERROR.setMessage("QueryCancellationError");
    SERVER_SCHEDULER_DOWN_ERROR.setMessage("ServerShuttingDown");
    SERVER_OUT_OF_CAPACITY_ERROR.setMessage("ServerOutOfCapacity");
    SERVER_TABLE_MISSING_ERROR.setMessage("ServerTableMissing");
    SERVER_SEGMENT_MISSING_ERROR.setMessage("ServerSegmentMissing");
    QUERY_SCHEDULING_TIMEOUT_ERROR.setMessage("QuerySchedulingTimeoutError");
    SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR.setMessage("ServerResourceLimitExceededError");
    EXECUTION_TIMEOUT_ERROR.setMessage("ExecutionTimeoutError");
    BROKER_TIMEOUT_ERROR.setMessage("BrokerTimeoutError");
    BROKER_RESOURCE_MISSING_ERROR.setMessage("BrokerResourceMissingError");
    BROKER_INSTANCE_MISSING_ERROR.setMessage("BrokerInstanceMissingError");
    INTERNAL_ERROR.setMessage("InternalError");
    MERGE_RESPONSE_ERROR.setMessage("MergeResponseError");
    QUERY_VALIDATION_ERROR.setMessage("QueryValidationError");
    UNKNOWN_COLUMN_ERROR.setMessage("UnknownColumnError");
    UNKNOWN_ERROR.setMessage("UnknownError");
    QUOTA_EXCEEDED_ERROR.setMessage("QuotaExceededError");
  }

  public static ProcessingException getException(ProcessingException processingException, Throwable t) {
    return getException(processingException, getTruncatedStackTrace(t));
  }

  public static ProcessingException getException(ProcessingException processingException, String errorMessage) {
    String errorType = processingException.getMessage();
    ProcessingException copiedProcessingException = processingException.deepCopy();
    copiedProcessingException.setMessage(errorType + ":\n" + errorMessage);
    return copiedProcessingException;
  }

  // TODO: getTruncatedStackTrace(Throwable) always precede by t.getMessage();
  public static String getTruncatedStackTrace(Throwable t) {
    StringWriter stringWriter = new StringWriter();
    t.printStackTrace(new PrintWriter(stringWriter));
    String fullStackTrace = stringWriter.toString();
    String[] lines = StringUtils.split(fullStackTrace, '\n');
    // exception should at least have one line, no need to check here.
    StringBuilder sb = new StringBuilder(lines[0]);
    int lineOfStackTracePerFrame = 1;
    for (int i = 1; i < lines.length; i++) {
      if (CAUSE_CAPTION_REGEXP.matcher(lines[i]).find() || SUPPRESSED_CAPTION_REGEXP.matcher(lines[i]).find()) {
        // reset stack trace print counter when a new cause or suppressed Throwable were found.
        if (lineOfStackTracePerFrame >= _maxLinesOfStackTracePerFrame) {
          sb.append('\n').append(OMITTED_SIGNAL);
        }
        sb.append('\n').append(lines[i]);
        lineOfStackTracePerFrame = 1;
      } else if (lineOfStackTracePerFrame < _maxLinesOfStackTracePerFrame) {
        // only print numLinesOfStackTrace stack trace and ignore any additional lines.
        sb.append('\n').append(lines[i]);
        lineOfStackTracePerFrame++;
      }
    }
    return sb.toString();
  }

  /**
   * Determines if a query-exception-error-code represents an error on the client side.
   * @param errorCode  the error code from processing the query
   * @return whether the code indicates client error or not
   */
  public static boolean isClientError(int errorCode) {
    switch (errorCode) {
      // NOTE: QueryException.BROKER_RESOURCE_MISSING_ERROR can be triggered due to issues
      // with EV updates. For cases where access to tables is controlled via ACLs, for an
      // incorrect table name we expect ACCESS_DENIED_ERROR to be thrown. Hence, we currently
      // don't treat BROKER_RESOURCE_MISSING_ERROR as client error.
      case QueryException.ACCESS_DENIED_ERROR_CODE:
      case QueryException.JSON_PARSING_ERROR_CODE:
      case QueryException.QUERY_CANCELLATION_ERROR_CODE:
      case QueryException.QUERY_VALIDATION_ERROR_CODE:
      case QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE:
      case QueryException.SQL_PARSING_ERROR_CODE:
      case QException.SQL_RUNTIME_ERROR_CODE:
      case QueryException.TOO_MANY_REQUESTS_ERROR_CODE:
      case QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE:
      case QueryException.TABLE_IS_DISABLED_ERROR_CODE:
      case QueryException.UNKNOWN_COLUMN_ERROR_CODE:
        return true;
      default:
        return false;
    }
  }
}
