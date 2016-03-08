/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.exception;

import com.linkedin.pinot.common.response.ProcessingException;
import java.io.PrintWriter;
import java.io.StringWriter;


public class QueryException {
  public static final int JSON_PARSING_ERROR_CODE = 100;
  public static final int JSON_COMPILATION_ERROR_CODE = 101;
  public static final int PQL_PARSING_ERROR_CODE = 150;
  public static final int QUERY_EXECUTION_ERROR_CODE = 200;
  public static final int EXECUTION_TIMEOUT_ERROR_CODE = 250;
  public static final int BROKER_GATHER_ERROR_CODE = 300;
  public static final int FUTURE_CALL_ERROR_CODE = 350;
  public static final int BROKER_TIMEOUT_ERROR_CODE = 400;
  public static final int COMBINE_SEGMENT_PLAN_TIMEOUT_ERROR_CODE = 1100;
  public static final int BROKER_RESOURCE_MISSING_ERROR_CODE = 410;
  public static final int BROKER_INSTANCE_MISSING_ERROR_CODE = 420;
  public static final int INTERNAL_ERROR_CODE = 450;
  public static final int MERGE_RESPONSE_ERROR_CODE = 500;
  public static final int FEDERATED_BROKER_UNAVAILABLE_ERROR_CODE = 550;
  public static final int UNKNOWN_ERROR_CODE = 1000;

  public static final ProcessingException JSON_PARSING_ERROR = new ProcessingException(JSON_PARSING_ERROR_CODE);
  public static final ProcessingException JSON_COMPILATION_ERROR = new ProcessingException(JSON_COMPILATION_ERROR_CODE);
  public static final ProcessingException PQL_PARSING_ERROR = new ProcessingException(PQL_PARSING_ERROR_CODE);
  public static final ProcessingException QUERY_EXECUTION_ERROR = new ProcessingException(QUERY_EXECUTION_ERROR_CODE);
  public static final ProcessingException EXECUTION_TIMEOUT_ERROR =
      new ProcessingException(EXECUTION_TIMEOUT_ERROR_CODE);
  public static final ProcessingException BROKER_GATHER_ERROR = new ProcessingException(BROKER_GATHER_ERROR_CODE);
  public static final ProcessingException FUTURE_CALL_ERROR = new ProcessingException(FUTURE_CALL_ERROR_CODE);
  public static final ProcessingException BROKER_TIMEOUT_ERROR = new ProcessingException(BROKER_TIMEOUT_ERROR_CODE);
  public static final ProcessingException COMBINE_SEGMENT_PLAN_TIMEOUT_ERROR =
      new ProcessingException(COMBINE_SEGMENT_PLAN_TIMEOUT_ERROR_CODE);
  public static final ProcessingException BROKER_RESOURCE_MISSING_ERROR =
      new ProcessingException(BROKER_RESOURCE_MISSING_ERROR_CODE);
  public static final ProcessingException BROKER_INSTANCE_MISSING_ERROR =
      new ProcessingException(BROKER_INSTANCE_MISSING_ERROR_CODE);
  public static final ProcessingException INTERNAL_ERROR = new ProcessingException(INTERNAL_ERROR_CODE);
  public static final ProcessingException MERGE_RESPONSE_ERROR = new ProcessingException(MERGE_RESPONSE_ERROR_CODE);
  public static final ProcessingException FEDERATED_BROKER_UNAVAILABLE_ERROR =
      new ProcessingException(FEDERATED_BROKER_UNAVAILABLE_ERROR_CODE);
  public static final ProcessingException UNKNOWN_ERROR = new ProcessingException(UNKNOWN_ERROR_CODE);

  static {
    JSON_PARSING_ERROR.setMessage("JsonParsingError");
    JSON_COMPILATION_ERROR.setMessage("JsonCompilationError");
    PQL_PARSING_ERROR.setMessage(" PQLParsingError");
    QUERY_EXECUTION_ERROR.setMessage("QueryExecutionError");
    EXECUTION_TIMEOUT_ERROR.setMessage("ExecutionTimeout");
    BROKER_GATHER_ERROR.setMessage("BrokerGatherError");
    FUTURE_CALL_ERROR.setMessage("FutureCallError");
    BROKER_TIMEOUT_ERROR.setMessage("BrokerTimeout");
    COMBINE_SEGMENT_PLAN_TIMEOUT_ERROR.setMessage("CombineSegmentPlanTimeOut");
    BROKER_RESOURCE_MISSING_ERROR.setMessage("BrokerResourceMissingError");
    BROKER_INSTANCE_MISSING_ERROR.setMessage("BrokerInstanceMissingError");
    INTERNAL_ERROR.setMessage("InternalError");
    MERGE_RESPONSE_ERROR.setMessage("MergeResponseError");
    FEDERATED_BROKER_UNAVAILABLE_ERROR.setMessage("FederatedBrokerUnavailable");
    UNKNOWN_ERROR.setMessage("UnknownError");
  }

  public static ProcessingException getException(ProcessingException processingException, Exception exception,
      int sizeOfStackTraceToTruncate) {
    ProcessingException retProcessingException = QueryException.FUTURE_CALL_ERROR.deepCopy();
    StringWriter sw = new StringWriter(sizeOfStackTraceToTruncate);
    exception.printStackTrace(new PrintWriter(sw));
    retProcessingException.setMessage(sw.toString());
    return retProcessingException;
  }

  public static ProcessingException getException(ProcessingException processingException, Exception exception) {
    return getException(processingException, exception, 1000);
  }
}
