package com.linkedin.pinot.common.exception;

import com.linkedin.pinot.common.response.ProcessingException;


public class QueryException {

  public static final ProcessingException JSON_PARSING_ERROR = new ProcessingException(100);
  public static final ProcessingException JSON_COMPILATION_ERROR = new ProcessingException(101);
  public static final ProcessingException PQL_PARSING_ERROR = new ProcessingException(150);
  public static final ProcessingException QUERY_EXECUTION_ERROR = new ProcessingException(200);
  public static final ProcessingException EXECUTION_TIMEOUT_ERROR = new ProcessingException(250);
  public static final ProcessingException BROKER_GATHER_ERROR = new ProcessingException(300);
  public static final ProcessingException FUTURE_CALL_ERROR = new ProcessingException(350);
  public static final ProcessingException BROKER_TIMEOUT_ERROR = new ProcessingException(400);
  public static final ProcessingException BROKER_RESOURCE_MISSING_ERROR = new ProcessingException(410);
  public static final ProcessingException BROKER_INSTANCE_MISSING_ERROR = new ProcessingException(420);
  public static final ProcessingException INTERNAL_ERROR = new ProcessingException(450);
  public static final ProcessingException MERGE_RESPONSE_ERROR = new ProcessingException(500);
  public static final ProcessingException FEDERATED_BROKER_UNAVAILABLE_ERROR = new ProcessingException(550);
  public static final ProcessingException UNKNOWN_ERROR = new ProcessingException(1000);

  static {
    JSON_PARSING_ERROR.setMessage("JsonParsingError");
    JSON_COMPILATION_ERROR.setMessage("JsonCompilationError");
    PQL_PARSING_ERROR.setMessage(" PQLParsingError");
    QUERY_EXECUTION_ERROR.setMessage("QueryExecutionError");
    EXECUTION_TIMEOUT_ERROR.setMessage("ExecutionTimeout");
    BROKER_GATHER_ERROR.setMessage("BrokerGatherError");
    FUTURE_CALL_ERROR.setMessage("FutureCallError");
    BROKER_TIMEOUT_ERROR.setMessage("BrokerTimeout");
    BROKER_RESOURCE_MISSING_ERROR.setMessage("BrokerResourceMissingError");
    BROKER_INSTANCE_MISSING_ERROR.setMessage("BrokerInstanceMissingError");
    INTERNAL_ERROR.setMessage("InternalError");
    MERGE_RESPONSE_ERROR.setMessage("MergeResponseError");
    FEDERATED_BROKER_UNAVAILABLE_ERROR.setMessage("FederatedBrokerUnavailable");
    UNKNOWN_ERROR.setMessage("UnknownError");
  }

}
