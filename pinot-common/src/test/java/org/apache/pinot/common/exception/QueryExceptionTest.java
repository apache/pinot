package org.apache.pinot.common.exception;

import org.apache.pinot.common.response.ProcessingException;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;


public class QueryExceptionTest {

  @Test
  public void testExceptionMessage() {
    Exception exception = new UnsupportedOperationException("Caught exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, exception);

    // there's one more 1 lines for the top level wrapper QUERY_EXECUTION_ERROR
    assertEquals(5 + 1, processingException.getMessage().split("\n").length);

    Exception withSuppressedException = new IllegalStateException("Suppressed exception");
    withSuppressedException.addSuppressed(processingException);
    ProcessingException withSuppressedProcessingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, withSuppressedException);

    // QueryException._maxLinesOfStackTracePerFrame * 2 + 1 line separator + 1 QUERY_EXECUTION_ERROR wrapper.
    assertEquals(5 * 2 + 1 + 1, withSuppressedProcessingException.getMessage().split("\n").length);

    Exception withNestedException = new IllegalStateException("Outer exception", withSuppressedProcessingException);
    ProcessingException withNestedProcessingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, withNestedException);

    // QueryException._maxLinesOfStackTracePerFrame * 3 + 2 line separators + 1 QUERY_EXECUTION_ERROR wrapper.
    assertEquals(5 * 3 + 2 + 1, withNestedProcessingException.getMessage().split("\n").length);
  }
}
