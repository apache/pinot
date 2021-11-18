package org.apache.pinot.query;

import org.apache.pinot.query.exception.QueryException;


public interface PinotQueryRunner {

  void execute(PinotQueryPlan query) throws QueryException;
}
