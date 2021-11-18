package org.apache.pinot.query;

import org.apache.pinot.query.exception.QueryException;


public interface QueryRunner {

  void execute(QueryPlan query) throws QueryException;
}
