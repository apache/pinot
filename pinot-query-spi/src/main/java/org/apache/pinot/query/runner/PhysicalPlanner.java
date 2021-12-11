package org.apache.pinot.query.runner;

import org.apache.pinot.query.exception.QueryException;
import org.apache.pinot.query.planner.QueryProgram;
import org.apache.pinot.query.planner.QueryStage;


/**
 * QueryWorker can create locally optimized Plan based on {@link QueryStage}.
 *
 * @param <T> the type of optimized physical plan this QueryWorker produces.
 */
public interface PhysicalPlanner<T extends QueryProgram> {

  T plan(QueryStage queryStage) throws QueryException;
}
