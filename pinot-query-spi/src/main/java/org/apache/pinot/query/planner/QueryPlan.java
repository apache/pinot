package org.apache.pinot.query.planner;

import java.io.Serializable;
import java.util.Collection;
import org.apache.pinot.query.exception.QueryException;


/**
 * QueryPlan consist of the optimized plan spit out from the query planner.
 *
 * It should contain a list of flattened {@link QueryStage}s translated from the relational plan root.
 */
public interface QueryPlan extends Serializable {

  String explain(QueryContext queryContext);

  Collection<QueryStage> composeStages(QueryContext queryContext) throws QueryException;
}
