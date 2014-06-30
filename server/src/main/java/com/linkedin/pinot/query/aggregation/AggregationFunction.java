package com.linkedin.pinot.query.aggregation;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.utils.IntArray;


/**
 * By extending this interface, one can access the index segment data, produce intermediate results for a given
 * segment, then aggregate those results on instance and router level. 
 * 
 */
public interface AggregationFunction extends Serializable {

  /**
   * {"aggregation_functions":
   *     [{"function":"com.linkedin.pinot.query.aggregation.functions.MaxAggregationFunction",
   *       "params":{"column":"metric_0"}
   *      },
   *      {"function":"com.linkedin.pinot.query.aggregation.functions.SumAggregationFunction",
   *       "params":{"column":"metric_1"}
   *      }
   *     ]
   * }
   * the argument corresponds to the parameters object in Json request. It is used to initialize the aggregation job
   * 
   * @param params
   */
  public void init(JSONObject params);

  /**
   * The map function. It can get the docId  from the docIds array containing value from 0 to docIdCount. 
   * All the docIds with array indexes >= docIdCount should be ignored
   * 
   * @param docIds
   * @param docIdCount
   * @param indexSegment
   * @return arbitrary map function results
   */
  AggregationResult aggregate(IntArray docIds, int docIdCount, IndexSegment indexSegment);

  /**
   * Take a list of intermediate results and do intermediate merge.
   * 
   * @param aggregationResultList
   * @param combineLevel
   * @return intermediate merge results
   */
  List<AggregationResult> combine(List<AggregationResult> aggregationResultList, CombineLevel combineLevel);

  /**
   * Take a list of intermediate results and merge them.
   * 
   * @param aggregationResultList
   * @return final merged results
   */
  AggregationResult reduce(List<AggregationResult> aggregationResultList);

  /**
   * Return a JsonObject representation for the final aggregation result.
   * 
   * @param finalAggregationResult
   * @return final results in Json format
   */
  JSONObject render(AggregationResult finalAggregationResult);

}
