package org.apache.pinot.query.planner;

import java.util.Map;
import org.apache.pinot.query.planner.nodes.StageNode;


public class QueryPlan {
  private Map<String, StageNode> _queryStageMap;
  private Map<String, StageMetadata> _stageMetadataMap;

  public QueryPlan(Map<String, StageNode> queryStageMap, Map<String, StageMetadata> stageMetadataMap) {
    _queryStageMap = queryStageMap;
    _stageMetadataMap = stageMetadataMap;
  }

  public Map<String, StageNode> getQueryStageMap() {
    return _queryStageMap;
  }

  public Map<String, StageMetadata> getStageMetadataMap() {
    return _stageMetadataMap;
  }
}
