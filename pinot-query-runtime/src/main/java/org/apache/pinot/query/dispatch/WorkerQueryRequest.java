package org.apache.pinot.query.dispatch;

import java.io.Serializable;
import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.StageNode;


/**
 * WorkerQueryRequest is the extended version of the {@link org.apache.pinot.core.query.request.ServerQueryRequest}.
 */
public class WorkerQueryRequest implements Serializable {
  private final String _stageId;
  private final ServerInstance _serverInstance;
  private final StageNode _stageRoot;
  private final Map<String, StageMetadata> _metadataMap;

  public WorkerQueryRequest(String stageId, ServerInstance serverInstance, StageNode stageRoot, Map<String, StageMetadata> metadataMap) {
    _stageId = stageId;
    _serverInstance = serverInstance;
    _stageRoot = stageRoot;
    _metadataMap = metadataMap;
  }

  public String getStageId() {
    return _stageId;
  }

  public ServerInstance getServerInstance() {
    return _serverInstance;
  }

  public StageNode getStageRoot() {
    return _stageRoot;
  }

  public Map<String, StageMetadata> getMetadataMap() {
    return _metadataMap;
  }
}
