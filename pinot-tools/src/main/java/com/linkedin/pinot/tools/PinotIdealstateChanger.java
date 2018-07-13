package com.linkedin.pinot.tools;

import com.google.common.base.Function;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;


public class PinotIdealstateChanger extends PinotZKChanger {

  private final String _tableNameWithType;
  private final boolean _dryRun;

  public PinotIdealstateChanger(String zkAddress, String clusterName, String tableNameWithType, boolean dryRun) {
    super(zkAddress, clusterName);
    _tableNameWithType = tableNameWithType;
    _dryRun = dryRun;
  }

  public void updateIdealState() {
    HelixManager helixManager = getHelixManager();
    HelixHelper.updateIdealState(helixManager, _tableNameWithType, new Function<IdealState, IdealState>() {
          @Nullable
          @Override
          public IdealState apply(@Nullable IdealState input) {
            return applyChangesToIdealState(input);
          }
        },
    RetryPolicies.exponentialBackoffRetryPolicy(5, 100, 1.2));
  }

  private IdealState applyChangesToIdealState(IdealState input) {
    // Add code here as needed to change idealstate
    return input;
  }

  public static void main(String[] args) {
    final boolean dryRun = true;
    final String zkAddress = "localhost:2191";
    final String clusterName = "LLCRealtimeClusterIntegrationTest";
    final String tableName = "mytable_REALTIME";

    PinotIdealstateChanger changer = new PinotIdealstateChanger(zkAddress, clusterName, tableName, dryRun);
    changer.updateIdealState();
  }
}
