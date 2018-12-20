package com.linkedin.pinot.controller.helix.core.periodictask;

import com.linkedin.pinot.controller.ControllerLeadershipManager;
import com.linkedin.pinot.controller.LeadershipChangeSubscriber;
import com.linkedin.pinot.core.periodictask.PeriodicTask;
import com.linkedin.pinot.core.periodictask.PeriodicTaskScheduler;
import java.util.List;


/**
 * A {@link PeriodicTaskScheduler} for scheduling {@link ControllerPeriodicTask} which are created on controller startup
 * and started/stopped on controller leadership changes
 */
public class ControllerPeriodicTaskScheduler extends PeriodicTaskScheduler implements LeadershipChangeSubscriber {

  private List<PeriodicTask> _controllerPeriodicTasks;

  /**
   * Initialize the {@link ControllerPeriodicTaskScheduler} with the {@link ControllerPeriodicTask} created at startup
   * @param controllerPeriodicTasks
   */
  public void init(List<PeriodicTask> controllerPeriodicTasks) {
    _controllerPeriodicTasks = controllerPeriodicTasks;
    ControllerLeadershipManager.getInstance().subscribe(ControllerPeriodicTaskScheduler.class.getName(), this);
  }

  @Override
  public void onBecomingLeader() {
    start(_controllerPeriodicTasks);
  }

  @Override
  public void onBecomingNonLeader() {
    stop();
  }
}
