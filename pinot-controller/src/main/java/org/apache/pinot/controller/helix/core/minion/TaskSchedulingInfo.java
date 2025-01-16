package org.apache.pinot.controller.helix.core.minion;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;


public class TaskSchedulingInfo {
  private List<String> _scheduledTaskNames;
  private final List<String> _generationErrors = new ArrayList<>();
  private final List<String> _schedulingErrors = new ArrayList<>();

  @Nullable
  public List<String> getScheduledTaskNames() {
    return _scheduledTaskNames;
  }

  public TaskSchedulingInfo setScheduledTaskNames(List<String> scheduledTaskNames) {
    _scheduledTaskNames = scheduledTaskNames;
    return this;
  }

  public List<String> getGenerationErrors() {
    return _generationErrors;
  }

  public void addGenerationError(String generationError) {
    _generationErrors.add(generationError);
  }

  public List<String> getSchedulingErrors() {
    return _schedulingErrors;
  }

  public void addSchedulingError(String schedulingError) {
    _schedulingErrors.add(schedulingError);
  }
}
