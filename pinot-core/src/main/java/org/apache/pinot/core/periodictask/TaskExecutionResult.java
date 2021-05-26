package org.apache.pinot.core.periodictask;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


/**
 * @author Harish Shankar 
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskExecutionResult {
  private final Status _status;
  private final String _description;
  private final String _taskName;

  @JsonCreator
  public TaskExecutionResult(@JsonProperty(value = "status", required = true) Status status,
      @JsonProperty(value = "description", required = true) String description,
      @JsonProperty("task_name") String taskName) {
    _status = status;
    _description = description;
    _taskName = taskName;
  }

  @JsonProperty
  public Status getStatus() {
    return _status;
  }

  @JsonProperty
  public String getDescription() {
    return _description;
  }

  @JsonProperty
  public String getTaskName() {
    return _taskName;
  }

  public enum Status {
    NO_OP, DONE, FAILED, IN_PROGRESS
  }
}
