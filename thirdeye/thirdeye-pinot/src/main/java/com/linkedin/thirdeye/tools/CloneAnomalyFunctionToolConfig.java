package com.linkedin.thirdeye.tools;

public class CloneAnomalyFunctionToolConfig {

  // File containing db details
  private String persistenceFile;
  // Start time and end time in ISO format for ad-hoc run
  private String startTimeIso;
  private String endTimeIso;

  // function ids to clone
  private String functionIds;
  // new name tags of the clone functions, [new name] = [old name] + [tag]
  private String cloneNameTags;
  private String doCloneAnomalyResults;

  public String getPersistenceFile() {
    return persistenceFile;
  }
  public void setPersistenceFile(String persistenceFile) {
    this.persistenceFile = persistenceFile;
  }
  public String getStartTimeIso() {
    return startTimeIso;
  }
  public void setStartTimeIso(String startTimeIso) {
    this.startTimeIso = startTimeIso;
  }
  public String getEndTimeIso() {
    return endTimeIso;
  }
  public void setEndTimeIso(String endTimeIso) {
    this.endTimeIso = endTimeIso;
  }
  public String getFunctionIds() {
    return functionIds;
  }
  public void setFunctionIds(String functionIds) {
    this.functionIds = functionIds;
  }
  public String getCloneNameTags() {
    return cloneNameTags;
  }
  public void setCloneNameTags(String cloneNameTags) {
    this.cloneNameTags = cloneNameTags;
  }
  public String getDoCloneAnomalyResults() {
    return doCloneAnomalyResults;
  }
  public void setDoCloneAnomalyResults(String doCloneAnomalyResults) {
    this.doCloneAnomalyResults = doCloneAnomalyResults;
  }

}
