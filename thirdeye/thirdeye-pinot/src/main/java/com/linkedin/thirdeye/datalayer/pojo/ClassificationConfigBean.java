package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;

public class ClassificationConfigBean extends AbstractBean {
  private String name;
  private long mainFunctionId;
  private List<Long> functionIdList;
  boolean active;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getMainFunctionId() {
    return mainFunctionId;
  }

  public void setMainFunctionId(long mainFunctionId) {
    this.mainFunctionId = mainFunctionId;
  }

  public List<Long> getFunctionIdList() {
    return functionIdList;
  }

  public void setFunctionIdList(List<Long> functionIdList) {
    this.functionIdList = functionIdList;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }
}
