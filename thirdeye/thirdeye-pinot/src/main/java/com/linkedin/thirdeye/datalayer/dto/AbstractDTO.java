package com.linkedin.thirdeye.datalayer.dto;

public abstract class AbstractDTO {
  private Long id;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }
}
