package com.linkedin.thirdeye.datalayer.pojo;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;

@MappedSuperclass
public abstract class AbstractBean extends AbstractDTO{
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }
}
