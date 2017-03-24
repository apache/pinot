package com.linkedin.thirdeye.datalayer.entity;

import java.sql.Timestamp;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Abstract superclass for entities with an id of type long.
 */
public abstract class AbstractEntity {
  protected Long id;

  protected Timestamp createTime;

  protected Timestamp updateTime;

  protected int version;

  protected AbstractEntity() {}

  protected AbstractEntity(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Timestamp getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Timestamp createTime) {
    this.createTime = createTime;
  }

  public Timestamp getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Timestamp updateTime) {
    this.updateTime = updateTime;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractEntity)) {
      return false;
    }
    AbstractEntity entity = (AbstractEntity) o;

    if (id != null ? !id.equals(entity.id) : entity.id != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
