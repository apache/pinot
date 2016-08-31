package com.linkedin.thirdeye.dbi.entity.base;

/**
 * Abstract superclass for entities with an id of type long.
 */
public abstract class AbstractEntity {
  protected Long id;

  protected AbstractEntity() {
  }

  protected AbstractEntity(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  @Override public boolean equals(Object o) {
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

  @Override public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
