package com.linkedin.thirdeye.rootcause;

import java.util.Set;


public class SearchContext {
  final Set<Entity> entities;

  public SearchContext(Set<Entity> entities) {
    this.entities = entities;
  }

  public Set<Entity> getEntities() {
    return entities;
  }
}
