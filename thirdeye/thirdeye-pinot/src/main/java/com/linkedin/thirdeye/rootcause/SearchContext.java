package com.linkedin.thirdeye.rootcause;

import java.util.Set;


/**
 * Holds the input eEntities for pipeline execution. The contract between SearchContext and
 * Pipeline guarantees that the search context already holds the specific subclass for each
 * Entity (e.g. each entity whose URN starts with "thirdeye:metric:" is an instance of MetricEntity)
 *
 */
public class SearchContext {
  final Set<Entity> entities;

  public SearchContext(Set<Entity> entities) {
    this.entities = entities;
  }

  public Set<Entity> getEntities() {
    return entities;
  }
}
