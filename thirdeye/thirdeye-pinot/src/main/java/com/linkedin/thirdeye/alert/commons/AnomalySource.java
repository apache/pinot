package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.datalayer.util.Predicate;


public enum AnomalySource {
  DATASET {
    @Override
    public Predicate getPredicate(String predicateValue) {
      return Predicate.IN("collection", predicateValue.split(","));
    }
  },
  METRIC {
    @Override
    public Predicate getPredicate(String predicateValue) {
      return Predicate.IN("metric", predicateValue.split(","));
    }
  },
  ANOMALY_FUNCTION {
    @Override
    public Predicate getPredicate(String predicateValue) {
      return Predicate.IN("functionId", predicateValue.split(","));
    }
  };

  AnomalySource(){

  }

  public abstract Predicate getPredicate(String predicateValue);
}
