package org.apache.pinot.core.query.request.context.predicate;


public abstract class BasePredicate implements Predicate {

  // null by default to indicate that predicate does not have precomputed results.
  Boolean _precomputed = null;

  @Override
  public void setPrecomputed(boolean precomputed) {
    _precomputed = precomputed;
  }

  @Override
  public Boolean getPrecomputed() {
    return _precomputed;
  }
}
