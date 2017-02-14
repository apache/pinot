package com.linkedin.thirdeye.detector.functionex;

public interface AnomalyFunctionExDataSource<Q, R> {
  R query(Q query, AnomalyFunctionExContext context) throws Exception;
}
