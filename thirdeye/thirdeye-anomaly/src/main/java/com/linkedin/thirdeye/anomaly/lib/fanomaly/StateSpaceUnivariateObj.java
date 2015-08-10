package com.linkedin.thirdeye.anomaly.lib.fanomaly;


import org.apache.commons.math3.analysis.UnivariateFunction;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateSpaceUnivariateObj implements UnivariateFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateSpaceUnivariateObj.class);

  private DoubleMatrix state_transition_matrix_;
  private DoubleMatrix observation_matrix_;
  private DoubleMatrix initial_state_mean_;
  private DoubleMatrix initial_state_covariance_;
  private double r_;
  private int dimension_of_states_ ;
  private int dimension_of_observations_;
  // Observations.
  private DoubleMatrix[] observations_;
  public StateSpaceUnivariateObj(
      DoubleMatrix state_transition_matrix,
      DoubleMatrix observation_matrix,
      double r,
      DoubleMatrix m0,
      DoubleMatrix initial_state_covariance,
      DoubleMatrix[] observations) {

    observation_matrix_ = observation_matrix.dup();
    state_transition_matrix_ = state_transition_matrix.dup();
    r_ = r;
    initial_state_mean_ = m0.dup();
    initial_state_covariance_ = initial_state_covariance.dup();
    observations_ = observations.clone();
    dimension_of_states_ = observation_matrix.getColumns();
    dimension_of_observations_ = observation_matrix.getRows();
  }

  public double value(double state_noise) {
    DoubleMatrix transition_covariance_matrix = DoubleMatrix.eye(dimension_of_states_).muli(state_noise);
    DoubleMatrix observation_covariance_matrix = DoubleMatrix.eye(dimension_of_observations_).muli(r_ * state_noise);

    StateSpaceModel model  = new StateSpaceModel(
        state_transition_matrix_,
        observation_matrix_,
        transition_covariance_matrix,
        observation_covariance_matrix,
        initial_state_mean_,
        initial_state_covariance_,
        observations_);
    double ll = model.GetLogLikeliHood();
    LOGGER.info("log likelihood : {}", state_noise);
    return ll;
  }

}
