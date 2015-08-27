package com.linkedin.thirdeye.anomaly.lib.kalman;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.MatrixDimensionMismatchException;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

public class StateSpaceModel {
  // The matrices describing a state space model.
  private DoubleMatrix state_transition_matrix_ref_;
  private DoubleMatrix observation_matrix_ref_;
  private DoubleMatrix transition_covariance_matrix_ref_;
  private DoubleMatrix observation_covariance_matrix_ref_;
  private DoubleMatrix initial_state_mean_ref_;
  private DoubleMatrix initial_state_covariance_ref_;

  // Dimensions.
  private int dimension_of_states_;
  private int dimension_of_observations_;
  private int number_of_observations_;

  // Observations.
  private DoubleMatrix[] observations_ref_;
  private DoubleMatrix[] estimated_state_means_;
  private DoubleMatrix[] estimated_state_covariances_;
  private DoubleMatrix[] predicted_observation_means_;
  private DoubleMatrix[] predicted_observation_covariances_;

  //prediction
  private DoubleMatrix[] predicted_means_;
  private DoubleMatrix[] predicted_variances_;

  // Estimations
  private DoubleMatrix[] estimated_means_;
  private DoubleMatrix[] estimated_covariances_;

  private double sum_log_likelihood_;

  public StateSpaceModel(DoubleMatrix state_transition_matrix,
      DoubleMatrix observation_matrix,
      DoubleMatrix transition_covariance_matrix,
      DoubleMatrix observation_covariance_matrix,
      DoubleMatrix initial_state_mean,
      DoubleMatrix initial_state_covariance,
      DoubleMatrix[] observations) {
    dimension_of_states_ = observation_matrix.getColumns();
    dimension_of_observations_ = observation_matrix.getRows();
    observation_matrix_ref_ = observation_matrix;

    if (!state_transition_matrix.isSquare() ||
        state_transition_matrix.getRows() != dimension_of_states_) {
      throw new MatrixDimensionMismatchException(state_transition_matrix.getRows(),
          state_transition_matrix.getColumns(),
          dimension_of_states_,
          dimension_of_states_);
    }
    state_transition_matrix_ref_ = state_transition_matrix;


    if (!transition_covariance_matrix.isSquare() ||
        transition_covariance_matrix.getRows() != dimension_of_states_) {
      throw new MatrixDimensionMismatchException(transition_covariance_matrix.getRows(),
          transition_covariance_matrix.getColumns(),
          dimension_of_states_,
          dimension_of_states_);
    }
    transition_covariance_matrix_ref_ = transition_covariance_matrix;

    if (!observation_covariance_matrix.isSquare() ||
        observation_covariance_matrix.getRows() != dimension_of_observations_) {
      throw new MatrixDimensionMismatchException(observation_covariance_matrix.getRows(),
          observation_covariance_matrix.getColumns(),
          dimension_of_observations_,
          dimension_of_observations_);
    }
    observation_covariance_matrix_ref_ = observation_covariance_matrix;

    if (initial_state_mean.getRows() != dimension_of_states_ ||
        initial_state_mean.getColumns() != 1) {
      throw new MatrixDimensionMismatchException(initial_state_mean.getRows(),
          initial_state_mean.getColumns(),
          dimension_of_states_,
          1);
    }
    initial_state_mean_ref_ = initial_state_mean;


    if (!initial_state_covariance.isSquare() ||
        initial_state_covariance.getRows() != dimension_of_states_) {
      throw new MatrixDimensionMismatchException(initial_state_covariance.getRows(),
          initial_state_covariance.getColumns(),
          dimension_of_states_,
          dimension_of_states_);
    }
    initial_state_covariance_ref_ = initial_state_covariance;

    number_of_observations_ = observations.length;

    observations_ref_ = observations;

    estimated_state_means_ = new DoubleMatrix[number_of_observations_];
    estimated_state_covariances_ = new DoubleMatrix[number_of_observations_];
    predicted_observation_means_ = new DoubleMatrix[number_of_observations_];
    predicted_observation_covariances_ = new DoubleMatrix[number_of_observations_];

    estimated_means_ = new DoubleMatrix[number_of_observations_];
    estimated_covariances_ = new DoubleMatrix[number_of_observations_];

    updateFilteredStates();
    calculateLogLikelihood();
  }

  public void calculatePrediction(int steps_ahead) {
    if (steps_ahead > 0) {
      predicted_means_ = new DoubleMatrix[steps_ahead];
      predicted_variances_ = new DoubleMatrix[steps_ahead];

      DoubleMatrix previous_state_mean = estimated_state_means_[estimated_state_means_.length-1];
      DoubleMatrix previous_state_covariance = estimated_state_covariances_[estimated_state_covariances_.length-1];
      for (int ii=0; ii<steps_ahead; ii++) {
        DoubleMatrix a = state_transition_matrix_ref_.mmul(previous_state_mean);
        DoubleMatrix R = state_transition_matrix_ref_
            .mmul(previous_state_covariance)
            .mmul(state_transition_matrix_ref_.transpose())
            .addi(transition_covariance_matrix_ref_);
        // One-step-ahead observation prediction.
        predicted_means_[ii] = observation_matrix_ref_.mmul(a);
        predicted_variances_[ii] = observation_matrix_ref_
            .mmul(R)
            .mmul(observation_matrix_ref_.transpose())
            .addi(observation_covariance_matrix_ref_);
        previous_state_mean = a;
        previous_state_covariance = R;
      }
    } else {
      //smoothing
      predicted_means_ = predicted_observation_means_;
      predicted_variances_ = predicted_observation_covariances_;
    }

  }

  private void updateFilteredStates() {
    DoubleMatrix state_transition_matrix_T = state_transition_matrix_ref_.transpose();
    DoubleMatrix observation_matrix_T = observation_matrix_ref_.transpose();

    for (int ii = 0; ii < number_of_observations_; ii++) {
      DoubleMatrix previous_state_mean =
          (ii == 0) ? initial_state_mean_ref_ : estimated_state_means_[ii - 1];
      DoubleMatrix previous_state_covariance =
          (ii == 0) ? initial_state_covariance_ref_ : estimated_state_covariances_[ii - 1];

      // One-step-ahead state prediction. (i)
      DoubleMatrix a = state_transition_matrix_ref_.mmul(previous_state_mean);
      DoubleMatrix R = state_transition_matrix_ref_
          .mmul(previous_state_covariance)
          .mmul(state_transition_matrix_T)
          .addi(transition_covariance_matrix_ref_);
      // One-step-ahead observation prediction(ii)
      predicted_observation_means_[ii] = observation_matrix_ref_.mmul(a);

      predicted_observation_covariances_[ii] = observation_matrix_ref_
          .mmul(R)
          .mmul(observation_matrix_T)
          .addi(observation_covariance_matrix_ref_);

      //if current observation is null:
      if (observations_ref_[ii] == null) {
        estimated_state_means_[ii] = a;
        estimated_state_covariances_[ii] = R;
      } else {
        DoubleMatrix inv_predicted_observation_covariances = Solve.pinv(predicted_observation_covariances_[ii]);
        DoubleMatrix R_obs_mat_T_inv_pred_obs_cov = R
            .mmul(observation_matrix_T)
            .mmuli(inv_predicted_observation_covariances);
        // forward filtering states.(iii)
        estimated_state_means_[ii] = R_obs_mat_T_inv_pred_obs_cov
            .mmul(observations_ref_[ii].sub(predicted_observation_means_[ii]))
            .addi(a);

        estimated_state_covariances_[ii] = R
            .sub(R_obs_mat_T_inv_pred_obs_cov
                .mmul(observation_matrix_ref_)
                .mmuli(R)
                );
        estimated_means_[ii] = observation_matrix_ref_.mmul(estimated_state_means_[ii]);
        estimated_covariances_[ii] = observation_matrix_ref_
            .mmul(estimated_state_covariances_[ii])
            .mmul(observation_matrix_T)
            .addi(observation_covariance_matrix_ref_);
      }
    }
  }

  private void calculateLogLikelihood() {
    sum_log_likelihood_ = 0;
    for (int ii = 0; ii < number_of_observations_; ii++) {
      if (observations_ref_[ii] == null) {
        continue;
      }
      //System.out.println(ii);
      //System.out.println(predicted_observation_covariances_[ii]);
      MultivariateNormalDistribution dist = new MultivariateNormalDistribution(
          predicted_observation_means_[ii].getColumn(0).toArray(),
          predicted_observation_covariances_[ii].toArray2());
      sum_log_likelihood_ += Math.log(dist.density(observations_ref_[ii].getColumn(0).toArray()));
    }
  }

  // Getters
  public DoubleMatrix getStateNoiseMatrix() {
    return transition_covariance_matrix_ref_.dup();
  }

  public DoubleMatrix getObservationNoiseMatrix() {
    return observation_covariance_matrix_ref_.dup();
  }

  public DoubleMatrix[] getEstimatedStateMeans() {
    return estimated_state_means_.clone();
  }

  public DoubleMatrix[] getEstimatedMeans() {
    return estimated_means_.clone();
  }

  public DoubleMatrix[] getEstimatedCovariances() {
    return estimated_covariances_.clone();
  }

  public DoubleMatrix[] getEstimatedStateCovariances() {
    return estimated_state_covariances_.clone();
  }

  public DoubleMatrix[] getPredictedObservationMeans() {
    return predicted_observation_means_.clone();
  }

  public DoubleMatrix[] getPredictedObservationCovariance() {
    return predicted_observation_covariances_.clone();
  }

  public DoubleMatrix[] getPredictedMeans() {
    return predicted_means_.clone();
  }

  public DoubleMatrix[] getPredictedCovariance() {
    return predicted_variances_.clone();
  }

  public DoubleMatrix[] getTrainingSequence() {
    return observations_ref_.clone();
  }

  public double getLogLikeliHood() {
    return sum_log_likelihood_;
  }
}
