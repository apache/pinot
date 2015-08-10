package com.linkedin.thirdeye.anomaly.lib.fanomaly;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.MatrixDimensionMismatchException;
import org.jblas.DoubleMatrix;

public class StateSpaceModel {
  // The matrices describing a state space model.
  private DoubleMatrix state_transition_matrix_;
  private DoubleMatrix observation_matrix_;
  private DoubleMatrix transition_covariance_matrix_;
  private DoubleMatrix observation_covariance_matrix_;
  private DoubleMatrix initial_state_mean_;
  private DoubleMatrix initial_state_covariance_;

  // Dimensions.
  private int dimension_of_states_;
  private int dimension_of_observations_;
  private int number_of_observations_;

  // Observations.
  private DoubleMatrix[] observations_;
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
    observation_matrix_ = observation_matrix.dup();

    if (!state_transition_matrix.isSquare() ||
        state_transition_matrix.getRows() != dimension_of_states_) {
      throw new MatrixDimensionMismatchException(state_transition_matrix.getRows(),
          state_transition_matrix.getColumns(),
          dimension_of_states_,
          dimension_of_states_);
    }
    state_transition_matrix_ = state_transition_matrix.dup();


    if (!transition_covariance_matrix.isSquare() ||
        transition_covariance_matrix.getRows() != dimension_of_states_) {
      throw new MatrixDimensionMismatchException(transition_covariance_matrix.getRows(),
          transition_covariance_matrix.getColumns(),
          dimension_of_states_,
          dimension_of_states_);
    }
    transition_covariance_matrix_ = transition_covariance_matrix.dup();

    if (!observation_covariance_matrix.isSquare() ||
        observation_covariance_matrix.getRows() != dimension_of_observations_) {
      throw new MatrixDimensionMismatchException(observation_covariance_matrix.getRows(),
          observation_covariance_matrix.getColumns(),
          dimension_of_observations_,
          dimension_of_observations_);
    }
    observation_covariance_matrix_ = observation_covariance_matrix.dup();

    if (initial_state_mean.getRows() != dimension_of_states_ ||
        initial_state_mean.getColumns() != 1) {
      throw new MatrixDimensionMismatchException(initial_state_mean.getRows(),
          initial_state_mean.getColumns(),
          dimension_of_states_,
          1);
    }
    initial_state_mean_ = initial_state_mean.dup();


    if (!initial_state_covariance.isSquare() ||
        initial_state_covariance.getRows() != dimension_of_states_) {
      throw new MatrixDimensionMismatchException(initial_state_covariance.getRows(),
          initial_state_covariance.getColumns(),
          dimension_of_states_,
          dimension_of_states_);
    }
    initial_state_covariance_ = initial_state_covariance.dup();

    number_of_observations_ = observations.length;

    observations_ = observations.clone();

    estimated_state_means_ = new DoubleMatrix[number_of_observations_];
    estimated_state_covariances_ = new DoubleMatrix[number_of_observations_];
    predicted_observation_means_ = new DoubleMatrix[number_of_observations_];
    predicted_observation_covariances_ = new DoubleMatrix[number_of_observations_];

    estimated_means_ = new DoubleMatrix[number_of_observations_];
    estimated_covariances_ = new DoubleMatrix[number_of_observations_];

    UpdateFilteredStates();
    CalculateLogLikelihood();
  }

  public void CalculatePrediction(int steps_ahead) {
    if (steps_ahead > 0) {
      predicted_means_ = new DoubleMatrix[steps_ahead];
      predicted_variances_ = new DoubleMatrix[steps_ahead];

      DoubleMatrix previous_state_mean = estimated_state_means_[estimated_state_means_.length-1];
      DoubleMatrix previous_state_covariance = estimated_state_covariances_[estimated_state_covariances_.length-1];
      for (int ii=0; ii<steps_ahead; ii++) {
        DoubleMatrix a = state_transition_matrix_.mmul(previous_state_mean);
        DoubleMatrix R = state_transition_matrix_
            .mmul(previous_state_covariance)
            .mmul(state_transition_matrix_.transpose())
            .addi(transition_covariance_matrix_);
        // One-step-ahead observation prediction.
        predicted_means_[ii] = observation_matrix_.mmul(a);
        predicted_variances_[ii] = observation_matrix_
            .mmul(R)
            .mmul(observation_matrix_.transpose())
            .addi(observation_covariance_matrix_);
        previous_state_mean = a;
        previous_state_covariance = R;
      }
    } else {
      //smoothing
      predicted_means_ = predicted_observation_means_;
      predicted_variances_ = predicted_observation_covariances_;
    }

  }

  private void UpdateFilteredStates() {
    for (int ii = 0; ii < number_of_observations_; ii++) {
      DoubleMatrix previous_state_mean =
          (ii == 0) ? initial_state_mean_ : estimated_state_means_[ii - 1];
      DoubleMatrix previous_state_covariance =
          (ii == 0) ? initial_state_covariance_ : estimated_state_covariances_[ii - 1];

      // One-step-ahead state prediction. (i)
      DoubleMatrix a = state_transition_matrix_.mmul(previous_state_mean);
      DoubleMatrix R = state_transition_matrix_
          .mmul(previous_state_covariance)
          .mmul(state_transition_matrix_.transpose())
          .addi(transition_covariance_matrix_);
      // One-step-ahead observation prediction(ii)
      predicted_observation_means_[ii] = observation_matrix_.mmul(a);

      predicted_observation_covariances_[ii] = observation_matrix_
          .mmul(R)
          .mmul(observation_matrix_.transpose())
          .addi(observation_covariance_matrix_);

      //if current observation is null:
      if (observations_[ii] == null) {
        estimated_state_means_[ii] = a;
        estimated_state_covariances_[ii] = R;
      } else {
        DoubleMatrix inv_predicted_observation_covariances = InverseMatrix.inverse(
            predicted_observation_covariances_[ii], 0);

        // forward filtering states.(iii)
        estimated_state_means_[ii] = R
            .mmul(observation_matrix_.transpose())
            .mmuli(inv_predicted_observation_covariances)
            .mmul(observations_[ii].sub(predicted_observation_means_[ii]))
            .addi(a);

        estimated_state_covariances_[ii] = R
            .sub(R
                .mmul(observation_matrix_.transpose())
                .mmuli(inv_predicted_observation_covariances)
                .mmul(observation_matrix_)
                .mmuli(R)
                );
        estimated_means_[ii] = observation_matrix_.mmul(estimated_state_means_[ii]);
        estimated_covariances_[ii] = observation_matrix_
            .mmul(estimated_state_covariances_[ii])
            .mmul(observation_matrix_.transpose())
            .addi(observation_covariance_matrix_);
      }
    }
  }

  private void CalculateLogLikelihood() {
    sum_log_likelihood_ = 0;
    for (int ii = 0; ii < number_of_observations_; ii++) {
      if (observations_[ii] == null) {
        continue;
      }
      //System.out.println(ii);
      //System.out.println(predicted_observation_covariances_[ii]);
      MultivariateNormalDistribution dist = new MultivariateNormalDistribution(
          predicted_observation_means_[ii].getColumn(0).toArray(),
          predicted_observation_covariances_[ii].toArray2());
      sum_log_likelihood_ += Math.log(dist.density(observations_[ii].getColumn(0).toArray()));
    }
  }

  // Getters
  public DoubleMatrix GetStateNoiseMatrix() {
    return transition_covariance_matrix_.dup();
  }

  public DoubleMatrix GetObservationNoiseMatrix() {
    return observation_covariance_matrix_.dup();
  }

  public DoubleMatrix[] GetEstimatedStateMeans() {
    return estimated_state_means_.clone();
  }

  public DoubleMatrix[] GetEstimatedMeans() {
    return estimated_means_.clone();
  }

  public DoubleMatrix[] GetEstimatedCovariances() {
    return estimated_covariances_.clone();
  }

  public DoubleMatrix[] GetEstimatedStateCovariances() {
    return estimated_state_covariances_.clone();
  }

  public DoubleMatrix[] GetPredictedObservationMeans() {
    return predicted_observation_means_.clone();
  }

  public DoubleMatrix[] GetPredictedObservationCovariance() {
    return predicted_observation_covariances_.clone();
  }

  public DoubleMatrix[] GetPredictedMeans() {
    return predicted_means_.clone();
  }

  public DoubleMatrix[] GetPredictedCovariance() {
    return predicted_variances_.clone();
  }

  public DoubleMatrix[] GetTrainingSequence() {
    return observations_.clone();
  }

  public double GetLogLikeliHood() {
    return sum_log_likelihood_;
  }
}
