package com.linkedin.thirdeye.anomaly.lib.fanomaly;

import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.commons.math3.linear.NonSquareMatrixException;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.commons.math3.util.MathUtils;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;


public class InverseMatrix {
  public static DoubleMatrix inverse(DoubleMatrix matrix, double threshold)
      throws NullArgumentException, SingularMatrixException, NonSquareMatrixException {
    MathUtils.checkNotNull(matrix);

    if (!matrix.isSquare()) {
      throw new NonSquareMatrixException(matrix.getRows(), matrix.getRows());
    }

    return Solve.pinv(matrix);
  }
}

