/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.readers.Vector;


/**
 * Inbuilt Vector Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * @ScalarFunction annotation is used with each method for the registration
 *
 * Example usage:
 */
public class VectorFunctions {
  private VectorFunctions() {
  }

  /**
   * Returns the cosine distance between two vectors
   * @param vector1 vector1
   * @param vector2 vector2
   * @return cosine distance
   */
  @ScalarFunction(names = {"cosinedistance", "cosine_distance"})
  public static double cosineDistance(float[] vector1, float[] vector2) {
    return cosineDistance(vector1, vector2, Double.NaN);
  }

  /**
   * Returns the cosine distance between two vectors, with a default value if the norm of either vector is 0.
   * @param vector1 vector1
   * @param vector2 vector2
   * @param defaultValue default value when either vector has a norm of 0
   * @return cosine distance
   */
  @ScalarFunction(names = {"cosinedistance", "cosine_distance"})
  public static double cosineDistance(float[] vector1, float[] vector2, double defaultValue) {
    validateVectors(vector1, vector2);
    double dotProduct = 0.0;
    double norm1 = 0.0;
    double norm2 = 0.0;
    for (int i = 0; i < vector1.length; i++) {
      dotProduct += vector1[i] * vector2[i];
      norm1 += Math.pow(vector1[i], 2);
      norm2 += Math.pow(vector2[i], 2);
    }
    if (norm1 == 0 || norm2 == 0) {
      return defaultValue;
    }
    return 1 - (dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2)));
  }

  /**
   * Returns the inner product between two vectors
   * @param vector1 vector1
   * @param vector2 vector2
   * @return inner product
   */
  @ScalarFunction(names = {"innerproduct", "inner_product"})
  public static double innerProduct(float[] vector1, float[] vector2) {
    validateVectors(vector1, vector2);
    double dotProduct = 0.0;
    for (int i = 0; i < vector1.length; i++) {
      dotProduct += vector1[i] * vector2[i];
    }
    return dotProduct;
  }

  /**
   * Returns the L2 distance between two vectors
   * @param vector1 vector1
   * @param vector2 vector2
   * @return L2 distance
   */
  @ScalarFunction(names = {"l2distance", "l2_distance"})
  public static double l2Distance(float[] vector1, float[] vector2) {
    validateVectors(vector1, vector2);
    double distance = 0.0;
    for (int i = 0; i < vector1.length; i++) {
      distance += Math.pow(vector1[i] - vector2[i], 2);
    }
    return Math.sqrt(distance);
  }

  /**
   * Returns the L1 distance between two vectors
   * @param vector1 vector1
   * @param vector2 vector2
   * @return L1 distance
   */
  @ScalarFunction(names = {"l1distance", "l1_distance"})
  public static double l1Distance(float[] vector1, float[] vector2) {
    validateVectors(vector1, vector2);
    double distance = 0.0;
    for (int i = 0; i < vector1.length; i++) {
      distance += Math.abs(vector1[i] - vector2[i]);
    }
    return distance;
  }

  /**
   * Returns the number of dimensions in a vector
   * @param vector input vector
   * @return number of dimensions
   */
  @ScalarFunction(names = {"vectordims", "vector_dims"})
  public static int vectorDims(float[] vector) {
    validateVector(vector);
    return vector.length;
  }

  /**
   * Returns the number of dimensions in a vector
   * @param vector input vector
   * @return number of dimensions
   */
  @ScalarFunction(names = {"vectordims_vec", "vector_dims_vec"})
  public static int vectorDims(Vector vector) {
    validateVector(vector);
    return vector.getDimension();
  }

  /**
   * Returns the norm of a vector
   * @param vector input vector
   * @return norm
   */
  @ScalarFunction(names = {"vectornorm", "vector_norm"})
  public static double vectorNorm(float[] vector) {
    validateVector(vector);
    double norm = 0.0;
    for (int i = 0; i < vector.length; i++) {
      norm += Math.pow(vector[i], 2);
    }
    return Math.sqrt(norm);
  }

  @ScalarFunction(names = {"vectornorm_vec", "vector_norm_vec"})
  public static double vectorNorm(Vector vector) {
    validateVector(vector);
    double norm = 0.0;
    switch (vector.getType()) {
      case FLOAT: {
        for (int i = 0; i < vector.getDimension(); i++) {
          norm += Math.pow(vector.getFloatValues()[i], 2);
        }
        break;
      }
      case INT: {
        for (int i = 0; i < vector.getDimension(); i++) {
          norm += Math.pow(vector.getIntValues()[i], 2);
        }
        break;
      }
    }
    return Math.sqrt(norm);
  }

  public static void validateVectors(float[] vector1, float[] vector2) {
    Preconditions.checkArgument(vector1 != null && vector2 != null, "Null vector passed");
    Preconditions.checkArgument(vector1.length == vector2.length, "Vector lengths do not match");
  }

  public static void validateVector(float[] vector) {
    Preconditions.checkArgument(vector != null, "Null vector passed");
    Preconditions.checkArgument(vector.length > 0, "Empty vector passed");
  }

  public static void validateVectors(Vector vector1, Vector vector2) {
    Preconditions.checkArgument(vector1 != null && vector2 != null, "Null vector passed");
    Preconditions.checkArgument(vector1.getDimension() == vector2.getDimension(), "Vector lengths do not match");
  }

  public static void validateVector(Vector vector) {
    Preconditions.checkArgument(vector != null, "Null vector passed");
    Preconditions.checkArgument(vector.getDimension() > 0, "Empty vector passed");
  }
  @ScalarFunction(names = {"cosinedistance_vec", "cosine_distance_vec"})
  public static double cosineDistance(Vector vector1, Vector vector2) {
    return cosineDistance(vector1, vector2, Double.NaN);
  }

  @ScalarFunction(names = {"cosinedistance_vec", "cosine_distance_vec"})
  public static double cosineDistance(Vector vector1, Vector vector2, double defaultValue) {
    if (vector1.getType() != Vector.VectorType.FLOAT || vector2.getType() != Vector.VectorType.FLOAT) {
      throw new IllegalArgumentException("Vectors must be of FLOAT type.");
    }

    float[] v1 = vector1.getFloatValues();
    float[] v2 = vector2.getFloatValues();
    validateVectors(v1, v2);

    double dotProduct = 0.0;
    double norm1 = 0.0;
    double norm2 = 0.0;
    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
      norm1 += Math.pow(v1[i], 2);
      norm2 += Math.pow(v2[i], 2);
    }
    if (norm1 == 0 || norm2 == 0) {
      return defaultValue;
    }
    return 1 - (dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2)));
  }

  @ScalarFunction(names = {"innerproduct_vec", "inner_product_vec"})
  public static double innerProduct(Vector vector1, Vector vector2) {
    if (vector1.getType() != Vector.VectorType.FLOAT || vector2.getType() != Vector.VectorType.FLOAT) {
      throw new IllegalArgumentException("Vectors must be of FLOAT type.");
    }

    float[] v1 = vector1.getFloatValues();
    float[] v2 = vector2.getFloatValues();
    validateVectors(v1, v2);

    double dotProduct = 0.0;
    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
    }
    return dotProduct;
  }

  @ScalarFunction(names = {"l2distance_vec", "l2_distance_vec"})
  public static double l2Distance(Vector vector1, Vector vector2) {
    if (vector1.getType() != Vector.VectorType.FLOAT || vector2.getType() != Vector.VectorType.FLOAT) {
      throw new IllegalArgumentException("Vectors must be of FLOAT type.");
    }

    float[] v1 = vector1.getFloatValues();
    float[] v2 = vector2.getFloatValues();
    validateVectors(v1, v2);

    double distance = 0.0;
    for (int i = 0; i < v1.length; i++) {
      distance += Math.pow(v1[i] - v2[i], 2);
    }
    return Math.sqrt(distance);
  }

  @ScalarFunction(names = {"l1distance_vec", "l1_distance_vec"})
  public static double l1Distance(Vector vector1, Vector vector2) {
    if (vector1.getType() != Vector.VectorType.FLOAT || vector2.getType() != Vector.VectorType.FLOAT) {
      throw new IllegalArgumentException("Vectors must be of FLOAT type.");
    }

    float[] v1 = vector1.getFloatValues();
    float[] v2 = vector2.getFloatValues();
    validateVectors(v1, v2);

    double distance = 0.0;
    for (int i = 0; i < v1.length; i++) {
      distance += Math.abs(v1[i] - v2[i]);
    }
    return distance;
  }

}
