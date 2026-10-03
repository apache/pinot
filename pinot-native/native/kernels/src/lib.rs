// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Pinot native aggregation kernels.
//!
//! Pure Rust kernels with no JNI dependency, exercised by the `ffi` crate
//! and by Rust-side unit / property tests.

pub mod sum;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_i64_to_f64_empty_is_zero() {
        assert_eq!(sum::sum_i64_to_f64(&[]), 0.0);
    }

    #[test]
    fn sum_i64_to_f64_matches_java_semantics_on_small_input() {
        let values: Vec<i64> = (1..=100).collect();
        let expected: f64 = (1..=100).map(|v| v as f64).sum();
        assert_eq!(sum::sum_i64_to_f64(&values), expected);
    }

    #[test]
    fn sum_i64_to_f64_negative_values() {
        let values = [-1_i64, -2, -3, i64::MIN + 1, 0, 1];
        let expected: f64 = values.iter().map(|&v| v as f64).sum();
        assert_eq!(sum::sum_i64_to_f64(&values), expected);
    }
}
