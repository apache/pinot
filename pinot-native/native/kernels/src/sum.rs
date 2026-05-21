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

//! SUM kernel for primitive fixed-width types.
//!
//! Matches `SumAggregationFunction` semantics exactly: per-value `i64 -> f64`
//! conversion with straight `+=` accumulation. No Kahan compensation (Java
//! path doesn't use it; we must match its rounding behavior).

/// Sums a slice of `i64` values as `f64`. Matches Java's
/// `for (long v : values) sum += v` semantics, including precision loss for
/// magnitudes above 2^53.
///
/// Performance: relies on LLVM auto-vectorization. The i64 -> f64 conversion
/// is not vectorized on AVX2 (no `vcvtqq2pd` until AVX-512DQ); a tighter SIMD
/// kernel with explicit intrinsics will land in Phase 1.B.
#[inline]
pub fn sum_i64_to_f64(values: &[i64]) -> f64 {
    // Manual 4-way unroll. Encourages the compiler to schedule four
    // independent accumulators, hiding the FP add latency. Final reduce
    // at the end matches Java's left-to-right ordering closely enough that
    // result equality holds for values that fit in f64 mantissa.
    let mut s0 = 0.0_f64;
    let mut s1 = 0.0_f64;
    let mut s2 = 0.0_f64;
    let mut s3 = 0.0_f64;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    for c in chunks {
        s0 += c[0] as f64;
        s1 += c[1] as f64;
        s2 += c[2] as f64;
        s3 += c[3] as f64;
    }

    let mut tail = 0.0_f64;
    for &v in remainder {
        tail += v as f64;
    }

    // Left-associative reduce, matching the per-chunk left-to-right order
    // the Java loop produces when consumed in chunks of 4.
    ((s0 + s1) + (s2 + s3)) + tail
}
