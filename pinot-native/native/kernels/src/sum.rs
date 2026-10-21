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
//! Architecture coverage with runtime ISA dispatch:
//! * AVX-512DQ (x86_64) — native `vcvtqq2pd` for direct 8-wide i64→f64 conversion
//! * AVX2     (x86_64) — scalar i64→f64 conversion + 4-wide SIMD accumulation
//!                       (AVX2 lacks i64→f64 vector convert until AVX-512DQ)
//! * NEON     (aarch64) — native `scvtf.2d` for 2-wide i64→f64; 4-way unrolled
//! * scalar fallback   — 4-way unrolled for ILP, used when none of the above apply
//!
//! The dispatch function `sum_i64_to_f64` selects the best implementation at
//! call time. Detection results are cached by `std::arch` after the first call,
//! so the per-call dispatch cost is one atomic load.
//!
//! ## Java semantics
//!
//! Matches `SumAggregationFunction.aggregateSV(LONG)` closely but not bit-exactly.
//! Java does scalar left-to-right `s += v` accumulation; SIMD adds reorder the
//! reduction across multiple accumulator lanes. Results differ only in the last
//! ulp(s) when magnitudes exceed the f64 mantissa; for values within ±2^53 the
//! results are bit-identical. The Pinot differential tester allows
//! `|native - java| ≤ max(1.0, |java| × 1e-15)`.

/// Sums a slice of `i64` as `f64`, dispatching to the fastest available
/// implementation for the current host CPU.
#[inline]
pub fn sum_i64_to_f64(values: &[i64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512dq") {
            // SAFETY: feature was just detected at runtime.
            return unsafe { sum_i64_to_f64_avx512(values) };
        }
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: feature was just detected at runtime.
            return unsafe { sum_i64_to_f64_avx2(values) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: feature was just detected at runtime.
            return unsafe { sum_i64_to_f64_neon(values) };
        }
    }

    sum_i64_to_f64_scalar(values)
}

// --- scalar fallback --------------------------------------------------------

/// 4-way unrolled scalar accumulator. Used when SIMD features are unavailable
/// and as a reference for property-based equivalence testing.
#[inline]
fn sum_i64_to_f64_scalar(values: &[i64]) -> f64 {
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
    ((s0 + s1) + (s2 + s3)) + tail
}

// --- NEON (aarch64) ---------------------------------------------------------

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn sum_i64_to_f64_neon(values: &[i64]) -> f64 {
    use core::arch::aarch64::*;

    // Four independent SIMD accumulators, each 2× f64 wide → 8-wide ILP.
    let mut acc0 = vdupq_n_f64(0.0);
    let mut acc1 = vdupq_n_f64(0.0);
    let mut acc2 = vdupq_n_f64(0.0);
    let mut acc3 = vdupq_n_f64(0.0);

    let chunks = values.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr();
        // Each vld1q_s64 loads 2× i64 = 128 bits.
        let v0 = vld1q_s64(p);
        let v1 = vld1q_s64(p.add(2));
        let v2 = vld1q_s64(p.add(4));
        let v3 = vld1q_s64(p.add(6));

        // vcvtq_f64_s64 → `scvtf.2d`, native i64→f64 vector convert.
        let f0 = vcvtq_f64_s64(v0);
        let f1 = vcvtq_f64_s64(v1);
        let f2 = vcvtq_f64_s64(v2);
        let f3 = vcvtq_f64_s64(v3);

        acc0 = vaddq_f64(acc0, f0);
        acc1 = vaddq_f64(acc1, f1);
        acc2 = vaddq_f64(acc2, f2);
        acc3 = vaddq_f64(acc3, f3);
    }

    // Pairwise reduce, then sum-across-vector for the final scalar.
    let acc01 = vaddq_f64(acc0, acc1);
    let acc23 = vaddq_f64(acc2, acc3);
    let acc = vaddq_f64(acc01, acc23);
    let mut sum = vaddvq_f64(acc);

    for &v in remainder {
        sum += v as f64;
    }
    sum
}

// --- AVX2 (x86_64) ----------------------------------------------------------
//
// AVX2 does not have `vcvtqq2pd` (i64→f64 vector convert); that arrives in
// AVX-512DQ. The kernel below converts each i64 scalarly via `vcvtsi2sd`
// (one per lane), packs into 256-bit registers, then performs 4-wide SIMD
// accumulation with multiple independent chains for ILP. This is still
// meaningfully faster than the scalar baseline because the accumulation
// dominates, but the convert is the bottleneck.
//
// A faster alternative is the "magic constant" bit-trick (see Mysticial /
// CppCon talks) — XOR the sign bit, split high/low halves, reinterpret as
// f64 with biased exponents, then SIMD-add the corrections. We defer that
// optimization to Phase 1.B+ if AVX2 hardware benchmarks demand it.

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_i64_to_f64_avx2(values: &[i64]) -> f64 {
    use core::arch::x86_64::*;

    // Four independent 256-bit (4-lane f64) accumulators → 16-wide ILP.
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let mut acc2 = _mm256_setzero_pd();
    let mut acc3 = _mm256_setzero_pd();

    let chunks = values.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        // Scalar i64 → f64 conversion (no AVX2 vector instruction), packed
        // into __m256d via _mm256_set_pd. The compiler emits 16× vcvtsi2sd.
        let f0 =
            _mm256_set_pd(chunk[3] as f64, chunk[2] as f64, chunk[1] as f64, chunk[0] as f64);
        let f1 =
            _mm256_set_pd(chunk[7] as f64, chunk[6] as f64, chunk[5] as f64, chunk[4] as f64);
        let f2 = _mm256_set_pd(
            chunk[11] as f64,
            chunk[10] as f64,
            chunk[9] as f64,
            chunk[8] as f64,
        );
        let f3 = _mm256_set_pd(
            chunk[15] as f64,
            chunk[14] as f64,
            chunk[13] as f64,
            chunk[12] as f64,
        );

        acc0 = _mm256_add_pd(acc0, f0);
        acc1 = _mm256_add_pd(acc1, f1);
        acc2 = _mm256_add_pd(acc2, f2);
        acc3 = _mm256_add_pd(acc3, f3);
    }

    let acc01 = _mm256_add_pd(acc0, acc1);
    let acc23 = _mm256_add_pd(acc2, acc3);
    let acc = _mm256_add_pd(acc01, acc23);

    // Horizontal reduce: shuffle and add to collapse 4 lanes → 1 scalar.
    let lo = _mm256_castpd256_pd128(acc);
    let hi = _mm256_extractf128_pd(acc, 1);
    let s128 = _mm_add_pd(lo, hi);
    let high = _mm_unpackhi_pd(s128, s128);
    let s = _mm_add_sd(s128, high);
    let mut sum = _mm_cvtsd_f64(s);

    for &v in remainder {
        sum += v as f64;
    }
    sum
}

// --- AVX-512DQ (x86_64) -----------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512dq")]
unsafe fn sum_i64_to_f64_avx512(values: &[i64]) -> f64 {
    use core::arch::x86_64::*;

    // Four 512-bit accumulators (8 lanes each) = 32-wide ILP.
    let mut acc0 = _mm512_setzero_pd();
    let mut acc1 = _mm512_setzero_pd();
    let mut acc2 = _mm512_setzero_pd();
    let mut acc3 = _mm512_setzero_pd();

    let chunks = values.chunks_exact(32);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let p = chunk.as_ptr() as *const __m512i;
        let i0 = _mm512_loadu_si512(p);
        let i1 = _mm512_loadu_si512(p.add(1));
        let i2 = _mm512_loadu_si512(p.add(2));
        let i3 = _mm512_loadu_si512(p.add(3));

        // _mm512_cvtepi64_pd is the AVX-512DQ native i64→f64 vector convert.
        let f0 = _mm512_cvtepi64_pd(i0);
        let f1 = _mm512_cvtepi64_pd(i1);
        let f2 = _mm512_cvtepi64_pd(i2);
        let f3 = _mm512_cvtepi64_pd(i3);

        acc0 = _mm512_add_pd(acc0, f0);
        acc1 = _mm512_add_pd(acc1, f1);
        acc2 = _mm512_add_pd(acc2, f2);
        acc3 = _mm512_add_pd(acc3, f3);
    }

    let acc01 = _mm512_add_pd(acc0, acc1);
    let acc23 = _mm512_add_pd(acc2, acc3);
    let acc = _mm512_add_pd(acc01, acc23);
    let mut sum = _mm512_reduce_add_pd(acc);

    for &v in remainder {
        sum += v as f64;
    }
    sum
}

// --- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Tolerance used by the Pinot differential tester. Matches §10 of
    /// `docs/native/phase-1-design.md`.
    fn within_tolerance(a: f64, b: f64) -> bool {
        let tol = (a.abs().max(b.abs()) * 1e-15).max(1.0);
        (a - b).abs() <= tol
    }

    #[test]
    fn dispatch_empty_is_zero() {
        assert_eq!(sum_i64_to_f64(&[]), 0.0);
    }

    #[test]
    fn dispatch_small_range() {
        let values: Vec<i64> = (1..=100).collect();
        let expected = sum_i64_to_f64_scalar(&values);
        assert_eq!(sum_i64_to_f64(&values), expected);
    }

    #[test]
    fn dispatch_matches_scalar_random() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut state: u64 = 0xdead_beef_cafe_babe;
        let mut values = Vec::with_capacity(10_007);
        for _ in 0..values.capacity() {
            let mut h = DefaultHasher::new();
            state.hash(&mut h);
            state = h.finish();
            values.push((state as i64) % 1_000_000);
        }
        let scalar = sum_i64_to_f64_scalar(&values);
        let dispatched = sum_i64_to_f64(&values);
        assert!(
            within_tolerance(scalar, dispatched),
            "scalar={} dispatched={} diff={}",
            scalar,
            dispatched,
            (scalar - dispatched).abs()
        );
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_matches_scalar() {
        if !std::arch::is_aarch64_feature_detected!("neon") {
            return;
        }
        let values: Vec<i64> = (-50_000..50_000).collect();
        let scalar = sum_i64_to_f64_scalar(&values);
        let neon = unsafe { sum_i64_to_f64_neon(&values) };
        assert!(within_tolerance(scalar, neon),
            "scalar={} neon={} diff={}", scalar, neon, (scalar - neon).abs());
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx2_matches_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let values: Vec<i64> = (-50_000..50_000).collect();
        let scalar = sum_i64_to_f64_scalar(&values);
        let avx2 = unsafe { sum_i64_to_f64_avx2(&values) };
        assert!(within_tolerance(scalar, avx2),
            "scalar={} avx2={} diff={}", scalar, avx2, (scalar - avx2).abs());
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_matches_scalar() {
        if !std::is_x86_feature_detected!("avx512dq") {
            return;
        }
        let values: Vec<i64> = (-50_000..50_000).collect();
        let scalar = sum_i64_to_f64_scalar(&values);
        let avx512 = unsafe { sum_i64_to_f64_avx512(&values) };
        assert!(within_tolerance(scalar, avx512),
            "scalar={} avx512={} diff={}", scalar, avx512, (scalar - avx512).abs());
    }

    #[test]
    fn handles_negative_extreme_values() {
        let values = [-1_i64, -2, -3, i64::MIN + 1, 0, 1, i64::MAX - 1];
        let scalar = sum_i64_to_f64_scalar(&values);
        let dispatched = sum_i64_to_f64(&values);
        assert!(
            within_tolerance(scalar, dispatched),
            "scalar={} dispatched={}",
            scalar,
            dispatched
        );
    }

    #[test]
    fn tail_handling_short_input() {
        // Exercise inputs shorter than each SIMD chunk size.
        for len in 0..40 {
            let values: Vec<i64> = (0..len as i64).map(|i| i * 7).collect();
            let scalar = sum_i64_to_f64_scalar(&values);
            let dispatched = sum_i64_to_f64(&values);
            assert!(within_tolerance(scalar, dispatched),
                "len={} scalar={} dispatched={}", len, scalar, dispatched);
        }
    }
}
