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

//! JNI bindings for Pinot's native aggregation engine.
//!
//! All exported functions follow the naming convention required by JNI:
//! `Java_<fully_qualified_class_with_underscores>_<methodName>`.
//!
//! Invariants enforced at this layer:
//! * `panic::catch_unwind` around every Rust call. A Rust panic must never
//!   unwind into the JVM.
//! * Returned scalar errors are encoded as the function's "null" sentinel
//!   (NaN for f64, 0 for integer return types) — the Java side checks for
//!   these explicitly. For richer error handling we will introduce an
//!   error-code parameter in Phase 1.B.

use std::panic::{self, AssertUnwindSafe};

use jni::objects::{JClass, JLongArray, ReleaseMode};
use jni::sys::{jdouble, jint};
use jni::JNIEnv;

use pinot_native_kernels::sum;

/// SUM aggregation over a `long[]` materialized by the Java caller.
///
/// The caller is expected to pass an array that has already been pinned to
/// a Java primitive array; we re-pin it here with `GetPrimitiveArrayCritical`
/// to obtain a zero-copy view for the duration of the call.
///
/// # Safety
/// * `values` must be a valid Java `long[]` reference.
/// * `length` must be `<= values.length` (caller guarantees).
/// * The caller must not invoke any JNI methods that allocate or block while
///   this function holds the critical pin.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_sumLong(
    mut env: JNIEnv,
    _class: JClass,
    values: JLongArray,
    length: jint,
) -> jdouble {
    let result = panic::catch_unwind(AssertUnwindSafe(|| -> jdouble {
        if length <= 0 {
            return 0.0;
        }
        // SAFETY: We hold the critical pin for the duration of the kernel
        // call; no other JNI calls are made in between. The slice we cast
        // is valid for the lifetime of `auto`.
        let auto = match unsafe { env.get_array_elements_critical(&values, ReleaseMode::NoCopyBack) }
        {
            Ok(a) => a,
            Err(_) => return f64::NAN,
        };
        let len_usize = length as usize;
        let array_len = auto.len();
        let effective = if len_usize > array_len {
            array_len
        } else {
            len_usize
        };
        // SAFETY: auto.as_ptr() points to a contiguous `jlong` (i64) region
        // of `array_len` elements; `effective <= array_len`.
        let slice: &[i64] =
            unsafe { std::slice::from_raw_parts(auto.as_ptr() as *const i64, effective) };
        sum::sum_i64_to_f64(slice)
    }));
    match result {
        Ok(v) => v,
        Err(_) => f64::NAN,
    }
}

/// Probe function. Returns a known value so the Java side can verify the
/// native library is loaded and the JNI symbol resolution works before any
/// real kernel is exercised.
#[no_mangle]
pub extern "system" fn Java_org_apache_pinot_nativeengine_agg_PinotNativeAgg_probe(
    _env: JNIEnv,
    _class: JClass,
) -> jint {
    0x5049_4E4F // 'PINO' — magic number for self-test
}
