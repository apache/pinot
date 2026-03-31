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

/**
 * Suppress known third-party library warnings
 * This file is imported early in the application lifecycle to suppress
 * console warnings from third-party libraries that are known and expected.
 */

const isDevelopment = process.env.NODE_ENV === 'development';

if (isDevelopment) {
  const originalWarn = console.warn;

  // Keep the suppression list narrow and dev-only so production warnings remain visible.
  const suppressedWarnings = [
    /findDOMNode is deprecated/,
    /componentWillReceiveProps has been renamed/,
    /componentWillMount has been renamed/,
    /Legacy context API has been detected/,
  ];

  console.warn = (...args: any[]) => {
    const message = args.join(' ');
    const shouldSuppress = suppressedWarnings.some((pattern) => pattern.test(message));

    if (!shouldSuppress) {
      originalWarn.apply(console, args);
    }
  };
}
