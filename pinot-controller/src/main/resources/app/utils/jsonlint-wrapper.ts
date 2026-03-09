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
 * Wrapper for jsonlint to handle CommonJS compatibility issues with Vite.
 * The jsonlint package uses CommonJS require() which doesn't work in browser ESM context.
 * This wrapper provides a safe import and exposes the parser for CodeMirror's json-lint addon.
 */

// Import the jsonlint module - Vite will handle the CJS transformation
import jsonlintModule from 'jsonlint';

// The jsonlint module exports { parser, parse, main }
// We only need the parser for CodeMirror's json-lint addon
// Extract the parser - it's the actual jsonlint parser object
const jsonlint = (jsonlintModule as any)?.parser || jsonlintModule;

// Export for use in the application
export default jsonlint;

// Set it globally for CodeMirror's json-lint addon
// CodeMirror expects window.jsonlint to be the parser object with a parse() method
if (typeof window !== 'undefined') {
  (window as any).jsonlint = jsonlint;
}
