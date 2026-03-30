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
 * Browser-safe JSON lint adapter for CodeMirror.
 * CodeMirror's json-lint addon only needs a parser object with `parse()` and
 * `parseError()` hooks, so we can avoid bundling the Node-oriented jsonlint package.
 */

type JsonLintLocation = {
  first_line: number;
  first_column: number;
  last_line: number;
  last_column: number;
};

type JsonLintHash = {
  loc: JsonLintLocation;
};

type JsonLintParser = {
  parseError?: (message: string, hash: JsonLintHash) => void;
  parse: (text: string) => unknown;
};

const getErrorIndex = (message: string) => {
  const positionMatch = message.match(/position (\d+)/i);
  if (positionMatch) {
    return Number(positionMatch[1]);
  }

  return 0;
};

const getLocationFromIndex = (text: string, index: number): JsonLintLocation => {
  const boundedIndex = Math.max(0, Math.min(index, text.length));
  let line = 1;
  let column = 0;

  for (let cursor = 0; cursor < boundedIndex; cursor += 1) {
    if (text[cursor] === '\n') {
      line += 1;
      column = 0;
    } else {
      column += 1;
    }
  }

  return {
    first_line: line,
    first_column: column,
    last_line: line,
    last_column: column + 1,
  };
};

const jsonlint: JsonLintParser = {
  parse(text: string) {
    try {
      return JSON.parse(text);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const hash = {
        loc: getLocationFromIndex(text, getErrorIndex(message)),
      };

      if (typeof jsonlint.parseError === 'function') {
        jsonlint.parseError(message, hash);
        return null;
      }

      throw error;
    }
  },
};

export default jsonlint;

if (typeof window !== 'undefined') {
  (window as any).jsonlint = jsonlint;
}
