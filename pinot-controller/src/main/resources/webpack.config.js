/*
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
const path = require("path");
const HtmlWebPackPlugin = require('html-webpack-plugin');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');


module.exports = (env, argv) => {
  const devMode = argv.mode === 'development';

  return {
    mode: !devMode ? 'production' : 'development',
    node: {
      fs: 'empty'
    },

    // Enable sourcemaps for debugging webpack's output.
    devtool: 'source-map',
    resolve: {
      // Add '.ts' and '.tsx' as resolvable extensions.
      extensions: ['.ts', '.tsx', '.js'],
      modules: ['./app', 'node_modules'],
    },
    entry: './app/index.tsx',
    output: {
      path: path.resolve(__dirname, 'dist/webapp'),
      filename: './js/main.js'
    },
    devServer: {
      compress: true,
      hot: true,
      proxy: [
        {
            context: "/",
            target: "http://localhost:9000",
            changeOrigin: true,
        },
      ],
    },
    module: {
      rules: [
        {
          test: /\.ts(x?)$/,
          exclude: /node_modules/,
          use: [
            {
              loader: 'ts-loader',
            },
          ],
        },
        // All output '.js' files will have any sourcemaps re-processed by 'source-map-loader'.
        {
          enforce: 'pre',
          test: /\.js$/,
          loader: 'source-map-loader',
        },
        {
          test: /\.html$/,
          use: ['html-loader'],
        },
        {
          test: /\.css$/i,
          use: ['style-loader', 'css-loader'],
        },
        {
          // Match woff2 in addition to patterns like .woff?v=1.1.1.
          test: /\.(woff|woff2)(\?v=\d+\.\d+\.\d+)?$/,
          use: {
            loader: 'url-loader',
            options: {
              // Limit at 50k. Above that it emits separate files
              limit: 50000,

              // url-loader sets mimetype if it's passed.
              // Without this it derives it from the file extension
              mimetype: 'application/font-woff',

              // Output below fonts directory
              name: './fonts/[name].[ext]',
            },
          },
        },
        {
          test: /\.csv$/,
          loader: 'csv-loader',
          options: {
            dynamicTyping: true,
            header: true,
            skipEmptyLines: true,
          },
        },
      ],
    },
    plugins: [
      new CleanWebpackPlugin(),
      new CopyWebpackPlugin([
        {
          from: path.join(__dirname, './favicon.ico'),
          to: 'images/favicon.ico'
        }
      ]),
      new HtmlWebPackPlugin({
        template: './app/index.html',
        meta: {
          viewport: 'width=device-width, initial-scale=1, shrink-to-fit=no',
          description: 'Pinot Controller UI',
        },
      }),
    ],
  };
};
