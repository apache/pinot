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

import axios from 'axios';
import { AuthWorkflow } from 'Models';
import app_state from '../app_state';
import { AxiosError, AxiosRequestConfig } from "axios";

/**
 * Returns axios request interceptor.
 *
 * When using SESSION workflow, no Authorization header is attached.
 * The browser automatically sends the HttpOnly session cookie with every request.
 * This eliminates the "Authorization: Basic <base64-credentials>" visible in browser
 * dev-tools network tab.
 *
 * Flow:
 *   NONE     → no auth header, no cookie (open access)
 *   BASIC    → Authorization: Basic <token> header (legacy)
 *   SESSION  → no Authorization header; browser sends HttpOnly cookie automatically
 *   OIDC     → Authorization: Bearer <token> header
 */
export const getAxiosRequestInterceptor = (
    accessToken?: string
): ((requestConfig: AxiosRequestConfig) => AxiosRequestConfig) => {
    const requestInterceptor = (
        requestConfig: AxiosRequestConfig
    ): AxiosRequestConfig => {
        // SESSION workflow: rely entirely on the HttpOnly cookie set by POST /auth/login.
        // Do NOT attach any Authorization header.
        if (app_state.authWorkflow === AuthWorkflow.SESSION) {
            if (requestConfig.headers) {
                delete requestConfig.headers['Authorization'];
                delete requestConfig.headers['authorization'];
            }
            return requestConfig;
        }

        // BASIC auth: attach the stored token as Authorization header (legacy behaviour)
        if (app_state.authWorkflow === AuthWorkflow.BASIC && app_state.authToken) {
            requestConfig.headers = {
                ...requestConfig.headers,
                Authorization: app_state.authToken,
            };
        }

        // OIDC auth: attach the Bearer access token
        if (accessToken) {
            requestConfig.headers = {
                ...requestConfig.headers,
                Authorization: accessToken,
            };
        }

        return requestConfig;
    };

    return requestInterceptor;
};

/**
 * Returns axios rejected response interceptor.
 *
 * For SESSION workflow, 401/403 responses redirect to the login page so the user
 * must re-authenticate. For other workflows, the provided callback is invoked.
 */
export const getAxiosErrorInterceptor = (
    unauthenticatedAccessFn?: () => void
): ((error: AxiosError) => void) => {
    const rejectedResponseInterceptor = (error: AxiosError): any => {
        if (error && error.response && (error.response.status === 401 || error.response.status === 403)) {
            // SESSION workflow: session has expired. Redirect to login.
            if (app_state.authWorkflow === AuthWorkflow.SESSION) {
                app_state.authToken = null;
                if (window.location.hash && !window.location.hash.includes('/login')) {
                    window.location.href = '/#/login';
                } else if (!window.location.hash) {
                    window.location.href = '/#/login';
                }
                return error.response || error;
            }
            // Other workflows: invoke the callback
            unauthenticatedAccessFn && unauthenticatedAccessFn();
        }

        return error.response || error;
    };

    return rejectedResponseInterceptor;
};

// Returns axios fulfilled response interceptor
export const getAxiosResponseInterceptor = (): (<T>(
    response: T
) => T | Promise<T>) => {
    const fulfilledResponseInterceptor = <T>(response: T): T | Promise<T> => {
        // Forward the fulfilled response
        return response;
    };

    return fulfilledResponseInterceptor;
};

export const baseApi = axios.create({ baseURL: location.pathname });
baseApi.interceptors.request.use(getAxiosRequestInterceptor(), getAxiosErrorInterceptor());
baseApi.interceptors.response.use(getAxiosResponseInterceptor(), getAxiosErrorInterceptor());

export const transformApi = axios.create({baseURL: location.pathname, transformResponse: [data => data]});
transformApi.interceptors.request.use(getAxiosRequestInterceptor(), getAxiosErrorInterceptor());
transformApi.interceptors.response.use(getAxiosResponseInterceptor(), getAxiosErrorInterceptor());

// baseApi axios instance does not throw an error when API fails hence the control will never go to catch block
// changing the handleError method of baseApi will cause current UI to break (as UI might have not handle error properly)
// creating a new axios instance baseApiWithErrors which can be used when adding new API's
// NOTE: It is an add-on utility and can be used in case you want to handle/show UI when API fails.
export const baseApiWithErrors = axios.create({ baseURL: location.pathname });
baseApiWithErrors.interceptors.request.use(getAxiosRequestInterceptor());
baseApiWithErrors.interceptors.response.use(getAxiosResponseInterceptor());
