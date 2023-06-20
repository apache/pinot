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

import { AuthLocalStorageKeys, AuthWorkflow } from 'Models';
import React, { createContext, useContext, useEffect, useState } from 'react'
import { useHistory, useLocation } from 'react-router';
import { baseApi, getAxiosErrorInterceptor, getAxiosRequestInterceptor, getAxiosResponseInterceptor, transformApi } from '../../utils/axios-config';
import PinotMethodUtils from '../../utils/PinotMethodUtils';
import { AppLoadingIndicator } from '../AppLoadingIndicator';

interface AuthProviderContextProps {
    accessToken: string;
    authUserName: string;
    authUserEmail: string;
    authenticated: boolean;
    authWorkflow: AuthWorkflow
}

export const AuthProvider = ({ children }) => {
    const [loading, setLoading] = useState<boolean>(true);
    const [redirectUri, setRedirectUri] = useState<string | null>(null);
    const [clientId, setClientId] = useState<string | null>(null);
    const [authWorkflow, setAuthWorkflow] = useState<AuthWorkflow | null>(null);
    const [accessToken, setAccessToken] = useState<string>("");
    const [authUserName, setAuthUserName] = useState<string>("");
    const [authUserEmail, setAuthUserEmail] = useState<string>("");
    const [authenticated, setAuthenticated] = useState<boolean>(false);
    const [authorizationEndpoint, setAuthorizationEndpoint] = useState<string | null>(null);
    const [autoLogout, setAutoLogout] = useState<boolean>(false);
    const history = useHistory();
    const location = useLocation();
    const [axiosRequestInterceptorIds, setAxiosRequestInterceptorIds] =
        useState([0, 1]);
    const [axiosResponseInterceptorIds, setAxiosResponseInterceptorIds] =
        useState([0, 1]);
    const oidcSignInFormRef = React.useRef<HTMLFormElement>(null);

    useEffect(() => {
        initAuthDetails();
    }, []);

    useEffect(() => {
        if (loading || authenticated) {
            return;
        }

        initOidcAuth();
    }, [loading, authenticated]);

    useEffect(() => {
        if (!autoLogout) {
            return;
        }

        submitLoginForm();
    }, [autoLogout])

    const initAuthDetails = async () => {
        // fetch auth info details
        const authInfoResponse = await PinotMethodUtils.getAuthInfo();

        const authWorkFlowInternal =
            authInfoResponse && authInfoResponse.workflow
                ? authInfoResponse.workflow
                : AuthWorkflow.NONE;

        // set auth workflow
        setAuthWorkflow(authWorkFlowInternal);

        if (authWorkFlowInternal === AuthWorkflow.NONE) {
            // No authentication required
            setAuthenticated(true);
        }

        if (authWorkFlowInternal === AuthWorkflow.BASIC) {
            // basic auth is handled by login page
        }

        // set OIDC auth details 
        if (authWorkFlowInternal === AuthWorkflow.OIDC) {
            const issuer =
                authInfoResponse && authInfoResponse.issuer ? authInfoResponse.issuer : '';

            setAuthorizationEndpoint(`${issuer}/auth`);
            setRedirectUri(
                authInfoResponse && authInfoResponse.redirectUri
                    ? authInfoResponse.redirectUri
                    : ''
            );
            setClientId(
                authInfoResponse && authInfoResponse.clientId
                    ? authInfoResponse.clientId
                    : ''
            );
        }

        // auth loading complete
        setLoading(false);
    }

    const initOidcAuth = () => {
        // access token already available in the localStorage
        const accessToken = getAuthLocalStorageValue(AuthLocalStorageKeys.AccessToken);
        if (accessToken) {
            setAccessToken(accessToken);
            setAuthUserName(PinotMethodUtils.getAuthUserNameFromAccessToken(accessToken.replace("Bearer ", "")))
            setAuthUserEmail(PinotMethodUtils.getAuthUserEmailFromAccessToken(accessToken.replace("Bearer ", "")))
            
            initAxios(accessToken);
            setAuthenticated(true);

            return;
        }

        // access token available in hash params
        const accessTokenFromHashParam = PinotMethodUtils.getAccessTokenFromHashParams();
        if (accessTokenFromHashParam) {
            const accessToken = `Bearer ${accessTokenFromHashParam}`;
            setAccessToken(accessToken);
            setAuthUserName(PinotMethodUtils.getAuthUserNameFromAccessToken(accessTokenFromHashParam))
            setAuthUserEmail(PinotMethodUtils.getAuthUserEmailFromAccessToken(accessTokenFromHashParam))

            setAuthLocalStorageValue(AuthLocalStorageKeys.AccessToken, accessToken);
            initAxios(accessToken);
            setAuthenticated(true);
            redirectToApp();

            return;
        }

        // no access token available
        const redirectPathAfterLogin = location.pathname;
        // save current path to redirect after login
        setAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation, redirectPathAfterLogin);
        // login
        submitLoginForm();
    }

    const submitLoginForm = () => {
        // submit auth login form
        if (clientId && authorizationEndpoint && redirectUri && oidcSignInFormRef && oidcSignInFormRef.current) {
            oidcSignInFormRef.current.submit();
        }
    }

    const handleUnauthenticatedAccess = () => {
        setAuthLocalStorageValue(AuthLocalStorageKeys.AccessToken, "");
        setAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation, "");

        setAutoLogout(true);
    }

    // initialize axios instance with authToken
    const initAxios = (accessToken: string) => {

        // Clear existing interceptors
        baseApi.interceptors.request.eject(axiosRequestInterceptorIds[0]);
        baseApi.interceptors.response.eject(axiosResponseInterceptorIds[0]);

        transformApi.interceptors.request.eject(axiosRequestInterceptorIds[1]);
        transformApi.interceptors.response.eject(axiosResponseInterceptorIds[1]);

        const requestInterceptorId1 = baseApi.interceptors.request.use(
            getAxiosRequestInterceptor(accessToken),
            getAxiosErrorInterceptor(handleUnauthenticatedAccess)
        );

        const requestInterceptorId2 = transformApi.interceptors.request.use(
            getAxiosRequestInterceptor(accessToken),
            getAxiosErrorInterceptor(handleUnauthenticatedAccess)
        );

        const responseInterceptor1 = baseApi.interceptors.response.use(
            getAxiosResponseInterceptor(),
            getAxiosErrorInterceptor(handleUnauthenticatedAccess)
        );

        const responseInterceptor2 = transformApi.interceptors.response.use(
            getAxiosResponseInterceptor(),
            getAxiosErrorInterceptor(handleUnauthenticatedAccess)
        );

        // Set new interceptors
        setAxiosRequestInterceptorIds([requestInterceptorId1, requestInterceptorId2]);
        setAxiosResponseInterceptorIds([responseInterceptor1, responseInterceptor2]);
    }

    // redirect to app with appropriate location after login
    const redirectToApp = () => {
        const redirectLocation = getAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation);
        if (redirectLocation && redirectLocation !== "/login" && redirectLocation !== "/logout") {
            setAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation, "");
            history.push(redirectLocation);
        }
    }

    const getAuthLocalStorageValue = (key: AuthLocalStorageKeys) => {
        return (localStorage.getItem(key) || "");
    }

    const setAuthLocalStorageValue = (key: AuthLocalStorageKeys, value: string) => {
        localStorage.setItem(key, value);
    }

    const authProvider: AuthProviderContextProps = {
        authWorkflow: authWorkflow,
        accessToken: accessToken,
        authUserName: authUserName,
        authUserEmail: authUserEmail,
        authenticated: authenticated
    }

    if (loading) {
        return <AppLoadingIndicator />
    }


    return (
        <AuthProviderContext.Provider value={authProvider}>
            {children}

            {authWorkflow === AuthWorkflow.OIDC && (<div>
                {/* Login form */}
                <form
                    hidden
                    // When the user is automatically logged out, attaching automatic-logout=true
                    // query parameter displays appropriate message to the user (dex only)
                    action={`${authorizationEndpoint && authorizationEndpoint
                        }?automatic-logout=${autoLogout}`
                    }
                    method="post"
                    ref={oidcSignInFormRef}
                >
                    <input
                        readOnly
                        name="response_type"
                        value="token id_token"
                    />
                    <input readOnly name="client_id" value={clientId} />
                    <input
                        readOnly
                        name="redirect_uri"
                        value={redirectUri}
                    />
                    <input
                        readOnly
                        name="scope"
                        value="openid email profile groups"
                    />
                    <input readOnly name="state" value="true-redirect-uri" />
                    <input readOnly name="nonce" value="random_string" />
                    <input type="submit" value="" />
                </form>
            </div>)}
        </AuthProviderContext.Provider>
    )
}

const AuthProviderContext = createContext<AuthProviderContextProps>(
    {} as AuthProviderContextProps
);

export const useAuthProvider = (): AuthProviderContextProps => {
    return useContext(AuthProviderContext);
};
