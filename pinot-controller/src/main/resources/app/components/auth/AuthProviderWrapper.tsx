import { AuthWorkflow } from 'Models';
import React, { createContext, useContext, useEffect, useState } from 'react'
import { useHistory, useLocation } from 'react-router';
import { AuthLocalStorageKeys } from '../../interfaces/auth.interfaces';
import { baseApi } from '../../utils/axios-config';
import { getFulfilledResponseInterceptorV1, getRejectedResponseInterceptorV1, getRequestInterceptorV1 } from '../../utils/axios.util';
import PinotMethodUtils from '../../utils/PinotMethodUtils';
import { AppLoadingIndicator } from '../AppLoadingIndicator';

interface AuthProviderContextProps {
    accessToken: string;
    authenticated: boolean;
    authWorkflow: AuthWorkflow
}

export const AuthProvider = ({ children }) => {
    const [loading, setLoading] = useState<boolean>(true);
    const [redirectUri, setRedirectUri] = useState<string | null>(null);
    const [clientId, setClientId] = useState<string | null>(null);
    const [authWorkflow, setAuthWorkflow] = useState<AuthWorkflow | null>(null);
    const [accessToken, setAccessToken] = useState<string>("");
    const [authenticated, setAuthenticated] = useState<boolean>(false);
    const oidcSignInFormRef = React.useRef<HTMLFormElement>(null);
    const [authorizationEndpoint, setAuthorizationEndpoint] = useState<string | null>(null);
    const [autoLogout, setAutoLogout] = useState<boolean>(false);
    const history = useHistory();
    const location = useLocation();
    const [loginTrigger, setLoginTrigger] = useState<boolean>(false);
    const [axiosRequestInterceptorId, setAxiosRequestInterceptorId] =
        useState(0);
    const [axiosResponseInterceptorId, setAxiosResponseInterceptorId] =
        useState(0);

    useEffect(() => {
        getAuthInfo();
    }, []);

    useEffect(() => {
        if(!authWorkflow) {
            return;
        }

        if(authWorkflow === AuthWorkflow.NONE) {
            setAuthenticated(true);

            return;
        }

        if(authWorkflow === AuthWorkflow.BASIC) {
            return;
        }

        initOidcAuth();
    }, [authWorkflow]);

    useEffect(() => {
        if(!loginTrigger) {
            return;
        }

        // login
        if(clientId && authorizationEndpoint && oidcSignInFormRef && oidcSignInFormRef.current) {
            oidcSignInFormRef.current.submit();
        }
    }, [loginTrigger]);

    const getAuthInfo = async () => {
        // fetch auth info
        const authInfoResponse = await PinotMethodUtils.getAuthInfo();

        const authWorkFlow =
        authInfoResponse && authInfoResponse.workflow
            ? authInfoResponse.workflow
            : AuthWorkflow.NONE;

        const issuer = 
            authInfoResponse && authInfoResponse.issuer ? authInfoResponse.issuer : ''
        setAuthorizationEndpoint(`${issuer}/auth`);

        // Redirect URI, if available
        setRedirectUri(
            authInfoResponse && authInfoResponse.redirectUri
                ? authInfoResponse.redirectUri
                : ''
        );
        // Client Id, if available
        setClientId(
            authInfoResponse && authInfoResponse.clientId
                ? authInfoResponse.clientId
                : ''
        );

        setLoading(false);

         // Authentication workflow
         setAuthWorkflow(authWorkFlow);
        
    };

    const initOidcAuth = () => {
        const accessToken = getAuthLocalStorageValue(AuthLocalStorageKeys.AccessToken);
        if(accessToken) {
            setAccessToken(accessToken);
            initAxios(accessToken);
            setAuthenticated(true);

            return;
        }

        const accessTokenFromHashParam = PinotMethodUtils.getAccessTokenFromHashParams();
        if(accessTokenFromHashParam) {
            const accessToken = `Bearer ${accessTokenFromHashParam}`;
            setAccessToken(accessToken);
            setAuthLocalStorageValue(AuthLocalStorageKeys.AccessToken, accessToken);
            initAxios(accessToken);
            setAuthenticated(true);
            redirectToApp();

            return;
        }

        const redirectPathAfterLogin = location.pathname;
        setAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation, redirectPathAfterLogin);
        setLoginTrigger(true);
    }

    const handleUnauthenticatedAccess = () => {
        setAuthLocalStorageValue(AuthLocalStorageKeys.AccessToken, "");
        setAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation, "");

        setAutoLogout(true);
        setLoginTrigger(true);
    }

    const initAxios = (accessToken: string) => {
        // Clear existing interceptors
        baseApi.interceptors.request.eject(axiosRequestInterceptorId);
        baseApi.interceptors.response.eject(axiosResponseInterceptorId);

        // Set new interceptors
        setAxiosRequestInterceptorId(
            baseApi.interceptors.request.use(
                getRequestInterceptorV1(accessToken), 
                getRejectedResponseInterceptorV1(handleUnauthenticatedAccess)
            )
        );
        setAxiosResponseInterceptorId(
            baseApi.interceptors.response.use(
                getFulfilledResponseInterceptorV1(),
                getRejectedResponseInterceptorV1(handleUnauthenticatedAccess)
            )
        );
    }

    const redirectToApp = () => {
        const redirectLocation = getAuthLocalStorageValue(AuthLocalStorageKeys.RedirectLocation);
        if(redirectLocation && redirectLocation !== "/login" && redirectLocation !== "/logout") {
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
        authenticated: authenticated
    }

    if (loading) {
        return <AppLoadingIndicator />
    }

    if (authWorkflow === AuthWorkflow.OIDC) {
        return (
            <AuthProviderContext.Provider value={authProvider}>
                {children}

                <div>
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
                            value={"http://localhost:8080" || redirectUri}
                        />
                        <input
                            readOnly
                            name="scope"
                            value="openid email"
                        />
                        <input readOnly name="state" value="true-redirect-uri" />
                        <input readOnly name="nonce" value="random_string" />
                        <input type="submit" value="" />
                    </form>
                </div>
            </AuthProviderContext.Provider>
        )
    }

    return (
        <>{children}</>
    )
}

const AuthProviderContext = createContext<AuthProviderContextProps>(
    {} as AuthProviderContextProps
);

export const useAuthProviderV1 = (): AuthProviderContextProps => {
    return useContext(AuthProviderContext);
};


