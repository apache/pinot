import { ReactNode } from "react";

export interface OidcAuthProviderV1Props {
    clientId?: string;
    redirectMethod: OidcAuthRedirectMethodV1;
    oidcIssuerAudiencePath?: string;
    oidcIssuerLogoutPath?: string;
    redirectPathBlacklist?: string[];
    appInitFailure?: boolean;
    className?: string;
    children?: ReactNode;
}

export interface OidcAuthProviderV1ContextProps {
    authUser: OidcAuthUserV1;
    authenticated: boolean;
    authDisabled: boolean;
    accessToken: string;
    authDisabledNotification: boolean;
    authExceptionCode: string;
    login: () => void;
    logout: () => void;
    clearAuthDisabledNotification: () => void;
}

export interface OidcAuthUserV1 {
    name: string;
    email: string;
}

export enum OidcAuthRedirectMethodV1 {
    Post = "POST",
    Get = "GET",
}

export enum OidcAuthHashParamV1 {
    AccessToken = "access_token",
    Error = "error",
}

export enum OidcAuthHashParamErrorValuesV1 {
    AccessDenied = "access_denied",
}

export enum OidcAuthQueryParamV1 {
    AutomaticLogout = "automatic-logout",
    LoggedOut = "logged-out",
}

export enum OidcAuthExceptionCodeV1 {
    AppInitFailure = "app_init_failure",
    InitFailure = "init_failure",
    InfoCallFailure = "info_call_failure",
    OpenIDConfigurationCallFailure = "openid_configuration_call_failure",
    InfoMissing = "info_missing",
    OpenIDConfigurationMissing = "openid_configuration_missing",
    UnauthorizedAccess = "unauthorized_access",
    AccessDenied = "access_denied",
}

export enum OidcAuthActionV1 {
    TriggerReload = "TRIGGER_RELOAD",
    TriggerLocationRedirect = "TRIGGER_LOCATION_REDIRECT",
    TriggerAuthLoginRedirect = "TRIGGER_AUTH_LOGIN_REDIRECT",
    TriggerAuthLogoutRedirect = "TRIGGER_AUTH_LOGOUT_REDIRECT",
}

export enum AuthLocalStorageKeys {
    RedirectLocation = "redirectLocation",
    AccessToken = "AccessToken",
}