import create from "zustand";
import { persist } from "zustand/middleware";

const KEY_AUTH = "auth-v1";

export interface AuthV1 {
    authenticated: boolean;
    accessToken: string;
    authExceptionCode: string;
    redirectHref: string;
    authAction: string;
    authActionData: string;
    disableAuth: () => void;
    setAccessToken: (token: string) => void;
    clearAuth: (exceptionCode?: string) => void;
    clearAuthException: () => void;
    setRedirectHref: (href: string) => void;
    clearRedirectHref: () => void;
    setAuthAction: (action: string, actionData?: string) => void;
    clearAuthAction: () => void;
}


// App store for auth, persisted in browser local storage
export const useAuthV1 = create<AuthV1>(
    persist(
        (set) => ({
            authenticated: false,
            accessToken: "",
            authExceptionCode: "",
            redirectHref: "",
            authAction: "",
            authActionData: "",

            disableAuth: () => {
                set({
                    authenticated: false,
                    accessToken: "",
                    authExceptionCode: "",
                });
            },

            setAccessToken: (token) => {
                set({
                    authenticated: Boolean(token),
                    accessToken: token || "",
                    authExceptionCode: "",
                });
            },

            clearAuth: (exceptionCode) => {
                set({
                    authenticated: false,
                    accessToken: "",
                    authExceptionCode: exceptionCode || "",
                });
            },

            clearAuthException: () => {
                set({
                    authExceptionCode: "",
                });
            },

            setRedirectHref: (href) => {
                set({
                    redirectHref: href,
                });
            },

            clearRedirectHref: () => {
                set({
                    redirectHref: "",
                });
            },

            setAuthAction: (action, actionData) => {
                set({
                    authAction: action || "",
                    authActionData: actionData || "",
                });
            },

            clearAuthAction: () => {
                set({
                    authAction: "",
                    authActionData: "",
                });
            },
        }),
        {
            name: KEY_AUTH, // Persist in browser local storage
            blacklist: ["authAction", "authActionData"], // Prevent persisting in state
        }
    )
);
