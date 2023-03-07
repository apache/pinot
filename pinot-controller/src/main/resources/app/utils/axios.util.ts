import { AxiosError, AxiosRequestConfig } from "axios";

// Returns axios request interceptor
export const getRequestInterceptorV1 = (
    accessToken?: string
): ((requestConfig: AxiosRequestConfig) => AxiosRequestConfig) => {
    const requestInterceptor = (
        requestConfig: AxiosRequestConfig
    ): AxiosRequestConfig => {
        // If access token is available, attach it to the request
        if (accessToken) {
            requestConfig.headers = {
                Authorization: accessToken,
            };
        }

        return requestConfig;
    };

    return requestInterceptor;
};

// Returns axios fulfilled response interceptor
export const getFulfilledResponseInterceptorV1 = (): (<T>(
    response: T
) => T | Promise<T>) => {
    const fulfilledResponseInterceptor = <T>(response: T): T | Promise<T> => {
        // Forward the fulfilled response
        return response;
    };

    return fulfilledResponseInterceptor;
};

// Returns axios rejected response interceptor
export const getRejectedResponseInterceptorV1 = (
    unauthenticatedAccessFn?: () => void
): ((error: AxiosError) => void) => {
    const rejectedResponseInterceptor = (error: AxiosError): any => {
        if (error && error.response && (error.response.status === 401 || error.response.status === 403)) {
            // Unauthenticated access
            unauthenticatedAccessFn && unauthenticatedAccessFn();
        }

        return error.response || error;
    };

    return rejectedResponseInterceptor;
};
