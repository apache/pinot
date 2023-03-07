import { OidcAuthExceptionCodeV1 } from "../interfaces/auth.interfaces";

export const isBlockingAuthExceptionV1 = (
    authExceptionCode: OidcAuthExceptionCodeV1
): boolean => {
    if (!authExceptionCode) {
        return false;
    }

    switch (authExceptionCode) {
        case OidcAuthExceptionCodeV1.UnauthorizedAccess: {
            return false;
        }
    }

    return true;
};
