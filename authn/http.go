package authn

import (
	"net/http"
)

type HTTPErrorWriter func(http.ResponseWriter, *http.Request, error)

// HTTPMiddleware authenticates using URL.Path and places the principal in the
// request context. Use a router adapter when policy targets are route templates.
func HTTPMiddleware(policy Policy, writeError HTTPErrorWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			ctx, err := policy.Authenticate(request.Context(), request.URL.Path, request.Header.Get("Authorization"))
			if err != nil {
				if writeError != nil {
					writeError(response, request, err)
					return
				}
				http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(response, request.WithContext(ctx))
		})
	}
}
