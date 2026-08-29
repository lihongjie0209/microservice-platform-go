package authz

import "net/http"

type HTTPResolver func(*http.Request) (Requirement, bool)
type HTTPErrorWriter func(http.ResponseWriter, *http.Request, error)

func HTTPMiddleware(authorizer Authorizer, resolve HTTPResolver, writeError HTTPErrorWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			requirement, protected := resolve(request)
			if !protected {
				next.ServeHTTP(response, request)
				return
			}
			if err := Enforce(request.Context(), authorizer, requirement); err != nil {
				if writeError != nil {
					writeError(response, request, err)
					return
				}
				http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
				return
			}
			next.ServeHTTP(response, request)
		})
	}
}
