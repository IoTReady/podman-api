package auth

// AllScopes is the complete set of bearer-token scopes recognised by the API.
var AllScopes = []string{
	"hosts:read",
	"templates:read",
	"templates:write",
	"instances:read",
	"instances:write",
	"secrets:read",
	"secrets:write",
	"jobs:read",
}
