package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

// Ported from netflow's HTTPIAMLookup (apps/api/internal/auth/iam.go): orbit only
// needs GET /users/current, so the directory surface is not carried over.

// ErrIAMUnavailable marks a failure of the identity provider itself (network
// error, timeout, 408/429, 5xx, undecodable body). Callers must surface it as
// a retryable 503: treating an IAM outage as 401 would sign every user out.
var ErrIAMUnavailable = errors.New("iam unavailable")

var errIAMRejected = errors.New("iam rejected token")

const iamBodyLimit = 1 << 20

type iamLookup interface {
	Lookup(ctx context.Context, token string) (iamIdentity, error)
}

// iamIdentity is the subset of the baize user orbit maps onto a principal.
// ProjectID/ProjectName carry the token's current baize project (netflow
// applyCurrentProject: currentProject, else defaultProject, else defaultProjectId).
type iamIdentity struct {
	UserID      string
	Email       string
	Name        string
	Verified    bool
	ProjectID   string
	ProjectName string
}

type httpIAMLookup struct {
	baseURL string
	client  *http.Client
}

func newHTTPLookup(baseURL string, client *http.Client) *httpIAMLookup {
	if client == nil {
		client = &http.Client{
			Timeout: iamLookupTimeout,
			// A redirecting IAM is a login wall, not an identity source.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &httpIAMLookup{baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"), client: client}
}

type iamEnvelope struct {
	Code int             `json:"code"`
	Data json.RawMessage `json:"data"`
}

type iamUser struct {
	ID               string         `json:"id"`
	UserID           string         `json:"userId"`
	Username         string         `json:"username"`
	Nickname         string         `json:"nickname"`
	Email            string         `json:"email"`
	Status           string         `json:"status"`
	DefaultProjectID string         `json:"defaultProjectId"`
	DefaultProject   *iamProjectRef `json:"defaultProject"`
	CurrentProject   *iamProjectRef `json:"currentProject"`
}

type iamProjectRef struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

// Lookup calls GET {base}/users/current with the caller's bearer token.
func (h *httpIAMLookup) Lookup(ctx context.Context, token string) (iamIdentity, error) {
	body, err := h.get(ctx, token, "/users/current")
	if err != nil {
		return iamIdentity{}, err
	}
	return identityFromCurrentUser(body)
}

func (h *httpIAMLookup) get(ctx context.Context, token, path string) (json.RawMessage, error) {
	if h == nil || h.baseURL == "" {
		return nil, fmt.Errorf("%w: iam base url not configured", ErrIAMUnavailable)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errIAMRejected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIAMUnavailable, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	res, err := h.client.Do(req)
	if err != nil {
		// A spent deadline (the lookup's own budget, since the cache detaches the
		// caller's cancellation) is an IAM outage; only the caller canceling its
		// own request passes through unmapped.
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %v", ErrIAMUnavailable, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, iamBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIAMUnavailable, err)
	}
	switch {
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		return nil, errIAMRejected
	case res.StatusCode == http.StatusRequestTimeout || res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
		return nil, fmt.Errorf("%w: iam status %d", ErrIAMUnavailable, res.StatusCode)
	case res.StatusCode >= 300:
		return nil, errIAMRejected
	}
	var envelope iamEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("%w: decode iam response", ErrIAMUnavailable)
	}
	// A well-formed envelope with a non-zero code is an auth decision, not an outage.
	if envelope.Code != 0 {
		return nil, errIAMRejected
	}
	return envelope.Data, nil
}

// identityFromCurrentUser prefers data.user and falls back to data itself, matching
// netflow's identityFromCurrentUser.
func identityFromCurrentUser(body json.RawMessage) (iamIdentity, error) {
	var wrapped struct {
		User iamUser `json:"user"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return iamIdentity{}, fmt.Errorf("%w: decode iam current user", ErrIAMUnavailable)
	}
	user := wrapped.User
	if strings.TrimSpace(user.ID) == "" && strings.TrimSpace(user.UserID) == "" && strings.TrimSpace(user.Username) == "" {
		var bare iamUser
		if err := json.Unmarshal(body, &bare); err != nil {
			return iamIdentity{}, fmt.Errorf("%w: decode iam current user", ErrIAMUnavailable)
		}
		user = bare
	}
	return identityFromUser(user)
}

func identityFromUser(user iamUser) (iamIdentity, error) {
	if strings.EqualFold(user.Status, "disabled") || strings.EqualFold(user.Status, "inactive") {
		return iamIdentity{}, errIAMRejected
	}
	id := firstNonEmpty(user.ID, user.UserID)
	// orbit keys principals on the IAM user id; a username-only record has no subject.
	if id == "" {
		return iamIdentity{}, errIAMRejected
	}
	name := firstNonEmpty(user.Nickname, user.Username, id)
	email := strings.TrimSpace(user.Email)
	// baize does not confirm email ownership here, so an iam identity is never
	// verified; otherwise an IAM-sourced email could claim invitations.
	identity := iamIdentity{UserID: id, Email: email, Name: name, Verified: false}
	applyCurrentProject(&identity, user.CurrentProject, user.DefaultProject, user.DefaultProjectID)
	return identity, nil
}

// applyCurrentProject mirrors netflow: the current project, else the default
// project, else the bare defaultProjectId. A bare id carries no name; the
// workspace is named by the project id until a full ref arrives.
func applyCurrentProject(identity *iamIdentity, current, fallback *iamProjectRef, defaultProjectID string) {
	project := current
	if project == nil || strings.TrimSpace(project.ID) == "" {
		project = fallback
	}
	if project != nil && strings.TrimSpace(project.ID) != "" {
		identity.ProjectID = strings.TrimSpace(project.ID)
		identity.ProjectName = firstNonEmpty(project.DisplayName, project.Name)
		return
	}
	identity.ProjectID = strings.TrimSpace(defaultProjectID)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if t := strings.TrimSpace(v); t != "" {
			return t
		}
	}
	return ""
}

// iamError maps a lookup failure onto the HTTP error Authenticate returns.
func iamError(err error) error {
	if errors.Is(err, ErrIAMUnavailable) {
		// NOT_READY is the documented retryable-503 code (see /ready); a new code
		// would have to be added to the Error schema enum.
		e := httpx.E(503, "NOT_READY", "Identity provider unavailable")
		e.Retryable = true
		return e
	}
	return httpx.E(401, "UNAUTHENTICATED", "Invalid token")
}
