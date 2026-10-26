package auth

import (
	"encoding/json"
	"testing"
)

func TestIAMProjectFields(t *testing.T) {
	project := func(id, name, display string) *iamProjectRef {
		return &iamProjectRef{ID: id, Name: name, DisplayName: display}
	}
	for _, test := range []struct {
		name string
		user iamUser
		want iamIdentity
	}{
		{name: "current project",
			user: iamUser{ID: "u-1", CurrentProject: project("p-1", "alpha", "Alpha")},
			want: iamIdentity{UserID: "u-1", Name: "u-1", ProjectID: "p-1", ProjectName: "Alpha"}},
		{name: "current over default",
			user: iamUser{ID: "u-1", CurrentProject: project("p-1", "alpha", "Alpha"), DefaultProject: project("p-0", "zero", "Zero")},
			want: iamIdentity{UserID: "u-1", Name: "u-1", ProjectID: "p-1", ProjectName: "Alpha"}},
		{name: "default fallback",
			user: iamUser{ID: "u-1", DefaultProject: project("p-0", "zero", "")},
			want: iamIdentity{UserID: "u-1", Name: "u-1", ProjectID: "p-0", ProjectName: "zero"}},
		{name: "ref without id",
			user: iamUser{ID: "u-1", CurrentProject: project("", "alpha", "Alpha")},
			want: iamIdentity{UserID: "u-1", Name: "u-1"}},
		{name: "bare default project id",
			user: iamUser{ID: "u-1", DefaultProjectID: "p-0"},
			want: iamIdentity{UserID: "u-1", Name: "u-1", ProjectID: "p-0"}},
		{name: "no project",
			user: iamUser{ID: "u-1"},
			want: iamIdentity{UserID: "u-1", Name: "u-1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := identityFromUser(test.user)
			if err != nil || got != test.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, test.want)
			}
		})
	}
}

func TestIAMCurrentUserProject(t *testing.T) {
	body, err := json.Marshal(map[string]any{"user": map[string]any{
		"id": "u-1", "nickname": "Ada",
		"currentProject": map[string]any{"id": "p-1", "name": "alpha", "displayName": "Alpha"}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := identityFromCurrentUser(body)
	if err != nil || got.ProjectID != "p-1" || got.ProjectName != "Alpha" {
		t.Fatalf("got %+v, %v", got, err)
	}
}
