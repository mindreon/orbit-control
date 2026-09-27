package skillhub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAllowRedirectStaysOnAPIOrFixedObjectHost(t *testing.T) {
	api, err := http.NewRequest(http.MethodGet, "https://api.skillhub.cn/api/v1/download?slug=demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	same, err := http.NewRequest(http.MethodGet, "https://api.skillhub.cn/next", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := allowRedirect(same, []*http.Request{api}); err != nil {
		t.Fatal(err)
	}
	object, err := http.NewRequest(http.MethodGet, "https://"+packageHost+"/skills/demo/1.0.0.zip", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := allowRedirect(object, []*http.Request{api}); err != nil {
		t.Fatal(err)
	}
	other, err := http.NewRequest(http.MethodGet, "https://evil.example/zip", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := allowRedirect(other, []*http.Request{api}); err == nil {
		t.Fatal("other host was allowed")
	}
	plain, err := http.NewRequest(http.MethodGet, "http://"+packageHost+"/skills/demo/1.0.0.zip", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := allowRedirect(plain, []*http.Request{api}); err == nil {
		t.Fatal("plain object host was allowed")
	}
	if err := allowRedirect(object, []*http.Request{api, same}); err == nil {
		t.Fatal("second hop was allowed")
	}
}

func TestGetRefusesRedirectOffTheAPIHost(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example/package.zip", http.StatusFound)
	}))
	defer upstream.Close()
	_, err := NewClient(upstream.URL).get(context.Background(), "/api/v1/download?slug=demo")
	if err == nil {
		t.Fatal("redirect to another host succeeded")
	}
}
