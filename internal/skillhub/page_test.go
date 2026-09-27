package skillhub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPageCopyKeepsDisplayFieldsAndDropsSignedURLs(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.RawQuery != "namespace=indiv-ebandao" && !strings.Contains(r.URL.RawQuery, "namespace=indiv-ebandao") {
			t.Errorf("query %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/skills/dev-expert":
			_, _ = w.Write([]byte(`{
				"slug":"dev-expert",
				"latestVersion":{"changelog":"Initial release","createdAt":1790427537001,"version":"1.21.10","securityReports":{"keen":{"reportUrl":"https://example.invalid/report?q-signature=abc"}}},
				"securityReports":{"keen":{"reportUrl":"https://example.invalid/r?q-signature=abc","status":"benign","statusText":"安全，无风险"}},
				"skill":{"slug":"dev-expert","summary":"en","summary_zh":"编程专家摘要","updatedAt":1790490406126,"subCategories":[{"key":"dev-code-gen","name":"代码生成"}]}
			}`))
		case "/api/v1/skills/dev-expert/evaluation":
			_, _ = w.Write([]byte(`{
				"createdAt":1790236925952,
				"userSummary":"这个助手质量扎实",
				"dimensions":{
					"trust":{"userReason":"双实验室","items":{"scan":{"score":5,"userReason":"无风险","reason":"lab"},"domestic":{"score":5,"userReason":"中文"}}},
					"reliability":{"userReason":"稳定","items":{"stability":{"score":4.7},"func":{"score":4.8},"errorHandling":{"score":4.6}}},
					"adaptability":{"userReason":"边界清楚","items":{"boundary":{"score":4.6},"trigger":{"score":4.8}}},
					"convention":{"userReason":"文档清楚","items":{"structure":{"score":4.8},"docQuality":{"score":4.8},"progressive":{"score":4.8},"antiPatternFaq":{"score":4.8}}},
					"effectiveness":{"userReason":"功能全面","items":{"accuracy":{"score":4.5},"completeness":{"score":4.5},"usability":{"score":4.3},"creativity":{"score":4.3}}}
				}
			}`))
		case "/api/v1/skills/dev-expert/versions":
			_, _ = w.Write([]byte(`{"versions":[{"version":"1.21.10","changelog":"Initial release","createdAt":1790427537001,"securityReports":{"sanbu":{"reportUrl":"https://example.invalid/signed"}}}]}`))
		case "/api/v1/skills/dev-expert/files":
			if r.URL.Query().Get("version") != "1.21.10" {
				t.Errorf("version %q", r.URL.Query().Get("version"))
			}
			_, _ = w.Write([]byte(`{"count":2,"files":[{"path":"SKILL.md","sha256":"abc","size":15826},{"path":"../secret.txt","size":3}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	raw, err := NewClient(srv.URL).PageCopy(context.Background(), "indiv-ebandao", "dev-expert")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, banned := range []string{"reportUrl", "q-signature", "sha256", "secret.txt", `"reason":"lab"`} {
		if strings.Contains(text, banned) {
			t.Fatalf("stored %s in %s", banned, text)
		}
	}
	var got pageCopy
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SummaryZh != "编程专家摘要" || !got.Safe || got.Score == nil || *got.Score != 4.7 {
		t.Fatalf("copy = %+v", got)
	}
	if len(got.SubCategories) != 1 || got.SubCategories[0].Name != "代码生成" {
		t.Fatalf("subs = %+v", got.SubCategories)
	}
	if len(got.FileIndex) != 1 || got.FileIndex[0].Path != "SKILL.md" || got.FileIndex[0].Size != 15826 {
		t.Fatalf("files = %+v", got.FileIndex)
	}
	if len(got.Versions) != 1 || got.Versions[0].Changelog != "Initial release" {
		t.Fatalf("versions = %+v", got.Versions)
	}
	if got.Evaluation == nil || got.Evaluation.UserSummary != "这个助手质量扎实" || len(got.Evaluation.Dimensions) != 5 {
		t.Fatalf("eval = %+v", got.Evaluation)
	}
	if got.Evaluation.Dimensions[0].Items[0].Name != "安全性扫描" {
		t.Fatalf("items = %+v", got.Evaluation.Dimensions[0].Items)
	}
	if hits != 4 {
		t.Fatalf("hits = %d", hits)
	}
}

func TestPageCopyRejectsDotDotWithoutCallingUpstream(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		http.NotFound(w, r)
	}))
	defer srv.Close()
	if _, err := NewClient(srv.URL).PageCopy(context.Background(), "", ".."); err == nil {
		t.Fatal("accepted ..")
	}
	if hits != 0 {
		t.Fatalf("hits = %d", hits)
	}
}

func TestPageCopyKeepsHeaderWhenEvaluationIsMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/skills/notes":
			_, _ = w.Write([]byte(`{"slug":"notes","skill":{"slug":"notes","summary_zh":"随手记","updatedAt":10}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	raw, err := NewClient(srv.URL).PageCopy(context.Background(), "", "notes")
	if err != nil {
		t.Fatal(err)
	}
	var got pageCopy
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SummaryZh != "随手记" || got.Score != nil || got.Evaluation != nil {
		t.Fatalf("copy = %+v", got)
	}
}
