package market

import (
	"net/url"
	"testing"
)

func TestPlazaListsImportedCatalogs(t *testing.T) {
	skills, err := ListSkills(url.Values{"q": {"jira-workflow"}, "limit": {"5"}})
	if err != nil {
		t.Fatal(err)
	}
	if skills.Total < 1 || skills.Items[0].Name == "" {
		t.Fatalf("skill search = %+v", skills)
	}
	item, ok, err := GetSkill(skills.Items[0].Handle, skills.Items[0].Slug)
	if err != nil || !ok || item.Description == "" {
		t.Fatalf("skill detail ok=%v err=%v item=%+v", ok, err, item)
	}
	mcp, err := ListMCP(url.Values{"limit": {"5"}})
	if err != nil {
		t.Fatal(err)
	}
	if mcp.Total < 1000 || len(mcp.Items) != 5 || mcp.Items[0].Name == "" {
		t.Fatalf("mcp page total=%d items=%d", mcp.Total, len(mcp.Items))
	}
	detail, ok, err := GetMCP(mcp.Items[0].ID)
	if err != nil || !ok || detail.Summary == "" {
		t.Fatalf("mcp detail ok=%v err=%v", ok, err)
	}
	agents, err := ListAgents(url.Values{"limit": {"5"}})
	if err != nil {
		t.Fatal(err)
	}
	if agents.Total < 100 || len(agents.Items) != 5 || agents.Items[0].Name == "" {
		t.Fatalf("agent page total=%d items=%d", agents.Total, len(agents.Items))
	}
	agent, ok, err := GetAgent(agents.Items[0].Handle, agents.Items[0].Slug)
	if err != nil || !ok || agent.Name == "" {
		t.Fatalf("agent detail ok=%v err=%v", ok, err)
	}
}
