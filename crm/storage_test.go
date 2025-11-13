package crm_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/steveyegge/beads"
	"github.com/steveyegge/beads/crm"
)

func newTestStore(t *testing.T) (*crm.Storage, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := dir + "/beads.db"

	s, err := crm.Open(dbPath)
	if err != nil {
		t.Fatalf("open CRM: %v", err)
	}
	ctx := context.Background()
	if err := s.Beads().SetConfig(ctx, "issue_prefix", "crm"); err != nil {
		t.Fatalf("set issue_prefix: %v", err)
	}
	cleanup := func() {
		_ = s.Close()
	}
	return s, cleanup
}

func TestSchemaInit_Idempotent(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	ctx := context.Background()
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("init schema again: %v", err)
	}
}

func TestContactUpsertAndEmailUniqueness(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	id1, err := s.FindOrCreateContactByEmail(ctx, "Alice@Example.com", "Alice Example")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.FindOrCreateContactByEmail(ctx, "alice@example.com", "Alice Example")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("expected same contact; got %s vs %s", id1, id2)
	}

	notes := "Updated notes"
	upd := crm.ContactUpdate{ID: id1, Notes: &notes}
	if err := s.UpdateContact(ctx, upd); err != nil {
		t.Fatalf("update contact: %v", err)
	}

	if err := s.RefreshContactFTS(ctx); err != nil {
		t.Fatalf("refresh fts: %v", err)
	}
	results, err := s.SearchContacts(ctx, "alice", 10)
	if err != nil {
		t.Fatalf("search contacts: %v", err)
	}
	if len(results) != 1 || results[0].ID != id1 {
		t.Fatalf("unexpected search results: %+v", results)
	}

	if err := s.SetPrimaryEmail(ctx, id1, "alice@example.com"); err != nil {
		t.Fatalf("set primary email: %v", err)
	}

	count, err := countRows(ctx, s.DB(), "crm_audits")
	if err != nil {
		t.Fatalf("count audits: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected audit records to be written")
	}
}

func TestIssueLinkingAndJoin(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	issue := &beads.Issue{
		Title:       "Customer request",
		Description: "Feature ask",
		Status:      beads.StatusOpen,
		Priority:    1,
		IssueType:   beads.TypeTask,
	}
	if err := s.Beads().CreateIssue(ctx, issue, "test"); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	cid, err := s.FindOrCreateContactByEmail(ctx, "pm@acme.io", "Pat PM")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.LinkIssueToContact(ctx, issue.ID, cid, "requester"); err != nil {
		t.Fatalf("link issue contact: %v", err)
	}

	summary, err := s.GetOpenIssuesByOrganization(ctx, "")
	if err != nil {
		t.Fatalf("get open issues: %v", err)
	}
	if len(summary) != 0 {
		t.Fatalf("expected no org filtering when org absent: %+v", summary)
	}

	org := crm.Organization{Name: "Acme", Domain: "acme.io"}
	orgID, err := s.CreateOrganization(ctx, org)
	if err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := s.AttachDomain(ctx, orgID, "service.acme.io"); err != nil {
		t.Fatalf("attach domain: %v", err)
	}

	clearOrg := true
	if err := s.UpdateContact(ctx, crm.ContactUpdate{ID: cid, OrgID: &orgID}); err != nil {
		t.Fatalf("assign org: %v", err)
	}
	_ = clearOrg

	issues, err := s.GetOpenIssuesByOrganization(ctx, orgID)
	if err != nil {
		t.Fatalf("get open issues by org: %v", err)
	}
	if len(issues) != 1 || issues[0].ID != issue.ID {
		t.Fatalf("unexpected issues: %+v", issues)
	}
}

func TestCascadeDeleteIssue(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	issue := &beads.Issue{Title: "Bug", Status: beads.StatusOpen, Priority: 2, IssueType: beads.TypeBug}
	if err := s.Beads().CreateIssue(ctx, issue, "test"); err != nil {
		t.Fatal(err)
	}
	cid, _ := s.FindOrCreateContactByEmail(ctx, "qa@acme.io", "Quinn QA")
	if err := s.LinkIssueToContact(ctx, issue.ID, cid, "stakeholder"); err != nil {
		t.Fatal(err)
	}
	if err := s.Beads().DeleteIssue(ctx, issue.ID); err != nil {
		t.Fatal(err)
	}

	remaining, err := countRows(ctx, s.DB(), "crm_issue_contacts")
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("expected cascade delete; still have %d rows", remaining)
	}
}

func TestDealPipelineAndClose(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	pipe := crm.Pipeline{Name: "Default"}
	pipelineID, err := s.CreatePipeline(ctx, pipe)
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	stage1 := crm.Stage{PipelineID: pipelineID, Name: "Qualification", Position: 1}
	stage1.ID = "stage-qual"
	if err := s.CreateStage(ctx, stage1); err != nil {
		t.Fatalf("create stage1: %v", err)
	}
	stage2 := crm.Stage{PipelineID: pipelineID, Name: "Close", Position: 2}
	stage2.ID = "stage-close"
	if err := s.CreateStage(ctx, stage2); err != nil {
		t.Fatalf("create stage2: %v", err)
	}

	orgID, err := s.CreateOrganization(ctx, crm.Organization{Name: "Acme", Domain: "acme.io"})
	if err != nil {
		t.Fatalf("create org: %v", err)
	}

	deal := crm.Deal{
		Title:       "Acme pilot",
		OrgID:       sql.NullString{String: orgID, Valid: true},
		PipelineID:  pipelineID,
		StageID:     stage1.ID,
		AmountCents: sql.NullInt64{Int64: 500000, Valid: true},
	}
	dealID, err := s.CreateDeal(ctx, deal)
	if err != nil {
		t.Fatalf("create deal: %v", err)
	}

	openSummaries, err := s.GetDealsByStage(ctx)
	if err != nil {
		t.Fatalf("get deals by stage (open): %v", err)
	}
	if len(openSummaries) != 1 {
		t.Fatalf("expected one open stage summary, got %d", len(openSummaries))
	}

	if err := s.MoveDealStage(ctx, dealID, stage2.ID); err != nil {
		t.Fatalf("move deal: %v", err)
	}
	if err := s.CloseDeal(ctx, dealID, "won", sql.NullString{String: "Signed", Valid: true}); err != nil {
		t.Fatalf("close deal: %v", err)
	}

	summaries, err := s.GetDealsByStage(ctx)
	if err != nil {
		t.Fatalf("get deals by stage: %v", err)
	}
	if len(summaries) != 0 {
		t.Fatalf("expected closed deal to be excluded from open pipeline counts")
	}
}

func TestActivityAndFTS(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	cid, _ := s.FindOrCreateContactByEmail(ctx, "sales@acme.io", "Sally Sales")
	act := crm.Activity{
		Type:       "email",
		Direction:  sql.NullString{String: "outbound", Valid: true},
		Summary:    "Follow up",
		OccurredAt: time.Now().UTC(),
		ContactID:  sql.NullString{String: cid, Valid: true},
	}
	if err := s.RecordActivity(ctx, act); err != nil {
		t.Fatalf("record activity: %v", err)
	}

	if err := s.RefreshContactFTS(ctx); err != nil {
		t.Fatalf("refresh fts: %v", err)
	}

	res, err := s.SearchContacts(ctx, "sally", 5)
	if err != nil {
		t.Fatalf("search contacts: %v", err)
	}
	if len(res) != 1 || res[0].ID != cid {
		t.Fatalf("unexpected search result: %+v", res)
	}

	timeline, err := s.GetRecentActivitiesForContact(ctx, cid, 10)
	if err != nil {
		t.Fatalf("recent activities: %v", err)
	}
	if len(timeline) != 1 || timeline[0].Type != "email" {
		t.Fatalf("unexpected timeline: %+v", timeline)
	}
}

func TestMergeUtilities(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	targetID, _ := s.FindOrCreateContactByEmail(ctx, "owner@acme.io", "Owner")
	sourceID, _ := s.FindOrCreateContactByEmail(ctx, "duplicate@acme.io", "Dup")

	phoneID, err := s.AddPhone(ctx, sourceID, "123", false)
	if err != nil {
		t.Fatalf("add phone: %v", err)
	}
	if err := s.NormalizePhone(ctx, phoneID, "+1123"); err != nil {
		t.Fatalf("normalize phone: %v", err)
	}

	if err := s.MergeContacts(ctx, targetID, sourceID); err != nil {
		t.Fatalf("merge contacts: %v", err)
	}

	totalPhones, err := countRows(ctx, s.DB(), "crm_contact_phones")
	if err != nil {
		t.Fatal(err)
	}
	if totalPhones != 1 {
		t.Fatalf("expected phones consolidated, got %d", totalPhones)
	}

	org1, _ := s.CreateOrganization(ctx, crm.Organization{Name: "Org1", Domain: "org1.io"})
	org2, _ := s.CreateOrganization(ctx, crm.Organization{Name: "Org2", Domain: "org2.io"})
	if err := s.AttachDomain(ctx, org2, "alias.org2.io"); err != nil {
		t.Fatalf("attach domain: %v", err)
	}

	if err := s.MergeOrganizations(ctx, org1, org2); err != nil {
		t.Fatalf("merge orgs: %v", err)
	}

	domains, err := countRows(ctx, s.DB(), "crm_org_domains")
	if err != nil {
		t.Fatal(err)
	}
	if domains == 0 {
		t.Fatalf("expected domains moved to target org")
	}
}

func countRows(ctx context.Context, db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
	return count, err
}
