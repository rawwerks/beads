package crm

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/beads"
)

type Storage struct {
	beads beads.Storage
	db    *sql.DB
}

func Open(dbPath string) (*Storage, error) {
	store, err := beads.NewSQLiteStorage(dbPath)
	if err != nil {
		return nil, err
	}
	db := store.UnderlyingDB()
	s := &Storage{beads: store, db: db}
	if err := s.InitSchema(context.Background()); err != nil {
		_ = store.Close()
		return nil, err
	}
	return s, nil
}

func (s *Storage) Close() error {
	return s.beads.Close()
}

func (s *Storage) DB() *sql.DB {
	return s.db
}

func (s *Storage) Beads() beads.Storage {
	return s.beads
}

func (s *Storage) InitSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS crm_schema_versions (version TEXT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)`); err != nil {
		return err
	}
	for _, m := range migrations {
		applied, err := s.isMigrationApplied(ctx, m.Version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := s.applyMigration(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

type migration struct {
	Version    string
	Statements []string
}

var migrations = []migration{
	{
		Version: "0001_init",
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS crm_organizations (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  website       TEXT,
  industry      TEXT,
  size_bucket   TEXT,
  billing_note  TEXT DEFAULT '',
  domain        TEXT,
  canonical_domain TEXT GENERATED ALWAYS AS (lower(trim(domain))) STORED,
  custom        TEXT DEFAULT '{}',
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_org_domain ON crm_organizations(canonical_domain)
  WHERE canonical_domain IS NOT NULL`,
			`CREATE INDEX IF NOT EXISTS idx_crm_org_name ON crm_organizations(name COLLATE NOCASE)`,
			`CREATE TRIGGER IF NOT EXISTS trg_crm_org_updated_at
AFTER UPDATE ON crm_organizations
FOR EACH ROW BEGIN
  UPDATE crm_organizations SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END`,
			`CREATE TABLE IF NOT EXISTS crm_org_domains (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id      TEXT NOT NULL,
  domain      TEXT NOT NULL,
  canonical_domain TEXT GENERATED ALWAYS AS (lower(trim(domain))) STORED,
  FOREIGN KEY (org_id) REFERENCES crm_organizations(id) ON DELETE CASCADE
)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_org_domains_unique ON crm_org_domains(canonical_domain)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_org_domains_org ON crm_org_domains(org_id)`,
			`CREATE TABLE IF NOT EXISTS crm_contacts (
  id            TEXT PRIMARY KEY,
  full_name     TEXT NOT NULL,
  given_name    TEXT,
  family_name   TEXT,
  title         TEXT,
  org_id        TEXT,
  timezone      TEXT,
  linkedin_url  TEXT,
  github_url    TEXT,
  notes         TEXT DEFAULT '',
  custom        TEXT DEFAULT '{}',
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (org_id) REFERENCES crm_organizations(id) ON DELETE SET NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_contacts_name ON crm_contacts(full_name COLLATE NOCASE)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_contacts_org ON crm_contacts(org_id)`,
			`CREATE TRIGGER IF NOT EXISTS trg_crm_contacts_updated_at
AFTER UPDATE ON crm_contacts
FOR EACH ROW BEGIN
  UPDATE crm_contacts SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END`,
			`CREATE TABLE IF NOT EXISTS crm_contact_emails (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  contact_id    TEXT NOT NULL,
  email         TEXT NOT NULL,
  canonical_email TEXT GENERATED ALWAYS AS (lower(trim(email))) STORED,
  is_primary    INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
  is_verified   INTEGER NOT NULL DEFAULT 0 CHECK (is_verified IN (0,1)),
  FOREIGN KEY (contact_id) REFERENCES crm_contacts(id) ON DELETE CASCADE
)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_contact_emails_canonical ON crm_contact_emails(canonical_email)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_contact_emails_contact ON crm_contact_emails(contact_id)`,
			`CREATE TABLE IF NOT EXISTS crm_contact_phones (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  contact_id      TEXT NOT NULL,
  phone_raw       TEXT NOT NULL,
  phone_normalized TEXT,
  is_primary      INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
  FOREIGN KEY (contact_id) REFERENCES crm_contacts(id) ON DELETE CASCADE
)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_contact_phones_contact ON crm_contact_phones(contact_id)`,
			`CREATE TABLE IF NOT EXISTS crm_tags (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  tag       TEXT NOT NULL UNIQUE
)`,
			`CREATE TABLE IF NOT EXISTS crm_contact_tags (
  contact_id TEXT NOT NULL,
  tag_id     INTEGER NOT NULL,
  PRIMARY KEY (contact_id, tag_id),
  FOREIGN KEY (contact_id) REFERENCES crm_contacts(id) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES crm_tags(id) ON DELETE CASCADE
)`,
			`CREATE TABLE IF NOT EXISTS crm_org_tags (
  org_id  TEXT NOT NULL,
  tag_id  INTEGER NOT NULL,
  PRIMARY KEY (org_id, tag_id),
  FOREIGN KEY (org_id) REFERENCES crm_organizations(id) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES crm_tags(id) ON DELETE CASCADE
)`,
			`CREATE TABLE IF NOT EXISTS crm_pipelines (
  id        TEXT PRIMARY KEY,
  name      TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`,
			`CREATE TABLE IF NOT EXISTS crm_stages (
  id           TEXT PRIMARY KEY,
  pipeline_id  TEXT NOT NULL,
  name         TEXT NOT NULL,
  position     INTEGER NOT NULL,
  FOREIGN KEY (pipeline_id) REFERENCES crm_pipelines(id) ON DELETE CASCADE
)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_stages_unique ON crm_stages(pipeline_id, position)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_stages_pipeline ON crm_stages(pipeline_id)`,
			`CREATE TABLE IF NOT EXISTS crm_deals (
  id           TEXT PRIMARY KEY,
  title        TEXT NOT NULL,
  org_id       TEXT,
  owner        TEXT,
  amount_cents INTEGER,
  currency     TEXT DEFAULT 'USD',
  pipeline_id  TEXT NOT NULL,
  stage_id     TEXT NOT NULL,
  status       TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','won','lost','abandoned')),
  expected_close_date DATE,
  close_reason TEXT,
  custom       TEXT DEFAULT '{}',
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  closed_at    DATETIME,
  FOREIGN KEY (org_id) REFERENCES crm_organizations(id) ON DELETE SET NULL,
  FOREIGN KEY (pipeline_id) REFERENCES crm_pipelines(id) ON DELETE CASCADE,
  FOREIGN KEY (stage_id) REFERENCES crm_stages(id) ON DELETE RESTRICT
)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_deals_org ON crm_deals(org_id)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_deals_status ON crm_deals(status)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_deals_owner ON crm_deals(owner)`,
			`CREATE TRIGGER IF NOT EXISTS trg_crm_deals_updated_at
AFTER UPDATE ON crm_deals
FOR EACH ROW BEGIN
  UPDATE crm_deals SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
END`,
			`CREATE TABLE IF NOT EXISTS crm_activities (
  id           TEXT PRIMARY KEY,
  type         TEXT NOT NULL CHECK (type IN ('note','call','email','meeting','task')),
  direction    TEXT CHECK (direction IN ('inbound','outbound')),
  summary      TEXT,
  payload      TEXT,
  occurred_at  DATETIME NOT NULL,
  contact_id   TEXT,
  org_id       TEXT,
  deal_id      TEXT,
  issue_id     TEXT,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (contact_id) REFERENCES crm_contacts(id) ON DELETE SET NULL,
  FOREIGN KEY (org_id) REFERENCES crm_organizations(id) ON DELETE SET NULL,
  FOREIGN KEY (deal_id) REFERENCES crm_deals(id) ON DELETE SET NULL,
  FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE SET NULL
)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_act_when ON crm_activities(occurred_at)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_act_contact ON crm_activities(contact_id)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_act_org ON crm_activities(org_id)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_act_deal ON crm_activities(deal_id)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_act_issue ON crm_activities(issue_id)`,
			`CREATE TABLE IF NOT EXISTS crm_issue_contacts (
  issue_id   TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  role       TEXT NOT NULL CHECK (role IN ('requester','reporter','assignee','stakeholder','approver','customer','watcher')),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (issue_id, contact_id, role),
  FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE,
  FOREIGN KEY (contact_id) REFERENCES crm_contacts(id) ON DELETE CASCADE
)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_issue_contacts_contact ON crm_issue_contacts(contact_id)`,
			`CREATE VIRTUAL TABLE IF NOT EXISTS crm_contacts_fts USING fts5(full_name, emails, phones, content='')`,
			`CREATE TABLE IF NOT EXISTS crm_search_markers (kind TEXT PRIMARY KEY, last_refresh DATETIME)`,
			`CREATE VIEW IF NOT EXISTS crm_contact_directory AS
SELECT
  c.id,
  c.full_name,
  c.title,
  o.name AS organization,
  (SELECT email FROM crm_contact_emails e WHERE e.contact_id=c.id AND e.is_primary=1 LIMIT 1) AS primary_email,
  (SELECT phone_normalized FROM crm_contact_phones p WHERE p.contact_id=c.id AND p.is_primary=1 LIMIT 1) AS primary_phone,
  (SELECT MAX(occurred_at) FROM crm_activities a WHERE a.contact_id=c.id) AS last_activity_at
FROM crm_contacts c
LEFT JOIN crm_organizations o ON o.id = c.org_id`,
		},
	},
	{
		Version: "0002_audit",
		Statements: []string{
			`CREATE TABLE IF NOT EXISTS crm_audits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  event TEXT NOT NULL,
  old TEXT,
  new TEXT,
  actor TEXT DEFAULT '',
  at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)`,
			`CREATE INDEX IF NOT EXISTS idx_crm_audits_entity ON crm_audits(entity, entity_id)`},
	},
}

func (s *Storage) isMigrationApplied(ctx context.Context, version string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM crm_schema_versions WHERE version = ?`, version).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Storage) applyMigration(ctx context.Context, m migration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, stmt := range m.Statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", m.Version, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO crm_schema_versions(version) VALUES (?)`, m.Version); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func canonicalize(input string) string {
	return strings.ToLower(strings.TrimSpace(input))
}

func (s *Storage) FindOrCreateContactByEmail(ctx context.Context, email, fullName string) (string, error) {
	if strings.TrimSpace(email) == "" {
		return "", errors.New("email required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	canonical := canonicalize(email)
	var contactID string
	err = tx.QueryRowContext(ctx, `SELECT c.id FROM crm_contact_emails e JOIN crm_contacts c ON c.id=e.contact_id WHERE e.canonical_email = ?`, canonical).Scan(&contactID)
	switch {
	case err == sql.ErrNoRows:
		contactID = NewID()
		name := strings.TrimSpace(fullName)
		if name == "" {
			name = email
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO crm_contacts (id, full_name) VALUES (?, ?)`, contactID, name); err != nil {
			_ = tx.Rollback()
			return "", err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO crm_contact_emails (contact_id, email, is_primary, is_verified) VALUES (?, ?, 1, 0)`, contactID, email); err != nil {
			_ = tx.Rollback()
			return "", err
		}
		newState, err := contactStateTx(ctx, tx, contactID)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
		if err := recordAuditTx(ctx, tx, "contact", contactID, "create", "{}", newState); err != nil {
			_ = tx.Rollback()
			return "", err
		}
	case err != nil:
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return contactID, nil
}

type ContactUpdate struct {
	ID          string
	FullName    *string
	GivenName   *string
	FamilyName  *string
	Title       *string
	OrgID       *string
	ClearOrg    bool
	Timezone    *string
	LinkedinURL *string
	GithubURL   *string
	Notes       *string
	Custom      map[string]any
}

func (s *Storage) UpdateContact(ctx context.Context, upd ContactUpdate) error {
	if upd.ID == "" {
		return errors.New("contact id required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	oldState, err := contactStateTx(ctx, tx, upd.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	var sets []string
	var args []any
	if upd.FullName != nil {
		sets = append(sets, "full_name = ?")
		args = append(args, *upd.FullName)
	}
	if upd.GivenName != nil {
		sets = append(sets, "given_name = ?")
		args = append(args, *upd.GivenName)
	}
	if upd.FamilyName != nil {
		sets = append(sets, "family_name = ?")
		args = append(args, *upd.FamilyName)
	}
	if upd.Title != nil {
		sets = append(sets, "title = ?")
		args = append(args, *upd.Title)
	}
	if upd.OrgID != nil {
		sets = append(sets, "org_id = ?")
		args = append(args, *upd.OrgID)
	} else if upd.ClearOrg {
		sets = append(sets, "org_id = NULL")
	}
	if upd.Timezone != nil {
		sets = append(sets, "timezone = ?")
		args = append(args, *upd.Timezone)
	}
	if upd.LinkedinURL != nil {
		sets = append(sets, "linkedin_url = ?")
		args = append(args, *upd.LinkedinURL)
	}
	if upd.GithubURL != nil {
		sets = append(sets, "github_url = ?")
		args = append(args, *upd.GithubURL)
	}
	if upd.Notes != nil {
		sets = append(sets, "notes = ?")
		args = append(args, *upd.Notes)
	}
	if upd.Custom != nil {
		b, err := json.Marshal(upd.Custom)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		sets = append(sets, "custom = ?")
		args = append(args, string(b))
	}
	if len(sets) == 0 {
		_ = tx.Rollback()
		return nil
	}
	args = append(args, upd.ID)
	query := "UPDATE crm_contacts SET " + strings.Join(sets, ", ") + " WHERE id = ?"
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	newState, err := contactStateTx(ctx, tx, upd.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := recordAuditTx(ctx, tx, "contact", upd.ID, "update", oldState, newState); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Storage) SetPrimaryEmail(ctx context.Context, contactID, email string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	canonical := canonicalize(email)
	var emailID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM crm_contact_emails WHERE contact_id=? AND canonical_email=?`, contactID, canonical).Scan(&emailID)
	if err != nil {
		_ = tx.Rollback()
		if err == sql.ErrNoRows {
			return fmt.Errorf("email not found for contact %s", contactID)
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_contact_emails SET is_primary = CASE WHEN id=? THEN 1 ELSE 0 END WHERE contact_id=?`, emailID, contactID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Storage) SearchContacts(ctx context.Context, q string, limit int) ([]ContactSearchResult, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT c.id, c.full_name, IFNULL(o.name, '')
FROM crm_contacts_fts f
JOIN crm_contacts c ON c.rowid = f.rowid
LEFT JOIN crm_organizations o ON o.id = c.org_id
WHERE crm_contacts_fts MATCH ?
ORDER BY bm25(crm_contacts_fts)
LIMIT ?`, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ContactSearchResult
	for rows.Next() {
		var r ContactSearchResult
		if err := rows.Scan(&r.ID, &r.FullName, &r.Organization); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

type ContactSearchResult struct {
	ID           string
	FullName     string
	Organization string
}

func (s *Storage) RefreshContactFTS(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_contacts_fts`); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO crm_contacts_fts(rowid, full_name, emails, phones)
SELECT
  c.rowid,
  c.full_name,
  IFNULL((SELECT GROUP_CONCAT(email, ' ') FROM crm_contact_emails e WHERE e.contact_id=c.id), ''),
  IFNULL((SELECT GROUP_CONCAT(COALESCE(phone_normalized, phone_raw), ' ') FROM crm_contact_phones p WHERE p.contact_id=c.id), '')
FROM crm_contacts c`); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO crm_search_markers(kind, last_refresh)
VALUES('contacts_fts', CURRENT_TIMESTAMP)
ON CONFLICT(kind) DO UPDATE SET last_refresh=excluded.last_refresh`); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Storage) AddPhone(ctx context.Context, contactID, phone string, primary bool) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	if primary {
		if _, err := tx.ExecContext(ctx, `UPDATE crm_contact_phones SET is_primary=0 WHERE contact_id=?`, contactID); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO crm_contact_phones(contact_id, phone_raw, is_primary) VALUES(?, ?, ?)`, contactID, phone, boolToInt(primary))
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Storage) NormalizePhone(ctx context.Context, phoneID int64, normalized string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE crm_contact_phones SET phone_normalized=? WHERE id=?`, normalized, phoneID)
	return err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

type Organization struct {
	ID          string
	Name        string
	Website     string
	Industry    string
	SizeBucket  string
	BillingNote string
	Domain      string
	Custom      map[string]any
}

func (s *Storage) CreateOrganization(ctx context.Context, org Organization) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	id, err := s.createOrganizationTx(ctx, tx, org)
	if err != nil {
		_ = tx.Rollback()
		return "", err
	}
	return id, tx.Commit()
}

func (s *Storage) createOrganizationTx(ctx context.Context, tx *sql.Tx, org Organization) (string, error) {
	id := strings.TrimSpace(org.ID)
	if id == "" {
		id = NewID()
	}
	custom := "{}"
	if org.Custom != nil {
		b, err := json.Marshal(org.Custom)
		if err != nil {
			return "", err
		}
		custom = string(b)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO crm_organizations(id, name, website, industry, size_bucket, billing_note, domain, custom) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		id, org.Name, nullIfEmpty(org.Website), nullIfEmpty(org.Industry), nullIfEmpty(org.SizeBucket), org.BillingNote, nullIfEmpty(org.Domain), custom); err != nil {
		return "", err
	}
	if org.Domain != "" {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_org_domains(org_id, domain) VALUES(?, ?)`, id, org.Domain); err != nil {
			return "", err
		}
	}
	newState, err := organizationStateTx(ctx, tx, id)
	if err != nil {
		return "", err
	}
	if err := recordAuditTx(ctx, tx, "organization", id, "create", "{}", newState); err != nil {
		return "", err
	}
	return id, nil
}

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func (s *Storage) AttachDomain(ctx context.Context, orgID, domain string) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO crm_org_domains(org_id, domain) VALUES(?, ?)`, orgID, domain)
	return err
}

func (s *Storage) UpsertOrgByDomain(ctx context.Context, domain string, org Organization) (string, error) {
	if strings.TrimSpace(domain) == "" {
		return "", errors.New("domain required")
	}
	canonical := canonicalize(domain)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM crm_organizations WHERE canonical_domain=?`, canonical).Scan(&id)
	if err == sql.ErrNoRows {
		err = tx.QueryRowContext(ctx, `SELECT org_id FROM crm_org_domains WHERE canonical_domain=?`, canonical).Scan(&id)
	}
	switch {
	case err == sql.ErrNoRows:
		org.Domain = domain
		id, err = s.createOrganizationTx(ctx, tx, org)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
	case err != nil:
		_ = tx.Rollback()
		return "", err
	default:
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_org_domains(org_id, domain) VALUES(?, ?)`, id, domain); err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

type Pipeline struct {
	ID   string
	Name string
}

func (s *Storage) CreatePipeline(ctx context.Context, p Pipeline) (string, error) {
	id := strings.TrimSpace(p.ID)
	if id == "" {
		id = NewID()
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO crm_pipelines(id, name) VALUES(?, ?)`, id, p.Name); err != nil {
		return "", err
	}
	return id, nil
}

type Stage struct {
	ID         string
	PipelineID string
	Name       string
	Position   int
}

func (s *Storage) CreateStage(ctx context.Context, sInfo Stage) error {
	id := strings.TrimSpace(sInfo.ID)
	if id == "" {
		id = NewID()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO crm_stages(id, pipeline_id, name, position) VALUES(?, ?, ?, ?)`, id, sInfo.PipelineID, sInfo.Name, sInfo.Position)
	return err
}

type Deal struct {
	ID            string
	Title         string
	OrgID         sql.NullString
	Owner         sql.NullString
	AmountCents   sql.NullInt64
	Currency      string
	PipelineID    string
	StageID       string
	Status        string
	ExpectedClose sql.NullTime
	CloseReason   sql.NullString
	Custom        map[string]any
}

func (s *Storage) CreateDeal(ctx context.Context, d Deal) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	var stagePipeline string
	if err := tx.QueryRowContext(ctx, `SELECT pipeline_id FROM crm_stages WHERE id=?`, d.StageID).Scan(&stagePipeline); err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if stagePipeline != d.PipelineID {
		_ = tx.Rollback()
		return "", fmt.Errorf("stage %s does not belong to pipeline %s", d.StageID, d.PipelineID)
	}
	id := strings.TrimSpace(d.ID)
	if id == "" {
		id = NewID()
	}
	currency := d.Currency
	if strings.TrimSpace(currency) == "" {
		currency = "USD"
	}
	status := d.Status
	if strings.TrimSpace(status) == "" {
		status = "open"
	}
	custom := "{}"
	if d.Custom != nil {
		b, err := json.Marshal(d.Custom)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
		custom = string(b)
	}
	var expected interface{}
	if d.ExpectedClose.Valid {
		expected = d.ExpectedClose.Time.Format("2006-01-02")
	}
	var org interface{}
	if d.OrgID.Valid {
		org = d.OrgID.String
	}
	var owner interface{}
	if d.Owner.Valid {
		owner = d.Owner.String
	}
	var amount interface{}
	if d.AmountCents.Valid {
		amount = d.AmountCents.Int64
	}
	var reason interface{}
	if d.CloseReason.Valid {
		reason = d.CloseReason.String
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO crm_deals(id, title, org_id, owner, amount_cents, currency, pipeline_id, stage_id, status, expected_close_date, close_reason, custom) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, d.Title, org, owner, amount, currency, d.PipelineID, d.StageID, status, expected, reason, custom); err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Storage) MoveDealStage(ctx context.Context, dealID, stageID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var pipelineID string
	if err := tx.QueryRowContext(ctx, `SELECT pipeline_id FROM crm_deals WHERE id=?`, dealID).Scan(&pipelineID); err != nil {
		_ = tx.Rollback()
		return err
	}
	var stagePipeline string
	if err := tx.QueryRowContext(ctx, `SELECT pipeline_id FROM crm_stages WHERE id=?`, stageID).Scan(&stagePipeline); err != nil {
		_ = tx.Rollback()
		return err
	}
	if stagePipeline != pipelineID {
		_ = tx.Rollback()
		return fmt.Errorf("stage %s does not belong to pipeline %s", stageID, pipelineID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_deals SET stage_id=? WHERE id=?`, stageID, dealID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Storage) CloseDeal(ctx context.Context, dealID, status string, reason sql.NullString) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_deals SET status=?, close_reason=?, closed_at=CURRENT_TIMESTAMP WHERE id=?`, status, nullableString(reason), dealID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func nullableString(ns sql.NullString) interface{} {
	if ns.Valid {
		return ns.String
	}
	return nil
}

type Activity struct {
	ID         string
	Type       string
	Direction  sql.NullString
	Summary    string
	Payload    map[string]any
	OccurredAt time.Time
	ContactID  sql.NullString
	OrgID      sql.NullString
	DealID     sql.NullString
	IssueID    sql.NullString
}

func (s *Storage) RecordActivity(ctx context.Context, a Activity) error {
	if a.OccurredAt.IsZero() {
		return errors.New("occurred_at required")
	}
	id := strings.TrimSpace(a.ID)
	if id == "" {
		id = NewID()
	}
	payload := ""
	if a.Payload != nil {
		b, err := json.Marshal(a.Payload)
		if err != nil {
			return err
		}
		payload = string(b)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO crm_activities(id, type, direction, summary, payload, occurred_at, contact_id, org_id, deal_id, issue_id) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, a.Type, nullableString(a.Direction), nullIfEmpty(a.Summary), nullIfEmpty(payload), a.OccurredAt.UTC(), nullableString(a.ContactID), nullableString(a.OrgID), nullableString(a.DealID), nullableString(a.IssueID))
	return err
}

func (s *Storage) LinkIssueToContact(ctx context.Context, issueID, contactID, role string) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO crm_issue_contacts(issue_id, contact_id, role) VALUES(?, ?, ?)`, issueID, contactID, role)
	return err
}

type IssueSummary struct {
	ID           string
	Title        string
	Priority     int
	Status       string
	Organization string
}

func (s *Storage) GetOpenIssuesByOrganization(ctx context.Context, orgID string) ([]IssueSummary, error) {
	if strings.TrimSpace(orgID) == "" {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT i.id, i.title, i.priority, i.status, o.name
FROM issues i
JOIN crm_issue_contacts ic ON ic.issue_id = i.id
JOIN crm_contacts c ON c.id = ic.contact_id
JOIN crm_organizations o ON o.id = c.org_id
WHERE i.status IN ('open','in_progress','blocked')
  AND o.id = ?
ORDER BY i.priority ASC, i.created_at ASC`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []IssueSummary
	for rows.Next() {
		var r IssueSummary
		if err := rows.Scan(&r.ID, &r.Title, &r.Priority, &r.Status, &r.Organization); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, rows.Err()
}

type DealStageSummary struct {
	Pipeline      string
	StagePosition int
	StageName     string
	Deals         int
}

func (s *Storage) GetDealsByStage(ctx context.Context) ([]DealStageSummary, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.name, s.position, s.name, COUNT(*)
FROM crm_deals d
JOIN crm_stages s ON s.id = d.stage_id
JOIN crm_pipelines p ON p.id = d.pipeline_id
WHERE d.status = 'open'
GROUP BY p.id, s.position
ORDER BY p.name, s.position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []DealStageSummary
	for rows.Next() {
		var r DealStageSummary
		if err := rows.Scan(&r.Pipeline, &r.StagePosition, &r.StageName, &r.Deals); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, rows.Err()
}

type ActivityRecord struct {
	ID         string
	Type       string
	Summary    sql.NullString
	OccurredAt time.Time
}

func (s *Storage) GetRecentActivitiesForContact(ctx context.Context, contactID string, limit int) ([]ActivityRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, type, summary, occurred_at FROM crm_activities WHERE contact_id=? ORDER BY occurred_at DESC LIMIT ?`, contactID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ActivityRecord
	for rows.Next() {
		var r ActivityRecord
		if err := rows.Scan(&r.ID, &r.Type, &r.Summary, &r.OccurredAt); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, rows.Err()
}

func (s *Storage) GetRecentActivitiesForOrganization(ctx context.Context, orgID string, limit int) ([]ActivityRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, type, summary, occurred_at FROM crm_activities WHERE org_id=? ORDER BY occurred_at DESC LIMIT ?`, orgID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ActivityRecord
	for rows.Next() {
		var r ActivityRecord
		if err := rows.Scan(&r.ID, &r.Type, &r.Summary, &r.OccurredAt); err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, rows.Err()
}

func (s *Storage) MergeContacts(ctx context.Context, targetID, sourceID string) error {
	if targetID == sourceID {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_contact_emails(contact_id, email, is_primary, is_verified)
SELECT ?, email, is_primary, is_verified FROM crm_contact_emails WHERE contact_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_contact_emails WHERE contact_id=?`, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_contact_phones(contact_id, phone_raw, phone_normalized, is_primary)
SELECT ?, phone_raw, phone_normalized, is_primary FROM crm_contact_phones WHERE contact_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_contact_phones WHERE contact_id=?`, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_issue_contacts(issue_id, contact_id, role)
SELECT issue_id, ?, role FROM crm_issue_contacts WHERE contact_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_issue_contacts WHERE contact_id=?`, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_activities SET contact_id=? WHERE contact_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_contact_tags(contact_id, tag_id)
SELECT ?, tag_id FROM crm_contact_tags WHERE contact_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	oldState, err := contactStateTx(ctx, tx, sourceID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_contacts WHERE id=?`, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	targetState, err := contactStateTx(ctx, tx, targetID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := recordAuditTx(ctx, tx, "contact", targetID, "merge", oldState, targetState); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Storage) MergeOrganizations(ctx context.Context, targetID, sourceID string) error {
	if targetID == sourceID {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_contacts SET org_id=? WHERE org_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_deals SET org_id=? WHERE org_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE crm_activities SET org_id=? WHERE org_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_org_tags(org_id, tag_id)
SELECT ?, tag_id FROM crm_org_tags WHERE org_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO crm_org_domains(org_id, domain)
SELECT ?, domain FROM crm_org_domains WHERE org_id=?`, targetID, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_org_domains WHERE org_id=?`, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	oldState, err := organizationStateTx(ctx, tx, sourceID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM crm_organizations WHERE id=?`, sourceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	targetState, err := organizationStateTx(ctx, tx, targetID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := recordAuditTx(ctx, tx, "organization", targetID, "merge", oldState, targetState); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func contactStateTx(ctx context.Context, tx *sql.Tx, id string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT json_object(
      'id', id,
      'full_name', full_name,
      'given_name', given_name,
      'family_name', family_name,
      'title', title,
      'org_id', org_id,
      'timezone', timezone,
      'linkedin_url', linkedin_url,
      'github_url', github_url,
      'notes', notes,
      'custom', custom
    ) FROM crm_contacts WHERE id=?`, id).Scan(&state)
	if err != nil {
		return "", err
	}
	return state, nil
}

func organizationStateTx(ctx context.Context, tx *sql.Tx, id string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT json_object(
      'id', id,
      'name', name,
      'website', website,
      'industry', industry,
      'size_bucket', size_bucket,
      'billing_note', billing_note,
      'domain', domain,
      'custom', custom
    ) FROM crm_organizations WHERE id=?`, id).Scan(&state)
	if err != nil {
		return "", err
	}
	return state, nil
}

func recordAuditTx(ctx context.Context, tx *sql.Tx, entity, entityID, event, oldState, newState string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO crm_audits(entity, entity_id, event, old, new) VALUES(?, ?, ?, ?, ?)`, entity, entityID, event, oldState, newState)
	return err
}
