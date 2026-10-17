package team

// WaitingStatusSQL shares the approval-before-input precedence between the run
// projection and the member view. Arguments are trusted SQL expressions, never input.
// Empty member aggregates all members of the run.
func WaitingStatusSQL(tenant, run, member, fallback string) string {
	predicate := "i.tenant_id=" + tenant + " AND i.run_id=" + run + " AND i.status IN ('pending','escalated')"
	if member != "" {
		predicate += " AND i.member=" + member
	}
	return "CASE WHEN EXISTS(SELECT 1 FROM interactions i WHERE " + predicate +
		" AND i.kind<>'ask_user') THEN 'waiting_approval' WHEN EXISTS(SELECT 1 FROM interactions i WHERE " + predicate +
		" AND i.kind='ask_user') THEN 'waiting_input' ELSE " + fallback + " END"
}
