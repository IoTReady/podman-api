package instance

import (
	"context"
	"errors"
)

// Plan issue codes. Stable strings exposed in the POST /evacuate/plan response
// and documented in the OpenAPI spec.
const (
	codeDestinationDraining = "destination_draining"
	codeInstanceExists      = "instance_exists"
	codeHostSecretMissing   = "host_secret_missing"
	codePortConflict        = "port_conflict"
	codeCheckError          = "check_error"
)

// EvacuationPlan is the result of PlanEvacuation: the resolved per-instance
// moves plus, for each, whether the destination would currently accept it.
type EvacuationPlan struct {
	FromHost string        `json:"from_host"`
	Moves    []PlannedMove `json:"moves"`
}

// PlannedMove is one instance's planned move and its preflight verdict.
type PlannedMove struct {
	Slug     string      `json:"slug"`
	Template string      `json:"template"`
	ToHost   string      `json:"to_host"`
	OK       bool        `json:"ok"` // true iff Issues is empty
	Issues   []PlanIssue `json:"issues"`
}

// PlanIssue is a single reason a move is not clean: a blocking destination
// condition or an inconclusive (check_error) check.
type PlanIssue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PlanEvacuation previews an evacuation without mutating anything or enqueuing a
// job. It defers to ResolveEvacuation for static map validation (returning the
// same sentinel errors the real POST /evacuate would), then runs the live
// destination preflight checks per resolved move, collecting every problem. A
// move with no issues would currently be accepted by the destination.
func (s *Service) PlanEvacuation(ctx context.Context, req EvacuateRequest) (EvacuationPlan, error) {
	moves, err := s.ResolveEvacuation(ctx, req)
	if err != nil {
		return EvacuationPlan{}, err
	}
	plan := EvacuationPlan{FromHost: req.FromHost, Moves: make([]PlannedMove, 0, len(moves))}
	for _, m := range moves {
		plan.Moves = append(plan.Moves, s.planMove(ctx, m))
	}
	return plan, nil
}

// planMove runs the live preflight for one resolved move and classifies the
// result. Spec-load / template-lookup failures are reported as check_error so
// the move is still surfaced rather than blanking the whole plan.
func (s *Service) planMove(ctx context.Context, m MigrateRequest) PlannedMove {
	pm := PlannedMove{Slug: m.Slug, Template: m.Template, ToHost: m.ToHost, Issues: []PlanIssue{}}
	tmpl, err := s.lookup(m.ToHost, m.Template)
	if err != nil {
		pm.Issues = append(pm.Issues, PlanIssue{Code: codeCheckError, Message: err.Error()})
		return pm
	}
	spec, err := s.store.GetSpec(ctx, m.FromHost, m.Template, m.Slug)
	if err != nil {
		pm.Issues = append(pm.Issues, PlanIssue{Code: codeCheckError, Message: err.Error()})
		return pm
	}
	eff := mergeParams(spec.Parameters, m.Parameters)
	eff["slug"] = m.Slug // canonical slug always wins; pod name must match podName()
	for _, e := range s.preflightIssues(ctx, m, tmpl, eff) {
		pm.Issues = append(pm.Issues, classifyPlanIssue(e))
	}
	pm.OK = len(pm.Issues) == 0
	return pm
}

// classifyPlanIssue maps a preflight error to a stable plan issue code. Anything
// that is not a known blocking sentinel is an inconclusive check_error.
func classifyPlanIssue(err error) PlanIssue {
	switch {
	case errors.Is(err, ErrHostDraining):
		return PlanIssue{Code: codeDestinationDraining, Message: err.Error()}
	case errors.Is(err, ErrInstanceExists):
		return PlanIssue{Code: codeInstanceExists, Message: err.Error()}
	case errors.Is(err, ErrHostSecretMissing):
		return PlanIssue{Code: codeHostSecretMissing, Message: err.Error()}
	case errors.Is(err, ErrPortConflict):
		return PlanIssue{Code: codePortConflict, Message: err.Error()}
	default:
		return PlanIssue{Code: codeCheckError, Message: err.Error()}
	}
}
