// Package execution owns Run state transitions and History facts.
//
// It prepares commands, commits History and mutable state atomically, records
// task lifecycle events, and exposes projections used by dispatchers.
package execution
