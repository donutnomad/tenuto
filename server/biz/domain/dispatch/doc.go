// Package dispatch owns task routing and delivery.
//
// It selects queues and worker routes, creates delivery leases, signs task
// tokens, and rebuilds pending deliveries after a process restart.
package dispatch
