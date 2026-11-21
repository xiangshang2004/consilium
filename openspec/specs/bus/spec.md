# bus Specification

## Purpose
TBD - created by archiving change conversation-status-sync. Update Purpose after archive.

## Requirements

### Requirement: Preserve delivery evidence independently from task execution
The bus MUST retain the result and time of wake and notification attempts, receipt acknowledgement, task claim, progress, and completion as distinct facts associated with the relevant task and attempt.

#### Scenario: A cold notification follows an unacknowledged hot wake
- **WHEN** the bus sends a hot wake and later escalates to a system notification
- **THEN** the bus MUST expose both delivery attempts and MUST leave the task queued until an agent claims it.

#### Scenario: A prior attempt reports late
- **WHEN** an agent reports progress or completion for an attempt that has been closed or replaced
- **THEN** the bus MUST reject the update and MUST preserve the currently visible task state.

### Requirement: Reconcile a redundant attempt with a recorded result
The conversation coordinator MUST be able to close an outstanding duplicate attempt against a confirmed result in the same conversation without losing its execution record.

#### Scenario: A matching result has already been delivered
- **WHEN** the coordinator closes a duplicate and specifies its active attempt and related completed result
- **THEN** the bus MUST atomically record the reason and link, preserve the original attempt history, and reject later writes from that attempt.

#### Scenario: The selection has become stale
- **WHEN** the selected attempt has changed or the proposed related result belongs to a different conversation
- **THEN** the bus MUST reject the reconciliation and preserve both tasks.
