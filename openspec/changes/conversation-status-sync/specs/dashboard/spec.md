## ADDED Requirements

### Requirement: Show evidence-backed conversation and task status
The conversation and task drawer MUST display consistent status from one snapshot projection and preserve the distinction between delivery, task activity, standby, and quota.

#### Scenario: Notification sent without a claim
- **WHEN** a notification is sent but the assigned agent has not claimed the task
- **THEN** the conversation and task drawer MUST present 「待认领」 and the last delivery fact without presenting an execution status.

#### Scenario: Account changes during a queued task
- **WHEN** Cockpit identifies an account switch and subsequently observes the new account quota
- **THEN** the view MUST wait for the matching sample before presenting the new quota and MUST preserve the task status until the respective task event arrives.

#### Scenario: The conversation sidebar is hidden
- **WHEN** the sidebar is closed or the window uses its narrow layout
- **THEN** the conversation header MUST still show the current quota summary and offer access to account details. It MUST distinguish a known zero balance from an unconfirmed account or a missing sample.

#### Scenario: Old progress is still on record
- **WHEN** a task has a prior progress update but lacks a completion result
- **THEN** the task card MUST show the last report time and use the authoritative lease status for an expiry indication.

#### Scenario: An automatic follow-up uses the conversation coordinator
- **WHEN** a sent follow-up is routed to the coordinator or another eligible peer
- **THEN** the conversation MUST display the actual recipient and routing reason on that message.

### Requirement: Recover the conversation view after a server restart
The dashboard MUST accept a fresh state snapshot after a service restart, independently of the previous instance's version number.

#### Scenario: A new instance starts at a lower version
- **WHEN** the client reconnects to a new service instance with a lower state version
- **THEN** the client MUST reset the version baseline from the first full snapshot, ignore late frames from the previous connection, and preserve draft text and the selected conversation.

#### Scenario: Polling and the SSE connection overlap
- **WHEN** the browser polls for state and reconnects its event stream
- **THEN** snapshots from both transports MUST carry a version from the same ordered sequence within one daemon instance, and a reconnected stream MUST begin with a complete snapshot.

### Requirement: Attach images to a conversation message
The composer MUST support selecting, dragging, and pasting image files and MUST show previews and removal actions before sending. Uploaded images MUST be linked to the committed message and made available to its intended tasks.

#### Scenario: An image upload is interrupted
- **WHEN** a file fails to upload or the message response is lost
- **THEN** the composer MUST preserve the draft and MUST allow resuming with the same successful attachments and request identifier.

#### Scenario: A sent message is opened again
- **WHEN** a user returns to the conversation after reconnecting
- **THEN** its committed images MUST be available from the message and failed attachments MUST retain an actionable explanation.

### Requirement: Provide an accessible slash command menu
The composer MUST expose the existing local slash commands through a visible trigger and keyboard input.

#### Scenario: Slash commands are used with a draft
- **WHEN** a user opens the slash menu and selects a local command
- **THEN** the dashboard MUST perform the action in the current window, preserve any draft text or images, and return focus appropriately.

#### Scenario: Text starts with an unrecognized slash command
- **WHEN** no slash menu command is selected or matched and the user explicitly sends the text
- **THEN** the composer MUST allow ordinary message submission without treating the text as a local action.

#### Scenario: Several conversations are composed in the same window
- **WHEN** the user creates a new conversation or starts one in the current conversation's folder
- **THEN** the composer MUST create an independent draft and keep existing drafts available in the conversation list.

#### Scenario: Parallel routing or a native session is chosen
- **WHEN** the user selects parallel routing or opens a peer's dedicated-session command
- **THEN** the composer MUST select all three recipients for parallel routing or offer the selected peer's workspace-aware binding flow, preserving existing bindings.

#### Scenario: The user selects plan or goal
- **WHEN** a message is sent with plan or goal mode selected
- **THEN** the UI MUST preserve the original message text, display the chosen mode, and deliver corresponding task instructions. The composer MUST clear the mode only after successful delivery or user cancellation.

#### Scenario: The user requests several native subagents
- **WHEN** the user configures a count using the native subagents command and sends a message
- **THEN** a single recipient MUST receive one task with the requested count and instructions to invoke its own native subagents. The UI MUST distinguish the request from the execution reported by that recipient.

#### Scenario: The user requests a public process summary
- **WHEN** the assigned peer reports progress for a message with the process summary command selected
- **THEN** the dashboard MUST keep the submitted progress within the current task attempt and make it viewable in task details. The task instructions MUST request public observations, decisions, and verification results.
