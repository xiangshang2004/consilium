## ADDED Requirements

### Requirement: Show evidence-backed conversation and task status
The conversation and task drawer MUST display consistent status from one snapshot projection and preserve the distinction between delivery, task activity, standby, and quota.

#### Scenario: Notification sent without a claim
- **WHEN** a notification is sent but the assigned agent has not claimed the task
- **THEN** the conversation and task drawer MUST present 「待认领」 and the last delivery fact without presenting an execution status.

#### Scenario: Account changes during a queued task
- **WHEN** Cockpit identifies an account switch and subsequently observes the new account quota
- **THEN** the view MUST wait for the matching sample before presenting the new quota and MUST preserve the task status until the respective task event arrives.

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
