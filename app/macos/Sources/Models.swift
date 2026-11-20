import Foundation

struct DaemonInfo: Decodable {
    let version: String?
    let uptime_seconds: Double?
    let host: String?
    let port: Int?
}

struct PeerInfo: Decodable {
    let peer_id: String
    let status: String
    let last_seen_at: String?
    let session_label: String?
}

struct QuotaPeerInfo: Decodable {
    let peer_id: String
    let min_remaining_percent: Double?
    let current_account_unknown: Bool?
    let account_switch_suspected: Bool?
    let active_account_alias: String?
    let switch_hint: String?
}

struct EventPayload: Decodable {
    let task_id: String?
    let mission_id: String?
    let title: String?
    let prompt: String?
    let summary: String?
    let progress_note: String?
    let result_summary: String?
    let reason: String?
    let question: String?
    let workspace_path: String?
    let delivery_dry_run: Bool?
}

struct EventItem: Decodable {
    let event_id: Int
    let kind: String
    let from_peer: String?
    let to_peer: String?
    let acked_at: String?
    let created_at: String?
    let payload: EventPayload?
}

struct TaskItem: Decodable {
    let task_id: String
    let assignee_peer: String?
    let status: String?
    let title: String?
}

struct MissionInfo: Decodable {
    let mission_id: String
    let title: String?
    let goal: String?
    let status: String?
    let workspace_path: String?
    let tasks: [TaskItem]?
    let events: [EventItem]?
}

struct DashboardState: Decodable {
    let daemon: DaemonInfo?
    let peers: [PeerInfo]?
    let quotas: [QuotaPeerInfo]?
    let missions: [MissionInfo]?
    let state_version: Int?

    var maxEventId: Int {
        var best = 0
        for mission in missions ?? [] {
            for ev in mission.events ?? [] where ev.event_id > best {
                best = ev.event_id
            }
        }
        return best
    }

    var primaryMission: MissionInfo? {
        guard let list = missions, !list.isEmpty else { return nil }
        return list.first(where: { $0.status == "active" || $0.status == "ready_to_report" }) ?? list.first
    }
}

struct SSEUpdateEnvelope: Decodable {
    let reason: String?
    let version: Int?
    let state: DashboardState?
}

struct PeerDisplayMeta {
    let peerId: String
    let label: String
    let appName: String

    static let ordered: [PeerDisplayMeta] = [
        PeerDisplayMeta(peerId: "claude", label: "Claude", appName: "Claude.app · Code"),
        PeerDisplayMeta(peerId: "chatgpt", label: "Codex", appName: "ChatGPT.app · Codex"),
        PeerDisplayMeta(peerId: "antigravity", label: "Antigravity", appName: "Antigravity.app"),
    ]
}

struct SmokeReport: Encodable {
    let ok: Bool
    let owns_daemon: Bool
    let daemon_pid: Int32?
    let port: Int
    let state_dir: String
    let rendezvous_dir: String
    let health_status: Int
    let login_status: Int
    let state_status: Int
    let sse_connected: Bool
    let webview_logged_in: Bool
    let webview_conn_text: String
    let mission_count: Int
    let error: String?
}
