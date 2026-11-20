import Cocoa
import Foundation
import UserNotifications

final class NotificationManager: NSObject, UNUserNotificationCenterDelegate {
    private let config: AppConfig
    private var center: UNUserNotificationCenter?
    private var highestSeenEventId: Int?
    private let notableKinds: Set<String> = [
        "task_assigned",
        "review_request",
        "loop_guard_reached",
        "task_stale",
    ]

    var onFocusConsilium: (() -> Void)?

    init(config: AppConfig) {
        self.config = config
        super.init()
        if !config.noNotifications && Bundle.main.bundleIdentifier != nil {
            let uc = UNUserNotificationCenter.current()
            uc.delegate = self
            self.center = uc
        }
    }

    func requestAuthorizationIfNeeded() {
        guard let uc = center, !config.noNotifications, !config.dryRun else { return }
        uc.requestAuthorization(options: [.alert, .sound]) { _, _ in }
    }

    func processState(_ state: DashboardState) {
        let maxId = state.maxEventId
        guard let prevHigh = highestSeenEventId else {
            highestSeenEventId = maxId
            return
        }
        guard maxId > prevHigh else { return }
        highestSeenEventId = maxId

        guard let uc = center, !config.noNotifications, !config.dryRun else { return }

        var newEvents: [(event: EventItem, mission: MissionInfo)] = []
        for mission in state.missions ?? [] {
            for ev in mission.events ?? [] where ev.event_id > prevHigh {
                if notableKinds.contains(ev.kind) && ((ev.acked_at ?? "").isEmpty) {
                    newEvents.append((ev, mission))
                }
            }
        }

        newEvents.sort { $0.event.event_id < $1.event.event_id }
        for item in newEvents.suffix(3) {
            postNotification(for: item.event, mission: item.mission, center: uc)
        }
    }

    private func postNotification(for ev: EventItem, mission: MissionInfo, center: UNUserNotificationCenter) {
        let toPeer = (ev.to_peer ?? "").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let content = UNMutableNotificationContent()
        content.title = "Consilium · \(mission.title ?? mission.mission_id)"

        let p = ev.payload
        let promptText = p?.prompt ?? p?.question ?? p?.summary ?? p?.reason ?? "请调用 consilium_pickup_inbox 认领任务"
        switch ev.kind {
        case "task_assigned":
            content.subtitle = "新任务待认领 (@\(peerDisplayName(toPeer)))"
        case "review_request":
            content.subtitle = "交叉复核请求 (@\(peerDisplayName(toPeer)))"
        case "loop_guard_reached":
            content.subtitle = "已触达复核轮次上限"
        case "task_stale":
            content.subtitle = "任务进度超时提醒"
        default:
            content.subtitle = "会诊事件更新"
        }
        content.body = String(promptText.prefix(180))
        content.sound = .default
        content.userInfo = [
            "peer_id": toPeer,
            "mission_id": mission.mission_id,
            "task_id": p?.task_id ?? "",
            "workspace_path": mission.workspace_path ?? "",
            "prompt": String(promptText.prefix(500)),
        ]

        let req = UNNotificationRequest(
            identifier: "consilium-ev-\(ev.event_id)",
            content: content,
            trigger: nil
        )
        center.add(req, withCompletionHandler: nil)
    }

    // Verified R003 deep links ONLY: claude://code/new and codex://new?prompt=
    static func buildVerifiedDeepLink(peerId: String, prompt: String, workspacePath: String) -> URL? {
        let peer = peerId.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let clipPrompt = String(prompt.prefix(1200))

        if peer == "claude" {
            var comps = URLComponents()
            comps.scheme = "claude"
            comps.host = "code"
            comps.path = "/new"
            if !clipPrompt.isEmpty {
                comps.queryItems = [URLQueryItem(name: "q", value: clipPrompt)]
            }
            guard let url = comps.url, url.scheme == "claude" else { return nil }
            return url
        }

        if peer == "chatgpt" || peer == "codex" {
            var comps = URLComponents()
            comps.scheme = "codex"
            comps.host = "new"
            var items = [URLQueryItem(name: "prompt", value: clipPrompt)]
            if !workspacePath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                items.append(URLQueryItem(name: "path", value: workspacePath))
            }
            comps.queryItems = items
            guard let url = comps.url, url.scheme == "codex" else { return nil }
            return url
        }

        return nil
    }

    // MARK: - UNUserNotificationCenterDelegate

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        completionHandler([.banner, .sound])
    }

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping () -> Void
    ) {
        defer { completionHandler() }
        let info = response.notification.request.content.userInfo
        let peer = (info["peer_id"] as? String) ?? ""
        let prompt = (info["prompt"] as? String) ?? "检查 Consilium 收件箱并调用 consilium_pickup_inbox 认领任务"
        let ws = (info["workspace_path"] as? String) ?? ""

        if let url = NotificationManager.buildVerifiedDeepLink(peerId: peer, prompt: prompt, workspacePath: ws) {
            DispatchQueue.main.async {
                NSWorkspace.shared.open(url)
            }
            return
        }

        if peer == "antigravity" {
            DispatchQueue.main.async { [weak self] in
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(prompt, forType: .string)
                self?.onFocusConsilium?()
            }
        } else {
            DispatchQueue.main.async { [weak self] in
                self?.onFocusConsilium?()
            }
        }
    }

    private func peerDisplayName(_ peerId: String) -> String {
        switch peerId {
        case "claude": return "Claude"
        case "chatgpt", "codex": return "Codex"
        case "antigravity": return "Antigravity"
        default: return peerId.isEmpty ? "Peer" : peerId
        }
    }
}
