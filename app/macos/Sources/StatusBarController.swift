import Cocoa
import Foundation

final class StatusBarController: NSObject {
    private let statusItem: NSStatusItem
    private let menu = NSMenu()
    private let headerItem = NSMenuItem(title: "Consilium · 连接中", action: nil, keyEquivalent: "")
    private let missionItem = NSMenuItem(title: "暂无进行中会诊", action: #selector(handleOpen), keyEquivalent: "")
    private var peerItems: [String: NSMenuItem] = [:]

    private var connectionLabel: String = "连接中"
    private var lastState: DashboardState?

    var onOpenRequested: (() -> Void)?
    var onQuitRequested: (() -> Void)?

    override init() {
        self.statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        super.init()
        configureStatusButton()
        buildMenu()
    }

    private func configureStatusButton() {
        guard let button = statusItem.button else { return }
        if let img = loadTemplateIcon() {
            img.isTemplate = true
            img.size = NSSize(width: 18, height: 18)
            button.image = img
        } else {
            button.title = "C"
        }
        button.toolTip = "Consilium 三端会诊"
        button.setAccessibilityLabel("Consilium")
    }

    private func loadTemplateIcon() -> NSImage? {
        if let url = Bundle.main.url(forResource: "StatusIconTemplate@2x", withExtension: "png"),
           let img2x = NSImage(contentsOf: url) {
            return img2x
        }
        if let url = Bundle.main.url(forResource: "StatusIconTemplate", withExtension: "png"),
           let img1x = NSImage(contentsOf: url) {
            return img1x
        }
        return nil
    }

    private func buildMenu() {
        menu.autoenablesItems = false
        headerItem.isEnabled = false
        menu.addItem(headerItem)
        menu.addItem(.separator())

        for meta in PeerDisplayMeta.ordered {
            let item = NSMenuItem(
                title: "\(meta.label) · 离线 · 账号未知",
                action: #selector(handleOpen),
                keyEquivalent: ""
            )
            item.target = self
            item.image = StatusBarController.dotImage(forStatus: "offline")
            peerItems[meta.peerId] = item
            menu.addItem(item)
        }

        menu.addItem(.separator())
        missionItem.target = self
        menu.addItem(missionItem)
        menu.addItem(.separator())

        let openItem = NSMenuItem(title: "打开 Consilium", action: #selector(handleOpen), keyEquivalent: "o")
        openItem.target = self
        menu.addItem(openItem)

        let quitItem = NSMenuItem(title: "退出", action: #selector(handleQuit), keyEquivalent: "q")
        quitItem.target = self
        menu.addItem(quitItem)

        statusItem.menu = menu
    }

    func updateConnectionState(_ text: String) {
        connectionLabel = text
        headerItem.title = "Consilium · \(text)"
    }

    func updateState(_ state: DashboardState) {
        lastState = state
        headerItem.title = "Consilium · \(connectionLabel)"

        var peersById: [String: PeerInfo] = [:]
        for p in state.peers ?? [] {
            peersById[p.peer_id] = p
        }
        var quotasById: [String: QuotaPeerInfo] = [:]
        for q in state.quotas ?? [] {
            quotasById[q.peer_id] = q
        }

        for meta in PeerDisplayMeta.ordered {
            guard let item = peerItems[meta.peerId] else { continue }
            let stKey = peersById[meta.peerId]?.status ?? "offline"
            let stText = localizedPeerStatus(stKey)
            let q = quotasById[meta.peerId]
            let qText = localizedQuota(q)
            item.title = "\(meta.label) · \(stText) · \(qText)"
            item.image = StatusBarController.dotImage(forStatus: stKey)
        }

        if let m = state.primaryMission {
            let title = (m.title ?? m.mission_id)
            let clipped = title.count > 24 ? String(title.prefix(24)) + "…" : title
            missionItem.title = "当前会诊：\(clipped)"
        } else {
            missionItem.title = "暂无进行中会诊"
        }
    }

    @objc private func handleOpen() {
        onOpenRequested?()
    }

    @objc private func handleQuit() {
        onQuitRequested?()
    }

    private func localizedPeerStatus(_ status: String) -> String {
        switch status {
        case "standby": return "待命"
        case "working": return "工作中"
        case "online": return "在线"
        case "stale": return "陈旧"
        default: return "离线"
        }
    }

    private func localizedQuota(_ q: QuotaPeerInfo?) -> String {
        guard let q = q else { return "账号未知" }
        if q.account_switch_suspected == true { return "可能已切换" }
        if q.current_account_unknown == true { return "账号未知" }
        guard let pct = q.min_remaining_percent else { return "账号未知" }
        return "\(Int(pct.rounded()))%"
    }

    static func dotImage(forStatus status: String) -> NSImage {
        let size = NSSize(width: 10, height: 10)
        let img = NSImage(size: size)
        img.lockFocus()
        let color: NSColor
        switch status {
        case "standby":
            color = NSColor(calibratedRed: 0.086, green: 0.639, blue: 0.290, alpha: 1.0) // #16A34A
        case "working":
            color = NSColor(calibratedRed: 0.145, green: 0.388, blue: 0.922, alpha: 1.0) // #2563EB
        case "online":
            color = NSColor(calibratedRed: 0.051, green: 0.580, blue: 0.533, alpha: 1.0) // #0D9488
        case "stale":
            color = NSColor(calibratedRed: 0.851, green: 0.467, blue: 0.024, alpha: 1.0) // #D97706
        default:
            color = NSColor(calibratedRed: 0.50, green: 0.55, blue: 0.61, alpha: 1.0)
        }
        color.setFill()
        NSBezierPath(ovalIn: NSRect(x: 1, y: 1, width: 8, height: 8)).fill()
        img.unlockFocus()
        img.isTemplate = false
        return img
    }

    /// Renders a pixel-accurate Retina menu bar + dropdown PNG directly from live NSMenu items.
    func captureDropdownSnapshot(to path: String) throws {
        let width: CGFloat = 410
        let barHeight: CGFloat = 28
        let menuWidth: CGFloat = 324
        let paddingV: CGFloat = 10
        let rowHeight: CGFloat = 26
        let sepHeight: CGFloat = 10

        var itemHeights: [CGFloat] = []
        for item in menu.items {
            itemHeights.append(item.isSeparatorItem ? sepHeight : rowHeight)
        }
        let menuHeight = itemHeights.reduce(0, +) + paddingV * 2
        let totalHeight = barHeight + 10 + menuHeight + 24

        let scale: CGFloat = 2.0
        guard let rep = NSBitmapImageRep(
            bitmapDataPlanes: nil,
            pixelsWide: Int(width * scale),
            pixelsHigh: Int(totalHeight * scale),
            bitsPerSample: 8,
            samplesPerPixel: 4,
            hasAlpha: true,
            isPlanar: false,
            colorSpaceName: .deviceRGB,
            bytesPerRow: 0,
            bitsPerPixel: 0
        ) else {
            throw NSError(domain: "Consilium", code: 1, userInfo: [NSLocalizedDescriptionKey: "Bitmap alloc failed"])
        }
        rep.size = NSSize(width: width, height: totalHeight)

        NSGraphicsContext.saveGraphicsState()
        guard let ctx = NSGraphicsContext(bitmapImageRep: rep) else {
            NSGraphicsContext.restoreGraphicsState()
            throw NSError(domain: "Consilium", code: 2, userInfo: [NSLocalizedDescriptionKey: "GraphicsContext failed"])
        }
        NSGraphicsContext.current = ctx

        // Backdrop canvas
        NSColor(calibratedRed: 0.91, green: 0.93, blue: 0.96, alpha: 1.0).setFill()
        NSRect(x: 0, y: 0, width: width, height: totalHeight).fill()

        // Top macOS Menu Bar Strip
        let barY = totalHeight - barHeight
        NSColor(calibratedWhite: 0.98, alpha: 0.94).setFill()
        NSRect(x: 0, y: barY, width: width, height: barHeight).fill()
        NSColor(calibratedWhite: 0.0, alpha: 0.12).setFill()
        NSRect(x: 0, y: barY, width: width, height: 1).fill()

        // Highlighted StatusItem capsule on the right side of the menu bar
        let iconPillRect = NSRect(x: width - 56, y: barY + 3, width: 32, height: barHeight - 6)
        NSColor(calibratedRed: 0.28, green: 0.39, blue: 0.89, alpha: 0.16).setFill()
        NSBezierPath(roundedRect: iconPillRect, xRadius: 5, yRadius: 5).fill()
        if let iconImg = loadTemplateIcon() {
            iconImg.draw(
                in: NSRect(x: iconPillRect.midX - 9, y: iconPillRect.midY - 9, width: 18, height: 18),
                from: .zero,
                operation: .sourceOver,
                fraction: 1.0
            )
        }

        // Dropdown NSMenu panel anchored below the StatusItem
        let panelX = width - menuWidth - 14
        let panelY = totalHeight - barHeight - 6 - menuHeight
        let panelRect = NSRect(x: panelX, y: panelY, width: menuWidth, height: menuHeight)

        ctx.saveGraphicsState()
        let shadow = NSShadow()
        shadow.shadowColor = NSColor(calibratedWhite: 0.0, alpha: 0.18)
        shadow.shadowOffset = NSSize(width: 0, height: -4)
        shadow.shadowBlurRadius = 14
        shadow.set()
        NSColor(calibratedWhite: 0.99, alpha: 0.98).setFill()
        NSBezierPath(roundedRect: panelRect, xRadius: 10, yRadius: 10).fill()
        ctx.restoreGraphicsState()

        NSColor(calibratedWhite: 0.0, alpha: 0.12).setStroke()
        let borderPath = NSBezierPath(roundedRect: panelRect.insetBy(dx: 0.5, dy: 0.5), xRadius: 10, yRadius: 10)
        borderPath.lineWidth = 1.0
        borderPath.stroke()

        // Draw live NSMenu items top-to-bottom
        var cursorY = panelRect.maxY - paddingV
        for (idx, item) in menu.items.enumerated() {
            let h = itemHeights[idx]
            cursorY -= h
            if item.isSeparatorItem {
                NSColor(calibratedWhite: 0.0, alpha: 0.10).setFill()
                NSRect(x: panelRect.minX + 12, y: cursorY + h / 2.0, width: menuWidth - 24, height: 1).fill()
                continue
            }

            let textColor: NSColor = item.isEnabled
                ? NSColor(calibratedRed: 0.09, green: 0.11, blue: 0.15, alpha: 1.0)
                : NSColor(calibratedRed: 0.42, green: 0.46, blue: 0.53, alpha: 1.0)
            let font = NSFont.systemFont(ofSize: item.isEnabled ? 13 : 12, weight: item.isEnabled ? .regular : .semibold)

            if let dot = item.image {
                dot.draw(
                    in: NSRect(x: panelRect.minX + 14, y: cursorY + (h - 10) / 2.0, width: 10, height: 10),
                    from: .zero,
                    operation: .sourceOver,
                    fraction: 1.0
                )
            }

            let textX = panelRect.minX + (item.image != nil ? 32 : 14)
            let attrs: [NSAttributedString.Key: Any] = [.font: font, .foregroundColor: textColor]
            let attrStr = NSAttributedString(string: item.title, attributes: attrs)
            let strSize = attrStr.size()
            attrStr.draw(at: NSPoint(x: textX, y: cursorY + (h - strSize.height) / 2.0))

            if !item.keyEquivalent.isEmpty {
                let scAttrs: [NSAttributedString.Key: Any] = [
                    .font: NSFont.systemFont(ofSize: 12),
                    .foregroundColor: NSColor(calibratedWhite: 0.5, alpha: 1.0),
                ]
                let sc = NSAttributedString(string: "⌘" + item.keyEquivalent.uppercased(), attributes: scAttrs)
                let scSize = sc.size()
                sc.draw(at: NSPoint(x: panelRect.maxX - 14 - scSize.width, y: cursorY + (h - scSize.height) / 2.0))
            }
        }

        NSGraphicsContext.restoreGraphicsState()

        guard let pngData = rep.representation(using: .png, properties: [:]) else {
            throw NSError(domain: "Consilium", code: 3, userInfo: [NSLocalizedDescriptionKey: "PNG encode failed"])
        }
        try pngData.write(to: URL(fileURLWithPath: path))
    }
}
