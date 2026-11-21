import Cocoa
import Foundation
import UniformTypeIdentifiers
import WebKit

struct WebViewProbeResult {
    let loggedIn: Bool
    let connText: String
    let missionCount: Int
}

/// Avoids a retain cycle between WKUserContentController and the window controller.
private final class WeakScriptHandler: NSObject, WKScriptMessageHandler {
    weak var target: WKScriptMessageHandler?
    init(_ target: WKScriptMessageHandler) { self.target = target }
    func userContentController(_ c: WKUserContentController, didReceive message: WKScriptMessage) {
        target?.userContentController(c, didReceive: message)
    }
}

final class MainWindowController: NSWindowController, NSWindowDelegate, WKNavigationDelegate, WKUIDelegate, WKScriptMessageHandler {
    private let config: AppConfig
    private let dataStore: WKWebsiteDataStore
    private let webView: WKWebView

    private var didRetryCookieReload = false
    private var initialLoadWaiters: [() -> Void] = []
    private var initialLoadCompleted = false

    var onReauthenticate: ((@escaping (HTTPCookie?) -> Void) -> Void)?

    init(config: AppConfig) {
        self.config = config
        self.dataStore = WKWebsiteDataStore.nonPersistent()

        let webConfig = WKWebViewConfiguration()
        webConfig.websiteDataStore = self.dataStore
        let contentController = WKUserContentController()
        webConfig.userContentController = contentController
        webConfig.preferences.setValue(false, forKey: "developerExtrasEnabled")

        self.webView = WKWebView(frame: NSRect(x: 0, y: 0, width: 1280, height: 840), configuration: webConfig)

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1280, height: 840),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "Consilium"
        window.minSize = NSSize(width: 420, height: 600)
        window.isReleasedWhenClosed = false
        window.center()
        window.contentView = self.webView
        defer { contentController.add(WeakScriptHandler(self), name: "consilium") }

        super.init(window: window)
        window.delegate = self
        self.webView.navigationDelegate = self
        self.webView.uiDelegate = self
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    func setSessionCookie(_ cookie: HTTPCookie, completion: @escaping () -> Void) {
        dataStore.httpCookieStore.setCookie(cookie) {
            DispatchQueue.main.async {
                completion()
            }
        }
    }

    func loadDashboard(with cookie: HTTPCookie, completion: (() -> Void)? = nil) {
        if let completion = completion {
            initialLoadWaiters.append(completion)
        }
        didRetryCookieReload = false
        setSessionCookie(cookie) { [weak self] in
            guard let self = self else { return }
            self.webView.load(URLRequest(url: self.config.uiURL))
        }
    }

    func showWindowAndActivate() {
        NSApp.setActivationPolicy(.regular)
        if let win = window {
            win.makeKeyAndOrderFront(nil)
        }
        NSApp.activate(ignoringOtherApps: true)
    }

    func prepareForOffscreenRender() {
        if let win = window {
            win.orderBack(nil)
        }
    }

    func probeDom(completion: @escaping (WebViewProbeResult) -> Void) {
        let js = """
        (() => {
          const ov = document.getElementById('login-overlay');
          const ct = document.getElementById('conn-text');
          const ms = document.querySelectorAll('#mission-list .mission-item');
          return {
            loggedIn: Boolean(ov && ov.classList.contains('hidden')),
            connText: ct ? ct.textContent.trim() : '',
            missionCount: ms ? ms.length : 0
          };
        })()
        """
        webView.evaluateJavaScript(js) { res, _ in
            if let dict = res as? [String: Any] {
                completion(
                    WebViewProbeResult(
                        loggedIn: (dict["loggedIn"] as? Bool) ?? false,
                        connText: (dict["connText"] as? String) ?? "",
                        missionCount: (dict["missionCount"] as? Int) ?? 0
                    )
                )
            } else {
                completion(WebViewProbeResult(loggedIn: false, connText: "", missionCount: 0))
            }
        }
    }

    func waitForLiveDashboard(timeoutSec: TimeInterval = 12.0, completion: @escaping (WebViewProbeResult) -> Void) {
        let deadline = Date().addingTimeInterval(timeoutSec)
        func poll() {
            self.probeDom { res in
                if (res.loggedIn && res.connText == "SSE 实时") || Date() >= deadline {
                    completion(res)
                } else {
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                        poll()
                    }
                }
            }
        }
        if initialLoadCompleted {
            poll()
        } else {
            initialLoadWaiters.append {
                poll()
            }
        }
    }

    func captureWindowSnapshot(
        appearance: NSAppearance.Name,
        uiTheme: String,
        to outputPath: String,
        completion: @escaping (Error?) -> Void
    ) {
        let isDark = (appearance == .darkAqua)
        window?.appearance = NSAppearance(named: appearance)

        let clickJs = """
        (() => {
          document.documentElement.classList.add('no-theme-transition');
          const b = document.querySelector('[data-set-theme="\(uiTheme)"]');
          if (b) {
            b.click();
          }
          void document.documentElement.offsetHeight;
          return document.documentElement.getAttribute('data-theme') || '';
        })()
        """
        webView.evaluateJavaScript(clickJs) { [weak self] val, err in
            guard let self = self else { return }
            if let err = err {
                fputs("captureWindowSnapshot JS warning: \(err)\n", stderr)
            } else if let got = val as? String, got != uiTheme {
                fputs("captureWindowSnapshot theme mismatch: expected \(uiTheme), got \(got)\n", stderr)
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.35) {
                let snapCfg = WKSnapshotConfiguration()
                snapCfg.rect = self.webView.bounds
                self.webView.takeSnapshot(with: snapCfg) { image, err in
                    if let err = err {
                        completion(err)
                        return
                    }
                    guard let webImage = image else {
                        completion(NSError(domain: "Consilium", code: 10, userInfo: [NSLocalizedDescriptionKey: "Nil snapshot"]))
                        return
                    }
                    do {
                        try self.compositeWindowChrome(
                            webImage: webImage,
                            boundsSize: self.webView.bounds.size,
                            isDark: isDark,
                            to: outputPath
                        )
                        completion(nil)
                    } catch {
                        completion(error)
                    }
                }
            }
        }
    }

    private func compositeWindowChrome(webImage: NSImage, boundsSize: NSSize, isDark: Bool, to path: String) throws {
        let titleBarHeight: CGFloat = 28
        let totalSize = NSSize(width: boundsSize.width, height: boundsSize.height + titleBarHeight)
        let scale: CGFloat = 2.0

        guard let rep = NSBitmapImageRep(
            bitmapDataPlanes: nil,
            pixelsWide: Int(totalSize.width * scale),
            pixelsHigh: Int(totalSize.height * scale),
            bitsPerSample: 8,
            samplesPerPixel: 4,
            hasAlpha: true,
            isPlanar: false,
            colorSpaceName: .deviceRGB,
            bytesPerRow: 0,
            bitsPerPixel: 0
        ) else {
            throw NSError(domain: "Consilium", code: 11, userInfo: [NSLocalizedDescriptionKey: "Bitmap alloc failed"])
        }
        rep.size = totalSize

        NSGraphicsContext.saveGraphicsState()
        guard let ctx = NSGraphicsContext(bitmapImageRep: rep) else {
            NSGraphicsContext.restoreGraphicsState()
            throw NSError(domain: "Consilium", code: 12, userInfo: [NSLocalizedDescriptionKey: "GraphicsContext failed"])
        }
        NSGraphicsContext.current = ctx

        let barRect = NSRect(x: 0, y: boundsSize.height, width: totalSize.width, height: titleBarHeight)
        (isDark
            ? NSColor(calibratedRed: 0.11, green: 0.13, blue: 0.17, alpha: 1.0)
            : NSColor(calibratedRed: 0.96, green: 0.97, blue: 0.98, alpha: 1.0)
        ).setFill()
        barRect.fill()

        (isDark
            ? NSColor(calibratedWhite: 1.0, alpha: 0.08)
            : NSColor(calibratedWhite: 0.0, alpha: 0.08)
        ).setFill()
        NSRect(x: 0, y: boundsSize.height, width: totalSize.width, height: 1).fill()

        // Standard macOS traffic-light dots
        let dots: [(CGFloat, NSColor)] = [
            (14, NSColor(calibratedRed: 1.0, green: 0.37, blue: 0.34, alpha: 1.0)),
            (34, NSColor(calibratedRed: 0.99, green: 0.74, blue: 0.18, alpha: 1.0)),
            (54, NSColor(calibratedRed: 0.15, green: 0.79, blue: 0.25, alpha: 1.0)),
        ]
        for (xPos, col) in dots {
            col.setFill()
            NSBezierPath(ovalIn: NSRect(x: xPos, y: boundsSize.height + 8, width: 12, height: 12)).fill()
        }

        let titleAttrs: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 13, weight: .semibold),
            .foregroundColor: isDark
                ? NSColor(calibratedWhite: 0.92, alpha: 1.0)
                : NSColor(calibratedRed: 0.12, green: 0.14, blue: 0.18, alpha: 1.0),
        ]
        let titleStr = NSAttributedString(string: "Consilium", attributes: titleAttrs)
        let titleSz = titleStr.size()
        titleStr.draw(at: NSPoint(x: (totalSize.width - titleSz.width) / 2.0, y: boundsSize.height + (titleBarHeight - titleSz.height) / 2.0))

        webImage.draw(
            in: NSRect(x: 0, y: 0, width: boundsSize.width, height: boundsSize.height),
            from: .zero,
            operation: .copy,
            fraction: 1.0
        )

        NSGraphicsContext.restoreGraphicsState()

        guard let pngData = rep.representation(using: .png, properties: [:]) else {
            throw NSError(domain: "Consilium", code: 13, userInfo: [NSLocalizedDescriptionKey: "PNG encode failed"])
        }
        try pngData.write(to: URL(fileURLWithPath: path))
    }

    // MARK: - NSWindowDelegate

    func windowWillClose(_ notification: Notification) {
        // Keep the Dock icon: closing the window leaves Consilium running (menu bar + Dock); reopen via either.
    }

    // MARK: - WKNavigationDelegate

    func webView(
        _ webView: WKWebView,
        decidePolicyFor navigationAction: WKNavigationAction,
        decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
    ) {
        guard let url = navigationAction.request.url else {
            decisionHandler(.cancel)
            return
        }
        let scheme = (url.scheme ?? "").lowercased()
        if scheme == "claude" || scheme == "codex" {
            // Only the deep links verified in R003: claude://code/new and codex://new (new session with a pre-filled prompt).
            let currentURL = webView.url
            let fromLocalDashboard = currentURL?.scheme == "http" && currentURL?.host == "127.0.0.1" && currentURL?.port == config.port
            let userInitiatedOrRedirect = navigationAction.navigationType == .linkActivated || navigationAction.navigationType == .other
            let host = (url.host ?? "").lowercased()
            let path = url.path
            let allowed = fromLocalDashboard && userInitiatedOrRedirect && (
                (scheme == "claude" && host == "code" && path == "/new")
                || (scheme == "codex" && host == "new" && (path.isEmpty || path == "/"))
            )
            if allowed {
                NSWorkspace.shared.open(url)
            } else {
                NSLog("Consilium: blocked unverified deep link %@", url.absoluteString)
            }
            decisionHandler(.cancel)
            return
        }
        if scheme == "http" && url.host == "127.0.0.1" && (url.port ?? 80) == config.port {
            decisionHandler(.allow)
            return
        }
        decisionHandler(.cancel)
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        probeDom { [weak self] res in
            guard let self = self else { return }
            if !res.loggedIn && !self.didRetryCookieReload {
                self.didRetryCookieReload = true
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                    self.webView.load(URLRequest(url: self.config.uiURL))
                }
                return
            }
            self.initialLoadCompleted = true
            let waiters = self.initialLoadWaiters
            self.initialLoadWaiters.removeAll()
            waiters.forEach { $0() }
        }
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        completeInitialLoadOnFailure()
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        completeInitialLoadOnFailure()
    }

    private func completeInitialLoadOnFailure() {
        initialLoadCompleted = true
        let waiters = initialLoadWaiters
        initialLoadWaiters.removeAll()
        waiters.forEach { $0() }
    }

    func webView(
        _ webView: WKWebView,
        runOpenPanelWith parameters: WKOpenPanelParameters,
        initiatedByFrame frame: WKFrameInfo,
        completionHandler: @escaping ([URL]?) -> Void
    ) {
        guard frame.isMainFrame, let url = frame.request.url,
              url.scheme == "http", url.host == "127.0.0.1", url.port == config.port else {
            completionHandler(nil)
            return
        }
        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = parameters.allowsMultipleSelection
        panel.allowedContentTypes = [.png, .jpeg, .webP, .gif]
        panel.prompt = "添加图片"
        if let window = window {
            panel.beginSheetModal(for: window) { response in
                completionHandler(response == .OK ? panel.urls : nil)
            }
        } else {
            completionHandler(panel.runModal() == .OK ? panel.urls : nil)
        }
    }

    // MARK: - Native bridge (folder picker, reveal in Finder)

    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        guard message.name == "consilium",
              message.frameInfo.isMainFrame,
              let currentURL = webView.url,
              currentURL.scheme == "http",
              currentURL.host == "127.0.0.1",
              currentURL.port == config.port,
              let body = message.body as? [String: Any],
              let action = body["action"] as? String else { return }
        if action == "pickFolder" {
            presentFolderPicker()
        } else if action == "revealPath" {
            revealInFinder(body["path"] as? String ?? "")
        } else if action == "reauthenticate" {
            refreshSession()
        }
    }

    private func refreshSession() {
        if let onReauthenticate = onReauthenticate {
            onReauthenticate { [weak self] cookie in
                guard let self = self else { return }
                if let cookie = cookie {
                    self.setSessionCookie(cookie) {
                        self.webView.evaluateJavaScript("window.__consiliumReauthenticated?.(true)", completionHandler: nil)
                    }
                } else {
                    self.webView.evaluateJavaScript("window.__consiliumReauthenticated?.(false)", completionHandler: nil)
                }
            }
            return
        }
        let configuration = config
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let cookie = try? AuthAndStreamClient(config: configuration).loginWithStateToken().cookie
            DispatchQueue.main.async { [weak self] in
                guard let self = self else { return }
                if let cookie = cookie {
                    self.setSessionCookie(cookie) {
                        self.webView.evaluateJavaScript("window.__consiliumReauthenticated?.(true)", completionHandler: nil)
                    }
                } else {
                    self.webView.evaluateJavaScript("window.__consiliumReauthenticated?.(false)", completionHandler: nil)
                }
            }
        }
    }

    /// Show a file the peers reported in Finder. Nothing is opened or executed: the path must be
    /// absolute and exist, and Finder only selects it.
    private func revealInFinder(_ rawPath: String) {
        let path = (rawPath as NSString).standardizingPath
        let ok = path.hasPrefix("/") && FileManager.default.fileExists(atPath: path)
        if ok {
            NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: path)])
        }
        webView.evaluateJavaScript("window.__consiliumRevealResult && window.__consiliumRevealResult(\(ok ? "true" : "false"))", completionHandler: nil)
    }

    private func presentFolderPicker() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = true
        panel.prompt = "选择"
        panel.message = "选择这个会诊要在其中工作的文件夹（可以在这里新建文件夹）"
        let finish: (NSApplication.ModalResponse) -> Void = { [weak self] resp in
            guard let self = self else { return }
            let path = (resp == .OK) ? (panel.url?.path ?? "") : ""
            let data = (try? JSONSerialization.data(withJSONObject: [path], options: [])) ?? Data("[\"\"]".utf8)
            let arg = String(data: data, encoding: .utf8) ?? "[\"\"]"
            self.webView.evaluateJavaScript("window.__consiliumFolderPicked && window.__consiliumFolderPicked(\(arg)[0])", completionHandler: nil)
        }
        if let win = window {
            panel.beginSheetModal(for: win, completionHandler: finish)
        } else {
            finish(panel.runModal())
        }
    }
}
