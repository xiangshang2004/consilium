import Cocoa
import Darwin
import Foundation

final class AppDelegate: NSObject, NSApplicationDelegate {
    private let config: AppConfig
    private let daemonManager: DaemonManager
    private let client: AuthAndStreamClient
    private let notifier: NotificationManager

    private var statusBarController: StatusBarController?
    private var mainWindowController: MainWindowController?
    private var sigtermSource: DispatchSourceSignal?
    private var sigintSource: DispatchSourceSignal?

    init(config: AppConfig) {
        self.config = config
        self.daemonManager = DaemonManager(config: config)
        self.client = AuthAndStreamClient(config: config)
        self.notifier = NotificationManager(config: config)
        super.init()
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.accessory)
        installSignalHandlers()

        statusBarController = StatusBarController()
        statusBarController?.onOpenRequested = { [weak self] in
            self?.mainWindowController?.showWindowAndActivate()
        }
        statusBarController?.onQuitRequested = {
            NSApp.terminate(nil)
        }

        mainWindowController = MainWindowController(config: config)
        notifier.onFocusConsilium = { [weak self] in
            self?.mainWindowController?.showWindowAndActivate()
        }

        client.onConnectionStateChange = { [weak self] text in
            self?.statusBarController?.updateConnectionState(text)
        }
        client.onStateUpdate = { [weak self] state in
            self?.statusBarController?.updateState(state)
            self?.notifier.processState(state)
        }
        client.onSessionCookieRefreshed = { [weak self] cookie in
            self?.mainWindowController?.setSessionCookie(cookie, completion: {})
        }

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.bootstrapSequence()
        }
    }

    func applicationWillTerminate(_ notification: Notification) {
        client.stopSSE()
        daemonManager.stopIfOwned()
    }

    private func installSignalHandlers() {
        signal(SIGTERM, SIG_IGN)
        signal(SIGINT, SIG_IGN)

        let termSrc = DispatchSource.makeSignalSource(signal: SIGTERM, queue: .main)
        termSrc.setEventHandler {
            NSApp.terminate(nil)
        }
        termSrc.resume()
        sigtermSource = termSrc

        let intSrc = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
        intSrc.setEventHandler {
            NSApp.terminate(nil)
        }
        intSrc.resume()
        sigintSource = intSrc
    }

    private func bootstrapSequence() {
        do {
            let healthStatus = try daemonManager.ensureRunning()
            let (cookie, loginStatus) = try client.loginWithStateToken()
            let (initialState, stateStatus) = try client.fetchStateWithCookie()

            DispatchQueue.main.async { [weak self] in
                guard let self = self else { return }
                self.statusBarController?.updateState(initialState)
                self.notifier.processState(initialState)
                self.notifier.requestAuthorizationIfNeeded()
                self.client.startSSE()

                guard let win = self.mainWindowController else { return }
                win.loadDashboard(with: cookie)

                if let smokePath = self.config.smokeOutPath {
                    win.prepareForOffscreenRender()
                    self.runSmokeVerification(
                        healthStatus: healthStatus,
                        loginStatus: loginStatus,
                        stateStatus: stateStatus,
                        to: smokePath
                    )
                } else if let capDir = self.config.captureDirPath {
                    win.prepareForOffscreenRender()
                    self.runCaptureSequence(to: capDir)
                } else if self.config.openOnLaunch {
                    win.showWindowAndActivate()
                }
            }
        } catch {
            DispatchQueue.main.async { [weak self] in
                guard let self = self else { return }
                let msg = error.localizedDescription
                if let smokePath = self.config.smokeOutPath {
                    let report = SmokeReport(
                        ok: false,
                        owns_daemon: self.daemonManager.ownsDaemon,
                        daemon_pid: self.daemonManager.managedPID,
                        port: self.config.port,
                        state_dir: self.config.stateDir,
                        rendezvous_dir: self.config.rendezvousDir,
                        health_status: self.daemonManager.lastHealthStatus,
                        login_status: self.client.lastLoginStatus,
                        state_status: self.client.lastStateStatus,
                        sse_connected: self.client.sseConnected,
                        webview_logged_in: false,
                        webview_conn_text: "",
                        mission_count: 0,
                        error: msg
                    )
                    self.writeSmokeReportAndExit(report, to: smokePath, code: 1)
                    return
                }
                fputs("Consilium.app startup error: \(msg)\n", stderr)
                if self.config.captureDirPath != nil {
                    self.shutdownAndExit(1)
                    return
                }
                let alert = NSAlert()
                alert.messageText = "Consilium 启动失败"
                alert.informativeText = msg
                alert.alertStyle = .critical
                alert.runModal()
                self.shutdownAndExit(1)
            }
        }
    }

    private func runSmokeVerification(
        healthStatus: Int,
        loginStatus: Int,
        stateStatus: Int,
        to smokePath: String
    ) {
        mainWindowController?.waitForLiveDashboard(timeoutSec: 12.0) { [weak self] probe in
            guard let self = self else { return }
            let ok = (healthStatus == 200)
                && (loginStatus == 200)
                && (stateStatus == 200)
                && self.client.sseConnected
                && probe.loggedIn
                && (probe.connText == "SSE 实时")

            let report = SmokeReport(
                ok: ok,
                owns_daemon: self.daemonManager.ownsDaemon,
                daemon_pid: self.daemonManager.managedPID,
                port: self.config.port,
                state_dir: self.config.stateDir,
                rendezvous_dir: self.config.rendezvousDir,
                health_status: healthStatus,
                login_status: loginStatus,
                state_status: stateStatus,
                sse_connected: self.client.sseConnected,
                webview_logged_in: probe.loggedIn,
                webview_conn_text: probe.connText,
                mission_count: probe.missionCount,
                error: ok ? nil : "Verification condition failed (loggedIn=\(probe.loggedIn), conn=\(probe.connText), sse=\(self.client.sseConnected))"
            )
            self.writeSmokeReportAndExit(report, to: smokePath, code: ok ? 0 : 1)
        }
    }

    private func runCaptureSequence(to capDir: String) {
        let fm = FileManager.default
        try? fm.createDirectory(atPath: capDir, withIntermediateDirectories: true, attributes: nil)

        mainWindowController?.waitForLiveDashboard(timeoutSec: 12.0) { [weak self] probe in
            guard let self = self, let win = self.mainWindowController, let bar = self.statusBarController else { return }
            guard probe.loggedIn else {
                fputs("Consilium.app capture failed: login overlay not hidden (conn=\(probe.connText))\n", stderr)
                self.shutdownAndExit(1)
                return
            }

            let tag = self.config.dateTag
            let menuOut = URL(fileURLWithPath: capDir).appendingPathComponent("app_menubar_\(tag).png").path
            let lightOut = URL(fileURLWithPath: capDir).appendingPathComponent("app_window_light_\(tag).png").path
            let darkOut = URL(fileURLWithPath: capDir).appendingPathComponent("app_window_dark_\(tag).png").path

            do {
                try bar.captureDropdownSnapshot(to: menuOut)
            } catch {
                fputs("Failed to capture menubar snapshot: \(error)\n", stderr)
                self.shutdownAndExit(1)
                return
            }

            win.captureWindowSnapshot(appearance: .aqua, uiTheme: "light", to: lightOut) { errLight in
                if let errLight = errLight {
                    fputs("Failed to capture light window snapshot: \(errLight)\n", stderr)
                    self.shutdownAndExit(1)
                    return
                }
                win.captureWindowSnapshot(appearance: .darkAqua, uiTheme: "dark", to: darkOut) { errDark in
                    if let errDark = errDark {
                        fputs("Failed to capture dark window snapshot: \(errDark)\n", stderr)
                        self.shutdownAndExit(1)
                        return
                    }
                    self.shutdownAndExit(0)
                }
            }
        }
    }

    private func writeSmokeReportAndExit(_ report: SmokeReport, to path: String, code: Int32) {
        do {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let data = try encoder.encode(report)
            try data.write(to: URL(fileURLWithPath: path))
        } catch {
            fputs("Failed to write smoke report to \(path): \(error)\n", stderr)
            shutdownAndExit(1)
            return
        }
        shutdownAndExit(code)
    }

    private func shutdownAndExit(_ code: Int32) {
        client.stopSSE()
        daemonManager.stopIfOwned()
        exit(code)
    }
}
