import Darwin
import Foundation

enum DaemonStartupError: LocalizedError {
    case bundledPythonNotFound(String)
    case portBusyNotConsilium(Int, String)
    case processExitedEarly(Int32, String)
    case startupTimeout(Int, String)

    var errorDescription: String? {
        switch self {
        case let .bundledPythonNotFound(path):
            return "App 包内缺少 bundled consilium Python 模块：\(path)"
        case let .portBusyNotConsilium(port, detail):
            return "端口 127.0.0.1:\(port) 已被其他进程占用且无法使用当前令牌健康检查（\(detail)），未重复启动 daemon。"
        case let .processExitedEarly(code, log):
            return "Consilium 后台服务提前退出（exit=\(code)）：\(log)"
        case let .startupTimeout(port, log):
            return "等待 127.0.0.1:\(port)/health 就绪超时：\(log)"
        }
    }
}

final class DaemonManager {
    private let config: AppConfig
    private var process: Process?
    private var stderrBuffer = Data()
    private let bufferLock = NSLock()

    private(set) var ownsDaemon: Bool = false
    private(set) var managedPID: Int32?
    private(set) var lastHealthStatus: Int = 0

    init(config: AppConfig) {
        self.config = config
    }

    func bundledPythonPath() -> String? {
        if let resPath = Bundle.main.resourceURL?.appendingPathComponent("python").path,
           FileManager.default.fileExists(atPath: resPath + "/consilium/cli.py") {
            return resPath
        }
        return nil
    }

    func ensureRunning() throws -> Int {
        if isLoopbackPortOpen(port: config.port) {
            let probe = probeHealthSync()
            if probe.status == 200 {
                ownsDaemon = false
                managedPID = nil
                lastHealthStatus = 200
                return 200
            }
            throw DaemonStartupError.portBusyNotConsilium(config.port, probe.detail)
        }

        guard let pythonPath = bundledPythonPath() else {
            let expected = Bundle.main.resourceURL?.appendingPathComponent("python/consilium/cli.py").path ?? "<unknown>"
            throw DaemonStartupError.bundledPythonNotFound(expected)
        }

        try ensurePrivateDirs()

        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: "/usr/bin/python3")
        proc.arguments = [
            "-B",
            "-m",
            "consilium.cli",
            "serve",
            "--host", config.host,
            "--port", String(config.port),
            "--data-dir", config.stateDir,
            "--rendezvous-dir", config.rendezvousDir,
        ]

        var env = ProcessInfo.processInfo.environment
        env.removeValue(forKey: "CLAUDE_CONFIG_DIR")
        env["PYTHONPATH"] = pythonPath
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["CONSILIUM_STATE_DIR"] = config.stateDir
        env["CONSILIUM_HOME"] = config.stateDir
        env["CONSILIUM_RENDEZVOUS_DIR"] = config.rendezvousDir
        if config.dryRun {
            env["CONSILIUM_DRY_RUN"] = "1"
        }
        if config.disableLocalQuotaSync {
            env["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] = "1"
        }
        proc.environment = env
        proc.currentDirectoryURL = URL(fileURLWithPath: "/tmp")

        let stderrPipe = Pipe()
        let stdoutPipe = Pipe()
        proc.standardError = stderrPipe
        proc.standardOutput = stdoutPipe

        let appendLog: (Data) -> Void = { [weak self] chunk in
            guard let self = self, !chunk.isEmpty else { return }
            self.bufferLock.lock()
            self.stderrBuffer.append(chunk)
            if self.stderrBuffer.count > 16384 {
                self.stderrBuffer.removeFirst(self.stderrBuffer.count - 16384)
            }
            self.bufferLock.unlock()
        }
        stderrPipe.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            if data.isEmpty {
                handle.readabilityHandler = nil
            } else {
                appendLog(data)
            }
        }
        stdoutPipe.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            if data.isEmpty {
                handle.readabilityHandler = nil
            } else {
                appendLog(data)
            }
        }

        try proc.run()
        self.process = proc
        self.ownsDaemon = true
        self.managedPID = proc.processIdentifier

        let deadline = Date().addingTimeInterval(12.0)
        var lastProbe = (status: 0, detail: "pending")
        while Date() < deadline {
            if !proc.isRunning {
                let code = proc.terminationStatus
                let logText = readCapturedLog()
                self.process = nil
                self.ownsDaemon = false
                self.managedPID = nil
                throw DaemonStartupError.processExitedEarly(code, logText)
            }
            lastProbe = probeHealthSync()
            if lastProbe.status == 200 {
                self.lastHealthStatus = 200
                return 200
            }
            Thread.sleep(forTimeInterval: 0.12)
        }

        let logText = readCapturedLog()
        stopIfOwned()
        throw DaemonStartupError.startupTimeout(config.port, "\(lastProbe.detail); \(logText)")
    }

    func stopIfOwned() {
        guard ownsDaemon, let proc = process else { return }
        ownsDaemon = false
        if proc.isRunning {
            let pid = proc.processIdentifier
            proc.terminate()
            let waitUntil = Date().addingTimeInterval(3.0)
            while proc.isRunning && Date() < waitUntil {
                Thread.sleep(forTimeInterval: 0.05)
            }
            if proc.isRunning {
                kill(pid, SIGKILL)
                proc.waitUntilExit()
            }
        }
        process = nil
    }

    private func ensurePrivateDirs() throws {
        let fm = FileManager.default
        let attrs: [FileAttributeKey: Any] = [.posixPermissions: NSNumber(value: 0o700)]
        for dir in [config.stateDir, config.rendezvousDir] {
            if !fm.fileExists(atPath: dir) {
                try fm.createDirectory(atPath: dir, withIntermediateDirectories: true, attributes: attrs)
            }
        }
    }

    private func readCapturedLog() -> String {
        bufferLock.lock()
        let text = String(data: stderrBuffer, encoding: .utf8) ?? ""
        bufferLock.unlock()
        return text.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func readCurrentToken() -> String? {
        guard let raw = try? String(contentsOfFile: config.authTokenPath, encoding: .utf8) else {
            return nil
        }
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private func probeHealthSync() -> (status: Int, detail: String) {
        let sessionConfig = URLSessionConfiguration.ephemeral
        sessionConfig.timeoutIntervalForRequest = 1.5
        sessionConfig.timeoutIntervalForResource = 2.0
        let session = URLSession(configuration: sessionConfig)
        defer { session.invalidateAndCancel() }

        var req = URLRequest(url: config.healthURL)
        req.httpMethod = "GET"
        if let token = readCurrentToken() {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        let sem = DispatchSemaphore(value: 0)
        var result: (Int, String) = (0, "no response")
        session.dataTask(with: req) { data, resp, err in
            defer { sem.signal() }
            if let err = err {
                result = (0, err.localizedDescription)
                return
            }
            guard let http = resp as? HTTPURLResponse else {
                result = (0, "non-http response")
                return
            }
            if http.statusCode == 200,
               let body = data,
               let obj = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
               (obj["ok"] as? Bool) == true {
                result = (200, "ok")
            } else {
                result = (http.statusCode, "HTTP \(http.statusCode)")
            }
        }.resume()
        _ = sem.wait(timeout: .now() + 2.5)
        return result
    }

    private func isLoopbackPortOpen(port: Int) -> Bool {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { return false }
        defer { close(fd) }

        var addr = sockaddr_in()
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = in_port_t(UInt16(port).bigEndian)
        addr.sin_addr.s_addr = inet_addr("127.0.0.1")

        let res = withUnsafePointer(to: &addr) { ptr -> Int32 in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockPtr in
                Darwin.connect(fd, sockPtr, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        return res == 0
    }
}
