import Foundation
import WebKit

enum AuthStreamError: LocalizedError {
    case missingAuthToken(String)
    case loginFailed(Int, String)
    case missingSessionCookie
    case stateFetchFailed(Int, String)

    var errorDescription: String? {
        switch self {
        case let .missingAuthToken(path):
            return "未找到令牌文件：\(path)"
        case let .loginFailed(status, detail):
            return "POST /api/login 失败（HTTP \(status)）：\(detail)"
        case .missingSessionCookie:
            return "POST /api/login 响应缺少 consilium_session Cookie"
        case let .stateFetchFailed(status, detail):
            return "GET /api/state 失败（HTTP \(status)）：\(detail)"
        }
    }
}

final class AuthAndStreamClient: NSObject, URLSessionDataDelegate {
    private let config: AppConfig
    private let lock = NSLock()
    private let decodeQueue = DispatchQueue(label: "com.consilium.sse.decode")

    private var _sessionCookie: HTTPCookie?
    private(set) var lastLoginStatus: Int = 0
    private(set) var lastStateStatus: Int = 0
    private(set) var sseConnected: Bool = false

    private var streamSession: URLSession?
    private var streamTask: URLSessionDataTask?
    private var lineBuffer = Data()
    private var currentEventName: String = "message"
    private var currentDataLines: [String] = []
    private var sseHttpStatus: Int = 0
    private var stopped: Bool = false
    private var reconnectScheduled: Bool = false

    var onStateUpdate: ((DashboardState) -> Void)?
    var onConnectionStateChange: ((String) -> Void)?
    var onSessionCookieRefreshed: ((HTTPCookie) -> Void)?

    init(config: AppConfig) {
        self.config = config
        super.init()
    }

    func loginWithStateToken() throws -> (cookie: HTTPCookie, status: Int) {
        let token = (try? String(contentsOfFile: config.authTokenPath, encoding: .utf8))?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if token.isEmpty && !FileManager.default.fileExists(atPath: config.authTokenPath) {
            throw AuthStreamError.missingAuthToken(config.authTokenPath)
        }

        let cfg = URLSessionConfiguration.ephemeral
        cfg.httpShouldSetCookies = false
        cfg.timeoutIntervalForRequest = 5.0
        cfg.timeoutIntervalForResource = 6.0
        let session = URLSession(configuration: cfg)
        defer { session.invalidateAndCancel() }

        var req = URLRequest(url: config.loginURL)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.setValue(config.baseURL.absoluteString, forHTTPHeaderField: "Origin")
        req.httpBody = try JSONSerialization.data(withJSONObject: ["token": token], options: [])

        let sem = DispatchSemaphore(value: 0)
        var capturedResp: HTTPURLResponse?
        var capturedData: Data?
        var capturedErr: Error?

        session.dataTask(with: req) { data, resp, err in
            capturedData = data
            capturedResp = resp as? HTTPURLResponse
            capturedErr = err
            sem.signal()
        }.resume()
        _ = sem.wait(timeout: .now() + 7.0)

        if let err = capturedErr {
            throw AuthStreamError.loginFailed(0, err.localizedDescription)
        }
        guard let http = capturedResp else {
            throw AuthStreamError.loginFailed(0, "无效响应")
        }

        lock.lock()
        lastLoginStatus = http.statusCode
        lock.unlock()

        if http.statusCode != 200 {
            let msg = String(data: capturedData ?? Data(), encoding: .utf8) ?? ""
            throw AuthStreamError.loginFailed(http.statusCode, msg)
        }

        let headers = normalizedHeaders(from: http)
        let parsedCookies = HTTPCookie.cookies(withResponseHeaderFields: headers, for: config.baseURL)
        guard let rawCookie = parsedCookies.first(where: { $0.name == "consilium_session" }) else {
            throw AuthStreamError.missingSessionCookie
        }

        var props = rawCookie.properties ?? [:]
        props[.name] = "consilium_session"
        props[.value] = rawCookie.value
        props[.domain] = config.host
        props[.path] = "/"
        props[.originURL] = config.baseURL
        props[HTTPCookiePropertyKey("HttpOnly")] = "TRUE"
        props[.sameSitePolicy] = HTTPCookieStringPolicy.sameSiteStrict
        let preservedCookie = HTTPCookie(properties: props) ?? rawCookie

        lock.lock()
        _sessionCookie = preservedCookie
        lock.unlock()
        return (preservedCookie, http.statusCode)
    }

    var sessionCookie: HTTPCookie? {
        lock.lock()
        defer { lock.unlock() }
        return _sessionCookie
    }

    func fetchStateWithCookie() throws -> (state: DashboardState, status: Int) {
        lock.lock()
        let cookie = _sessionCookie
        lock.unlock()
        guard let cookie = cookie else {
            throw AuthStreamError.missingSessionCookie
        }

        let cfg = URLSessionConfiguration.ephemeral
        cfg.httpShouldSetCookies = false
        cfg.timeoutIntervalForRequest = 5.0
        cfg.timeoutIntervalForResource = 6.0
        let session = URLSession(configuration: cfg)
        defer { session.invalidateAndCancel() }

        var req = URLRequest(url: config.stateURL)
        req.httpMethod = "GET"
        req.setValue("\(cookie.name)=\(cookie.value)", forHTTPHeaderField: "Cookie")

        let sem = DispatchSemaphore(value: 0)
        var capturedResp: HTTPURLResponse?
        var capturedData: Data?
        var capturedErr: Error?

        session.dataTask(with: req) { data, resp, err in
            capturedData = data
            capturedResp = resp as? HTTPURLResponse
            capturedErr = err
            sem.signal()
        }.resume()
        _ = sem.wait(timeout: .now() + 7.0)

        if let err = capturedErr {
            throw AuthStreamError.stateFetchFailed(0, err.localizedDescription)
        }
        guard let http = capturedResp else {
            throw AuthStreamError.stateFetchFailed(0, "无效响应")
        }

        lock.lock()
        lastStateStatus = http.statusCode
        lock.unlock()

        guard http.statusCode == 200, let body = capturedData else {
            let msg = String(data: capturedData ?? Data(), encoding: .utf8) ?? ""
            throw AuthStreamError.stateFetchFailed(http.statusCode, msg)
        }

        let decoded = try JSONDecoder().decode(DashboardState.self, from: body)
        return (decoded, http.statusCode)
    }

    func startSSE() {
        lock.lock()
        stopped = false
        reconnectScheduled = false
        let cookie = _sessionCookie
        lock.unlock()

        stopSSEConnectionOnly()

        guard let cookie = cookie else {
            scheduleReconnect(reauth: true, delay: 0.5)
            return
        }

        let cfg = URLSessionConfiguration.ephemeral
        cfg.httpShouldSetCookies = false
        cfg.timeoutIntervalForRequest = 65.0
        cfg.timeoutIntervalForResource = Double(Int.max)

        let queue = OperationQueue()
        queue.maxConcurrentOperationCount = 1
        let session = URLSession(configuration: cfg, delegate: self, delegateQueue: queue)

        var req = URLRequest(url: config.streamURL)
        req.httpMethod = "GET"
        req.setValue("\(cookie.name)=\(cookie.value)", forHTTPHeaderField: "Cookie")
        req.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        req.setValue("no-cache", forHTTPHeaderField: "Cache-Control")

        let task = session.dataTask(with: req)
        lock.lock()
        streamSession = session
        streamTask = task
        lineBuffer.removeAll(keepingCapacity: false)
        currentEventName = "message"
        currentDataLines.removeAll(keepingCapacity: false)
        sseHttpStatus = 0
        lock.unlock()

        task.resume()
    }

    func stopSSE() {
        lock.lock()
        stopped = true
        sseConnected = false
        lock.unlock()
        stopSSEConnectionOnly()
    }

    private func stopSSEConnectionOnly() {
        lock.lock()
        let task = streamTask
        let sess = streamSession
        streamTask = nil
        streamSession = nil
        lock.unlock()
        task?.cancel()
        sess?.invalidateAndCancel()
    }

    private func scheduleReconnect(reauth: Bool, delay: TimeInterval = 1.5) {
        lock.lock()
        if stopped || reconnectScheduled {
            lock.unlock()
            return
        }
        reconnectScheduled = true
        sseConnected = false
        lock.unlock()

        DispatchQueue.main.async { [weak self] in
            self?.onConnectionStateChange?("重连中")
        }

        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + delay) { [weak self] in
            guard let self = self else { return }
            self.lock.lock()
            let isStopped = self.stopped
            self.reconnectScheduled = false
            self.lock.unlock()
            if isStopped { return }

            if reauth {
                if let (cookie, _) = try? self.loginWithStateToken() {
                    DispatchQueue.main.async { [weak self] in
                        self?.onSessionCookieRefreshed?(cookie)
                    }
                }
            }
            self.startSSE()
        }
    }

    // MARK: - URLSessionDataDelegate

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
    ) {
        let code = (response as? HTTPURLResponse)?.statusCode ?? 0
        lock.lock()
        let isCurrent = (dataTask === streamTask) && !stopped
        if isCurrent {
            sseHttpStatus = code
            sseConnected = (code == 200)
        }
        lock.unlock()

        guard isCurrent else {
            completionHandler(.cancel)
            return
        }

        if code == 200 {
            DispatchQueue.main.async { [weak self] in
                self?.onConnectionStateChange?("SSE 实时")
            }
            completionHandler(.allow)
        } else {
            completionHandler(.cancel)
            scheduleReconnect(reauth: (code == 401), delay: 1.5)
        }
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        lock.lock()
        if stopped || dataTask !== streamTask {
            lock.unlock()
            return
        }
        lineBuffer.append(data)

        while let nlRange = lineBuffer.firstRange(of: Data([0x0A])) {
            let rawLineData = lineBuffer.subdata(in: 0..<nlRange.lowerBound)
            lineBuffer.removeSubrange(0..<nlRange.upperBound)
            var line = String(data: rawLineData, encoding: .utf8) ?? ""
            if line.hasSuffix("\r") {
                line.removeLast()
            }
            processSSELineLocked(line)
        }
        lock.unlock()
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        lock.lock()
        let isCurrent = (task === streamTask)
        let isStopped = stopped
        let lastCode = sseHttpStatus
        if isCurrent {
            sseConnected = false
        }
        lock.unlock()
        if !isCurrent || isStopped { return }
        if let urlErr = error as? URLError, urlErr.code == .cancelled {
            return
        }
        scheduleReconnect(reauth: (lastCode == 401), delay: 1.5)
    }

    private func processSSELineLocked(_ line: String) {
        if line.isEmpty {
            if !currentDataLines.isEmpty {
                let rawJson = currentDataLines.joined(separator: "\n")
                let evName = currentEventName
                currentEventName = "message"
                currentDataLines.removeAll(keepingCapacity: true)
                decodeQueue.async { [weak self] in
                    self?.dispatchSSEFrame(event: evName, jsonText: rawJson)
                }
            } else {
                currentEventName = "message"
            }
            return
        }
        if line.hasPrefix(":") {
            return
        }
        if line.hasPrefix("event:") {
            currentEventName = String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)
        } else if line.hasPrefix("data:") {
            var val = String(line.dropFirst(5))
            if val.hasPrefix(" ") {
                val.removeFirst()
            }
            currentDataLines.append(val)
        }
    }

    private func dispatchSSEFrame(event: String, jsonText: String) {
        guard let data = jsonText.data(using: .utf8) else { return }
        let decoder = JSONDecoder()
        if event == "state" {
            if let st = try? decoder.decode(DashboardState.self, from: data) {
                DispatchQueue.main.async { [weak self] in
                    self?.onStateUpdate?(st)
                }
            }
        } else if event == "update" {
            if let env = try? decoder.decode(SSEUpdateEnvelope.self, from: data), let st = env.state {
                DispatchQueue.main.async { [weak self] in
                    self?.onStateUpdate?(st)
                }
            } else if let st = try? decoder.decode(DashboardState.self, from: data) {
                DispatchQueue.main.async { [weak self] in
                    self?.onStateUpdate?(st)
                }
            }
        }
    }

    private func normalizedHeaders(from http: HTTPURLResponse) -> [String: String] {
        var out: [String: String] = [:]
        for (k, v) in http.allHeaderFields {
            out[String(describing: k)] = String(describing: v)
        }
        return out
    }
}
