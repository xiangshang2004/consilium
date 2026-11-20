import Foundation

struct AppConfig {
    let host: String = "127.0.0.1"
    let port: Int
    let stateDir: String
    let rendezvousDir: String
    let dryRun: Bool
    let disableLocalQuotaSync: Bool
    let openOnLaunch: Bool
    let noNotifications: Bool
    let smokeOutPath: String?
    let captureDirPath: String?
    let dateTag: String = "2026-11-20"

    var baseURL: URL {
        URL(string: "http://\(host):\(port)")!
    }

    var uiURL: URL {
        baseURL.appendingPathComponent("ui")
    }

    var healthURL: URL {
        baseURL.appendingPathComponent("health")
    }

    var loginURL: URL {
        baseURL.appendingPathComponent("api/login")
    }

    var stateURL: URL {
        baseURL.appendingPathComponent("api/state")
    }

    var streamURL: URL {
        baseURL.appendingPathComponent("api/stream")
    }

    var authTokenPath: String {
        URL(fileURLWithPath: stateDir).appendingPathComponent("auth.token").path
    }

    static func fromProcess(arguments: [String] = CommandLine.arguments) -> AppConfig {
        let env = ProcessInfo.processInfo.environment

        let defaultState = NSString(string: "~/Library/Application Support/consilium").expandingTildeInPath
        let rawState = firstNonEmpty(env["CONSILIUM_STATE_DIR"], env["CONSILIUM_HOME"]) ?? defaultState
        let resolvedState = URL(fileURLWithPath: NSString(string: rawState).expandingTildeInPath).standardizedFileURL.path

        let defaultRv = "/tmp/consilium-\(getuid())"
        let rawRv = firstNonEmpty(env["CONSILIUM_RENDEZVOUS_DIR"], nil) ?? defaultRv
        let resolvedRv = URL(fileURLWithPath: NSString(string: rawRv).expandingTildeInPath).standardizedFileURL.path

        var port = 18920
        if let rawPort = env["CONSILIUM_PORT"], let parsed = Int(rawPort.trimmingCharacters(in: .whitespacesAndNewlines)), (1...65535).contains(parsed) {
            port = parsed
        }

        let dryRun = env["CONSILIUM_DRY_RUN"] == "1"
        let disableLocalQuotaSync = env["CONSILIUM_DISABLE_LOCAL_QUOTA_SYNC"] == "1"

        var openOnLaunch = true
        var noNotifications = false
        var smokeOutPath: String?
        var captureDirPath: String?

        var idx = 1
        while idx < arguments.count {
            let arg = arguments[idx]
            if arg == "--open-on-launch" {
                openOnLaunch = true
            } else if arg == "--background" {
                openOnLaunch = false
            } else if arg == "--no-notifications" {
                noNotifications = true
            } else if arg == "--smoke-out", idx + 1 < arguments.count {
                idx += 1
                smokeOutPath = arguments[idx]
            } else if arg.hasPrefix("--smoke-out=") {
                smokeOutPath = String(arg.dropFirst("--smoke-out=".count))
            } else if arg == "--capture-dir", idx + 1 < arguments.count {
                idx += 1
                captureDirPath = arguments[idx]
            } else if arg.hasPrefix("--capture-dir=") {
                captureDirPath = String(arg.dropFirst("--capture-dir=".count))
            } else if arg == "--port", idx + 1 < arguments.count {
                idx += 1
                if let p = Int(arguments[idx]), (1...65535).contains(p) {
                    port = p
                }
            }
            idx += 1
        }

        return AppConfig(
            port: port,
            stateDir: resolvedState,
            rendezvousDir: resolvedRv,
            dryRun: dryRun,
            disableLocalQuotaSync: disableLocalQuotaSync,
            openOnLaunch: openOnLaunch,
            noNotifications: noNotifications,
            smokeOutPath: smokeOutPath,
            captureDirPath: captureDirPath
        )
    }

    private static func firstNonEmpty(_ a: String?, _ b: String?) -> String? {
        for cand in [a, b] {
            if let s = cand?.trimmingCharacters(in: .whitespacesAndNewlines), !s.isEmpty {
                return s
            }
        }
        return nil
    }
}
