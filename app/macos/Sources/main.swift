import Cocoa

let app = NSApplication.shared
let config = AppConfig.fromProcess()
let delegate = AppDelegate(config: config)
app.delegate = delegate
app.run()
