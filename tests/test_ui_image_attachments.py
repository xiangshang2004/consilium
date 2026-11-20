from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from consilium.mcp_server import ConsiliumDaemon
from consilium.models import ConflictError
from tests.test_mcp_transports import _raw_http_roundtrip


class ImageAttachmentTests(unittest.TestCase):
    def test_image_roundtrip_and_idempotent_send_to_multiple_peers(self):
        with tempfile.TemporaryDirectory(prefix="consilium-images-", dir="/tmp") as root:
            daemon = ConsiliumDaemon(home_dir=root, rendezvous_dir=root, host="127.0.0.1", port=0)
            self.addCleanup(daemon.stop)
            image_id = "17" * 16
            image = (Path(__file__).resolve().parents[1] /
                     "app/macos/Resources/StatusIconTemplate.png").read_bytes()
            auth = {"Authorization": "Bearer " + daemon.auth_token}
            path = "/api/attachments/" + image_id
            status, _, _ = _raw_http_roundtrip(daemon, "GET", path, {})
            self.assertEqual(status, 401)
            status, _, _ = _raw_http_roundtrip(daemon, "POST", path, auth, body=image)
            self.assertEqual(status, 200)
            self.assertEqual(daemon.images.upload(image_id, io.BytesIO(image), len(image))["id"], image_id)
            with self.assertRaises(ValueError):
                different = b"GIF89a" + b"0123456789"
                daemon.images.upload(image_id, io.BytesIO(different), len(different))
            status, headers, downloaded = _raw_http_roundtrip(daemon, "GET", path, auth)
            self.assertEqual(status, 200)
            self.assertEqual(headers["content-type"], "image/png")
            self.assertEqual(downloaded, image)
            with patch("consilium.mcp_server.sync_local_quotas"), patch("consilium.mcp_server._safe_ring_peer", return_value={}):
                initial = daemon.ui_send("图片测试", ["claude"], workspace_hint=root)
                mission_id = initial["mission_id"]
                request = {"client_request_id": "request-one", "attachments": [{"id":image_id,"name":"截图.png"}]}
                first = daemon.ui_send("", ["antigravity","chatgpt"], mission_id, **request)
                retry = daemon.ui_send("", ["antigravity","chatgpt"], mission_id, **request)
                self.assertEqual(first["mission_id"], retry["mission_id"])
                with self.assertRaises(ConflictError):
                    daemon.ui_send("different", ["antigravity","chatgpt"], mission_id, **request)
            snapshot = daemon.get_dashboard_state(sync_quotas=False)
            mission = next(m for m in snapshot["missions"] if m["mission_id"] == mission_id)
            self.assertEqual(len(mission["tasks"]), 3)
            self.assertEqual(len(mission["user_messages"]), 2)
            sent = mission["user_messages"][-1]
            self.assertEqual(sent["text"], "")
            self.assertEqual(sent["attachments"][0]["name"], "截图.png")
            task = next(t for t in mission["tasks"] if t["assignee_peer"] == "antigravity")
            self.assertEqual(task["user_message_id"], sent["message_id"])
            self.assertIn(str(daemon.images.find(image_id)[0]), task["prompt"])
            pickup = daemon.dispatcher.store.pickup_inbox("antigravity", mission_id=mission_id)
            self.assertEqual(pickup["claimed_tasks"][0]["attachments"], sent["attachments"])
            status, _, _ = _raw_http_roundtrip(daemon, "POST", path + "/discard", auth)
            self.assertEqual(status, 409)
