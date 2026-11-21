"""Local image attachments shared by the dashboard and desktop agents."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO, Dict, List, Optional, Tuple

_IMAGE_TYPES = {"png": "image/png", "jpg": "image/jpeg", "webp": "image/webp", "gif": "image/gif"}
_CHUNK = 64 * 1024
MAX_IMAGE_BYTES = 20 * 1024 * 1024


def _validate_id(attachment_id: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{32}", attachment_id):
        raise ValueError("图片附件 ID 无效")


def _extension(header: bytes) -> str:
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if header.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "gif"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "webp"
    raise ValueError("请选择 PNG、JPEG、WebP 或 GIF 图片")


class ImageAttachments:
    def __init__(self, state_dir: str) -> None:
        self.directory = Path(state_dir) / "attachments"

    def _ensure_directory(self) -> None:
        if self.directory.is_symlink():
            raise ValueError("附件目录路径无效")
        if not self.directory.exists():
            self.directory.mkdir(mode=0o700, exist_ok=True)
        try:
            os.chmod(self.directory, 0o700)
        except OSError:
            pass

    def cleanup_stale_uploads(self, max_age_sec: float = 3600.0) -> int:
        """Remove abandoned `.upload-*` temp staging directories older than max_age_sec."""
        if not self.directory.is_dir() or self.directory.is_symlink():
            return 0
        now = time.time()
        removed = 0
        for entry in self.directory.iterdir():
            if not entry.name.startswith(".upload-") or entry.is_symlink() or not entry.is_dir():
                continue
            try:
                age = now - entry.stat().st_mtime
                if age >= max_age_sec:
                    shutil.rmtree(entry, ignore_errors=True)
                    removed += 1
            except OSError:
                pass
        return removed

    def find(self, attachment_id: str) -> Optional[Tuple[Path, str]]:
        _validate_id(attachment_id)
        if self.directory.is_symlink():
            return None
        item_dir = self.directory / attachment_id
        if item_dir.is_symlink() or not item_dir.is_dir():
            return None
        for extension, media_type in _IMAGE_TYPES.items():
            path = item_dir / f"image.{extension}"
            if not path.is_symlink() and path.is_file():
                return path, media_type
        return None

    def upload(self, attachment_id: str, source: BinaryIO, size: int) -> Dict[str, Any]:
        _validate_id(attachment_id)
        if size < 12:
            raise ValueError("图片文件不完整")
        if size > MAX_IMAGE_BYTES:
            raise ValueError("单张图片不能超过 20 MB")
        self._ensure_directory()
        if size > shutil.disk_usage(self.directory).free:
            raise OSError("本机存储空间不足")
        header = source.read(12)
        if len(header) < 12:
            raise ValueError("图片上传中断")
        ext = _extension(header)
        destination = self.directory / attachment_id
        digest = hashlib.sha256(header)
        with TemporaryDirectory(dir=self.directory, prefix=".upload-") as folder:
            staging = Path(folder)
            try:
                os.chmod(staging, 0o700)
            except OSError:
                pass
            target_file = staging / f"image.{ext}"
            with target_file.open("wb") as temp:
                temp.write(header)
                left = size - len(header)
                while left:
                    chunk = source.read(min(left, _CHUNK))
                    if not chunk:
                        raise ValueError("图片上传中断")
                    temp.write(chunk)
                    digest.update(chunk)
                    left -= len(chunk)
            try:
                os.chmod(target_file, 0o600)
            except OSError:
                pass
            # Renaming a populated directory reserves the ID even across formats.
            try:
                staging.rename(destination)
            except OSError:
                found = self.find(attachment_id)
                if found is None:
                    raise
                path, _ = found
                if path.suffix != f".{ext}":
                    raise ValueError("同一图片附件 ID 已用于另一张图片")
                with path.open("rb") as current:
                    existing = hashlib.sha256()
                    for block in iter(lambda: current.read(_CHUNK), b""):
                        existing.update(block)
                if existing.digest() != digest.digest():
                    raise ValueError("同一图片附件 ID 已用于另一张图片")
        return {"id": attachment_id, "media_type": _IMAGE_TYPES[ext], "size": size}

    def discard(self, attachment_id: str) -> None:
        result = self.find(attachment_id)
        if result is not None:
            path, _ = result
            path.unlink(missing_ok=True)
            try:
                path.parent.rmdir()
            except FileNotFoundError:
                pass

    def describe(self, values: Any) -> List[Dict[str, Any]]:
        if values is None:
            return []
        if not isinstance(values, list):
            raise ValueError("图片附件须为列表")
        items: List[Dict[str, Any]] = []
        seen = set()
        for item in values:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                raise ValueError("图片附件格式无效")
            attachment_id = item["id"]
            if attachment_id in seen:
                continue
            result = self.find(attachment_id)
            if result is None:
                raise ValueError("图片尚未上传，请重新上传后发送")
            path, media_type = result
            name = str(item.get("name") or "").replace("\\", "/").split("/")[-1]
            name = "".join(char for char in name if char.isprintable()).strip()
            items.append({"id": attachment_id, "name": name or f"图片{path.suffix}", "media_type": media_type})
            seen.add(attachment_id)
        return items

    def native_paths(self, attachments: List[Dict[str, Any]]) -> List[str]:
        paths = [self.find(item["id"]) for item in attachments]
        if any(path is None for path in paths):
            raise ValueError("图片文件已移除，请重新上传")
        return [str(result[0]) for result in paths if result is not None]
