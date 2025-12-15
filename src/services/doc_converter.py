"""Helpers for converting uploaded documents into markdown snippets."""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import aiofiles
from fastapi import UploadFile

from src.config import config as app_config
from src.knowledge.indexing import process_file_to_markdown
from src.utils import logger

ATTACHMENT_ALLOWED_EXTENSIONS: tuple[str, ...] = (".txt", ".md", ".docx", ".html", ".htm", ".xls", ".xlsx")
MAX_ATTACHMENT_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB
MAX_ATTACHMENT_MARKDOWN_CHARS = 32_000


@dataclass(slots=True)
class ConversionResult:
    """Represents the normalized output of an uploaded attachment."""

    file_id: str
    file_name: str
    file_type: str | None
    file_size: int
    markdown: str
    truncated: bool
    original_file_path: str | None = None


def _ensure_workdir() -> Path:
    workdir = Path(app_config.save_dir) / "uploads" / "chat_attachments"
    workdir.mkdir(parents=True, exist_ok=True)
    return workdir


async def _write_upload_to_disk(upload: UploadFile, dest: Path) -> int:
    await upload.seek(0)
    written = 0
    chunk_size = 1024 * 1024

    async with aiofiles.open(dest, "wb") as buffer:
        while True:
            chunk = await upload.read(chunk_size)
            if not chunk:
                break
            written += len(chunk)
            if written > MAX_ATTACHMENT_SIZE_BYTES:
                raise ValueError("附件过大，当前仅支持 5 MB 以内的文件")
            await buffer.write(chunk)

    return written


def _truncate_markdown(markdown: str) -> tuple[str, bool]:
    if len(markdown) <= MAX_ATTACHMENT_MARKDOWN_CHARS:
        return markdown, False

    truncated_content = markdown[: MAX_ATTACHMENT_MARKDOWN_CHARS - 100].rstrip()
    truncated_content = f"{truncated_content}\n\n[内容已截断，超出 {MAX_ATTACHMENT_MARKDOWN_CHARS} 字符限制]"
    return truncated_content, True


async def convert_upload_to_markdown(upload: UploadFile) -> ConversionResult:
    """Persist an UploadFile temporarily, convert it to markdown, and clean up."""
    if not upload.filename:
        raise ValueError("无法识别的文件名")

    file_name = Path(upload.filename).name
    suffix = Path(file_name).suffix.lower()

    if suffix not in ATTACHMENT_ALLOWED_EXTENSIONS:
        allowed = ", ".join(ATTACHMENT_ALLOWED_EXTENSIONS)
        raise ValueError(f"不支持的文件类型: {suffix or '未知'}，当前仅支持 {allowed}")

    temp_dir = _ensure_workdir()
    tmp_name = f"{uuid.uuid4().hex}{suffix}"
    temp_path = temp_dir / tmp_name
    file_id = uuid.uuid4().hex
    final_name = f"{file_id}_{file_name}"
    final_path = temp_dir / final_name
    cleanup_needed = True

    try:
        file_size = await _write_upload_to_disk(upload, temp_path)
        markdown = await process_file_to_markdown(str(temp_path), params={"return_binary": True})
        markdown, truncated = _truncate_markdown(markdown)
        os.replace(temp_path, final_path)
        cleanup_needed = False
        return ConversionResult(
            file_id=file_id,
            file_name=file_name,
            file_type=upload.content_type,
            file_size=file_size,
            markdown=markdown,
            truncated=truncated,
            original_file_path=str(final_path),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Attachment conversion failed: %s", exc)
        raise
    finally:
        # Remove the temp file in a thread to avoid blocking the event loop
        if cleanup_needed and temp_path.exists():
            await asyncio.to_thread(temp_path.unlink)
