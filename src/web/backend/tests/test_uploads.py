from __future__ import annotations

from io import BytesIO
from pathlib import Path
import zipfile

from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.services.uploads import sanitize_relative_path, store_archive_upload



def _upload_file(name: str, payload: bytes) -> UploadFile:
    return UploadFile(filename=name, file=BytesIO(payload))



def test_sanitize_relative_path_rejects_parent_traversal() -> None:
    try:
        sanitize_relative_path("../secret.pdf")
    except HTTPException as exc:
        assert exc.status_code == 400
        return
    raise AssertionError("expected HTTPException")



def test_store_archive_upload_rejects_when_no_supported_files(tmp_path: Path) -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("input.exe", b"bad")

    upload = _upload_file("bundle.zip", buffer.getvalue())
    try:
        store_archive_upload(
            upload=upload,
            input_root=tmp_path / "input",
            archive_root=tmp_path / "archives",
            max_archive_bytes=5 * 1024 * 1024,
            max_uncompressed_bytes=5 * 1024 * 1024,
            max_files=100,
            per_file_limit_bytes=5 * 1024 * 1024,
        )
    except HTTPException as exc:
        assert exc.status_code == 400
        return
    raise AssertionError("expected HTTPException")



def test_store_archive_upload_extracts_supported_files(tmp_path: Path) -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("docs/spec.pdf", b"pdf data")
        zf.writestr("docs/spec.xlsx", b"xlsx data")

    upload = _upload_file("bundle.zip", buffer.getvalue())
    result = store_archive_upload(
        upload=upload,
        input_root=tmp_path / "input",
        archive_root=tmp_path / "archives",
        max_archive_bytes=5 * 1024 * 1024,
        max_uncompressed_bytes=5 * 1024 * 1024,
        max_files=100,
        per_file_limit_bytes=5 * 1024 * 1024,
    )

    assert len(result.files) == 2
    assert result.warnings == []
    assert (tmp_path / "input" / "docs" / "spec.pdf").exists()
    assert (tmp_path / "input" / "docs" / "spec.xlsx").exists()


def test_store_archive_upload_skips_unsupported_files_with_warning(tmp_path: Path) -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("docs/spec.pdf", b"pdf data")
        zf.writestr("docs/binary.exe", b"bad")
        zf.writestr("docs/sheet.xlsx", b"xlsx data")

    upload = _upload_file("bundle.zip", buffer.getvalue())
    result = store_archive_upload(
        upload=upload,
        input_root=tmp_path / "input",
        archive_root=tmp_path / "archives",
        max_archive_bytes=5 * 1024 * 1024,
        max_uncompressed_bytes=5 * 1024 * 1024,
        max_files=100,
        per_file_limit_bytes=5 * 1024 * 1024,
    )

    assert len(result.files) == 2
    assert result.warnings == ["skipped unsupported file: docs/binary.exe"]
    assert (tmp_path / "input" / "docs" / "spec.pdf").exists()
    assert not (tmp_path / "input" / "docs" / "binary.exe").exists()
    assert (tmp_path / "input" / "docs" / "sheet.xlsx").exists()
