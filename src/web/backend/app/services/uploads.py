from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import shutil
import zipfile

from fastapi import HTTPException, UploadFile

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".xlsx"}
ALLOWED_ARCHIVE_EXTENSIONS = {".zip"}


@dataclass
class StoredFile:
    relative_path: str
    stored_path: Path
    size_bytes: int
    extension: str


@dataclass
class ArchiveUploadResult:
    files: list[StoredFile]
    warnings: list[str]



def sanitize_relative_path(raw_path: str) -> str:
    normalized = raw_path.replace("\\", "/").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="relative_path is empty")
    normalized = normalized.lstrip("/")
    parsed = PurePosixPath(normalized)
    if parsed.is_absolute():
        raise HTTPException(status_code=400, detail="absolute paths are not allowed")
    for part in parsed.parts:
        if part in {"", ".", ".."}:
            raise HTTPException(status_code=400, detail=f"invalid relative path segment: {part!r}")
    return str(parsed)



def validate_allowed_extension(filename: str, allowed_extensions: set[str] | None = None) -> str:
    suffix = Path(filename).suffix.lower()
    allowed = allowed_extensions or ALLOWED_EXTENSIONS
    if suffix not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported file extension {suffix or '<none>'}. allowed: {', '.join(sorted(allowed))}",
        )
    return suffix



def _stream_to_path(upload: UploadFile, destination: Path, max_bytes: int) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with destination.open("wb") as output:
        while True:
            chunk = upload.file.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(status_code=413, detail=f"file too large: exceeds {max_bytes} bytes")
            output.write(chunk)
    return total



def store_single_upload(
    *,
    upload: UploadFile,
    relative_path: str,
    input_root: Path,
    max_bytes: int,
) -> StoredFile:
    clean_relative = sanitize_relative_path(relative_path)
    extension = validate_allowed_extension(clean_relative, ALLOWED_EXTENSIONS)
    target_path = (input_root / clean_relative).resolve()
    input_root_resolved = input_root.resolve()
    if input_root_resolved not in target_path.parents and target_path != input_root_resolved:
        raise HTTPException(status_code=400, detail="invalid path traversal")

    size_bytes = _stream_to_path(upload, target_path, max_bytes=max_bytes)
    if size_bytes <= 0:
        raise HTTPException(status_code=400, detail="uploaded file is empty")

    return StoredFile(
        relative_path=clean_relative,
        stored_path=target_path,
        size_bytes=size_bytes,
        extension=extension,
    )



def store_archive_upload(
    *,
    upload: UploadFile,
    input_root: Path,
    archive_root: Path,
    max_archive_bytes: int,
    max_uncompressed_bytes: int,
    max_files: int,
    per_file_limit_bytes: int,
) -> ArchiveUploadResult:
    archive_suffix = validate_allowed_extension(upload.filename or "", ALLOWED_ARCHIVE_EXTENSIONS)
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_path = archive_root / f"upload{archive_suffix}"

    archive_size = _stream_to_path(upload, archive_path, max_bytes=max_archive_bytes)
    if archive_size <= 0:
        raise HTTPException(status_code=400, detail="archive is empty")

    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            infos = [info for info in zf.infolist() if not info.is_dir()]
            if len(infos) > max_files:
                raise HTTPException(status_code=400, detail=f"archive has too many files (> {max_files})")

            total_uncompressed = 0
            extracted: list[StoredFile] = []
            warnings: list[str] = []
            seen_paths: set[str] = set()

            for info in infos:
                raw_name = info.filename
                clean_relative = sanitize_relative_path(raw_name)
                if clean_relative in seen_paths:
                    raise HTTPException(status_code=400, detail=f"duplicate file in archive: {clean_relative}")
                seen_paths.add(clean_relative)

                try:
                    extension = validate_allowed_extension(clean_relative, ALLOWED_EXTENSIONS)
                except HTTPException as exc:
                    detail = str(exc.detail)
                    if exc.status_code == 400 and detail.startswith("unsupported file extension"):
                        warnings.append(f"skipped unsupported file: {clean_relative}")
                        continue
                    raise

                mode = (info.external_attr >> 16) & 0o170000
                if mode == 0o120000:
                    raise HTTPException(status_code=400, detail=f"symlink entries are not allowed: {clean_relative}")

                file_size = int(info.file_size)
                if file_size <= 0:
                    raise HTTPException(status_code=400, detail=f"archive entry is empty: {clean_relative}")
                if file_size > per_file_limit_bytes:
                    raise HTTPException(status_code=413, detail=f"archive entry too large: {clean_relative}")

                total_uncompressed += file_size
                if total_uncompressed > max_uncompressed_bytes:
                    raise HTTPException(status_code=413, detail="archive uncompressed size exceeds limit")

                target_path = (input_root / clean_relative).resolve()
                input_root_resolved = input_root.resolve()
                if input_root_resolved not in target_path.parents and target_path != input_root_resolved:
                    raise HTTPException(status_code=400, detail=f"invalid path traversal: {clean_relative}")

                target_path.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info, "r") as source, target_path.open("wb") as target:
                    shutil.copyfileobj(source, target)

                extracted.append(
                    StoredFile(
                        relative_path=clean_relative,
                        stored_path=target_path,
                        size_bytes=file_size,
                        extension=extension,
                    )
                )

            if not extracted:
                raise HTTPException(status_code=400, detail="archive does not contain supported files")
            return ArchiveUploadResult(files=extracted, warnings=warnings)
    except zipfile.BadZipFile as exc:
        raise HTTPException(status_code=400, detail="invalid zip archive") from exc
