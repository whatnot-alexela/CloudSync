"""
Локальный сервер:
  POST /api/ingest   — принимает JSON из photo-get-clusters-with-resources, пишет в SQLite
  POST /api/auth     — куки, sk и User-Agent из mitmproxy-аддона (лежат в отдельной базе auth.db)
  GET  /api/auth/status — есть ли авторизация (значения кук наружу не отдаются)
  GET  /             — галерея: сетка по дням, одним длинным списком
  GET  /chunk        — следующая порция дней для подгрузки при прокрутке
  GET  /status       — статус загрузки: вкладки «Фото» и «Видео» (#video)
  POST /api/dl/start|stop|threads|retry-failed, GET /api/dl/status — загрузка фото
  POST /api/vd/start|stop|settings|retry-failed, GET /api/vd/status — загрузка видео через recover.py
  POST /api/ingest-files — ресурсы из списков папок раздела «Файлы» (документы, архивы, музыка…)
  POST /api/fl/start|stop|settings|retry-failed, GET /api/fl/status — загрузка остальных файлов
  GET  /api/fl/crawl, POST /api/fl/crawl — обход папок (выполняет окно приложения, см. app/main.py)
  GET  /api/items    — те же данные в JSON
  GET  /api/stats    — статистика

Запуск:
    pip install flask pillow pillow-heif   # pillow-heif — чтобы делать превью из HEIC
    python server.py            # http://127.0.0.1:8001

Для видео рядом с server.py должны лежать recover.py и recover_runner.py, а ffmpeg — в PATH
(ffmpeg нужен для сборки повреждённых видео и для превью видео).
"""
import bisect
import codecs
import errno
import hashlib
import io
import json
import os
import random
import re
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote, urlencode, urlparse
from contextlib import contextmanager
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, render_template_string, request, send_file

DB_PATH = os.environ.get("YDISK_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "gallery.db"))
HOST = "127.0.0.1"
PORT = int(os.environ.get("YDISK_PORT", "8001"))
DAYS_PER_CHUNK = 10  # сколько дней отдаём за одну подгрузку

# --- скачивание файлов ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FILES_DIR = os.environ.get("YDISK_FILES", os.path.join(BASE_DIR, "files"))
THUMBS_DIR = os.environ.get("YDISK_THUMBS", os.path.join(BASE_DIR, "thumbs"))


class _FilesDirHolder:
    """Папка для фото/видео можно сменить на лету (например, на другой диск —
    C: часто маленький). Храним путь в settings (ключ files_dir), а не в
    константе, чтобы его можно было поменять без перезапуска сервера."""
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.lock = threading.Lock()


_files_dir_holder = _FilesDirHolder(DEFAULT_FILES_DIR)


def files_dir():
    return _files_dir_holder.path


def validate_writable_dir(path):
    """Пытается создать папку и правда ли в неё можно писать. Возвращает
    (abs_path, None) при успехе либо (None, текст ошибки)."""
    if not path or not str(path).strip():
        return None, "путь не может быть пустым"
    abspath = os.path.abspath(os.path.expanduser(str(path).strip()))
    try:
        os.makedirs(abspath, exist_ok=True)
    except OSError as e:
        return None, f"не удалось создать папку: {e}"
    probe = os.path.join(abspath, f".ydisk_write_test_{os.getpid()}_{random.randint(0, 999999)}")
    try:
        with open(probe, "wb") as f:
            f.write(b"ok")
    except OSError as e:
        return None, f"нет прав на запись в эту папку: {e}"
    finally:
        try:
            os.remove(probe)
        except OSError:
            pass
    return abspath, None


def set_files_dir(path):
    """Проверяет путь и, если он доступен для записи и обе загрузки сейчас
    не идут, переключает files_dir() на него. Существующие уже скачанные
    файлы НЕ переносятся — только новые скачивания пойдут в новую папку."""
    if DL.state() != "idle" or VD.state() != "idle" or FL.state() != "idle":
        return None, "сначала останови загрузку (фото, видео и файлы), потом меняй папку"
    abspath, err = validate_writable_dir(path)
    if err:
        return None, err
    with _files_dir_holder.lock:
        _files_dir_holder.path = abspath
        set_setting("files_dir", abspath)
    reconcile_existing_downloads()
    reconcile_existing_files()
    return abspath, None


def load_files_dir_setting():
    saved = get_setting("files_dir")
    if not saved:
        return
    abspath, err = validate_writable_dir(saved)
    if err:
        print(f"(!) сохранённая папка для файлов недоступна ({saved}): {err} — использую {files_dir()}")
        return
    _files_dir_holder.path = abspath


def verify_saved_file(root, relative_path, expected_size=None, min_size=1):
    """Проверяет, что итоговый файл существует внутри выбранной папки сохранения."""
    root_real = os.path.realpath(root)
    candidate = os.path.realpath(os.path.join(root_real, relative_path))
    try:
        if os.path.commonpath((root_real, candidate)) != root_real:
            raise OSError("итоговый путь оказался за пределами папки сохранения")
    except ValueError as e:
        raise OSError("итоговый путь оказался за пределами папки сохранения") from e
    if not os.path.isfile(candidate):
        raise OSError(f"файл не найден после сохранения: {relative_path}")
    actual_size = os.path.getsize(candidate)
    if actual_size < min_size:
        raise OSError(f"файл слишком мал после сохранения: {relative_path} ({actual_size} байт)")
    if expected_size is not None and actual_size != expected_size:
        raise OSError(f"размер файла после сохранения не совпадает: {actual_size}/{expected_size} байт")
    return candidate


THUMB_MAX_BYTES = 30 * 1024  # целевой размер превью
THUMB_SIDE = 360             # длинная сторона превью, px (если не влезает в 30 КБ — уменьшаем)
QUEUE_SIZE = 100            # сколько файлов всегда ждут в очереди
DEFAULT_THREADS = 8
MAX_THREADS = 32
# порядок попыток: сначала оригинал, потом превью от большего к меньшему
SIZE_ORDER = ["ORIGINAL", "XXXL", "XXL", "XL", "L", "M", "S", "XS", "XXS", "XXXS", "DEFAULT"]
ATTEMPT_TIMEOUT = 10        # сек: на соединение и на ожидание каждого блока данных
ATTEMPT_DEADLINE = 180      # сек: потолок на одну попытку целиком (против «капающих» соединений)
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
DL_COLUMNS = {
    "dl_status": "TEXT",     # NULL / queued / downloading / done / failed
    "dl_size": "TEXT",       # ORIGINAL / XXXL / ... — что реально скачали
    "dl_file": "TEXT",       # путь относительно корня, сохранённого в dl_root
    "dl_root": "TEXT",       # абсолютный корень, где лежит этот скачанный файл
    "dl_bytes": "INTEGER",
    "dl_error": "TEXT",
    "dl_attempts": "TEXT",
    "dl_at": "TEXT",
}
# --- видео: свои колонки, чтобы не пересекаться с загрузкой фото ---
VD_COLUMNS = {
    "vd_status": "TEXT",     # NULL / queued / downloading / done / failed
    "vd_phase": "TEXT",
    "vd_file": "TEXT",       # путь относительно корня, сохранённого в vd_root
    "vd_root": "TEXT",       # абсолютный корень, где лежит этот скачанный файл
    "vd_bytes": "INTEGER",   # размер файла на Диске
    "vd_lost": "INTEGER",    # сколько байт потеряно (0 — оригинал без потерь)
    "vd_error": "TEXT",
    "vd_note": "TEXT",
    "vd_at": "TEXT",
}
EXT_BY_CTYPE = {"image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png", "image/webp": ".webp",
                "image/heic": ".heic", "image/heif": ".heif", "image/gif": ".gif"}

# --- видео и авторизация ---
MB = 1024 * 1024
AUTH_DB_PATH = os.environ.get("YDISK_AUTH_DB", os.path.join(BASE_DIR, "auth.db"))   # куки из mitmproxy: отдельная база
API_BASE = os.environ.get("YDISK_API_BASE", "https://disk.yandex.ru")
API_TIMEOUT = 20            # сек: запрос ссылки на скачивание (mpfs/url)
RUNNER = os.path.join(BASE_DIR, "recover_runner.py")
VD_QUEUE_SIZE = 20          # сколько видео ждут в очереди
VD_LINK_RETRIES = 5         # сколько раз на фазу брать свежую ссылку (просрочка, 403)
VD_FAIL_STREAK_STOP = 5     # столько видео подряд не скачались — останавливаемся
VD_DEFAULTS = {
    "videos": 2,            # сколько видео качается одновременно
    "jobs": 12,             # потоки проверки блоков внутри одного видео (recover.py --jobs)
    "block_kb": 256,        # --block
    "sub_kb": 16,           # --sub
    "timeout": 20,          # --timeout
    "sub_timeout": 6,       # --sub-timeout
    "retries": 2,           # --retries
    "probe_size": 4096,     # --probe-size
    "probe_timeout": 10,    # --probe-timeout
    "refine": True,         # после скачивания — прогон --refine
    "refine_jobs": 12,      # потоки refine (--jobs на этапе уточнения)
    "stride_kb": 64,        # --stride
    "reencode": False,      # повреждённые видео сразу перекодировать в H.264
}
VD_LIMITS = {
    "videos": (1, 8), "jobs": (1, 64), "block_kb": (16, 8192), "sub_kb": (1, 1024), "timeout": (3, 600),
    "sub_timeout": (1, 120), "retries": (1, 10), "probe_size": (512, 1048576), "probe_timeout": (1, 300),
    "refine_jobs": (1, 64), "stride_kb": (4, 4096),
}

# --- остальные файлы Диска (раздел «Файлы»): документы, архивы, музыка и т. д. ---
FILES_SUBDIR = "Диск"       # files_dir()/Диск/<путь на Диске>, структура папок сохраняется
PUBLIC_SUBDIR = "Ссылки"    # общие папки по ссылкам (в базе /public/<id>/…): files_dir()/Ссылки/<id>/<путь в папке>
PUBLIC_API = os.environ.get("YDISK_PUBLIC_API", "https://cloud-api.yandex.net/v1/disk/public/resources")
FL_QUEUE_SIZE = 50          # сколько файлов ждут в очереди
FL_DEFAULT_THREADS = 3
FL_MAX_THREADS = 8
FL_ATTEMPTS = 5             # попыток на файл: каждая со свежей ссылкой, докачка с места обрыва
FL_READ_TIMEOUT = 12        # сек: на соединение и на каждый блок данных (общего лимита нет — файлы бывают большими)
FL_CRAWL_AGENT_TIMEOUT = 6  # сек: окно приложения не спрашивало /api/fl/crawl дольше — считаем, что его нет
# Спасение повреждённых файлов (после аварии на Диске сервер на битых местах отдаёт пустой ответ или зависает):
FL_SALVAGE_BLOCK = 256 * 1024   # читаем кусками такого размера
FL_SALVAGE_SUB = 32 * 1024      # проба начала куска и дочитывание у края дыры
FL_SALVAGE_TIMEOUT = 6          # сек на кусок: целые куски Диск отдаёт за 0,5–3 с, битые не отдаёт вовсе
FL_SALVAGE_JOBS = 4             # кусков одновременно
FL_SALVAGE_LINK_REFRESH = 5     # сколько раз за файл можно взять новую ссылку
FL_SALVAGE_BUDGET = 600         # сек на файл сверх времени при 50 КБ/с; потом недочитанное считается потерянным
STARTED_AT = time.time()    # запуск программы: по нему vps/fetch.py видит, что её перезапустили (сбой, нехватка памяти)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024

VIDEO_EXT = {"mov", "mp4", "m4v", "avi", "mkv", "3gp", "mts", "webm"}
IMAGE_EXT = {"jpg", "jpeg", "png", "heic", "heif", "webp", "gif", "bmp", "tif", "tiff", "dng", "cr2", "cr3",
             "nef", "arw", "orf", "rw2", "raf"}
NAME_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2})-(\d{2})-(\d{2})")


# ---------------------------------------------------------------- DB

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS clusters (
                id          TEXT PRIMARY KEY,
                size        INTEGER,
                updated_at  TEXT
            );
            CREATE TABLE IF NOT EXISTS items (
                path        TEXT PRIMARY KEY,   -- например /photounlim/2026-10-09 12-08-58.MOV
                name        TEXT,
                taken       TEXT,               -- ISO-время из имени файла (для сортировки)
                mediatype   TEXT,               -- image / video
                mimetype    TEXT,
                size        INTEGER,            -- байты (есть только если пришёл resource)
                width       INTEGER,
                height      INTEGER,
                duration_ms INTEGER,
                etime       INTEGER,
                ctime       INTEGER,
                mtime       INTEGER,
                resource_id TEXT,
                file_id     TEXT,
                item_id     TEXT,
                cluster_id  TEXT,
                albums      TEXT,               -- JSON-список
                raw         TEXT,               -- полный JSON ресурса (с подписанными ссылками, они протухают)
                first_seen  TEXT,
                updated_at  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_items_taken ON items(taken);
            CREATE INDEX IF NOT EXISTS idx_items_type  ON items(mediatype);
            """
        )
        cols = {r[1] for r in c.execute("PRAGMA table_info(items)")}
        for col, typ in list(DL_COLUMNS.items()) + list(VD_COLUMNS.items()):
            if col not in cols:
                c.execute(f"ALTER TABLE items ADD COLUMN {col} {typ}")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_dl ON items(dl_status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_vd ON items(vd_status)")
        c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        # Раздел «Файлы»: отдельная таблица, чтобы галерея и загрузка фото/видео её не видели.
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS files (
                path        TEXT PRIMARY KEY,   -- /disk/Документы/отчёт.pdf
                parent      TEXT,               -- /disk/Документы (у /disk — NULL)
                name        TEXT,
                kind        TEXT,               -- file / dir
                mediatype   TEXT,               -- document / compressed / audio / image / video / ...
                mimetype    TEXT,
                size        INTEGER,            -- байты
                mtime       INTEGER,            -- время изменения на Диске (unix)
                resource_id TEXT,
                is_media    INTEGER,            -- 1 — фото/видео (их по умолчанию качают вкладки «Фото»/«Видео»)
                raw         TEXT,               -- полный JSON ресурса
                first_seen  TEXT,
                updated_at  TEXT,
                listed_at   TEXT,               -- папка: когда впервые увидели её содержимое
                crawled_at  TEXT,               -- папка: когда её прошёл автоматический обход
                fl_status   TEXT,               -- NULL / queued / downloading / done / failed
                fl_file     TEXT,               -- путь относительно корня, сохранённого в fl_root
                fl_root     TEXT,
                fl_bytes    INTEGER,
                fl_error    TEXT,
                fl_note     TEXT,
                fl_at       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent);
            CREATE INDEX IF NOT EXISTS idx_files_fl     ON files(fl_status);
            """
        )
        # после перезапуска «зависшие» элементы очереди возвращаем в исходное состояние
        c.execute("UPDATE items SET dl_status=NULL WHERE dl_status IN ('queued','downloading')")
        c.execute("UPDATE items SET vd_status=NULL WHERE vd_status IN ('queued','downloading')")
        c.execute("UPDATE files SET fl_status=NULL WHERE fl_status IN ('queued','downloading')")


# ---------------------------------------------------------------- ingest

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def name_from_path(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def taken_from_name(name: str):
    m = NAME_DATE_RE.match(name)
    if not m:
        return None
    y, mo, d, h, mi, s = m.groups()
    return f"{y}-{mo}-{d}T{h}:{mi}:{s}"


def guess_mediatype(name: str, albums=None):
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext in VIDEO_EXT or (albums and "videos" in albums):
        return "video"
    return "image"


def upsert_cluster_item(c, cluster_id, it, ts):
    path = it.get("id")
    if not path:
        return
    name = name_from_path(path)
    albums = it.get("albums") or []
    c.execute(
        """
        INSERT INTO items(path, name, taken, mediatype, width, height, item_id, cluster_id, albums, first_seen, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
            width      = COALESCE(excluded.width,  items.width),
            height     = COALESCE(excluded.height, items.height),
            item_id    = COALESCE(excluded.item_id, items.item_id),
            cluster_id = COALESCE(excluded.cluster_id, items.cluster_id),
            albums     = COALESCE(excluded.albums, items.albums),
            mediatype  = COALESCE(items.mediatype, excluded.mediatype),
            updated_at = excluded.updated_at
        """,
        (
            path, name, taken_from_name(name), guess_mediatype(name, albums),
            it.get("width"), it.get("height"), it.get("itemId"), cluster_id,
            json.dumps(albums, ensure_ascii=False), ts, ts,
        ),
    )


def upsert_resource(c, r, ts):
    path = r.get("path") or r.get("id")
    if not path:
        return
    name = r.get("name") or name_from_path(path)
    meta = r.get("meta") or {}
    video_info = meta.get("video_info") or {}
    c.execute(
        """
        INSERT INTO items(path, name, taken, mediatype, mimetype, size, duration_ms,
                          etime, ctime, mtime, resource_id, file_id, cluster_id, raw, first_seen, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
            name        = excluded.name,
            mediatype   = COALESCE(excluded.mediatype, items.mediatype),
            mimetype    = COALESCE(excluded.mimetype, items.mimetype),
            size        = COALESCE(excluded.size, items.size),
            duration_ms = COALESCE(excluded.duration_ms, items.duration_ms),
            etime       = COALESCE(excluded.etime, items.etime),
            ctime       = COALESCE(excluded.ctime, items.ctime),
            mtime       = COALESCE(excluded.mtime, items.mtime),
            resource_id = COALESCE(excluded.resource_id, items.resource_id),
            file_id     = COALESCE(excluded.file_id, items.file_id),
            cluster_id  = COALESCE(items.cluster_id, excluded.cluster_id),
            raw         = excluded.raw,  -- свежий снимок ресурса заменяет устаревшие подписанные download URL
            updated_at  = excluded.updated_at
        """,
        (
            path, name, taken_from_name(name), meta.get("mediatype"), meta.get("mimetype"),
            meta.get("size"), video_info.get("duration"),
            r.get("etime"), r.get("ctime"), r.get("mtime"),
            meta.get("resource_id"), meta.get("file_id"), r.get("clusterId"),
            json.dumps(r, ensure_ascii=False), ts, ts,
        ),
    )


def ingest(payload: dict):
    ts = now_iso()
    clusters = (payload.get("clusters") or {}).get("fetched") or []
    resources = (payload.get("resources") or {}).get("fetched") or []
    affected_paths = set()
    for cl in clusters:
        for it in cl.get("items") or []:
            if it.get("id"):
                affected_paths.add(it["id"])
    for r in resources:
        path = r.get("path") or r.get("id")
        if path:
            affected_paths.add(path)

    with db() as c:
        before = c.execute("SELECT COUNT(*) FROM items").fetchone()[0]

        for cl in clusters:
            cid = cl.get("id")
            c.execute(
                "INSERT INTO clusters(id,size,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET size=excluded.size, updated_at=excluded.updated_at",
                (cid, cl.get("size"), ts),
            )
            for it in cl.get("items") or []:
                upsert_cluster_item(c, cid, it, ts)

        for r in resources:
            upsert_resource(c, r, ts)

        after = c.execute("SELECT COUNT(*) FROM items").fetchone()[0]

    # Если данные о файле появились уже после запуска, тоже проверяем, не лежит ли
    # он на диске с предыдущего запуска/версии программы.
    reconcile_existing_downloads(affected_paths)

    return {
        "clusters": len(clusters),
        "resources": len(resources),
        "new_items": after - before,
        "total_items": after,
    }


@app.post("/api/ingest")
def api_ingest():
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="expected JSON object"), 400
    result = ingest(payload)
    print(f"[ingest] {result}")
    return jsonify(result)


# ---------------------------------------------------------------- ingest: раздел «Файлы»

def _as_int(v):
    try:
        return int(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def is_media_file(name, mediatype):
    if mediatype in ("image", "video"):
        return True
    ext = name.rsplit(".", 1)[-1].lower() if "." in (name or "") else ""
    return ext in VIDEO_EXT or ext in IMAGE_EXT


def parent_of(path):
    return None if path == "/disk" else (path.rsplit("/", 1)[0] or "/disk")


def ingest_files(payload: dict):
    """
    Ресурсы из списков папок Диска (перехват находит их в ответах /models* и в HTML страниц Диска).
    Берём только /disk/… : файлы (type=file или есть meta.size/mimetype) и папки (type=dir).
    Родительские папки заводим сами — так обход знает о каждой папке, которую уже открывали.
    """
    ts = now_iso()
    n_files = n_dirs = 0
    file_paths, parents = [], set()
    with dbc() as c:
        before = c.execute("SELECT COUNT(*) FROM files WHERE kind='file'").fetchone()[0]
        for r in payload.get("resources") or []:
            if not isinstance(r, dict):
                continue
            path = r.get("path") or r.get("id")
            if not isinstance(path, str):
                continue
            path = path.rstrip("/") or "/"
            if path != "/disk" and not path.startswith("/disk/"):
                continue
            meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
            kind = r.get("type")
            if kind not in ("file", "dir"):
                kind = "file" if (meta.get("size") is not None or meta.get("mimetype")) else None
            if kind is None:
                continue
            name = r.get("name") if isinstance(r.get("name"), str) and r.get("name") else name_from_path(path)
            if path == "/disk":
                name = FILES_SUBDIR
            mediatype = meta.get("mediatype") if kind == "file" else None
            parent = parent_of(path)
            c.execute(
                """
                INSERT INTO files(path, parent, name, kind, mediatype, mimetype, size, mtime, resource_id,
                                  is_media, raw, first_seen, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(path) DO UPDATE SET
                    parent      = excluded.parent,
                    name        = excluded.name,
                    kind        = excluded.kind,
                    mediatype   = COALESCE(excluded.mediatype, files.mediatype),
                    mimetype    = COALESCE(excluded.mimetype, files.mimetype),
                    size        = COALESCE(excluded.size, files.size),
                    mtime       = COALESCE(excluded.mtime, files.mtime),
                    resource_id = COALESCE(excluded.resource_id, files.resource_id),
                    is_media    = MAX(COALESCE(excluded.is_media, 0), COALESCE(files.is_media, 0)),
                    raw         = excluded.raw,
                    updated_at  = excluded.updated_at
                """,
                (
                    path, parent, name, kind, mediatype, meta.get("mimetype") if kind == "file" else None,
                    _as_int(meta.get("size")) if kind == "file" else None, _as_int(r.get("mtime")),
                    meta.get("resource_id"), int(kind == "file" and is_media_file(name, mediatype)),
                    json.dumps(r, ensure_ascii=False), ts, ts,
                ),
            )
            if kind == "file":
                n_files += 1
                file_paths.append(path)
            else:
                n_dirs += 1
            if parent:
                parents.add(parent)
        ancestors = set()
        for p in parents:
            q = p
            while q and q not in ancestors:
                ancestors.add(q)
                q = parent_of(q)
        for p in sorted(ancestors):
            c.execute("INSERT INTO files(path, parent, name, kind, first_seen, updated_at) VALUES(?,?,?,'dir',?,?) "
                      "ON CONFLICT(path) DO NOTHING",
                      (p, parent_of(p), FILES_SUBDIR if p == "/disk" else name_from_path(p), ts, ts))
        for p in parents:   # содержимое пришло только у непосредственных родителей
            c.execute("UPDATE files SET listed_at=COALESCE(listed_at, ?) WHERE path=? AND kind='dir'", (ts, p))
        after = c.execute("SELECT COUNT(*) FROM files WHERE kind='file'").fetchone()[0]

    found = reconcile_existing_files(file_paths) if file_paths else 0
    return {"files": n_files, "dirs": n_dirs, "new_files": after - before, "already_on_disk": found,
            "total_files": after}


@app.post("/api/ingest-files")
def api_ingest_files():
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="expected JSON object"), 400
    result = ingest_files(payload)
    print(f"[ingest-files] {result}")
    return jsonify(result)


# ---------------------------------------------------------------- авторизация (auth.db)

@contextmanager
def authc():
    c = sqlite3.connect(AUTH_DB_PATH, timeout=30)
    try:
        yield c
        c.commit()
    finally:
        c.close()


def init_auth_db():
    new = not os.path.exists(AUTH_DB_PATH)
    with authc() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("CREATE TABLE IF NOT EXISTS cookies ("
                  "name TEXT PRIMARY KEY, value TEXT, expires REAL, pos INTEGER, updated_at TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    if new:
        try:
            os.chmod(AUTH_DB_PATH, 0o600)    # в базе сессионные куки: читать должен только владелец
        except OSError:
            pass


def ingest_auth(p: dict):
    """
    cookies      — полный снимок заголовка Cookie из запроса браузера: заменяет всю банку целиком;
    set_cookies  — обновления из Set-Cookie ответов Яндекса: применяются поверх (с учётом удаления);
    sk, connection_id, user_agent, host — последние виденные значения.
    """
    ts = now_iso()
    n_snap = n_set = 0
    with authc() as c:
        snap = p.get("cookies")
        if isinstance(snap, list) and snap:
            rows = [(str(x[0]), str(x[1])) for x in snap if isinstance(x, (list, tuple)) and len(x) == 2 and x[0]]
            if rows:
                c.execute("DELETE FROM cookies")
                for i, (name, value) in enumerate(rows):
                    c.execute("INSERT OR REPLACE INTO cookies(name,value,expires,pos,updated_at) VALUES(?,?,NULL,?,?)",
                              (name, value, i, ts))
                n_snap = len(rows)
        for ck in p.get("set_cookies") or []:
            if not isinstance(ck, dict) or not ck.get("name"):
                continue
            if ck.get("delete"):
                c.execute("DELETE FROM cookies WHERE name=?", (ck["name"],))
            else:
                c.execute(
                    "INSERT INTO cookies(name,value,expires,pos,updated_at) "
                    "VALUES(?,?,?,(SELECT COALESCE(MAX(pos),0)+1 FROM cookies),?) "
                    "ON CONFLICT(name) DO UPDATE SET value=excluded.value, expires=excluded.expires, "
                    "updated_at=excluded.updated_at",
                    (ck["name"], ck.get("value", ""), ck.get("expires"), ts))
            n_set += 1
        have_sk = False
        for key in ("sk", "connection_id", "user_agent", "host"):
            v = p.get(key)
            if isinstance(v, str) and v:
                c.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, v))
                have_sk = have_sk or key == "sk"
        if n_snap or n_set or have_sk:
            c.execute("INSERT INTO meta(key,value) VALUES('updated_at',?) "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (ts,))
    return {"cookies": n_snap, "set_cookies": n_set, "sk": have_sk}


def load_auth():
    with authc() as c:
        rows = c.execute("SELECT name,value,expires FROM cookies ORDER BY pos").fetchall()
        meta = {k: v for k, v in c.execute("SELECT key,value FROM meta")}
    now = time.time()
    return {
        "cookies": [(n, v) for n, v, e in rows if e is None or e > now],
        "sk": meta.get("sk"),
        "connection_id": meta.get("connection_id"),
        "user_agent": meta.get("user_agent"),
        "updated_at": meta.get("updated_at"),
    }


def auth_summary():
    """Для интерфейса: без значений кук."""
    a = load_auth()
    age = None
    if a["updated_at"]:
        try:
            age = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(a["updated_at"])).total_seconds()))
        except ValueError:
            pass
    names = {n for n, _ in a["cookies"]}
    return {"cookies": len(a["cookies"]), "has_sk": bool(a["sk"]), "has_session": "Session_id" in names,
            "age_sec": age}


@app.post("/api/auth")
def api_auth():
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="expected JSON object"), 400
    res = ingest_auth(payload)
    print(f"[auth] кук в снимке: {res['cookies']}, обновлений: {res['set_cookies']}, sk: {res['sk']}")
    return jsonify(res)


@app.get("/api/auth/status")
def api_auth_status():
    return jsonify(auth_summary())


class AuthError(Exception):
    """Нет авторизации или она протухла: нужно заново открыть Диск через mitmproxy."""


class VideoError(Exception):
    """Это видео не удалось скачать/собрать (остальные продолжаются)."""


def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_key(v, key)
            if r is not None:
                return r
    return None


class LinkLimit(AuthError):
    """Диск больше не даёт качать по общей ссылке (лимит скачиваний): загрузку остановить, повторить позже."""

    def __init__(self, what=""):
        super().__init__("Яндекс не даёт качать по ссылке: превышен лимит скачиваний" + (f" ({what})" if what else ""))


def public_roots():
    """Общие папки по ссылкам: {id: {"key": ссылка, "name": имя, "file": ссылка на один файл,
    "listed": {путь подпапки: когда её список собран целиком}}}."""
    try:
        roots = json.loads(get_setting("fl_public", "{}"))
    except (TypeError, ValueError):
        return {}
    return roots if isinstance(roots, dict) else {}


def public_root_of(path):
    """/public/<id>/Папка/книга.pdf -> («<id>», «/Папка/книга.pdf»); не из общей папки — (None, None)."""
    if not path.startswith("/public/"):
        return None, None
    pid, _, rel = path[len("/public/"):].partition("/")
    return pid, "/" + rel


def public_download_url(path):
    """Файл общей папки: прямая ссылка из открытого API Диска (вход не нужен, куки и sk не участвуют)."""
    pid, rel = public_root_of(path)
    root = public_roots().get(pid)
    if not root:
        raise VideoError("эту общую ссылку программа не знает — запусти fetch с самой ссылкой")
    params = {"public_key": root["key"]} if root.get("file") else {"public_key": root["key"], "path": rel}
    req = urllib.request.Request(PUBLIC_API + "/download?" + urlencode(params), headers={"User-Agent": USER_AGENT})
    try:
        with VD.opener.open(req, timeout=API_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read() or b"{}")
        except ValueError:
            body = {}
        what = f"{body.get('error') or ''} {body.get('description') or ''}".strip()
        if e.code == 429 or "limit" in what.lower() or "лимит" in what.lower():
            raise LinkLimit(what)
        raise VideoError(f"ссылка на файл: HTTP {e.code}" + (f" {what}" if what else ""))
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError, ValueError) as e:
        raise VideoError(f"ссылка на файл: {short_err(e)}")
    if not isinstance(data, dict) or not data.get("href"):
        raise VideoError("Диск не дал ссылку на файл")
    return data["href"]


def get_download_url(path: str) -> str:
    """POST models-v2?m=mpfs/url с сохранёнными куками и sk -> прямая ссылка на скачивание."""
    if path.startswith("/public/"):
        return public_download_url(path)
    a = load_auth()
    if not a["cookies"] or not a["sk"]:
        raise AuthError("нет данных авторизации: открой Яндекс Диск в браузере через mitmproxy "
                        "(обнови страницу), аддон передаст куки и sk")
    cid = a["connection_id"] or ""
    cid = (cid[:-3] + f"{random.randint(0, 999):03d}") if len(cid) > 6 else str(int(time.time() * 1000))
    body = {"sk": a["sk"], "connection_id": cid, "apiMethod": "mpfs/url", "requestParams": {"path": path}}
    req = urllib.request.Request(
        API_BASE + "/models-v2?m=mpfs/url",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Cookie": "; ".join(f"{n}={v}" for n, v in a["cookies"]),
            "User-Agent": a["user_agent"] or USER_AGENT,
            "Origin": API_BASE,
            "Referer": API_BASE + "/client/photo",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    try:
        with VD.opener.open(req, timeout=API_TIMEOUT) as resp:
            text = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise AuthError(f"Яндекс ответил HTTP {e.code}: сессия или sk протухли — обнови страницу Диска "
                            "в браузере через mitmproxy")
        raise VideoError(f"запрос ссылки: HTTP {e.code}")
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as e:
        raise VideoError(f"запрос ссылки: {short_err(e)}")
    try:
        data = json.loads(text)
    except ValueError:
        raise AuthError("Яндекс вернул не JSON (скорее всего, страницу входа): сессия протухла")
    url = find_key(data, "file")
    if not isinstance(url, str) or not url:
        err = find_key(data, "error") or find_key(data, "code") or data
        raise VideoError("Яндекс не вернул ссылку: " + json.dumps(err, ensure_ascii=False)[:200])
    return "https:" + url if url.startswith("//") else url


# ---------------------------------------------------------------- queries

def build_filters(args):
    where, params = [], []
    q = (args.get("q") or "").strip()
    if q:
        where.append("name LIKE ?")
        params.append(f"%{q}%")
    mt = args.get("type")
    if mt in ("image", "video"):
        where.append("mediatype = ?")
        params.append(mt)
    album = (args.get("album") or "").strip()
    if album:
        where.append("albums LIKE ?")
        params.append(f'%"{album}"%')
    sql = (" WHERE " + " AND ".join(where)) if where else ""
    return sql, params


def stats():
    with db() as c:
        row = c.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(mediatype='image') AS images,
                   SUM(mediatype='video') AS videos,
                   SUM(resource_id IS NOT NULL) AS with_resource,
                   COALESCE(SUM(size),0) AS bytes
            FROM items
            """
        ).fetchone()
        clusters = c.execute("SELECT COUNT(*) FROM clusters").fetchone()[0]
    d = dict(row)
    d["clusters"] = clusters
    return {k: (v or 0) for k, v in d.items()}


def all_albums():
    found = set()
    with db() as c:
        for (a,) in c.execute("SELECT DISTINCT albums FROM items WHERE albums IS NOT NULL"):
            try:
                found.update(json.loads(a))
            except Exception:  # noqa: BLE001
                pass
    return sorted(found)


def human_size(n):
    if not n:
        return ""
    n = float(n)
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024 or unit == "ГБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024


def human_duration(ms):
    if not ms:
        return ""
    s = int(ms // 1000)
    return f"{s // 60}:{s % 60:02d}"


@app.get("/api/stats")
def api_stats():
    return jsonify(stats())


@app.get("/api/items")
def api_items():
    sql, params = build_filters(request.args)
    limit = min(int(request.args.get("limit", 1000)), 10000)
    offset = int(request.args.get("offset", 0))
    with db() as c:
        rows = c.execute(
            "SELECT path,name,taken,mediatype,mimetype,size,width,height,duration_ms,albums,cluster_id,resource_id "
            f"FROM items{sql} ORDER BY COALESCE(taken,'') DESC, name DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["albums"] = json.loads(d["albums"]) if d["albums"] else []
        out.append(d)
    return jsonify(out)


# ---------------------------------------------------------------- gallery

MONTHS = ["января", "февраля", "марта", "апреля", "мая", "июня",
          "июля", "августа", "сентября", "октября", "ноября", "декабря"]


def day_label(day):
    if not day:
        return "Без даты"
    try:
        d = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return day
    s = f"{d.day} {MONTHS[d.month - 1]}"
    if d.year != datetime.now().year:
        s += f" {d.year}"
    return s


PAGE = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Фото — локальная галерея</title>
<style>
  :root { color-scheme: dark light; --tile:130px; --bg:#202024; --fg:#ececf1; --mut:#8e8e99; --line:#33333a; --cell:#44444c; --badge:rgba(0,0,0,.55); }
  @media (prefers-color-scheme: light) { :root { --bg:#fff; --fg:#1b1b1f; --mut:#6b6b76; --line:#e3e3ea; --cell:#e4e4ea; --badge:rgba(0,0,0,.55); } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:13px/1.4 system-ui, sans-serif; }
  main { padding:14px 24px 60px; }
  header { display:flex; flex-wrap:wrap; gap:10px 16px; align-items:center; margin-bottom:6px; }
  h1 { font-size:17px; margin:0; font-weight:600; }
  .stats { color:var(--mut); font-size:12px; flex:1 1 auto; }
  form { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:8px 0 4px; }
  input, select, button { font:inherit; padding:5px 10px; border:1px solid var(--line); border-radius:8px; background:transparent; color:var(--fg); }
  input[type=range] { padding:0; width:110px; }
  h2 { font-size:13px; font-weight:600; margin:22px 0 8px; }
  h2 .cnt { color:var(--mut); font-weight:400; margin-left:6px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(var(--tile), 1fr)); gap:4px; }
  .tile { position:relative; aspect-ratio:1; background:var(--cell); border-radius:2px; overflow:hidden; }
  .tile a.open { position:absolute; inset:0; z-index:2; cursor:zoom-in; }
  .tile .badge, .tile .dl, .tile .tname { pointer-events:none; z-index:3; }
  .tile img { position:absolute; inset:0; width:100%; height:100%; object-fit:cover; display:block; }
  .tile.has-img .tname { opacity:0; background:linear-gradient(transparent, rgba(0,0,0,.7)); color:#fff;
                         left:0; right:0; bottom:0; padding:14px 6px 4px; transition:opacity .15s; }
  .tile.has-img:hover .tname { opacity:1; }
  .tname { position:absolute; left:6px; right:6px; bottom:5px; font:10px/1.25 ui-monospace, Menlo, monospace;
           color:var(--fg); opacity:.75; word-break:break-all; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }
  .badge { position:absolute; left:6px; top:6px; background:var(--badge); color:#fff; border-radius:10px; padding:1px 7px; font-size:11px; }
  .dl { position:absolute; right:6px; top:6px; background:#2e7d32; color:#fff; border-radius:10px; padding:1px 7px; font-size:10px; letter-spacing:.02em; }
  .dl.low { background:#b26a00; }
  nav { display:flex; gap:14px; }
  nav a { color:var(--mut); text-decoration:none; }
  nav a.on { color:var(--fg); font-weight:600; }
  #status { color:var(--mut); text-align:center; padding:28px 0; min-height:20px; }
  .empty { color:var(--mut); margin-top:30px; }

  #viewer { position:fixed; inset:0; z-index:100; display:flex; align-items:center; justify-content:center;
            background:rgba(0,0,0,.82); backdrop-filter:blur(2px); padding:40px; }
  #viewer[hidden] { display:none; }
  #viewer .vwrap { position:relative; max-width:100%; max-height:100%; display:flex; }
  #viewer img, #viewer video { display:block; max-width:100%; max-height:calc(100vh - 80px); border-radius:4px;
                                box-shadow:0 10px 40px rgba(0,0,0,.5); }
  #viewer .vclose { position:absolute; top:-18px; right:-18px; width:36px; height:36px; border-radius:50%;
                     background:#1c1c20; color:#fff; border:1px solid rgba(255,255,255,.2); font-size:18px;
                     line-height:1; cursor:pointer; display:flex; align-items:center; justify-content:center; }
  #viewer .vclose:hover { background:#2a2a30; }
  #viewer .vcaption { position:absolute; left:0; right:0; bottom:-30px; text-align:center; color:#cfcfd6;
                       font-size:12px; word-break:break-all; }
</style>
</head>
<body>
<main>
  <header>
    <h1>Фото</h1>
    <nav><a href="/" class="on">Галерея</a><a href="/status">Статус загрузки</a></nav>
    <div class="stats">
      всего {{ s.total }} · фото {{ s.images }} · видео {{ s.videos }} ·
      с данными ресурса {{ s.with_resource }} · кластеров {{ s.clusters }} · {{ human(s.bytes) }}
    </div>
  </header>

  <form method="get">
    <input name="q" value="{{ base.q }}" placeholder="поиск по имени">
    <select name="type">
      <option value="">фото + видео</option>
      <option value="image" {{ 'selected' if base.type=='image' }}>только фото</option>
      <option value="video" {{ 'selected' if base.type=='video' }}>только видео</option>
    </select>
    <select name="album">
      <option value="">все альбомы</option>
      {% for a in albums %}<option value="{{ a }}" {{ 'selected' if a==base.album }}>{{ a }}</option>{% endfor %}
    </select>
    <button>Показать</button>
    <label class="stats">размер <input type="range" id="size" min="70" max="260" value="130"></label>
  </form>

  {% if not fragment.strip() %}<p class="empty">Пока пусто. Открой фото в Яндекс Диске через mitmproxy — данные появятся здесь.</p>{% endif %}

  <div id="list">{{ fragment|safe }}</div>
  <div id="sentinel"></div>
  <div id="status"></div>
</main>

<div id="viewer" hidden>
  <div class="vwrap">
    <button class="vclose" type="button" aria-label="Закрыть" title="Закрыть (Esc)">✕</button>
    <span class="vcaption"></span>
  </div>
</div>

<script>
  const FILTERS = {{ base|tojson }};
  let cursor = {{ cursor|tojson }};          // последний показанный день (null — ничего нет)
  let hasMore = {{ has_more|tojson }};
  let loading = false;

  const list = document.getElementById('list');
  const statusEl = document.getElementById('status');
  const sentinel = document.getElementById('sentinel');

  // просмотр фото/видео поверх галереи (вместо target=_blank — во встроенной
  // вкладке приложения новые окна/вкладки не открываются)
  const viewer = document.getElementById('viewer');
  const vwrap = viewer.querySelector('.vwrap');
  const vcaption = viewer.querySelector('.vcaption');
  let vmedia = null;

  function closeViewer() {
    viewer.hidden = true;
    document.body.style.overflow = '';
    if (vmedia) { vmedia.pause && vmedia.pause(); vmedia.remove(); vmedia = null; }
  }

  function openViewer(rid, type, name) {
    if (vmedia) vmedia.remove();
    vmedia = document.createElement(type === 'video' ? 'video' : 'img');
    if (type === 'video') { vmedia.controls = true; vmedia.autoplay = true; }
    else { vmedia.alt = name || ''; }
    vmedia.src = '/file/' + rid;
    vwrap.insertBefore(vmedia, vcaption);
    vcaption.textContent = name || '';
    viewer.hidden = false;
    document.body.style.overflow = 'hidden';
  }

  list.addEventListener('click', (e) => {
    const a = e.target.closest('a.open');
    if (!a) return;
    e.preventDefault();
    openViewer(a.dataset.rid, a.dataset.type, a.dataset.name);
  });

  viewer.addEventListener('click', (e) => {
    if (e.target === viewer) closeViewer();   // клик по затемнённому фону
  });
  viewer.querySelector('.vclose').onclick = closeViewer;
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !viewer.hidden) closeViewer();
  });

  // размер плиток
  const sizeEl = document.getElementById('size');
  try { const v = localStorage.getItem('tile'); if (v) sizeEl.value = v; } catch (e) {}
  function applySize() {
    document.documentElement.style.setProperty('--tile', sizeEl.value + 'px');
    try { localStorage.setItem('tile', sizeEl.value); } catch (e) {}
  }
  sizeEl.addEventListener('input', applySize); applySize();

  // подгрузка по мере прокрутки
  async function loadMore() {
    if (loading || !hasMore) return;
    loading = true;
    statusEl.textContent = 'загрузка…';
    try {
      const p = new URLSearchParams({ ...FILTERS, before: cursor });
      const r = await fetch('/chunk?' + p);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const j = await r.json();
      list.insertAdjacentHTML('beforeend', j.html);
      hasMore = j.has_more;
      if (j.cursor !== null) cursor = j.cursor;
      statusEl.textContent = hasMore ? '' : 'это всё';
    } catch (e) {
      statusEl.textContent = 'ошибка загрузки, пробую ещё раз…';
      setTimeout(() => { loading = false; loadMore(); }, 2000);
      return;
    }
    loading = false;
    // если после догрузки низ всё ещё на экране — проверим заново
    io.unobserve(sentinel); io.observe(sentinel);
  }

  const io = new IntersectionObserver((entries) => {
    if (entries.some(e => e.isIntersecting)) loadMore();
  }, { rootMargin: '1500px 0px' });
  if (hasMore) io.observe(sentinel); else if (list.children.length) statusEl.textContent = 'это всё';
</script>
</body>
</html>
"""

FRAGMENT = """{% for day, items in groups %}
<section>
  <h2>{{ day_label(day) }}<span class="cnt">{{ items|length }}</span></h2>
  <div class="grid">
    {% for it in items %}
    {% set got = it.dl_status=='done' or (it.mediatype=='video' and it.vd_status=='done') %}
    <div class="tile{{ ' has-img' if got }}" title="{{ it.name }}{% if it.size %} · {{ human(it.size) }}{% endif %}{% if it.width and it.height %} · {{ it.width }}×{{ it.height }}{% endif %}">
      {% if got %}<a class="open" href="/file/{{ it.rid }}" data-rid="{{ it.rid }}" data-type="{{ it.mediatype }}" data-name="{{ it.name }}" rel="noopener"></a>{% endif %}
      {% if got %}<img src="/thumb/{{ it.rid }}" loading="lazy" decoding="async" alt="" onerror="this.parentNode.classList.remove('has-img');this.remove()">{% endif %}
      {% if it.mediatype=='video' %}<span class="badge">▶{% if it.duration_ms %} {{ dur(it.duration_ms) }}{% endif %}</span>{% endif %}
      {% if it.dl_status=='done' and it.dl_size %}<span class="dl{{ ' low' if it.dl_size!='ORIGINAL' }}">{{ it.dl_size }}</span>{% endif %}
      {% if it.mediatype=='video' and it.vd_status=='done' %}<span class="dl{{ ' low' if it.vd_lost }}">{% if it.is_local_existing %}уже на диске{% elif it.vd_lost %}потеряно {{ it.lostpct }}%{% else %}оригинал{% endif %}</span>{% endif %}
      <span class="tname">{{ it.name }}</span>
    </div>
    {% endfor %}
  </div>
</section>
{% endfor %}"""

DAY_EXPR = "COALESCE(substr(taken,1,10),'')"


def load_chunk(args, before=None):
    """Следующая порция дней (новые -> старые). before — день-курсор: берём дни строго старше."""
    sql, params = build_filters(args)
    with db() as c:
        if before is None:
            q = f"SELECT {DAY_EXPR} AS day FROM items{sql} GROUP BY day ORDER BY day DESC LIMIT ?"
            qp = params + [DAYS_PER_CHUNK + 1]
        else:
            q = (f"SELECT day FROM (SELECT {DAY_EXPR} AS day FROM items{sql}) "
                 "WHERE day < ? GROUP BY day ORDER BY day DESC LIMIT ?")
            qp = params + [before, DAYS_PER_CHUNK + 1]
        days = [r[0] for r in c.execute(q, qp)]
        has_more = len(days) > DAYS_PER_CHUNK
        days = days[:DAYS_PER_CHUNK]

        rows = []
        if days:
            marks = ",".join("?" * len(days))
            cond = f"{DAY_EXPR} IN ({marks})"
            where = f"{sql} AND {cond}" if sql else f" WHERE {cond}"
            rows = c.execute(
                "SELECT rowid AS rid,name,taken,mediatype,size,width,height,duration_ms,dl_status,dl_size,"
                "vd_status,vd_lost,vd_note "
                f"FROM items{where} ORDER BY COALESCE(taken,'') DESC, name DESC",
                params + days,
            ).fetchall()

    by_day = {d: [] for d in days}
    for r in rows:
        d = dict(r)
        d["lostpct"] = round(100.0 * d["vd_lost"] / d["size"], 1) if d.get("vd_lost") and d.get("size") else 0
        d["is_local_existing"] = d.get("vd_note") == LOCAL_EXISTING_FILE_NOTE
        by_day[(r["taken"] or "")[:10]].append(d)
    groups = [(d, by_day[d]) for d in days]
    cursor = days[-1] if days else None
    return groups, has_more, cursor


def render_fragment(groups):
    return render_template_string(
        FRAGMENT, groups=groups, human=human_size, dur=human_duration, day_label=day_label
    )


@app.get("/chunk")
def chunk():
    groups, has_more, cursor = load_chunk(request.args, request.args.get("before"))
    return jsonify(html=render_fragment(groups), has_more=has_more, cursor=cursor)


@app.get("/")
def gallery():
    groups, has_more, cursor = load_chunk(request.args)
    base = {k: request.args.get(k, "") for k in ("q", "type", "album")}
    return render_template_string(
        PAGE,
        s=stats(), albums=all_albums(), base=base, human=human_size,
        fragment=render_fragment(groups), cursor=cursor, has_more=has_more,
    )


# ---------------------------------------------------------------- общие помощники загрузки

@contextmanager
def dbc():
    """Соединение, которое гарантированно коммитится и закрывается."""
    c = db()
    try:
        yield c
        c.commit()
    finally:
        c.close()


def get_setting(key, default=None):
    with dbc() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r[0] if r else default


def set_setting(key, value):
    with dbc() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


_BAD_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def safe_name(s):
    return _BAD_CHARS.sub("_", s or "").strip() or "file"


LOCAL_EXISTING_FILE_NOTE = (
    "Найден ранее сохранённый локальный файл; его содержимое повторно не проверялось."
)


def _local_file_candidates(row, is_video):
    """Кандидаты имён, которые создаёт загрузчик для этого элемента."""
    path = row["path"]
    name = safe_name(row["name"] or name_from_path(path))
    day = (row["taken"] or "")[:10] or "no-date"
    tag = hashlib.md5(path.encode("utf-8")).hexdigest()[:8]
    stem, source_ext = os.path.splitext(name)
    result = []
    seen = set()

    def add(filename, label, recovered=False):
        rel = os.path.join(day, filename)
        key = (rel, label, recovered)
        if key not in seen:
            seen.add(key)
            result.append((rel, label, recovered))

    if is_video:
        add(name, "LOCAL", False)
        if source_ext:
            add(f"{stem}_{tag}{source_ext}", "LOCAL", False)
        recovered = f"{stem}.recovered.mp4"
        add(recovered, "LOCAL", True)
        add(f"{stem}.recovered_{tag}.mp4", "LOCAL", True)
        return result

    add(name, "ORIGINAL", False)
    if source_ext:
        add(f"{stem}_{tag}{source_ext}", "ORIGINAL", False)

    output_exts = sorted(set(EXT_BY_CTYPE.values()) | {".jpg"})
    for quality in SIZE_ORDER:
        if quality == "ORIGINAL":
            continue
        for ext in output_exts:
            add(f"{stem}.{quality}{ext}", quality, False)
            add(f"{stem}.{quality}_{tag}{ext}", quality, False)
    return result


def reconcile_existing_downloads(paths=None):
    """Помечает как done файлы, уже существующие в каталоге загрузок.

    Статус done никогда не сбрасывается из-за отсутствующего файла: это намеренно,
    чтобы ручное удаление файла не запускало повторную загрузку автоматически.
    Если старый статус done указывает на устаревший корень, при обнаружении файла
    в другом известном каталоге обновляем только путь к нему.
    """
    roots = []
    for candidate in (files_dir(), DEFAULT_FILES_DIR):
        root = os.path.abspath(os.path.expanduser(candidate))
        if root not in roots:
            roots.append(root)

    if paths is None:
        with dbc() as c:
            rows = c.execute(
                "SELECT path,name,taken,mediatype,size,dl_status,dl_size,dl_file,dl_root,"
                "vd_status,vd_file,vd_root,vd_lost,vd_note FROM items "
                "ORDER BY CASE WHEN dl_status='done' OR vd_status='done' THEN 0 ELSE 1 END, path"
            ).fetchall()
    else:
        unique_paths = list(dict.fromkeys(str(p) for p in paths if p))
        rows = []
        # SQLite на некоторых версиях ограничивает число параметров запроса.
        with dbc() as c:
            for start in range(0, len(unique_paths), 800):
                batch = unique_paths[start:start + 800]
                marks = ",".join("?" for _ in batch)
                rows.extend(c.execute(
                    "SELECT path,name,taken,mediatype,size,dl_status,dl_size,dl_file,dl_root,"
                    "vd_status,vd_file,vd_root,vd_lost,vd_note FROM items "
                    f"WHERE path IN ({marks}) ORDER BY CASE WHEN dl_status='done' OR vd_status='done' THEN 0 ELSE 1 END, path",
                    batch,
                ).fetchall())

    # Кэшируем имена файлов по каталогу с датой — тысячи элементов одного дня
    # не должны приводить к тысячам повторных os.listdir().
    dir_cache = {}
    claimed = {}
    counts = {"photos": 0, "videos": 0, "repaired_roots": 0}
    now = now_iso()

    def root_order(row, is_video):
        saved = row["vd_root"] if is_video else row["dl_root"]
        ordered = []
        if saved:
            ordered.append(os.path.abspath(os.path.expanduser(saved)))
        ordered.extend(roots)
        out = []
        for item in ordered:
            real = os.path.realpath(item)
            if real not in out:
                out.append(real)
        return out

    def try_relative(root, rel, item_path):
        if not rel:
            return None
        rel_norm = os.path.normpath(str(rel))
        owner = claimed.get((os.path.realpath(root), rel_norm))
        if owner and owner != item_path:
            return None
        try:
            candidate = verify_saved_file(root, rel_norm, min_size=1)
            size = os.path.getsize(candidate)
        except (OSError, TypeError, ValueError):
            return None
        return {"root": os.path.realpath(root), "rel": rel_norm, "size": size,
                "label": None, "recovered": ".recovered" in os.path.basename(rel_norm)}

    def locate(row, is_video):
        field_file = "vd_file" if is_video else "dl_file"
        field_status = "vd_status" if is_video else "dl_status"
        stored_rel = row[field_file]
        status_done = row[field_status] == "done"
        roots_for_item = root_order(row, is_video)

        # Сначала используем конкретное имя из старой БД, если файл действительно есть.
        if status_done and stored_rel:
            for root in roots_for_item:
                match = try_relative(root, stored_rel, row["path"])
                if match:
                    return match

        # Иначе пытаемся сопоставить файлы по дате, исходному имени и схеме имён загрузчика.
        candidates = _local_file_candidates(row, is_video)
        for root in roots_for_item:
            for rel, label, recovered in candidates:
                day = os.path.dirname(rel) or "."
                cache_key = (root, day)
                if cache_key not in dir_cache:
                    try:
                        dir_cache[cache_key] = set(os.listdir(os.path.join(root, day)))
                    except OSError:
                        dir_cache[cache_key] = set()
                if os.path.basename(rel) not in dir_cache[cache_key]:
                    continue
                match = try_relative(root, rel, row["path"])
                if match:
                    match["label"] = label
                    match["recovered"] = recovered
                    return match
        return None

    with dbc() as c:
        for row in rows:
            if not row["path"] or not row["name"]:
                continue
            is_video = (row["mediatype"] == "video" or
                        (row["mediatype"] is None and guess_mediatype(row["name"]) == "video"))
            status_key = "vd_status" if is_video else "dl_status"
            file_key = "vd_file" if is_video else "dl_file"
            root_key = "vd_root" if is_video else "dl_root"
            is_already_done = row[status_key] == "done"
            match = locate(row, is_video)
            if not match:
                # Важно: не сбрасываем done, даже если файла больше нет на диске.
                continue

            claim_key = (match["root"], os.path.normpath(match["rel"]))
            claimed[claim_key] = row["path"]
            rel = match["rel"]
            root = match["root"]
            size = match["size"]
            old_root = row[root_key]
            old_rel = row[file_key]

            if is_video:
                # Если обнаружен восстановленный вариант, точный объём потерь неизвестен;
                # отдельно помечаем его как найденный локальный файл, не как новый recover.
                c.execute(
                    "UPDATE items SET vd_status='done', vd_phase='done', vd_file=?, vd_root=?, "
                    "vd_bytes=?, vd_lost=COALESCE(vd_lost,0), vd_error=NULL, "
                    "vd_note=CASE WHEN vd_status='done' THEN COALESCE(vd_note, ?) ELSE ? END, "
                    "vd_at=COALESCE(vd_at, ?) WHERE path=?",
                    (rel, root, size, LOCAL_EXISTING_FILE_NOTE, LOCAL_EXISTING_FILE_NOTE, now, row["path"]),
                )
            else:
                label = match["label"] or row["dl_size"] or "ORIGINAL"
                c.execute(
                    "UPDATE items SET dl_status='done', dl_size=?, dl_file=?, dl_root=?, dl_bytes=?, "
                    "dl_error=NULL, dl_attempts=NULL, dl_at=COALESCE(dl_at, ?) WHERE path=?",
                    (label, rel, root, size, now, row["path"]),
                )

            if is_already_done:
                if old_root != root or old_rel != rel:
                    counts["repaired_roots"] += 1
            elif is_video:
                counts["videos"] += 1
            else:
                counts["photos"] += 1

    return counts


def short_err(e):
    if isinstance(e, DeadlineError):
        return "превышен общий лимит времени"
    if isinstance(e, urllib.error.HTTPError):
        return f"HTTP {e.code}"
    if isinstance(e, urllib.error.URLError):
        r = e.reason
        return "timeout" if isinstance(r, (socket.timeout, TimeoutError)) else f"сеть: {r}"
    if isinstance(e, (socket.timeout, TimeoutError)):
        return "timeout"
    return f"{type(e).__name__}: {e}"


class Aborted(Exception):
    """Загрузка остановлена пользователем."""


class Unreadable(Exception):
    """Файл на Диске не отдаёт ни одного байта — спасать нечего."""


class NoSpace(Exception):
    """На диске, куда качаем, кончилось место — дальше упадёт каждый файл, загрузку надо остановить."""


class DeadlineError(Exception):
    """Попытка длится дольше ATTEMPT_DEADLINE."""


def build_opener():
    # Скачиваем напрямую: системные/переменные прокси (в т.ч. сам mitmproxy) игнорируем.
    handlers = [urllib.request.ProxyHandler({})]
    if os.environ.get("YDISK_INSECURE_SSL"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
    return urllib.request.build_opener(*handlers)


# Статус done хранится в SQLite и не сбрасывается при проверке наличия файла:
# если пользователь позже удалит файл вручную, загрузчик не будет качать его повторно.
ELIGIBLE = "mediatype='image' AND raw IS NOT NULL AND dl_status IS NULL"
ELIGIBLE_VD = "mediatype='video' AND raw IS NOT NULL AND vd_status IS NULL"
GALLERY_ORDER = "ORDER BY COALESCE(taken,'') DESC, name DESC"   # как в галерее: сверху вниз


class Downloader:
    def __init__(self):
        self.threads = DEFAULT_THREADS
        self.stop_event = threading.Event()
        self.stop_event.set()              # изначально «не запущено»
        self.finishing = threading.Event()  # менеджер закончил: воркеры выходят, когда очередь пуста
        self.manager = None
        self.workers = {}
        self.active = {}                   # path -> прогресс текущей загрузки
        self.alock = threading.Lock()
        self.slock = threading.Lock()
        self.state_lock = threading.Lock()
        self.claim_lock = threading.Lock()
        self.refill_lock = threading.Lock()
        self.window = deque()              # (monotonic, bytes) для скорости
        self.session = {"done": 0, "failed": 0, "bytes": 0, "started": None}
        self.message = ""
        self.opener = build_opener()

    # ---------- управление

    def load_settings(self):
        try:
            self.threads = max(1, min(MAX_THREADS, int(get_setting("threads", DEFAULT_THREADS))))
        except (TypeError, ValueError):
            self.threads = DEFAULT_THREADS

    def set_threads(self, n):
        self.threads = max(1, min(MAX_THREADS, int(n)))
        set_setting("threads", self.threads)
        return self.threads

    def _alive(self):
        return bool(self.manager and self.manager.is_alive()) or any(t.is_alive() for t in self.workers.values())

    def state(self):
        if not self._alive():
            return "idle"
        return "stopping" if self.stop_event.is_set() else "running"

    def start(self):
        with self.state_lock:
            if self._alive():
                return {"ok": False, "error": "уже запущено" if not self.stop_event.is_set() else "ещё останавливается, подожди секунду"}
            self.stop_event.clear()
            self.finishing.clear()
            self.message = ""
            self.workers = {}
            with self.alock:
                self.active = {}
            self.session = {"done": 0, "failed": 0, "bytes": 0, "started": time.time()}
            self.manager = threading.Thread(target=self._manager_loop, name="dl-manager", daemon=True)
            self.manager.start()
        return {"ok": True}

    def stop(self, message=""):
        if message:
            self.message = message
        self.stop_event.set()

    def retry_failed(self):
        with dbc() as c:
            cur = c.execute(
                "UPDATE items SET dl_status=NULL, dl_error=NULL, dl_attempts=NULL WHERE dl_status='failed'"
            )
            return cur.rowcount

    # ---------- менеджер

    def refill(self):
        """Держит в очереди ровно QUEUE_SIZE ожидающих файлов (самые верхние в галерее — первыми)."""
        with self.refill_lock:
            with dbc() as c:
                have = c.execute("SELECT COUNT(*) FROM items WHERE dl_status='queued'").fetchone()[0]
                need = QUEUE_SIZE - have
                if need <= 0:
                    return
                rows = c.execute(
                    f"SELECT path FROM items WHERE {ELIGIBLE} {GALLERY_ORDER} LIMIT ?", (need,)
                ).fetchall()
                if rows:
                    ts = now_iso()
                    c.executemany(
                        "UPDATE items SET dl_status='queued', dl_at=? WHERE path=?",
                        [(ts, r[0]) for r in rows],
                    )

    def _finished(self):
        with dbc() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(dl_status='queued'),0), COALESCE(SUM(dl_status='downloading'),0), "
                f"COALESCE(SUM({ELIGIBLE}),0) FROM items"
            ).fetchone()
        return not any(r)

    def _ensure_workers(self):
        for idx in range(self.threads):
            t = self.workers.get(idx)
            if t is None or not t.is_alive():
                t = threading.Thread(target=self._worker_loop, args=(idx,), name=f"dl-{idx}", daemon=True)
                self.workers[idx] = t
                t.start()

    def _manager_loop(self):
        natural = False
        try:
            while not self.stop_event.is_set():
                self._ensure_workers()
                self.refill()
                if self._finished():
                    natural = True
                    break
                time.sleep(0.5)
        except Exception as e:  # noqa: BLE001
            self.message = f"ошибка менеджера: {e}"
            self.stop_event.set()
        finally:
            if natural and not self.message:
                self.message = "Готово: очередь пуста."
            self.finishing.set()
            for t in list(self.workers.values()):
                t.join(timeout=30)
            with dbc() as c:
                c.execute("UPDATE items SET dl_status=NULL WHERE dl_status IN ('queued','downloading')")

    # ---------- воркер

    def _claim(self):
        with self.claim_lock:
            with dbc() as c:
                row = c.execute(
                    f"SELECT path,name,taken,raw FROM items WHERE dl_status='queued' {GALLERY_ORDER} LIMIT 1"
                ).fetchone()
                if not row:
                    return None
                c.execute("UPDATE items SET dl_status='downloading', dl_at=? WHERE path=?", (now_iso(), row["path"]))
                return dict(row)

    def _worker_loop(self, idx):
        while not self.stop_event.is_set() and idx < self.threads:
            row = self._claim()
            if row is None:
                if self.finishing.is_set():
                    break
                time.sleep(0.3)
                continue
            try:
                self._process(row)
            except Exception as e:  # noqa: BLE001
                self._record_fail(row["path"], f"внутренняя ошибка: {e}", [])
            self.refill()

    def _set_item(self, path, status, size=None, file=None, nbytes=None, error=None, attempts=None):
        # Запоминаем корень отдельно: смена папки сохранения не должна ломать доступ
        # к файлам, которые были скачаны раньше.
        saved_root = os.path.abspath(files_dir()) if status == "done" else None
        with dbc() as c:
            c.execute(
                "UPDATE items SET dl_status=?, dl_size=?, dl_file=?, dl_root=?, dl_bytes=?, dl_error=?, dl_attempts=?, dl_at=? WHERE path=?",
                (status, size, file, saved_root, nbytes, (error or None) and error[:500],
                 json.dumps(attempts, ensure_ascii=False) if attempts else None, now_iso(), path),
            )

    def _on_result(self, ok, nbytes=0):
        # Ошибки отдельных файлов не останавливают очередь: она идёт до конца.
        with self.slock:
            if ok:
                self.session["done"] += 1
                self.session["bytes"] += nbytes
            else:
                self.session["failed"] += 1

    def _record_fail(self, path, error, attempts):
        self._set_item(path, "failed", error=error, attempts=attempts)
        self._on_result(False)

    def _process(self, row):
        path, name = row["path"], row["name"]
        day = (row["taken"] or "")[:10] or "no-date"
        try:
            meta = (json.loads(row["raw"] or "{}").get("meta")) or {}
            sizes = {s["name"]: s["url"] for s in (meta.get("sizes") or []) if s.get("name") and s.get("url")}
        except Exception:  # noqa: BLE001
            sizes = {}
        order = [s for s in SIZE_ORDER if s in sizes]
        if not order:
            self._record_fail(path, "нет ссылок на скачивание", [])
            return

        prog = {"name": name, "size": order[0], "bytes": 0, "total": None,
                "attempt": 1, "of": len(order), "since": time.time()}
        with self.alock:
            self.active[path] = prog
        attempts = []
        try:
            for i, size in enumerate(order, 1):
                if self.stop_event.is_set():
                    raise Aborted()
                prog.update(size=size, bytes=0, total=None, attempt=i)
                url = sizes[size]
                if url.startswith("//"):
                    url = "https:" + url
                try:
                    rel, nbytes = self._fetch(path, name, day, size, url, prog)
                except Aborted:
                    raise
                except Exception as e:  # noqa: BLE001
                    attempts.append(f"{size}: {short_err(e)}")
                    continue
                self._set_item(path, "done", size=size, file=rel, nbytes=nbytes, attempts=attempts)
                self._on_result(True, nbytes)
                return
            self._record_fail(path, "; ".join(attempts), attempts)
        except Aborted:
            self._set_item(path, None)
        finally:
            with self.alock:
                self.active.pop(path, None)

    def _fetch(self, path, name, day, size, url, prog):
        day_dir = os.path.join(files_dir(), day)
        os.makedirs(day_dir, exist_ok=True)
        tag = hashlib.md5(path.encode("utf-8")).hexdigest()[:8]
        tmp = os.path.join(day_dir, f".{tag}.{size}.part")
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT, "Accept": "*/*", "Referer": "https://disk.yandex.ru/",
        })
        started = time.monotonic()
        got = 0
        try:
            with self.opener.open(req, timeout=ATTEMPT_TIMEOUT) as resp:
                ctype = resp.headers.get_content_type()
                if ctype in ("text/html", "application/json", "text/plain", "application/xml"):
                    raise ValueError(f"в ответе {ctype} вместо файла")
                clen = resp.headers.get("Content-Length")
                total = int(clen) if clen and clen.isdigit() else None
                prog["total"] = total
                with open(tmp, "wb") as f:
                    while True:
                        if self.stop_event.is_set():
                            raise Aborted()
                        if time.monotonic() - started > ATTEMPT_DEADLINE:
                            raise DeadlineError()
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        got += len(chunk)
                        prog["bytes"] = got
                        self.window.append((time.monotonic(), len(chunk)))
            if got == 0:
                raise IOError("пустой ответ")
            if total is not None and got != total:
                raise IOError(f"неполный файл {got}/{total}")

            stem, ext0 = os.path.splitext(safe_name(name))
            if size == "ORIGINAL":
                final = safe_name(name)
            else:
                final = f"{stem}.{size}{EXT_BY_CTYPE.get(ctype, '.jpg')}"
            rel = f"{day}/{final}"
            with dbc() as c:
                clash = c.execute("SELECT 1 FROM items WHERE dl_file=? AND path<>? LIMIT 1", (rel, path)).fetchone()
            if clash:
                s2, e2 = os.path.splitext(final)
                final = f"{s2}_{tag}{e2}"
                rel = f"{day}/{final}"
            final_path = os.path.join(day_dir, final)
            os.replace(tmp, final_path)
            try:
                verify_saved_file(files_dir(), rel, expected_size=got)
            except OSError:
                # Не оставляем файл, который не прошёл проверку, как будто он сохранён корректно.
                try:
                    os.remove(final_path)
                except OSError:
                    pass
                raise
            return rel, got
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    # ---------- статус для страницы

    def snapshot(self):
        with dbc() as c:
            r = c.execute(
                f"""SELECT
                    COALESCE(SUM(mediatype='image'),0),
                    COALESCE(SUM(mediatype='image' AND raw IS NOT NULL),0),
                    COALESCE(SUM(mediatype='image' AND raw IS NULL),0),
                    COALESCE(SUM(dl_status='done'),0),
                    COALESCE(SUM(dl_status='failed'),0),
                    COALESCE(SUM(dl_status='queued'),0),
                    COALESCE(SUM(dl_status='downloading'),0),
                    COALESCE(SUM({ELIGIBLE}),0),
                    COALESCE(SUM(CASE WHEN dl_status='done' THEN dl_bytes END),0)
                FROM items"""
            ).fetchone()
            counts = dict(zip(
                ["images", "with_links", "no_links", "done", "failed", "queued", "downloading", "remaining", "done_bytes"],
                list(r),
            ))
            by_size = {s: n for s, n in c.execute(
                "SELECT dl_size, COUNT(*) FROM items WHERE dl_status='done' GROUP BY dl_size")}
            queue = [x[0] for x in c.execute(
                f"SELECT name FROM items WHERE dl_status='queued' {GALLERY_ORDER} LIMIT ?", (QUEUE_SIZE,))]
            recent_done = [
                {"name": x["name"], "size": x["dl_size"], "bytes": x["dl_bytes"], "file": x["dl_file"], "at": x["dl_at"]}
                for x in c.execute(
                    "SELECT name,dl_size,dl_bytes,dl_file,dl_at FROM items WHERE dl_status='done' "
                    "ORDER BY dl_at DESC, name DESC LIMIT 25")
            ]
            recent_failed = [
                {"name": x["name"], "error": x["dl_error"], "at": x["dl_at"]}
                for x in c.execute(
                    "SELECT name,dl_error,dl_at FROM items WHERE dl_status='failed' "
                    "ORDER BY dl_at DESC, name DESC LIMIT 25")
            ]

        now = time.monotonic()
        try:
            while self.window and now - self.window[0][0] > 10:
                self.window.popleft()
            items = list(self.window)
        except IndexError:
            items = []
        speed = 0
        if items:
            span = max(1.0, now - items[0][0])
            speed = sum(n for _, n in items) / span

        with self.alock:
            active = sorted(self.active.values(), key=lambda p: p["since"])
            active = [dict(p, elapsed=round(time.time() - p["since"], 1)) for p in active]

        return {
            "state": self.state(),
            "message": self.message,
            "threads": self.threads,
            "queue_size": QUEUE_SIZE,
            "files_dir": os.path.abspath(files_dir()),
            "speed": speed,
            "session": dict(self.session),
            "counts": counts,
            "by_size": by_size,
            "active": active,
            "queue": queue,
            "recent_done": recent_done,
            "recent_failed": recent_failed,
        }


DL = Downloader()


# ---------------------------------------------------------------- видео: recover.py

def read_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            st = json.load(f)
        return st if isinstance(st, dict) and st.get("size") else None
    except (OSError, ValueError):
        return None


def ranges_sum(rs):
    return sum(max(0, int(e) - int(s)) for s, e in rs or [])


def _cover(ranges, starts, a, b):
    """Сколько байт отрезка [a,b) покрыто непересекающимися отсортированными диапазонами."""
    i = max(0, bisect.bisect_right(starts, a) - 1)
    tot = 0
    while i < len(ranges) and ranges[i][0] < b:
        tot += max(0, min(ranges[i][1], b) - max(ranges[i][0], a))
        i += 1
    return tot


def make_bar(size, ok, bad, n=120):
    """
    Полоска из n сегментов, по символу на сегмент:
      g — прочитан целиком, p — читается прямо сейчас (частично), u — ещё не проверялся,
      y — часть сегмента потеряна, r — сегмент потерян больше чем наполовину.
    """
    if not size:
        return "u" * n
    ok = sorted([int(s), int(e)] for s, e in ok or [])
    bad = sorted([int(s), int(e)] for s, e in bad or [])
    so, sb = [r[0] for r in ok], [r[0] for r in bad]
    out = []
    for i in range(n):
        a, b = size * i // n, size * (i + 1) // n
        if b <= a:
            out.append("u")
            continue
        length = b - a
        x = _cover(bad, sb, a, b)
        o = _cover(ok, so, a, b)
        if x > 0:
            out.append("r" if x * 2 >= length else "y")
        elif o >= length:
            out.append("g")
        elif o > 0:
            out.append("p")
        else:
            out.append("u")
    return "".join(out)


def move_over(src, dst):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.exists(dst):
        os.remove(dst)
    try:
        os.replace(src, dst)
    except OSError:
        shutil.move(src, dst)


class VideoDownloader:
    """
    Для каждого видео:
      1. свежая ссылка (mpfs/url с куками из auth.db) -> recover_runner.py (скачивание блоками, битые места -> нули);
      2. если есть потери и включено «уточнять»: снова свежая ссылка -> recover.py --refine (+ докачка найденных островов);
      3. нет потерь: сырой файл = оригинал, кладём как есть; есть потери: recover.py --skip-download
         (ffmpeg: переупаковка или перекодирование) -> <имя>.recovered.mp4.
    Ссылку recover.py получает через stdin (в списке процессов её не видно).
    Прогресс между запусками хранится в files_dir()/<дата>/.vd/<метка>/v.state.json — после остановки докачка продолжается.
    """

    def __init__(self):
        self.settings = dict(VD_DEFAULTS)
        self.stop_event = threading.Event()
        self.stop_event.set()
        self.finishing = threading.Event()
        self.manager = None
        self.workers = {}
        self.active = {}
        self.alock = threading.Lock()
        self.slock = threading.Lock()
        self.state_lock = threading.Lock()
        self.claim_lock = threading.Lock()
        self.refill_lock = threading.Lock()
        self.session = {"done": 0, "failed": 0, "bytes": 0, "started": None}
        self.fail_streak = 0
        self.message = ""
        self.opener = build_opener()

    # ---------- настройки

    @staticmethod
    def _clean(key, value):
        default = VD_DEFAULTS[key]
        if isinstance(default, bool):
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("1", "true", "yes", "on")
        lo, hi = VD_LIMITS[key]
        return max(lo, min(hi, int(float(value))))

    def load_settings(self):
        for key in VD_DEFAULTS:
            raw = get_setting("vd_" + key)
            if raw is None:
                continue
            try:
                self.settings[key] = self._clean(key, json.loads(raw))
            except (TypeError, ValueError):
                pass

    def update_settings(self, body):
        errors = []
        for key, value in (body or {}).items():
            if key not in VD_DEFAULTS:
                continue
            try:
                self.settings[key] = self._clean(key, value)
                set_setting("vd_" + key, json.dumps(self.settings[key]))
            except (TypeError, ValueError):
                errors.append(key)
        return dict(self.settings), errors

    # ---------- управление

    def _alive(self):
        return bool(self.manager and self.manager.is_alive()) or any(t.is_alive() for t in self.workers.values())

    def state(self):
        if not self._alive():
            return "idle"
        return "stopping" if self.stop_event.is_set() else "running"

    def start(self):
        with self.state_lock:
            if self._alive():
                return {"ok": False, "error": "уже запущено" if not self.stop_event.is_set() else "ещё останавливается, подожди"}
            a = auth_summary()
            if not a["cookies"] or not a["has_sk"]:
                return {"ok": False, "error": "нет данных авторизации: открой Диск в браузере через mitmproxy "
                                              "(аддон передаст куки и sk), потом повтори"}
            if not os.path.isfile(os.path.join(BASE_DIR, "recover.py")) or not os.path.isfile(RUNNER):
                return {"ok": False, "error": "рядом с server.py должны лежать recover.py и recover_runner.py"}
            self.stop_event.clear()
            self.finishing.clear()
            self.message = ""
            self.fail_streak = 0
            self.workers = {}
            with self.alock:
                self.active = {}
            self.session = {"done": 0, "failed": 0, "bytes": 0, "started": time.time()}
            self.manager = threading.Thread(target=self._manager_loop, name="vd-manager", daemon=True)
            self.manager.start()
        return {"ok": True}

    def stop(self, message=""):
        if message:
            self.message = message
        self.stop_event.set()

    def retry_failed(self):
        with dbc() as c:
            cur = c.execute("UPDATE items SET vd_status=NULL, vd_error=NULL WHERE vd_status='failed'")
            return cur.rowcount

    # ---------- менеджер

    def refill(self):
        with self.refill_lock:
            with dbc() as c:
                have = c.execute("SELECT COUNT(*) FROM items WHERE vd_status='queued'").fetchone()[0]
                need = VD_QUEUE_SIZE - have
                if need <= 0:
                    return
                rows = c.execute(
                    f"SELECT path FROM items WHERE {ELIGIBLE_VD} {GALLERY_ORDER} LIMIT ?", (need,)).fetchall()
                if rows:
                    ts = now_iso()
                    c.executemany("UPDATE items SET vd_status='queued', vd_at=? WHERE path=?",
                                  [(ts, r[0]) for r in rows])

    def _finished(self):
        with dbc() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(vd_status='queued'),0), COALESCE(SUM(vd_status='downloading'),0), "
                f"COALESCE(SUM({ELIGIBLE_VD}),0) FROM items").fetchone()
        return not any(r)

    def _ensure_workers(self):
        for idx in range(self.settings["videos"]):
            t = self.workers.get(idx)
            if t is None or not t.is_alive():
                t = threading.Thread(target=self._worker_loop, args=(idx,), name=f"vd-{idx}", daemon=True)
                self.workers[idx] = t
                t.start()

    def _manager_loop(self):
        natural = False
        try:
            while not self.stop_event.is_set():
                self._ensure_workers()
                self.refill()
                if self._finished():
                    natural = True
                    break
                time.sleep(0.5)
        except Exception as e:  # noqa: BLE001
            self.message = f"ошибка менеджера: {e}"
            self.stop_event.set()
        finally:
            if natural and not self.message:
                self.message = "Готово: очередь видео пуста."
            self.finishing.set()
            for t in list(self.workers.values()):
                t.join(timeout=60)
            with dbc() as c:
                c.execute("UPDATE items SET vd_status=NULL WHERE vd_status IN ('queued','downloading')")

    # ---------- воркер

    def _claim(self):
        with self.claim_lock:
            with dbc() as c:
                row = c.execute(
                    f"SELECT path,name,taken,size FROM items WHERE vd_status='queued' {GALLERY_ORDER} LIMIT 1"
                ).fetchone()
                if not row:
                    return None
                c.execute("UPDATE items SET vd_status='downloading', vd_at=? WHERE path=?", (now_iso(), row["path"]))
                return dict(row)

    def _worker_loop(self, idx):
        while not self.stop_event.is_set() and idx < self.settings["videos"]:
            row = self._claim()
            if row is None:
                if self.finishing.is_set():
                    break
                time.sleep(0.3)
                continue
            try:
                self._process(row)
            except Exception as e:  # noqa: BLE001
                self._record_fail(row["path"], f"внутренняя ошибка: {type(e).__name__}: {e}")
            self.refill()

    def _set_video(self, path, status, phase=None, file=None, nbytes=None, lost=None, error=None, note=None):
        saved_root = os.path.abspath(files_dir()) if status == "done" else None
        with dbc() as c:
            c.execute(
                "UPDATE items SET vd_status=?, vd_phase=?, vd_file=?, vd_root=?, vd_bytes=?, vd_lost=?, vd_error=?, vd_note=?, vd_at=? "
                "WHERE path=?",
                (status, phase, file, saved_root, nbytes, lost, (error or None) and error[:500], note, now_iso(), path))

    def _on_result(self, ok, nbytes=0):
        with self.slock:
            if ok:
                self.session["done"] += 1
                self.session["bytes"] += nbytes
                self.fail_streak = 0
            else:
                self.session["failed"] += 1
                self.fail_streak += 1
                if self.fail_streak >= VD_FAIL_STREAK_STOP and not self.stop_event.is_set():
                    self.message = (f"{self.fail_streak} видео подряд не скачались — остановился. "
                                    "Проверь ссылки/авторизацию (обнови страницу Диска через mitmproxy), "
                                    "потом «Повторить ошибки».")
                    self.stop_event.set()

    def _record_fail(self, path, error):
        self._set_video(path, "failed", error=error)
        self._on_result(False)

    # ---------- запуск recover.py

    @staticmethod
    def _terminate(proc):
        if proc.poll() is not None:
            return
        try:
            if os.name == "posix":
                proc.send_signal(signal.SIGINT)      # recover.py сохранит прогресс и выйдет с кодом 130
            else:
                proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        try:
            proc.wait(timeout=20)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass

    def _run_recover(self, prog, args, url, idle_limit):
        """Запускает recover_runner.py; ссылку отдаёт через stdin. Возвращает (код, важные строки «! …»)."""
        cmd = [sys.executable, "-u", RUNNER] + args
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE if url else subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, cwd=BASE_DIR)
        prog["proc"] = proc
        if url:
            try:
                proc.stdin.write((url + "\n").encode("utf-8"))
                proc.stdin.close()
            except OSError:
                pass
        important = deque(maxlen=6)
        last = [time.monotonic()]

        def reader():
            dec = codecs.getincrementaldecoder("utf-8")("replace")
            buf = ""
            fd = proc.stdout.fileno()
            while True:
                try:
                    data = os.read(fd, 4096)
                except OSError:
                    break
                if not data:
                    break
                last[0] = time.monotonic()
                buf += dec.decode(data)
                parts = re.split(r"[\r\n]", buf)
                buf = parts[-1]
                for p in parts[:-1]:
                    p = p.strip()
                    if not p:
                        continue
                    prog["line"] = p[:220]
                    if p.startswith("!") or p.startswith("ffmpeg") or "не вышло" in p:
                        important.append(p[:220])
                if buf.strip():
                    prog["line"] = buf.strip()[:220]

        rt = threading.Thread(target=reader, daemon=True)
        rt.start()
        try:
            while proc.poll() is None:
                if self.stop_event.is_set():
                    self._terminate(proc)
                    break
                if time.monotonic() - last[0] > idle_limit:
                    self._terminate(proc)
                    rt.join(5)
                    raise VideoError(f"recover.py молчит больше {int(idle_limit)} с — прервал")
                time.sleep(0.4)
            proc.wait()
            rt.join(5)
        finally:
            prog["proc"] = None
            if proc.poll() is None:
                self._terminate(proc)
        if self.stop_event.is_set():
            raise Aborted()
        return proc.returncode, list(important)

    # ---------- обработка одного видео

    def _unique_rel(self, day, final, path, tag):
        rel = f"{day}/{final}"
        with dbc() as c:
            clash = c.execute("SELECT 1 FROM items WHERE (dl_file=? OR vd_file=?) AND path<>? LIMIT 1",
                              (rel, rel, path)).fetchone()
        if clash:
            stem, ext = os.path.splitext(final)
            final = f"{stem}_{tag}{ext}"
            rel = f"{day}/{final}"
        return rel

    def _process(self, row):
        path, name = row["path"], row["name"]
        day = (row["taken"] or "")[:10] or "no-date"
        tag = hashlib.md5(path.encode("utf-8")).hexdigest()[:8]
        workdir = os.path.join(files_dir(), day, ".vd", tag)
        os.makedirs(workdir, exist_ok=True)
        out = os.path.join(workdir, "v.mp4")
        base = os.path.join(workdir, "v")
        state_path = base + ".state.json"
        cfg = dict(self.settings)
        prog = {"name": name, "phase": "link", "line": "", "since": time.time(), "state_path": state_path,
                "size": row.get("size") or 0, "attempt": 0, "proc": None, "speed": 0.0,
                "_pg": 0, "_pt": time.monotonic()}
        with self.alock:
            self.active[path] = prog
        try:
            st = read_state(state_path)
            complete = bool(st) and ranges_sum(st.get("ok")) + ranges_sum(st.get("bad")) >= st["size"]
            if not complete:
                self._download_phase(prog, path, out, state_path, cfg, row.get("size"), refine=False)
            st = read_state(state_path)
            if not st:
                raise VideoError("recover.py не создал файл состояния")
            marker = os.path.join(workdir, ".refined")
            if cfg["refine"] and ranges_sum(st.get("bad")) > 0 and not os.path.exists(marker):
                self._download_phase(prog, path, out, state_path, cfg, row.get("size"), refine=True)
                open(marker, "w").close()
            self._finalize(prog, row, day, tag, workdir, out, base, cfg)
        except Aborted:
            self._set_video(path, None)
        except AuthError as e:
            self._set_video(path, None)
            self.stop(f"Остановился: {e}")
        except VideoError as e:
            self._record_fail(path, str(e))
        finally:
            with self.alock:
                self.active.pop(path, None)

    def _download_phase(self, prog, path, out, state_path, cfg, size_hint, refine):
        prog["phase"] = "refine" if refine else "download"
        idle = max(900, 10 * max(cfg["timeout"] * cfg["retries"], cfg["probe_timeout"]))
        last_err = ""
        for attempt in range(1, VD_LINK_RETRIES + 1):
            if self.stop_event.is_set():
                raise Aborted()
            prog.update(attempt=attempt, line="запрашиваю ссылку на скачивание…")
            url = get_download_url(path)
            args = ["-o", out, "--no-ffmpeg",
                    "--block", str(cfg["block_kb"] * 1024), "--sub", str(cfg["sub_kb"] * 1024),
                    "--timeout", str(cfg["timeout"]), "--sub-timeout", str(cfg["sub_timeout"]),
                    "--retries", str(cfg["retries"]),
                    "--probe-size", str(cfg["probe_size"]), "--probe-timeout", str(cfg["probe_timeout"])]
            if size_hint:
                args += ["--size", str(int(size_hint))]
            if refine:
                args += ["--refine", "--jobs", str(cfg["refine_jobs"]), "--stride", str(cfg["stride_kb"] * 1024)]
            else:
                args += ["--jobs", str(cfg["jobs"])]
            prog["line"] = "запускаю recover.py…"
            rc, important = self._run_recover(prog, args, url, idle)
            if rc == 0:
                return
            if rc == 3:       # ссылка просрочена / 403 / нет Range: берём новую, прогресс сохранён
                last_err = "ссылка недоступна (HTTP 403/404/410 или нет Range)"
                if important:
                    last_err += ": " + " | ".join(important[-2:])
                time.sleep(1)
                continue
            raise VideoError(f"recover.py завершился с кодом {rc}" + (": " + " | ".join(important[-3:]) if important else ""))
        raise VideoError(f"{VD_LINK_RETRIES} раза подряд ссылка не сработала — {last_err}")

    def _finalize(self, prog, row, day, tag, workdir, out, base, cfg):
        path, name = row["path"], row["name"]
        st = read_state(base + ".state.json")
        if not st:
            raise VideoError("нет файла состояния recover.py")
        size = int(st["size"])
        lost = ranges_sum(st.get("bad"))
        covered = ranges_sum(st.get("ok")) + lost
        if covered < size:
            raise VideoError(f"обработано {covered} из {size} байт — запусти ещё раз")
        raw = base + ".raw.bin"

        if lost == 0:
            rel = self._unique_rel(day, safe_name(name), path, tag)
            move_over(raw, os.path.join(files_dir(), rel))
            verify_saved_file(files_dir(), rel, expected_size=size)
            shutil.rmtree(workdir, ignore_errors=True)
            self._set_video(path, "done", phase="done", file=rel, nbytes=size, lost=0, note="оригинал без потерь")
            self._on_result(True, size)
            return

        prog.update(phase="assemble", line="собираю файл через ffmpeg…")
        if os.path.exists(out):
            os.remove(out)
        args = ["--skip-download", "-o", out] + (["--reencode"] if cfg["reencode"] else [])
        rc, important = self._run_recover(prog, args, None, 3600)
        if os.path.isfile(out) and os.path.getsize(out) > 2048:
            stem = os.path.splitext(safe_name(name))[0]
            rel = self._unique_rel(day, f"{stem}.recovered.mp4", path, tag)
            move_over(out, os.path.join(files_dir(), rel))
            verify_saved_file(files_dir(), rel, min_size=2049)
            try:
                os.remove(base + ".patched.mp4")
            except OSError:
                pass
            note = (f"потеряно {human_size(lost)} из {human_size(size)} ({100.0 * lost / size:.2f}%); "
                    f"сырые данные: {os.path.join(files_dir(), day, '.vd', tag)}")
            self._set_video(path, "done", phase="done", file=rel, nbytes=size, lost=lost, note=note)
            self._on_result(True, size)
            return
        raise VideoError("не удалось собрать файл (нет ffmpeg или он не справился)"
                         + (": " + " | ".join(important[-2:]) if important else "")
                         + f". Сырые данные сохранены: files/{day}/.vd/{tag}/")

    # ---------- статус для страницы

    def snapshot(self):
        with dbc() as c:
            r = c.execute(
                f"""SELECT
                    COALESCE(SUM(mediatype='video'),0),
                    COALESCE(SUM(mediatype='video' AND raw IS NOT NULL),0),
                    COALESCE(SUM(mediatype='video' AND raw IS NULL),0),
                    COALESCE(SUM(vd_status='done'),0),
                    COALESCE(SUM(vd_status='done' AND vd_lost>0),0),
                    COALESCE(SUM(vd_status='failed'),0),
                    COALESCE(SUM(vd_status='queued'),0),
                    COALESCE(SUM(vd_status='downloading'),0),
                    COALESCE(SUM({ELIGIBLE_VD}),0),
                    COALESCE(SUM(CASE WHEN vd_status='done' THEN vd_bytes END),0),
                    COALESCE(SUM(CASE WHEN vd_status='done' THEN vd_lost END),0)
                FROM items"""
            ).fetchone()
            counts = dict(zip(
                ["videos", "with_links", "no_links", "done", "partial", "failed", "queued", "downloading",
                 "remaining", "done_bytes", "lost_bytes"], list(r)))
            queue = [x[0] for x in c.execute(
                f"SELECT name FROM items WHERE vd_status='queued' {GALLERY_ORDER} LIMIT ?", (VD_QUEUE_SIZE,))]
            recent_done = [
                {"name": x["name"], "bytes": x["vd_bytes"], "lost": x["vd_lost"], "note": x["vd_note"],
                 "file": x["vd_file"], "at": x["vd_at"]}
                for x in c.execute("SELECT name,vd_bytes,vd_lost,vd_note,vd_file,vd_at FROM items "
                                   "WHERE vd_status='done' ORDER BY vd_at DESC, name DESC LIMIT 25")]
            recent_failed = [
                {"name": x["name"], "error": x["vd_error"], "at": x["vd_at"]}
                for x in c.execute("SELECT name,vd_error,vd_at FROM items WHERE vd_status='failed' "
                                   "ORDER BY vd_at DESC, name DESC LIMIT 25")]

        active = []
        now = time.monotonic()
        with self.alock:
            progs = sorted(self.active.values(), key=lambda p: p["since"])
            for p in progs:
                st = read_state(p["state_path"])
                size = int(st["size"]) if st else int(p.get("size") or 0)
                ok = st.get("ok", []) if st else []
                bad = st.get("bad", []) if st else []
                got, lost = ranges_sum(ok), ranges_sum(bad)
                dt = now - p["_pt"]
                if dt >= 1.0:
                    inst = max(0.0, got - p["_pg"]) / dt
                    p["speed"] = inst if not p["speed"] else 0.5 * p["speed"] + 0.5 * inst
                    p["_pg"], p["_pt"] = got, now
                active.append({
                    "name": p["name"], "phase": p["phase"], "line": p.get("line", ""), "attempt": p["attempt"],
                    "elapsed": round(time.time() - p["since"], 1), "size": size, "got": got, "lost": lost,
                    "speed": p["speed"], "bar": make_bar(size, ok, bad, 120),
                })

        return {
            "state": self.state(),
            "message": self.message,
            "settings": dict(self.settings),
            "limits": VD_LIMITS,
            "queue_size": VD_QUEUE_SIZE,
            "files_dir": os.path.abspath(files_dir()),
            "have_ffmpeg": bool(shutil.which("ffmpeg")),
            "auth": auth_summary(),
            "session": dict(self.session),
            "counts": counts,
            "active": active,
            "queue": queue,
            "recent_done": recent_done,
            "recent_failed": recent_failed,
        }


VD = VideoDownloader()


# ---------------------------------------------------------------- остальные файлы (раздел «Файлы»)

_WIN_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}


_NAME_MAX_BYTES = 255     # предел длины имени файла/папки в Linux (ext4) — в байтах UTF-8, а кириллица и иврит по 2 байта
_NAME_CUT_BYTES = 240     # длинные имена режем до стольких байт: запас под метку _with_tag


def _fit_name(s, original):
    """Слишком длинное имя: укоротить основу, оставив расширение, и добавить ~метку от полного имени (чтобы не совпали)."""
    if len(s.encode("utf-8")) <= _NAME_MAX_BYTES:
        return s
    stem, ext = os.path.splitext(s)
    if len(ext.encode("utf-8")) > 20:
        stem, ext = s, ""
    tag = "~" + hashlib.md5(original.encode("utf-8")).hexdigest()[:8]
    budget = _NAME_CUT_BYTES - len((tag + ext).encode("utf-8"))
    return stem.encode("utf-8")[:budget].decode("utf-8", "ignore").rstrip(" .") + tag + ext


def _safe_component(part):
    """Одна часть пути: без запрещённых символов, без «..», без хвостовых точек/пробелов и имён вроде CON (Windows),
    не длиннее, чем позволяет файловая система."""
    s = safe_name(part).rstrip(" .")
    if not s:
        s = "_"
    if s.split(".", 1)[0].upper() in _WIN_RESERVED:
        s = "_" + s
    return _fit_name(s, part)


def disk_rel_path(path):
    """/disk/Папка/отчёт.pdf -> «Диск/Папка/отчёт.pdf», /public/<id>/Папка/книга.pdf -> «Ссылки/<id>/Папка/книга.pdf»
    (относительно files_dir())."""
    parts = [p for p in path.split("/") if p]
    top = FILES_SUBDIR
    if parts and parts[0] == "disk":
        parts = parts[1:]
    elif parts and parts[0] == "public":
        top, parts = PUBLIC_SUBDIR, parts[1:]
    return "/".join([top] + [_safe_component(p) for p in parts])


def normalize_disk_folder(value):
    """«Творчество», «/Творчество», «/disk/Творчество», «disk:/Творчество» или ссылка из адресной строки
    https://disk.yandex.ru/client/disk/Творчество -> («/disk/Творчество», None). Пусто — весь Диск («/disk»)."""
    s = (value or "").strip()
    if "://" in s:
        p = unquote(urlparse(s).path)
        if not (p == "/client/disk" or p.startswith("/client/disk/")):
            return None, "это не ссылка на папку Диска (нужна вида https://disk.yandex.ru/client/disk/Папка)"
        s = p[len("/client/disk"):]
    if s.startswith("disk:"):
        s = s[len("disk:"):]
    s = s.strip().strip("/")
    if s == "disk":
        s = ""
    elif s.startswith("disk/"):
        s = s[len("disk/"):]
    parts = [p.strip() for p in s.split("/") if p.strip()]
    if any(p in (".", "..") for p in parts):
        return None, "в пути не может быть «.» и «..»"
    return "/disk" + "".join("/" + p for p in parts), None


def known_folder_case(folder):
    """«/disk/творчество» -> «/disk/Творчество», если такая папка уже встречалась: регистр букв на Диске важен."""
    with dbc() as c:
        if c.execute("SELECT 1 FROM files WHERE path=? AND kind='dir'", (folder,)).fetchone():
            return folder
        low = folder.lower()
        matches = [r[0] for r in c.execute("SELECT path FROM files WHERE kind='dir'") if r[0].lower() == low]
    return matches[0] if len(matches) == 1 else folder


def _sql_str(s):
    """Строка как SQL-литерал SQLite (одинарные кавычки удваиваются)."""
    return "'" + s.replace("'", "''") + "'"


def subtree_sql(folder):
    """Условие «path — сама папка folder или что-то внутри неё». Без LIKE: в именах бывают % и _.
    Весь Диск — тоже только /disk/…: рядом в той же таблице лежат общие папки по ссылкам (/public/…)."""
    inside = _sql_str(folder + "/")
    return f"(path = {_sql_str(folder)} OR substr(path, 1, length({inside})) = {inside})"


def _with_tag(rel, path):
    stem, ext = os.path.splitext(rel)
    return f"{stem}_{hashlib.md5(path.encode('utf-8')).hexdigest()[:8]}{ext}"


def reconcile_existing_files(paths=None):
    """Помечает как done файлы, которые уже лежат в папке загрузок (тот же путь и тот же размер).
    Как и у фото/видео, статус done потом не сбрасывается, даже если файл удалить вручную."""
    root = os.path.abspath(files_dir())
    base = ("SELECT path,size FROM files WHERE kind='file' AND size IS NOT NULL "
            "AND (fl_status IS NULL OR fl_status='failed')")
    with dbc() as c:
        if paths is None:
            rows = c.execute(base).fetchall()
        else:
            rows = []
            unique = list(dict.fromkeys(str(p) for p in paths if p))
            for start in range(0, len(unique), 800):
                batch = unique[start:start + 800]
                rows.extend(c.execute(f"{base} AND path IN ({','.join('?' * len(batch))})", batch).fetchall())
        if not rows:
            return 0
        claimed = {r[0] for r in c.execute("SELECT fl_file FROM files WHERE fl_status='done' AND fl_file IS NOT NULL")}
        ts = now_iso()
        updates = []
        for row in rows:
            for rel in (disk_rel_path(row["path"]), _with_tag(disk_rel_path(row["path"]), row["path"])):
                if rel in claimed:
                    continue
                try:
                    verify_saved_file(root, rel, expected_size=row["size"], min_size=0)
                except OSError:
                    continue
                claimed.add(rel)
                updates.append((rel, root, row["size"], LOCAL_EXISTING_FILE_NOTE, ts, row["path"]))
                break
        c.executemany("UPDATE files SET fl_status='done', fl_file=?, fl_root=?, fl_bytes=?, fl_error=NULL, "
                      "fl_note=?, fl_at=? WHERE path=?", updates)
    return len(updates)


FL_ORDER = "ORDER BY path"


class FileDownloader:
    """
    Качает файлы из раздела «Файлы» Диска (документы, архивы, музыка, …) в files_dir()/Диск/<путь>.
    Ссылку берёт так же, как загрузка видео: models-v2?m=mpfs/url с куками и sk из auth.db.
    Обрыв или просроченная ссылка — новая ссылка и докачка с места обрыва (Range), до FL_ATTEMPTS попыток.
    Фото и видео из папок по умолчанию пропускает (их качают вкладки «Фото» и «Видео»), это настраивается.
    scope — папка, внутри которой работают загрузка и обход (по умолчанию весь Диск);
    exclude — её подпапки, которые при этом не обходить, не считать и не качать (статусы их файлов не меняются).
    """

    def __init__(self):
        self.threads = FL_DEFAULT_THREADS
        self.include_media = False
        self.scope = "/disk"
        self.exclude = []
        self.stop_event = threading.Event()
        self.stop_event.set()
        self.finishing = threading.Event()
        self.manager = None
        self.workers = {}
        self.active = {}
        self.alock = threading.Lock()
        self.slock = threading.Lock()
        self.state_lock = threading.Lock()
        self.claim_lock = threading.Lock()
        self.refill_lock = threading.Lock()
        self.window = deque()
        self.session = {"done": 0, "failed": 0, "bytes": 0, "started": None}
        self.message = ""
        self.opener = build_opener()

    def scope_sql(self):
        parts = [subtree_sql(self.scope)] + [f"NOT {subtree_sql(x)}" for x in self.exclude]
        return parts[0] if len(parts) == 1 else "(" + " AND ".join(parts) + ")"

    def eligible(self):
        return (f"kind='file' AND fl_status IS NULL AND {self.scope_sql()}"
                + ("" if self.include_media else " AND COALESCE(is_media,0)=0"))

    # ---------- настройки

    def load_settings(self):
        try:
            self.threads = max(1, min(FL_MAX_THREADS, int(get_setting("fl_threads", FL_DEFAULT_THREADS))))
        except (TypeError, ValueError):
            self.threads = FL_DEFAULT_THREADS
        self.include_media = get_setting("fl_include_media", "0") == "1"
        saved = get_setting("fl_scope", "/disk")
        if saved.startswith("/public/"):           # общая папка по ссылке: путь хранится как есть
            self.scope = saved
        else:
            scope, err = normalize_disk_folder(saved)
            self.scope = scope if not err else "/disk"
        try:
            self.exclude = [x for x in json.loads(get_setting("fl_exclude", "[]")) if isinstance(x, str)]
        except (TypeError, ValueError):
            self.exclude = []

    def set_scope(self, value, exclude=()):
        """Выбрать папку, внутри которой работают загрузка и обход (пусто — весь Диск), и её подпапки,
        которые при этом не трогать (пути как у value). Без exclude прежние исключения снимаются."""
        if self.state() != "idle" or CRAWL.run:
            return None, "сначала останови загрузку файлов и обход, потом меняй папку"
        folder, err = self._folder(value)
        if err:
            return None, err
        if isinstance(exclude, str):
            exclude = [exclude]
        if not isinstance(exclude, (list, tuple)):
            return None, "exclude — список папок"
        skip = []
        for x in exclude or ():
            sub, err = self._folder(x) if isinstance(x, str) else (None, "нужна строка")
            if err:
                return None, f"исключение «{x}»: {err}"
            if not sub.startswith(folder + "/"):
                return None, f"«{sub}» не внутри «{folder}» — исключить можно только её подпапку"
            with dbc() as c:
                known = c.execute("SELECT 1 FROM files WHERE path=? AND kind='dir'", (sub,)).fetchone()
                names = [r[0] for r in c.execute(
                    "SELECT name FROM files WHERE parent=? AND kind='dir' ORDER BY name", (parent_of(sub),))]
            if not known:
                return None, (f"папки «{sub}» программа на Диске не видела — проверь название"
                              + (f". Папки в «{parent_of(sub)}»: " + ", ".join(f"«{n}»" for n in names) if names else ""))
            if sub not in skip:
                skip.append(sub)
        self.scope, self.exclude = folder, skip
        set_setting("fl_scope", folder)
        set_setting("fl_exclude", json.dumps(skip, ensure_ascii=False))
        return folder, None

    @staticmethod
    def _folder(value):
        """Папка Диска («Творчество», ссылка на неё…) или общей папки по ссылке (/public/<id>/…) -> (путь, ошибка)."""
        if isinstance(value, str) and value.startswith("/public/"):
            pid, rel = public_root_of(value)
            if pid not in public_roots():
                return None, "эту общую ссылку программа не знает — запусти fetch с самой ссылкой"
            parts = [x.strip() for x in rel.split("/") if x.strip()]
            if any(x in (".", "..") for x in parts):
                return None, "в пути не может быть «.» и «..»"
            return "/public/" + pid + "".join("/" + x for x in parts), None
        folder, err = normalize_disk_folder(value)
        return (None, err) if err else (known_folder_case(folder), None)

    def update_settings(self, body):
        errors = []
        if "threads" in body:
            try:
                self.threads = max(1, min(FL_MAX_THREADS, int(body["threads"])))
                set_setting("fl_threads", self.threads)
            except (TypeError, ValueError):
                errors.append("threads")
        if "include_media" in body:
            self.include_media = bool(body["include_media"])
            set_setting("fl_include_media", "1" if self.include_media else "0")
        return {"threads": self.threads, "include_media": self.include_media}, errors

    # ---------- управление

    def _alive(self):
        return bool(self.manager and self.manager.is_alive()) or any(t.is_alive() for t in self.workers.values())

    def state(self):
        if not self._alive():
            return "idle"
        return "stopping" if self.stop_event.is_set() else "running"

    def start(self):
        with self.state_lock:
            if self._alive():
                return {"ok": False, "error": "уже запущено" if not self.stop_event.is_set() else "ещё останавливается, подожди"}
            a = auth_summary()
            if not a["cookies"] or not a["has_sk"]:
                return {"ok": False, "error": "нет данных авторизации: открой Яндекс Диск во вкладке браузера "
                                              "(перехват передаст куки и sk), потом повтори"}
            self.stop_event.clear()
            self.finishing.clear()
            self.message = ""
            self.workers = {}
            with self.alock:
                self.active = {}
            self.session = {"done": 0, "failed": 0, "bytes": 0, "started": time.time()}
            self.manager = threading.Thread(target=self._manager_loop, name="fl-manager", daemon=True)
            self.manager.start()
        return {"ok": True}

    def stop(self, message=""):
        if message:
            self.message = message
        self.stop_event.set()

    def retry_failed(self):
        with dbc() as c:
            return c.execute("UPDATE files SET fl_status=NULL, fl_error=NULL "
                             f"WHERE fl_status='failed' AND {self.scope_sql()}").rowcount

    # ---------- менеджер

    def refill(self):
        with self.refill_lock:
            with dbc() as c:
                have = c.execute("SELECT COUNT(*) FROM files WHERE fl_status='queued'").fetchone()[0]
                need = FL_QUEUE_SIZE - have
                if need <= 0:
                    return
                rows = c.execute(f"SELECT path FROM files WHERE {self.eligible()} {FL_ORDER} LIMIT ?", (need,)).fetchall()
                if rows:
                    ts = now_iso()
                    c.executemany("UPDATE files SET fl_status='queued', fl_at=? WHERE path=?", [(ts, r[0]) for r in rows])

    def _finished(self):
        with dbc() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(fl_status='queued'),0), COALESCE(SUM(fl_status='downloading'),0), "
                f"COALESCE(SUM({self.eligible()}),0) FROM files").fetchone()
        return not any(r)

    def _ensure_workers(self):
        for idx in range(self.threads):
            t = self.workers.get(idx)
            if t is None or not t.is_alive():
                t = threading.Thread(target=self._worker_loop, args=(idx,), name=f"fl-{idx}", daemon=True)
                self.workers[idx] = t
                t.start()

    def _manager_loop(self):
        natural = False
        try:
            while not self.stop_event.is_set():
                self._ensure_workers()
                self.refill()
                if self._finished():
                    natural = True
                    break
                time.sleep(0.5)
        except Exception as e:  # noqa: BLE001
            self.message = f"ошибка менеджера: {e}"
            self.stop_event.set()
        finally:
            if natural and not self.message:
                self.message = "Готово: очередь файлов пуста."
            self.finishing.set()
            for t in list(self.workers.values()):
                t.join(timeout=60)
            with dbc() as c:
                c.execute("UPDATE files SET fl_status=NULL WHERE fl_status IN ('queued','downloading')")

    # ---------- воркер

    def _claim(self):
        with self.claim_lock:
            with dbc() as c:
                row = c.execute(
                    f"SELECT path,name,size,mtime FROM files WHERE fl_status='queued' {FL_ORDER} LIMIT 1").fetchone()
                if not row:
                    return None
                c.execute("UPDATE files SET fl_status='downloading', fl_at=? WHERE path=?", (now_iso(), row["path"]))
                return dict(row)

    def _worker_loop(self, idx):
        while not self.stop_event.is_set() and idx < self.threads:
            row = self._claim()
            if row is None:
                if self.finishing.is_set():
                    break
                time.sleep(0.3)
                continue
            try:
                self._process(row)
            except Exception as e:  # noqa: BLE001
                self._record_fail(row["path"], f"внутренняя ошибка: {type(e).__name__}: {e}")
            self.refill()

    def _set_file(self, path, status, file=None, nbytes=None, error=None, note=None):
        saved_root = os.path.abspath(files_dir()) if status == "done" else None
        with dbc() as c:
            c.execute("UPDATE files SET fl_status=?, fl_file=?, fl_root=?, fl_bytes=?, fl_error=?, fl_note=?, fl_at=? "
                      "WHERE path=?",
                      (status, file, saved_root, nbytes, (error or None) and error[:500], note, now_iso(), path))

    def _on_result(self, ok, nbytes=0):
        with self.slock:
            if ok:
                self.session["done"] += 1
                self.session["bytes"] += nbytes
            else:
                self.session["failed"] += 1

    def _record_fail(self, path, error):
        self._set_file(path, "failed", error=error)
        self._on_result(False)

    def _target_rel(self, path):
        rel = disk_rel_path(path)
        with dbc() as c:
            clash = c.execute("SELECT 1 FROM files WHERE lower(fl_file)=lower(?) AND path<>? LIMIT 1",
                              (rel, path)).fetchone()
        return _with_tag(rel, path) if clash else rel

    def _wait(self, seconds):
        if self.stop_event.wait(seconds):
            raise Aborted()

    def _process(self, row):
        path, name = row["path"], row["name"]
        prog = {"name": name, "path": path, "bytes": 0, "total": row.get("size"), "attempt": 1, "of": FL_ATTEMPTS,
                "since": time.time()}
        with self.alock:
            self.active[path] = prog
        rel = self._target_rel(path)
        final_path = os.path.join(files_dir(), *rel.split("/"))
        tmp = os.path.join(os.path.dirname(final_path),
                           f".{hashlib.md5(path.encode('utf-8')).hexdigest()[:8]}.part")
        size = row.get("size")
        errors = []
        keep_tmp = False
        try:
            for attempt in range(1, FL_ATTEMPTS + 1):
                if self.stop_event.is_set():
                    raise Aborted()
                prog["attempt"] = attempt
                note = None
                try:
                    # Первая попытка — обычная загрузка. Оборвалась — файл, скорее всего, повреждён на стороне
                    # Яндекса: дальше спасаем его по кускам (размер нужен, чтобы знать, где куски).
                    if attempt == 1 or not size:
                        nbytes = self._fetch(path, tmp, size, prog)
                    else:
                        nbytes, note = self._salvage(path, tmp, size, prog)
                    os.replace(tmp, final_path)
                    verify_saved_file(files_dir(), rel, expected_size=nbytes, min_size=0)
                except (Aborted, AuthError):
                    raise
                except Unreadable as e:
                    errors.append(f"{attempt}: {e}")
                    break
                except Exception as e:  # noqa: BLE001
                    if isinstance(e, OSError) and e.errno == errno.ENOSPC:
                        raise NoSpace() from e
                    # свои ошибки (VideoError, голый OSError) показываем как есть, без имени класса
                    errors.append(f"{attempt}: {e if isinstance(e, VideoError) or type(e) is OSError else short_err(e)}")
                    if attempt < FL_ATTEMPTS and not size:
                        self._wait(min(30, 3 * attempt))
                    continue
                mtime = _as_int(row.get("mtime"))
                if mtime and mtime > 0:
                    try:
                        os.utime(final_path, (mtime, mtime))   # дата изменения как на Диске
                    except (OSError, OverflowError, ValueError):
                        pass
                self._set_file(path, "done", file=rel, nbytes=nbytes, note=note)
                self._on_result(True, nbytes)
                return
            self._record_fail(path, "; ".join(errors))
        except Aborted:
            # остановили (например, пауза для переноса в облако) — недокачанное остаётся, _fetch докачает с места
            keep_tmp = True
            self._set_file(path, None)
        except AuthError as e:
            keep_tmp = True
            self._set_file(path, None)
            self.stop(f"Остановился: {e}")
        except NoSpace:
            self._set_file(path, None)
            self.stop("Нет места на диске: загрузка остановлена, недокачанные файлы вернулись в очередь")
        finally:
            with self.alock:
                self.active.pop(path, None)
            if not keep_tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    def _fetch(self, path, tmp, expected, prog):
        """Качает файл в tmp (докачивает, если tmp уже есть от прошлой попытки). Возвращает размер."""
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        if expected == 0:            # пустой файл: качать нечего
            open(tmp, "wb").close()
            return 0
        url = get_download_url(path)
        have = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        if expected is not None and have >= expected:
            have = 0
        headers = {"User-Agent": USER_AGENT, "Accept": "*/*", "Referer": API_BASE + "/"}
        if have:
            headers["Range"] = f"bytes={have}-"
        req = urllib.request.Request(url, headers=headers)
        try:
            resp = self.opener.open(req, timeout=FL_READ_TIMEOUT)
        except urllib.error.HTTPError as e:
            if e.code == 416 and have:
                os.remove(tmp)
                raise IOError("сервер не принял докачку (HTTP 416) — начну заново")
            if e.code == 429 and path.startswith("/public/"):
                raise LinkLimit("HTTP 429") from e
            raise
        with resp:
            if have:
                content_range = resp.headers.get("Content-Range") or ""
                if resp.status == 200:
                    have = 0                 # докачку не поддержали — пишем с начала
                elif resp.status != 206 or not content_range.startswith(f"bytes {have}-"):
                    os.remove(tmp)
                    raise IOError(f"сервер вернул не тот диапазон ({content_range or resp.status})")
            total = expected
            if total is None:
                clen = resp.headers.get("Content-Length")
                total = have + int(clen) if clen and clen.isdigit() else None
            got = have
            prog.update(bytes=got, total=total)
            with open(tmp, "ab" if have else "wb") as f:
                while True:
                    if self.stop_event.is_set():
                        raise Aborted()
                    chunk = resp.read(262144)
                    if not chunk:
                        break
                    f.write(chunk)
                    got += len(chunk)
                    prog["bytes"] = got
                    self.window.append((time.monotonic(), len(chunk)))
        if total is not None and got != total:
            raise IOError(f"неполный файл {got}/{total}")
        return got

    def _salvage(self, path, tmp, size, prog):
        """
        Обычная загрузка оборвалась — файл, похоже, повреждён на стороне Яндекса: на битых местах сервер отдаёт
        пустой ответ или зависает. Читаем его кусками через Range: что читается — сохраняем, нечитаемые куски
        остаются нулями, смещения не сдвигаются (mp3/wav после этого обычно играются с паузами на месте дыр).
        Начало, которое успела скачать обычная загрузка, берётся из tmp. Возвращает (size, заметка о потерях
        или None, если файл в итоге прочитался целиком). Ни одного байта — Unreadable.
        """
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        link = {"url": get_download_url(path), "refresh": 0, "dead": False, "http": None}   # до того, как трогать tmp
        # Обычная загрузка пишет подряд: всё до конца tmp — настоящее начало файла. А tmp полного размера оставило
        # прошлое спасение, которое сорвалось (оно сразу растягивает файл, непрочитанное — нули): что в нём прочитано,
        # неизвестно, читаем всё заново — иначе дыры ушли бы в «целый» файл.
        have = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        if have >= size:
            have = 0
        with open(tmp, "ab"):
            pass
        with open(tmp, "r+b") as f:
            f.truncate(size)
        ok = [[0, have]] if have else []
        llock = threading.Lock()
        deadline = time.monotonic() + FL_SALVAGE_BUDGET + size / 50000
        prog.update(bytes=have, total=size, note="файл повреждён — спасаю по кускам")

        def fresh_url(old):
            with llock:
                if link["url"] == old and not link["dead"]:
                    if link["refresh"] >= FL_SALVAGE_LINK_REFRESH:
                        link["dead"] = True
                    else:
                        link["refresh"] += 1
                        try:
                            link["url"] = get_download_url(path)
                        except VideoError:
                            link["dead"] = True

        def read(a, b):
            """Байты [a, b), сколько прочиталось подряд с начала (b"" — ничего)."""
            for _ in range(3):
                if self.stop_event.is_set():
                    raise Aborted()
                if link["dead"] or time.monotonic() > deadline:
                    return b""
                url = link["url"]
                req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*",
                                                           "Referer": API_BASE + "/", "Range": f"bytes={a}-{b - 1}"})
                buf = bytearray()
                stop_at = time.monotonic() + FL_SALVAGE_TIMEOUT + (b - a) / 50000
                try:
                    with self.opener.open(req, timeout=FL_SALVAGE_TIMEOUT) as resp:
                        if resp.status != 206 and not (resp.status == 200 and a == 0 and b == size):
                            return b""
                        while len(buf) < b - a and time.monotonic() < stop_at:
                            if self.stop_event.is_set():
                                raise Aborted()
                            chunk = resp.read(min(16384, b - a - len(buf)))
                            if not chunk:
                                break
                            buf += chunk
                            self.window.append((time.monotonic(), len(chunk)))
                except urllib.error.HTTPError as e:
                    link["http"] = e.code
                    if e.code == 429 and path.startswith("/public/"):
                        raise LinkLimit("HTTP 429") from e
                    if e.code in (401, 403, 410):      # ссылка просрочена — берём новую
                        fresh_url(url)
                        continue
                    return b""
                except Aborted:
                    raise
                except Exception:  # noqa: BLE001 — таймаут/обрыв: что успело прийти, то и берём
                    pass
                return bytes(buf)
            return b""

        def block(a, b):
            first = read(a, min(b, a + FL_SALVAGE_SUB))
            if not first:
                return [], [(a, b)]                     # начало куска не читается — весь кусок потерян
            oks, bads, p = [(a, first)], [], a + len(first)
            if p < min(b, a + FL_SALVAGE_SUB):
                return oks, [(p, b)]
            if p < b:
                rest = read(p, b)
                if rest:
                    oks.append((p, rest))
                    p += len(rest)
            while p < b:                                # оборвалось внутри куска — дочитываем мелко у края дыры
                q = min(b, p + FL_SALVAGE_SUB)
                d = read(p, q)
                if d:
                    oks.append((p, d))
                if len(d) < q - p:
                    bads.append((p + len(d), q))
                p = q
            return oks, bads

        todo = [(p, min(size, p + FL_SALVAGE_BLOCK)) for p in range(have, size, FL_SALVAGE_BLOCK)]
        done_bytes = have
        with open(tmp, "r+b") as f, ThreadPoolExecutor(FL_SALVAGE_JOBS) as ex:
            futs = {ex.submit(block, a, b): (a, b) for a, b in todo}
            try:
                for fu in as_completed(futs):
                    oks, _bads = fu.result()
                    for off, data in oks:
                        f.seek(off)
                        f.write(data)
                        ok.append([off, off + len(data)])
                    a, b = futs[fu]
                    done_bytes += b - a
                    prog["bytes"] = done_bytes
            except BaseException:
                for fu in futs:
                    fu.cancel()
                raise

        def merged():
            out = []
            for a, b in sorted(ok):
                if out and a <= out[-1][1]:
                    out[-1][1] = max(out[-1][1], b)
                else:
                    out.append([a, b])
            return out

        def holes_of(good):
            out, p = [], 0
            for a, b in good + [[size, size]]:
                if a > p:
                    out.append((p, a))
                p = max(p, b)
            return out

        # Левый край дыры уже точный (кусок дочитывается до места обрыва), а правый — нет: кусок, начало которого
        # не прочиталось, целиком записан в потери, хотя дыра могла кончиться внутри него. Идём от правого края
        # дыры назад мелкими кусками, пока читается.
        with open(tmp, "r+b") as f:
            for a, b in holes_of(merged()):
                q = b
                while q > a:
                    p = max(a, q - FL_SALVAGE_SUB)
                    d = read(p, q)
                    if d:
                        f.seek(p)
                        f.write(d)
                        ok.append([p, p + len(d)])
                    if len(d) < q - p:
                        break
                    q = p

        good = merged()
        got = sum(b - a for a, b in good)
        if not got:
            raise Unreadable("не читается: Яндекс не отдал ни одного байта"
                             + (f" (HTTP {link['http']})" if link["http"] else ""))
        if got >= size:
            return size, None
        holes = holes_of(good)
        lost = size - got
        note = (f"повреждён: потеряно {lost} из {size} байт ({100 * lost / size:.1f}%); дыры: "
                + ", ".join(f"{a}-{b}" for a, b in holes[:8]) + (f" и ещё {len(holes) - 8}" if len(holes) > 8 else ""))
        return size, note

    # ---------- статус для страницы

    def snapshot(self):
        elig = self.eligible()
        scope = self.scope_sql()
        with dbc() as c:
            r = c.execute(
                f"""SELECT
                    COALESCE(SUM(kind='file'),0),
                    COALESCE(SUM(kind='file' AND COALESCE(is_media,0)=1),0),
                    COALESCE(SUM(kind='dir'),0),
                    COALESCE(SUM(kind='dir' AND listed_at IS NULL),0),
                    COALESCE(SUM(kind='dir' AND crawled_at IS NOT NULL),0),
                    COALESCE(SUM(fl_status='done'),0),
                    COALESCE(SUM(fl_status='failed'),0),
                    COALESCE(SUM(fl_status='queued'),0),
                    COALESCE(SUM(fl_status='downloading'),0),
                    COALESCE(SUM({elig}),0),
                    COALESCE(SUM(CASE WHEN {elig} THEN size END),0),
                    COALESCE(SUM(CASE WHEN fl_status='done' THEN fl_bytes END),0)
                FROM files WHERE {scope}"""
            ).fetchone()
            counts = dict(zip(
                ["files", "media", "dirs", "dirs_unopened", "dirs_crawled", "done", "failed", "queued",
                 "downloading", "remaining", "remaining_bytes", "done_bytes"], list(r)))
            queue = [x[0] for x in c.execute(
                f"SELECT path FROM files WHERE fl_status='queued' {FL_ORDER} LIMIT ?", (FL_QUEUE_SIZE,))]
            recent_done = [
                {"path": x["path"], "bytes": x["fl_bytes"], "note": x["fl_note"], "file": x["fl_file"], "at": x["fl_at"]}
                for x in c.execute("SELECT path,fl_bytes,fl_note,fl_file,fl_at FROM files WHERE fl_status='done' "
                                   f"AND {scope} ORDER BY fl_at DESC, path DESC LIMIT 25")]
            recent_failed = [
                {"path": x["path"], "error": x["fl_error"], "at": x["fl_at"]}
                for x in c.execute("SELECT path,fl_error,fl_at FROM files WHERE fl_status='failed' "
                                   f"AND {scope} ORDER BY fl_at DESC, path DESC LIMIT 25")]
            unopened = [x[0] for x in c.execute(
                f"SELECT path FROM files WHERE kind='dir' AND listed_at IS NULL AND {scope} ORDER BY path LIMIT 30")]

        now = time.monotonic()
        try:
            while self.window and now - self.window[0][0] > 10:
                self.window.popleft()
            items = list(self.window)
        except IndexError:
            items = []
        speed = sum(n for _, n in items) / max(1.0, now - items[0][0]) if items else 0

        with self.alock:
            active = sorted(self.active.values(), key=lambda p: p["since"])
            active = [dict(p, elapsed=round(time.time() - p["since"], 1)) for p in active]

        return {
            "state": self.state(),
            "message": self.message,
            "started": STARTED_AT,
            "threads": self.threads,
            "max_threads": FL_MAX_THREADS,
            "include_media": self.include_media,
            "scope": self.scope,
            "exclude": self.exclude,
            "public": public_roots().get(public_root_of(self.scope)[0]) if self.scope.startswith("/public/") else None,
            "queue_size": FL_QUEUE_SIZE,
            "files_dir": os.path.join(os.path.abspath(files_dir()), FILES_SUBDIR),
            "speed": speed,
            "auth": auth_summary(),
            "session": dict(self.session),
            "counts": counts,
            "active": active,
            "queue": queue,
            "recent_done": recent_done,
            "recent_failed": recent_failed,
            "unopened": unopened,
            "crawl": CRAWL.snapshot(),
        }


FL = FileDownloader()


class CrawlState:
    """
    Автоматический обход папок. Сам сервер в браузер не ходит: обход выполняет окно приложения
    (app/main.py) — открывает папки Диска во вкладке и прокручивает их, а перехват забирает списки
    файлов как при ручном просмотре. Здесь только флаг «обходить» и учёт пройденных папок.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.run = False
        self.current = None
        self.message = ""
        self.agent_seen = 0.0     # time.monotonic() последнего опроса от окна приложения
        self.visited = 0          # папок пройдено с момента запуска обхода
        self.current_since = None  # когда начали текущую папку: её «свежие» файлы — с updated_at не раньше

    def agent_alive(self):
        return self.agent_seen and time.monotonic() - self.agent_seen < FL_CRAWL_AGENT_TIMEOUT

    def snapshot(self):
        with self.lock:
            return {"run": self.run, "current": self.current, "message": self.message,
                    "agent": bool(self.agent_alive()), "visited": self.visited}

    def set_run(self, run, message=None, reset=False):
        if reset:
            with dbc() as c:
                c.execute(f"UPDATE files SET crawled_at=NULL WHERE kind='dir' AND {FL.scope_sql()}")
        with self.lock:
            if run and not self.run:
                self.visited = 0
            self.run = bool(run)
            if not self.run:
                self.current = None
            if message is not None:
                self.message = message
            elif run:
                self.message = ""

    def next_folder(self):
        """Следующая непройденная папка внутри выбранной (FL.scope, кроме FL.exclude), сначала верхние уровни.
        Саму выбранную папку ещё ни разу не видели — начинаем с неё."""
        scope = FL.scope
        with dbc() as c:
            row = c.execute(
                f"SELECT path FROM files WHERE kind='dir' AND crawled_at IS NULL AND {FL.scope_sql()} "
                "ORDER BY length(path) - length(replace(path, '/', '')), path LIMIT 1").fetchone()
            path = row[0] if row else None
            if path is None and not c.execute("SELECT 1 FROM files WHERE path=?", (scope,)).fetchone():
                path = scope
        with self.lock:
            self.current = path
            self.current_since = now_iso() if path else None
            return path, self.current_since

    def folder_done(self, path):
        ts = now_iso()
        with dbc() as c:
            c.execute("INSERT INTO files(path, parent, name, kind, first_seen, updated_at, crawled_at) "
                      "VALUES(?,?,?,'dir',?,?,?) ON CONFLICT(path) DO UPDATE SET crawled_at=excluded.crawled_at",
                      (path, parent_of(path), FILES_SUBDIR if path == "/disk" else name_from_path(path), ts, ts, ts))
        with self.lock:
            self.visited += 1
            if self.current == path:
                self.current = None


CRAWL = CrawlState()


def cleanup_part_files():
    for root, _dirs, files in os.walk(files_dir()):
        for f in files:
            if f.endswith(".part"):
                try:
                    os.remove(os.path.join(root, f))
                except OSError:
                    pass


@app.before_request
def _require_json_for_post():
    # POST только с Content-Type: application/json — чужая веб-страница не сможет дёрнуть
    # локальный сервер «простым» запросом (браузер потребует CORS-preflight, которого у нас нет).
    if request.method == "POST" and not request.is_json:
        return jsonify(error="expected application/json"), 415


@app.get("/status")
def status_page():
    return Response(STATUS_HTML, mimetype="text/html")


@app.get("/api/settings/files-dir")
def api_files_dir_get():
    return jsonify(ok=True, dir=files_dir())


@app.post("/api/settings/files-dir")
def api_files_dir_set():
    body = request.get_json(silent=True) or {}
    abspath, err = set_files_dir(body.get("dir", ""))
    if err:
        return jsonify(ok=False, error=err), 400
    return jsonify(ok=True, dir=abspath)


@app.get("/api/dl/status")
def api_dl_status():
    return jsonify(DL.snapshot())


@app.post("/api/dl/start")
def api_dl_start():
    res = DL.start()
    return jsonify(res), (200 if res.get("ok") else 409)


@app.post("/api/dl/stop")
def api_dl_stop():
    DL.stop()
    return jsonify(ok=True, state=DL.state())


@app.post("/api/dl/threads")
def api_dl_threads():
    body = request.get_json(silent=True) or {}
    try:
        n = DL.set_threads(int(body.get("threads")))
    except (TypeError, ValueError):
        return jsonify(ok=False, error="threads должен быть числом"), 400
    return jsonify(ok=True, threads=n)


@app.post("/api/dl/retry-failed")
def api_dl_retry_failed():
    return jsonify(ok=True, reset=DL.retry_failed())


@app.get("/api/vd/status")
def api_vd_status():
    return jsonify(VD.snapshot())


@app.post("/api/vd/start")
def api_vd_start():
    res = VD.start()
    return jsonify(res), (200 if res.get("ok") else 409)


@app.post("/api/vd/stop")
def api_vd_stop():
    VD.stop()
    return jsonify(ok=True, state=VD.state())


@app.post("/api/vd/settings")
def api_vd_settings():
    settings, errors = VD.update_settings(request.get_json(silent=True) or {})
    return jsonify(ok=not errors, settings=settings, invalid=errors)


@app.post("/api/vd/retry-failed")
def api_vd_retry_failed():
    return jsonify(ok=True, reset=VD.retry_failed())


@app.get("/api/fl/status")
def api_fl_status():
    return jsonify(FL.snapshot())


@app.post("/api/fl/start")
def api_fl_start():
    res = FL.start()
    return jsonify(res), (200 if res.get("ok") else 409)


@app.post("/api/fl/stop")
def api_fl_stop():
    # message — почему остановили (кнопка на странице: «вручную»): его покажут статус и отчёт vps/fetch.py
    message = (request.get_json(silent=True) or {}).get("message")
    FL.stop(message if isinstance(message, str) else "")
    return jsonify(ok=True, state=FL.state())


@app.post("/api/fl/settings")
def api_fl_settings():
    settings, errors = FL.update_settings(request.get_json(silent=True) or {})
    return jsonify(ok=not errors, settings=settings, invalid=errors)


@app.post("/api/fl/retry-failed")
def api_fl_retry_failed():
    return jsonify(ok=True, reset=FL.retry_failed())


@app.post("/api/fl/scope")
def api_fl_scope():
    body = request.get_json(silent=True) or {}
    folder, err = FL.set_scope(body.get("folder", ""), body.get("exclude") or ())
    if err:
        return jsonify(ok=False, error=err), 400
    return jsonify(ok=True, scope=folder, exclude=FL.exclude)


def ingest_public(pid, key, name, files, listed, single=False):
    """Кусок списка общей папки по ссылке (его собирает vps/fetch.py через открытое API Диска): файлы
    /public/<id><путь>, их папки — сразу пройденными (браузер их не открывает). listed — путь подпапки, список
    которой этим вызовом собран целиком: повторный fetch его не собирает заново."""
    ts = now_iso()
    roots = public_roots()
    root = roots.get(pid) or {}
    root.update(key=key, name=name or root.get("name") or pid)
    if single:
        root["file"] = True
    if listed is not None:
        root.setdefault("listed", {})[listed] = ts
    roots[pid] = root
    set_setting("fl_public", json.dumps(roots, ensure_ascii=False))
    base = "/public/" + pid
    dirs, n = {base}, 0
    with dbc() as c:
        for f in files:
            rel, size = (f.get("path"), f.get("size")) if isinstance(f, dict) else (None, None)
            if not isinstance(rel, str) or not rel.startswith("/") or any(x in (".", "..") for x in rel.split("/")):
                continue
            path = base + rel.rstrip("/")
            fname = name_from_path(path)
            c.execute("INSERT INTO files(path, parent, name, kind, size, is_media, first_seen, updated_at) "
                      "VALUES(?,?,?,'file',?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                      "size=COALESCE(excluded.size, files.size), kind='file', updated_at=excluded.updated_at",
                      (path, parent_of(path), fname, _as_int(size), int(is_media_file(fname, None)), ts, ts))
            n += 1
            q = parent_of(path)
            while q and q not in dirs and q.startswith(base + "/"):
                dirs.add(q)
                q = parent_of(q)
        for d in dirs:
            c.execute("INSERT INTO files(path, parent, name, kind, first_seen, updated_at, listed_at, crawled_at) "
                      "VALUES(?,?,?,'dir',?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                      "listed_at=COALESCE(files.listed_at, excluded.listed_at), crawled_at=excluded.crawled_at",
                      (d, parent_of(d), root["name"] if d == base else name_from_path(d), ts, ts, ts, ts))
    return n


@app.get("/api/fl/public")
def api_fl_public_get():
    return jsonify(roots=public_roots())


@app.post("/api/fl/public")
def api_fl_public():
    body = request.get_json(silent=True) or {}
    pid, key = body.get("id"), body.get("key")
    if not (isinstance(pid, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", pid)) \
            or not (isinstance(key, str) and key.startswith("http")):
        return jsonify(ok=False, error="нужны id (буквы, цифры, - и _) и key (ссылка)"), 400
    listed = body.get("listed")
    n = ingest_public(pid, key, body.get("name"), body.get("files") or [],
                      listed if isinstance(listed, str) else None, bool(body.get("file")))
    return jsonify(ok=True, files=n, root=public_roots().get(pid))


@app.get("/api/fl/damaged")
def api_fl_damaged():
    """Повреждённые (скачаны с дырами) и нечитаемые файлы в выбранной папке — для отчёта."""
    with dbc() as c:
        rows = c.execute(
            "SELECT path, size, fl_status, fl_note, fl_error, fl_file FROM files WHERE kind='file' AND "
            f"{FL.scope_sql()} AND ((fl_status='done' AND fl_note LIKE 'повреждён%') OR fl_status='failed') "
            "ORDER BY path").fetchall()
    return jsonify(items=[dict(r) for r in rows])


@app.get("/api/fl/crawl")
def api_fl_crawl_get():
    # ?agent=1 — опрос от окна приложения: так страница статуса знает, что обход есть кому выполнять
    if request.args.get("agent"):
        with CRAWL.lock:
            CRAWL.agent_seen = time.monotonic()
    return jsonify(CRAWL.snapshot())


@app.post("/api/fl/crawl")
def api_fl_crawl_set():
    body = request.get_json(silent=True) or {}
    message = body.get("message")
    CRAWL.set_run(bool(body.get("run")), message=message if isinstance(message, str) else None,
                  reset=bool(body.get("reset")))
    return jsonify(ok=True, crawl=CRAWL.snapshot())


@app.post("/api/fl/next-folder")
def api_fl_next_folder():
    path, since = CRAWL.next_folder()
    return jsonify(path=path, since=since)


@app.get("/api/fl/folder")
def api_fl_folder():
    """children — всего известно содержимого папки; fresh — сколько из него пришло начиная с since
    (обход по нему понимает, что прокрутка ещё подгружает новые порции)."""
    path = request.args.get("path") or ""
    since = request.args.get("since") or ""
    with dbc() as c:
        n, fresh = c.execute("SELECT COUNT(*), COALESCE(SUM(updated_at >= ?),0) FROM files WHERE parent=?",
                             (since, path)).fetchone()
    return jsonify(path=path, children=n, fresh=fresh)


@app.post("/api/fl/folder-done")
def api_fl_folder_done():
    path = (request.get_json(silent=True) or {}).get("path")
    if not isinstance(path, str) or not (path == "/disk" or path.startswith("/disk/")):
        return jsonify(ok=False, error="path должен начинаться с /disk"), 400
    CRAWL.folder_done(path)
    return jsonify(ok=True)


STATUS_HTML = r"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Статус загрузки</title>
<style>
  :root { color-scheme: dark light; --bg:#202024; --fg:#ececf1; --mut:#8e8e99; --line:#33333a; --cell:#2c2c33; --ok:#2e7d32; --low:#b26a00; --bad:#c62828; }
  @media (prefers-color-scheme: light) { :root { --bg:#fff; --fg:#1b1b1f; --mut:#6b6b76; --line:#e3e3ea; --cell:#f1f1f5; } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:13px/1.4 system-ui, sans-serif; }
  main { padding:14px 24px 60px; max-width:1300px; }
  header { display:flex; flex-wrap:wrap; gap:10px 16px; align-items:center; margin-bottom:8px; }
  h1 { font-size:17px; margin:0; font-weight:600; }
  nav { display:flex; gap:14px; }
  nav a { color:var(--mut); text-decoration:none; }
  nav a.on { color:var(--fg); font-weight:600; }
  .tabs { display:flex; gap:6px; margin:6px 0 4px; }
  .tab.on { background:var(--cell); font-weight:600; }
  h3 { font-size:13px; margin:22px 0 8px; font-weight:600; }
  h3 small { color:var(--mut); font-weight:400; margin-left:6px; }
  .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:10px 0; }
  button, input { font:inherit; padding:6px 12px; border:1px solid var(--line); border-radius:8px; background:transparent; color:var(--fg); }
  button { cursor:pointer; }
  button.primary { background:var(--ok); border-color:var(--ok); color:#fff; }
  button:disabled { opacity:.4; cursor:default; }
  input[type=number] { width:90px; }
  input[type=checkbox] { padding:0; width:auto; }
  .pill { padding:2px 10px; border-radius:10px; font-size:12px; background:var(--cell); }
  .pill.running { background:var(--ok); color:#fff; }
  .pill.stopping { background:var(--low); color:#fff; }
  .msg { background:var(--low); color:#fff; padding:8px 12px; border-radius:8px; margin:8px 0; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(130px, 1fr)); gap:8px; margin:12px 0; }
  .card { background:var(--cell); border-radius:8px; padding:10px 12px; }
  .card b { display:block; font-size:19px; }
  .card span { color:var(--mut); font-size:12px; }
  .bar { height:8px; background:var(--line); border-radius:4px; overflow:hidden; }
  .bar i { display:block; height:100%; background:var(--ok); width:0; transition:width .4s; }
  .chip { display:inline-block; padding:1px 8px; border-radius:10px; font-size:11px; color:#fff; background:var(--low); margin-right:6px; }
  .chip.ORIGINAL { background:var(--ok); }
  .chip.phase { background:#3a6ea5; }
  .cols { display:grid; grid-template-columns:repeat(auto-fit, minmax(420px, 1fr)); gap:0 28px; }
  table { width:100%; border-collapse:collapse; }
  td { padding:4px 10px 4px 0; border-bottom:1px solid var(--line); vertical-align:middle; }
  .mono { font-family:ui-monospace, Menlo, monospace; word-break:break-all; }
  .mut { color:var(--mut); }
  .box { max-height:280px; overflow:auto; border:1px solid var(--line); border-radius:8px; padding:6px 10px; }
  .box div { padding:1px 0; }
  .err { color:var(--bad); font-size:12px; }
  .warn { color:var(--low); }
  details.settings { margin:10px 0; border:1px solid var(--line); border-radius:8px; padding:8px 12px; }
  details.settings summary { cursor:pointer; font-weight:600; }
  .sgrid { display:grid; grid-template-columns:repeat(auto-fill, minmax(280px, 1fr)); gap:8px 20px; margin:10px 0; }
  .sgrid label { display:flex; flex-direction:column; gap:2px; font-size:12px; }
  .sgrid label small { color:var(--mut); }
  .sgrid label.chk { flex-direction:row; align-items:center; gap:8px; }
  .vcard { background:var(--cell); border-radius:8px; padding:10px 12px; margin:8px 0; }
  .vcard .top { display:flex; flex-wrap:wrap; gap:6px 12px; align-items:center; }
  .vcard canvas { display:block; width:100%; height:16px; margin:8px 0 4px; border-radius:3px; background:#444; }
  .legend { font-size:11px; color:var(--mut); display:flex; flex-wrap:wrap; gap:4px 14px; margin-top:4px; }
  .legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:4px; vertical-align:-1px; }
</style>
</head>
<body>
<main>
  <header>
    <h1>Статус загрузки</h1>
    <nav><a href="/">Галерея</a><a href="/status" class="on">Статус загрузки</a></nav>
  </header>
  <div class="tabs"><button id="tabPhoto" class="tab on">Фото</button><button id="tabVideo" class="tab">Видео</button><button id="tabFiles" class="tab">Файлы</button></div>

<!-- ============================== ФОТО ============================== -->
<div id="photo">
  <div class="controls">
    <span id="state" class="pill">…</span>
    <button id="btnStart" class="primary">Скачать только фото</button>
    <button id="btnStop">Остановить</button>
    <span class="mut">потоков:</span>
    <input id="threads" type="number" min="1" max="32" value="8">
    <button id="btnThreads">Применить</button>
    <button id="btnRetry">Повторить ошибки</button>
  </div>
  <div class="mut" id="dir"></div>
  <div class="controls" style="margin-top:4px">
    <span class="mut">папка для файлов (общая для фото, видео и файлов):</span>
    <input id="dirInput" type="text" style="min-width:320px" placeholder="например, D:\\YandexPhotos или /mnt/data/photos">
    <button id="btnDirApply">Сменить</button>
  </div>
  <div class="mut" id="dirMsg" style="margin-top:2px"></div>
  <div class="msg" id="msg" hidden></div>

  <div class="cards" id="cards"></div>
  <div class="bar"><i id="bar"></i></div>
  <div class="mut" id="barText" style="margin-top:4px"></div>
  <div id="bysize" style="margin-top:10px"></div>

  <h3>Качается сейчас <small id="activeCount"></small></h3>
  <table id="active"></table>

  <div class="cols">
    <div>
      <h3>В ожидании <small id="queueCount"></small></h3>
      <div class="box mono" id="queue"></div>
    </div>
    <div>
      <h3>Недавно скачано</h3>
      <table id="done"></table>
    </div>
  </div>

  <h3>Ошибки <small id="failCount"></small></h3>
  <table id="failed"></table>
</div>

<!-- ============================== ВИДЕО ============================== -->
<div id="video" hidden>
  <div class="controls">
    <span id="vstate" class="pill">…</span>
    <button id="vStart" class="primary">Скачать только видео</button>
    <button id="vStop">Остановить</button>
    <button id="vRetry">Повторить ошибки</button>
  </div>
  <div class="mut" id="vAuth"></div>
  <div class="mut" id="vDir"></div>
  <div class="msg" id="vMsg" hidden></div>

  <details class="settings" open>
    <summary>Настройки загрузки видео</summary>
    <div class="sgrid" id="vSettings"></div>
    <div class="controls">
      <button id="vApply">Применить</button>
      <span class="mut" id="vApplyNote">Размеры, таймауты и потоки действуют на видео, которые начнут качаться после «Применить». «Видео одновременно» — сразу.</span>
    </div>
  </details>

  <div class="cards" id="vCards"></div>
  <div class="bar"><i id="vBar"></i></div>
  <div class="mut" id="vBarText" style="margin-top:4px"></div>

  <h3>Качается сейчас <small id="vActiveCount"></small></h3>
  <div id="vActive"></div>
  <div class="legend">
    <span><i style="background:#2e7d32"></i>прочитано</span>
    <span><i style="background:#8bc34a"></i>читается сейчас</span>
    <span><i style="background:#666"></i>ещё не проверено</span>
    <span><i style="background:#ef6c00"></i>частично потеряно</span>
    <span><i style="background:#c62828"></i>потеряно (предположительно)</span>
  </div>

  <div class="cols">
    <div>
      <h3>В ожидании <small id="vQueueCount"></small></h3>
      <div class="box mono" id="vQueue"></div>
    </div>
    <div>
      <h3>Недавно скачано</h3>
      <table id="vDone"></table>
    </div>
  </div>

  <h3>Ошибки <small id="vFailCount"></small></h3>
  <table id="vFailed"></table>
</div>

<!-- ============================== ФАЙЛЫ ============================== -->
<div id="files" hidden>
  <div class="controls">
    <span id="fstate" class="pill">…</span>
    <button id="fStart" class="primary">Скачать файлы</button>
    <button id="fStop">Остановить</button>
    <span class="mut">потоков:</span>
    <input id="fThreads" type="number" min="1" max="8" value="3">
    <button id="fThreadsApply">Применить</button>
    <button id="fRetry">Повторить ошибки</button>
  </div>
  <label class="mut"><input id="fMedia" type="checkbox"> качать и фото/видео из папок (обычно их качают вкладки «Фото» и «Видео»)</label>
  <div class="mut" id="fAuth"></div>
  <div class="mut" id="fDir"></div>
  <div class="msg" id="fMsg" hidden></div>

  <h3>Откуда берётся список файлов</h3>
  <div class="mut">Открой во вкладке браузера раздел «Файлы» на Диске и заходи в папки, прокручивая каждую до конца, —
    перехват запомнит, что в них лежит. Или запусти обход: приложение само откроет все папки по очереди в отдельной вкладке «Обход».</div>
  <div class="controls">
    <span class="mut">только папка на Диске:</span>
    <input id="fScope" type="text" style="min-width:320px" placeholder="весь Диск; например: Творчество или ссылка на папку">
    <button id="fScopeApply">Выбрать</button>
    <button id="fScopeAll">Весь Диск</button>
  </div>
  <div class="mut" id="fScopeMsg"></div>
  <div class="controls">
    <span id="cstate" class="pill">…</span>
    <button id="cStart">Обойти все папки</button>
    <button id="cStop">Остановить обход</button>
    <button id="cReset">Обойти заново</button>
  </div>
  <div class="mut" id="cInfo"></div>

  <div class="cards" id="fCards"></div>
  <div class="bar"><i id="fBar"></i></div>
  <div class="mut" id="fBarText" style="margin-top:4px"></div>

  <h3>Качается сейчас <small id="fActiveCount"></small></h3>
  <table id="fActive"></table>

  <div class="cols">
    <div>
      <h3>В ожидании <small id="fQueueCount"></small></h3>
      <div class="box mono" id="fQueue"></div>
    </div>
    <div>
      <h3>Недавно скачано</h3>
      <table id="fDone"></table>
    </div>
  </div>

  <h3>Ошибки <small id="fFailCount"></small></h3>
  <table id="fFailed"></table>

  <h3>Папки, которые ещё не открывались <small id="fUnopenedCount"></small></h3>
  <div class="box mono" id="fUnopened"></div>
</div>
</main>

<script>
  const SIZES = ["ORIGINAL","XXXL","XXL","XL","L","M","S","XS","XXS","XXXS","DEFAULT"];
  const $ = (id) => document.getElementById(id);

  function fmtBytes(n) {
    n = Number(n) || 0;
    const u = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']; let i = 0;
    while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
    return (i ? n.toFixed(1) : Math.round(n)) + ' ' + u[i];
  }
  function fmtTime(iso) { try { return new Date(iso).toLocaleTimeString(); } catch (e) { return ''; } }
  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    return e;
  }
  function row(...cells) {
    const tr = document.createElement('tr');
    for (const c of cells) { const td = document.createElement('td'); if (c instanceof Node) td.appendChild(c); else td.textContent = c; tr.appendChild(td); }
    return tr;
  }
  async function api(path, body) {
    const opt = body === undefined ? {} : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
    const r = await fetch(path, opt);
    return r.json();
  }

  // ---------------- вкладки
  let tab = { '#video': 'video', '#files': 'files' }[location.hash] || 'photo';
  function showTab(t) {
    tab = t;
    $('photo').hidden = t !== 'photo'; $('video').hidden = t !== 'video'; $('files').hidden = t !== 'files';
    $('tabPhoto').classList.toggle('on', t === 'photo'); $('tabVideo').classList.toggle('on', t === 'video');
    $('tabFiles').classList.toggle('on', t === 'files');
    history.replaceState(null, '', '#' + t);
    refresh();
  }
  $('tabPhoto').onclick = () => showTab('photo');
  $('tabVideo').onclick = () => showTab('video');
  $('tabFiles').onclick = () => showTab('files');

  // ---------------- фото
  function render(s) {
    const c = s.counts;
    $('state').textContent = { idle: 'остановлено', running: 'идёт загрузка', stopping: 'останавливаюсь…' }[s.state] || s.state;
    $('state').className = 'pill ' + s.state;
    $('btnStart').disabled = s.state !== 'idle';
    $('btnStop').disabled = s.state !== 'running';
    $('btnRetry').disabled = c.failed === 0 || s.state !== 'idle';
    $('btnRetry').textContent = 'Повторить ошибки' + (c.failed ? ` (${c.failed})` : '');
    if (document.activeElement !== $('threads')) $('threads').value = s.threads;
    $('dir').textContent = 'Папка: ' + s.files_dir + '/<дата>/';
    if (document.activeElement !== $('dirInput')) $('dirInput').value = s.files_dir;
    $('btnDirApply').disabled = s.state !== 'idle';
    $('msg').hidden = !s.message; $('msg').textContent = s.message || '';

    const cards = [
      [fmtBytes(s.speed) + '/с', 'скорость'],
      [c.done, 'скачано (' + fmtBytes(c.done_bytes) + ')'],
      [c.remaining, 'ещё не в очереди'],
      [c.queued + ' / ' + s.queue_size, 'в ожидании'],
      [c.downloading + ' / ' + s.threads, 'качается'],
      [c.failed, 'ошибок'],
      [c.no_links, 'фото без ссылок'],
    ];
    $('cards').replaceChildren(...cards.map(([v, l]) => { const d = el('div', 'card'); d.append(el('b', null, v), el('span', null, l)); return d; }));

    const total = c.with_links;
    $('bar').style.width = total ? (100 * c.done / total).toFixed(1) + '%' : '0';
    $('barText').textContent = total ? `${c.done} из ${total} фото со ссылками (ошибок: ${c.failed})` : 'Пока нет фото со ссылками на скачивание — прокрути ленту на Диске через mitmproxy.';

    const chips = SIZES.filter(k => s.by_size[k]).map(k => el('span', 'chip ' + k, `${k} ${s.by_size[k]}`));
    $('bysize').replaceChildren(...chips);

    $('activeCount').textContent = s.active.length ? `(${s.active.length})` : '';
    $('active').replaceChildren(...s.active.map(p => {
      const pct = p.total ? Math.min(100, 100 * p.bytes / p.total) : 0;
      const bar = el('div', 'bar'); const i = el('i'); i.style.width = pct + '%'; bar.appendChild(i);
      return row(el('span', 'mono', p.name), el('span', 'chip ' + p.size, p.size), `${p.attempt}/${p.of}`,
                 bar, fmtBytes(p.bytes) + (p.total ? ' / ' + fmtBytes(p.total) : ''), p.elapsed + ' с');
    }));

    $('queueCount').textContent = `(${s.queue.length} из ${s.queue_size})`;
    $('queue').replaceChildren(...s.queue.map(n => el('div', null, n)));

    $('done').replaceChildren(...s.recent_done.map(d => row(el('span', 'mono', d.name), el('span', 'chip ' + d.size, d.size), fmtBytes(d.bytes), fmtTime(d.at))));
    $('failCount').textContent = c.failed ? `(${c.failed})` : '';
    $('failed').replaceChildren(...s.recent_failed.map(f => row(el('span', 'mono', f.name), el('span', 'err', f.error || ''), fmtTime(f.at))));
  }

  // ---------------- видео
  const VFIELDS = [
    ["videos", "Видео одновременно", "сколько видео качается параллельно"],
    ["jobs", "Потоки проверки блоков (на каждое видео)", "recover.py --jobs"],
    ["block_kb", "Размер блока, КБ", "--block"],
    ["sub_kb", "Мелкий кусок, КБ", "--sub: на что режем нечитаемый блок"],
    ["timeout", "Таймаут блока, с", "--timeout"],
    ["sub_timeout", "Таймаут куска, с", "--sub-timeout"],
    ["retries", "Попыток на блок", "--retries"],
    ["probe_size", "Размер пробы, байт", "--probe-size (refine)"],
    ["probe_timeout", "Таймаут пробы, с", "--probe-timeout (refine)"],
    ["refine_jobs", "Потоки refine", "--jobs на этапе уточнения"],
    ["stride_kb", "Шаг сетки refine, КБ", "--stride"],
  ];
  const VBOOLS = [
    ["refine", "После скачивания уточнять битые зоны (--refine)"],
    ["reencode", "Повреждённые видео перекодировать в H.264 (иначе: сначала переупаковка)"],
  ];
  const BAR_COLORS = { g: '#2e7d32', p: '#8bc34a', u: '#666', y: '#ef6c00', r: '#c62828' };
  const PHASES = { link: 'ссылка', download: 'скачивание', refine: 'уточнение битых зон', assemble: 'сборка ffmpeg' };
  let vDirty = false, vBuilt = false;

  function buildVSettings(limits) {
    const box = $('vSettings'); box.replaceChildren();
    for (const [k, label, hint] of VFIELDS) {
      const l = el('label'); const inp = el('input'); inp.type = 'number'; inp.id = 'vs_' + k;
      if (limits[k]) { inp.min = limits[k][0]; inp.max = limits[k][1]; }
      inp.addEventListener('input', () => { vDirty = true; });
      l.append(el('span', null, label), inp, el('small', null, hint));
      box.appendChild(l);
    }
    for (const [k, label] of VBOOLS) {
      const l = el('label', 'chk'); const inp = el('input'); inp.type = 'checkbox'; inp.id = 'vs_' + k;
      inp.addEventListener('change', () => { vDirty = true; });
      l.append(inp, el('span', null, label));
      box.appendChild(l);
    }
    vBuilt = true;
  }
  function fillVSettings(s) {
    if (vDirty) return;
    for (const [k] of VFIELDS) $('vs_' + k).value = s[k];
    for (const [k] of VBOOLS) $('vs_' + k).checked = !!s[k];
  }
  function drawBar(canvas, bar) {
    const W = canvas.width = canvas.clientWidth || 600, H = canvas.height = 16;
    const g = canvas.getContext('2d'), w = W / bar.length;
    for (let i = 0; i < bar.length; i++) {
      g.fillStyle = BAR_COLORS[bar[i]] || '#666';
      g.fillRect(Math.floor(i * w), 0, Math.ceil(w), H);
    }
  }
  function pct(a, b) { return b ? (100 * a / b).toFixed(b && a / b < 0.1 ? 2 : 1) : '0'; }

  function renderV(s) {
    const c = s.counts;
    if (!vBuilt) buildVSettings(s.limits);
    fillVSettings(s.settings);
    $('vstate').textContent = { idle: 'остановлено', running: 'идёт загрузка', stopping: 'останавливаюсь…' }[s.state] || s.state;
    $('vstate').className = 'pill ' + s.state;
    $('vStart').disabled = s.state !== 'idle';
    $('vStop').disabled = s.state !== 'running';
    $('vRetry').disabled = c.failed === 0 || s.state !== 'idle';
    $('vRetry').textContent = 'Повторить ошибки' + (c.failed ? ` (${c.failed})` : '');
    $('vDir').textContent = 'Папка: ' + s.files_dir + '/<дата>/  ·  ffmpeg: ' + (s.have_ffmpeg ? 'найден' : 'НЕ НАЙДЕН (повреждённые видео не соберутся)');
    $('vDir').className = s.have_ffmpeg ? 'mut' : 'warn';
    const a = s.auth;
    if (a.cookies && a.has_sk) {
      $('vAuth').textContent = `Авторизация: кук ${a.cookies}, sk есть` + (a.has_session ? '' : ', но нет Session_id') +
        (a.age_sec !== null ? `, обновлена ${a.age_sec < 90 ? a.age_sec + ' с' : Math.round(a.age_sec / 60) + ' мин'} назад` : '');
      $('vAuth').className = 'mut';
    } else {
      $('vAuth').textContent = 'Авторизации нет: открой Яндекс Диск в браузере через mitmproxy — аддон передаст куки и sk.';
      $('vAuth').className = 'warn';
    }
    $('vMsg').hidden = !s.message; $('vMsg').textContent = s.message || '';

    let speed = 0; for (const p of s.active) speed += p.speed || 0;
    const cards = [
      [fmtBytes(speed) + '/с', 'скорость'],
      [c.done, 'скачано (' + fmtBytes(c.done_bytes) + ')'],
      [c.partial, 'из них с потерями (' + fmtBytes(c.lost_bytes) + ')'],
      [c.remaining, 'ещё не в очереди'],
      [c.queued + ' / ' + s.queue_size, 'в ожидании'],
      [c.downloading + ' / ' + s.settings.videos, 'качается'],
      [c.failed, 'ошибок'],
      [c.no_links, 'видео без данных ресурса'],
    ];
    $('vCards').replaceChildren(...cards.map(([v, l]) => { const d = el('div', 'card'); d.append(el('b', null, v), el('span', null, l)); return d; }));
    const total = c.with_links;
    $('vBar').style.width = total ? (100 * c.done / total).toFixed(1) + '%' : '0';
    $('vBarText').textContent = total ? `${c.done} из ${total} видео (ошибок: ${c.failed})` : 'Пока нет видео с данными ресурса — прокрути ленту на Диске через mitmproxy.';

    $('vActiveCount').textContent = s.active.length ? `(${s.active.length})` : '';
    const cards2 = [], bars = [];
    for (const p of s.active) {
      const d = el('div', 'vcard'); const top = el('div', 'top');
      top.append(el('span', 'mono', p.name), el('span', 'chip phase', PHASES[p.phase] || p.phase),
                 el('span', 'mut', p.attempt > 1 ? `попытка ссылки ${p.attempt}` : ''), el('span', 'mut', p.elapsed + ' с'));
      const cv = document.createElement('canvas'); bars.push([cv, p.bar]);
      const info = el('div', 'mut', `${fmtBytes(p.got)} из ${fmtBytes(p.size)} (${pct(p.got, p.size)}%)  ·  предположительно потеряно ${fmtBytes(p.lost)} (${pct(p.lost, p.size)}%)  ·  ${fmtBytes(p.speed)}/с`);
      d.append(top, cv, info, el('div', 'mut mono', p.line || ''));
      cards2.push(d);
    }
    $('vActive').replaceChildren(...cards2);
    for (const [cv, b] of bars) drawBar(cv, b);

    $('vQueueCount').textContent = `(${s.queue.length} из ${s.queue_size})`;
    $('vQueue').replaceChildren(...s.queue.map(n => el('div', null, n)));
    $('vDone').replaceChildren(...s.recent_done.map(d => row(el('span', 'mono', d.name), fmtBytes(d.bytes),
      d.lost ? el('span', 'warn', 'потеряно ' + fmtBytes(d.lost)) : el('span', 'mut', 'без потерь'), fmtTime(d.at))));
    $('vFailCount').textContent = c.failed ? `(${c.failed})` : '';
    $('vFailed').replaceChildren(...s.recent_failed.map(f => row(el('span', 'mono', f.name), el('span', 'err', f.error || ''), fmtTime(f.at))));
  }

  // ---------------- файлы
  let fThreadsDirty = false, fScopeDirty = false, fScopeNote = '';
  $('fThreads').addEventListener('input', () => { fThreadsDirty = true; });
  $('fScope').addEventListener('input', () => { fScopeDirty = true; });
  async function setScope(folder) {
    const r = await api('/api/fl/scope', { folder });
    fScopeDirty = false;
    fScopeNote = r.ok ? '' : 'Ошибка: ' + r.error;
    $('fScopeMsg').textContent = fScopeNote; $('fScopeMsg').className = r.ok ? 'mut' : 'err';
    $('fScope').blur(); refresh();
  }
  $('fScopeApply').onclick = () => setScope($('fScope').value);
  $('fScopeAll').onclick = () => setScope('');
  $('fScope').addEventListener('keydown', (e) => { if (e.key === 'Enter') setScope($('fScope').value); });

  function renderF(s) {
    const c = s.counts, cr = s.crawl;
    $('fstate').textContent = { idle: 'остановлено', running: 'идёт загрузка', stopping: 'останавливаюсь…' }[s.state] || s.state;
    $('fstate').className = 'pill ' + s.state;
    $('fStart').disabled = s.state !== 'idle';
    $('fStop').disabled = s.state !== 'running';
    $('fRetry').disabled = c.failed === 0 || s.state !== 'idle';
    $('fRetry').textContent = 'Повторить ошибки' + (c.failed ? ` (${c.failed})` : '');
    $('fThreads').max = s.max_threads;
    if (!fThreadsDirty && document.activeElement !== $('fThreads')) $('fThreads').value = s.threads;
    $('fMedia').checked = s.include_media;
    $('fDir').textContent = 'Папка: ' + s.files_dir + '/<путь на Диске>/';
    const a = s.auth;
    if (a.cookies && a.has_sk) {
      $('fAuth').textContent = `Авторизация: кук ${a.cookies}, sk есть` + (a.has_session ? '' : ', но нет Session_id') +
        (a.age_sec !== null ? `, обновлена ${a.age_sec < 90 ? a.age_sec + ' с' : Math.round(a.age_sec / 60) + ' мин'} назад` : '');
      $('fAuth').className = 'mut';
    } else {
      $('fAuth').textContent = 'Авторизации нет: открой Яндекс Диск во вкладке браузера — перехват передаст куки и sk.';
      $('fAuth').className = 'warn';
    }
    $('fMsg').hidden = !s.message; $('fMsg').textContent = s.message || '';

    const scopeName = s.scope === '/disk' ? '' : s.scope.slice('/disk/'.length);
    if (document.activeElement !== $('fScope') && !fScopeDirty) $('fScope').value = scopeName;
    $('fScopeApply').disabled = $('fScopeAll').disabled = s.state !== 'idle' || cr.run;
    $('cStart').textContent = scopeName ? `Обойти «${scopeName}» с подпапками` : 'Обойти все папки';
    if (!fScopeNote) {
      const empty = scopeName && !cr.run && c.dirs_crawled > 0 && c.files === 0;
      const except = (s.exclude || []).map(x => `«${x.slice(s.scope.length + 1)}»`).join(', ');
      $('fScopeMsg').textContent = !scopeName ? 'Сейчас: весь Диск' + (except ? `, кроме ${except}.` : '.')
        : empty ? `В «${scopeName}» ничего не нашлось. Проверь название (регистр букв важен) — проще открыть папку во вкладке Диска и вставить сюда ссылку из адресной строки.`
        : `Сейчас: только «${scopeName}» и её подпапки${except ? ', кроме ' + except : ''} — обход, счётчики и загрузка.`;
      $('fScopeMsg').className = empty ? 'warn' : 'mut';
    }

    $('cstate').textContent = cr.run ? (cr.agent ? 'обход идёт' : 'обход ждёт окно приложения') : 'обход остановлен';
    $('cstate').className = 'pill ' + (cr.run ? (cr.agent ? 'running' : 'stopping') : 'idle');
    $('cStart').disabled = cr.run;
    $('cStop').disabled = !cr.run;
    $('cReset').disabled = cr.run || !c.dirs_crawled;
    $('cInfo').textContent = `Папок пройдено обходом: ${c.dirs_crawled} из ${c.dirs}` +
      (cr.run ? ` (за этот запуск: ${cr.visited})` : '') + (cr.current ? ` · сейчас: ${cr.current}` : '') +
      (cr.message ? ' · ' + cr.message : '') +
      (cr.agent ? '' : ' · Обход выполняет только окно приложения YandexDL Desktop; без него открывай папки сам.');

    const cards = [
      [fmtBytes(s.speed) + '/с', 'скорость'],
      [c.done, 'скачано (' + fmtBytes(c.done_bytes) + ')'],
      [c.remaining, 'ещё не в очереди (' + fmtBytes(c.remaining_bytes) + ')'],
      [c.queued + ' / ' + s.queue_size, 'в ожидании'],
      [c.downloading + ' / ' + s.threads, 'качается'],
      [c.failed, 'ошибок'],
      [c.files, 'файлов найдено'],
      [c.media, s.include_media ? 'из них фото/видео' : 'фото/видео пропущено'],
      [c.dirs, 'папок найдено'],
    ];
    $('fCards').replaceChildren(...cards.map(([v, l]) => { const d = el('div', 'card'); d.append(el('b', null, v), el('span', null, l)); return d; }));
    const total = c.done + c.failed + c.remaining + c.queued + c.downloading;
    $('fBar').style.width = total ? (100 * c.done / total).toFixed(1) + '%' : '0';
    $('fBarText').textContent = total ? `${c.done} из ${total} файлов (ошибок: ${c.failed})`
      : 'Пока нет файлов для скачивания — открой папки в разделе «Файлы» на Диске или запусти обход.';

    $('fActiveCount').textContent = s.active.length ? `(${s.active.length})` : '';
    $('fActive').replaceChildren(...s.active.map(p => {
      const pct = p.total ? Math.min(100, 100 * p.bytes / p.total) : 0;
      const bar = el('div', 'bar'); const i = el('i'); i.style.width = pct + '%'; bar.appendChild(i);
      return row(el('span', 'mono', p.path), `${p.attempt}/${p.of}`, bar,
                 fmtBytes(p.bytes) + (p.total ? ' / ' + fmtBytes(p.total) : ''), p.elapsed + ' с');
    }));
    $('fQueueCount').textContent = `(${s.queue.length} из ${s.queue_size})`;
    $('fQueue').replaceChildren(...s.queue.map(n => el('div', null, n)));
    $('fDone').replaceChildren(...s.recent_done.map(d => row(el('span', 'mono', d.path), fmtBytes(d.bytes),
      el('span', 'mut', d.note ? 'уже был на диске' : ''), fmtTime(d.at))));
    $('fFailCount').textContent = c.failed ? `(${c.failed})` : '';
    $('fFailed').replaceChildren(...s.recent_failed.map(f => row(el('span', 'mono', f.path), el('span', 'err', f.error || ''), fmtTime(f.at))));
    $('fUnopenedCount').textContent = c.dirs_unopened ? `(${c.dirs_unopened})` : '';
    $('fUnopened').replaceChildren(...(s.unopened.length ? s.unopened.map(n => el('div', null, n))
      : [el('div', 'mut', 'нет — содержимое всех найденных папок уже приходило')]));
  }

  let busy = false;
  async function refresh() {
    if (busy) return; busy = true;
    try {
      if (tab === 'photo') render(await api('/api/dl/status'));
      else if (tab === 'video') renderV(await api('/api/vd/status'));
      else renderF(await api('/api/fl/status'));
    } catch (e) { /* сервер перезапускается */ }
    busy = false;
  }
  $('btnStart').onclick = async () => { const r = await api('/api/dl/start', {}); if (r.error) alert(r.error); refresh(); };
  $('btnStop').onclick = async () => { await api('/api/dl/stop', {}); refresh(); };
  $('btnThreads').onclick = async () => { await api('/api/dl/threads', { threads: parseInt($('threads').value, 10) }); $('threads').blur(); refresh(); };
  $('btnRetry').onclick = async () => { await api('/api/dl/retry-failed', {}); refresh(); };
  $('btnDirApply').onclick = async () => {
    $('dirMsg').textContent = 'проверяю...';
    const r = await api('/api/settings/files-dir', { dir: $('dirInput').value });
    $('dirMsg').textContent = r.ok ? ('Готово, теперь папка: ' + r.dir) : ('Ошибка: ' + r.error);
    refresh();
  };

  $('vStart').onclick = async () => { if (vDirty) await applyV(); const r = await api('/api/vd/start', {}); if (r.error) alert(r.error); refresh(); };
  $('vStop').onclick = async () => { await api('/api/vd/stop', {}); refresh(); };
  $('vRetry').onclick = async () => { await api('/api/vd/retry-failed', {}); refresh(); };
  async function applyV() {
    const body = {};
    for (const [k] of VFIELDS) body[k] = parseInt($('vs_' + k).value, 10);
    for (const [k] of VBOOLS) body[k] = $('vs_' + k).checked;
    const r = await api('/api/vd/settings', body);
    vDirty = false;
    $('vApplyNote').textContent = r.invalid && r.invalid.length ? 'Не принято: ' + r.invalid.join(', ') : 'Применено.';
    refresh();
  }
  $('vApply').onclick = applyV;

  $('fStart').onclick = async () => { const r = await api('/api/fl/start', {}); if (r.error) alert(r.error); refresh(); };
  $('fStop').onclick = async () => { await api('/api/fl/stop', { message: 'Остановлено вручную.' }); refresh(); };
  $('fRetry').onclick = async () => { await api('/api/fl/retry-failed', {}); refresh(); };
  $('fThreadsApply').onclick = async () => {
    await api('/api/fl/settings', { threads: parseInt($('fThreads').value, 10) });
    fThreadsDirty = false; $('fThreads').blur(); refresh();
  };
  $('fMedia').onchange = async () => { await api('/api/fl/settings', { include_media: $('fMedia').checked }); refresh(); };
  $('cStart').onclick = async () => { await api('/api/fl/crawl', { run: true }); refresh(); };
  $('cStop').onclick = async () => { await api('/api/fl/crawl', { run: false, message: 'остановлен вручную' }); refresh(); };
  $('cReset').onclick = async () => { await api('/api/fl/crawl', { run: true, reset: true }); refresh(); };

  showTab(tab); setInterval(refresh, 1000);
</script>
</body>
</html>
"""


# ---------------------------------------------------------------- превью

try:
    from PIL import Image, ImageOps
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
        HAVE_HEIF = True
    except ImportError:
        HAVE_HEIF = False
    HAVE_PIL = True
except ImportError:  # без Pillow галерея работает, но без превью
    HAVE_PIL = False
    HAVE_HEIF = False

VIEW_MAX_SIDE = 4096          # длинная сторона JPEG, в который конвертируем HEIC для просмотра
_thumb_sem = threading.Semaphore(4)   # не больше 4 одновременных сжатий


def _encode_thumb(src, dst):
    """Сжимает картинку в JPEG ≤ THUMB_MAX_BYTES: сначала роняем качество, потом уменьшаем сторону."""
    with Image.open(src) as im:
        try:
            im.draft("RGB", (THUMB_SIDE * 2, THUMB_SIDE * 2))   # быстрый декод больших JPEG
        except Exception:  # noqa: BLE001
            pass
        im = ImageOps.exif_transpose(im)
        if im.mode != "RGB":
            im = im.convert("RGB")
        side = THUMB_SIDE
        data = b""
        while True:
            t = im.copy()
            t.thumbnail((side, side), Image.LANCZOS)
            for q in (78, 68, 58, 48, 40, 32):
                buf = io.BytesIO()
                t.save(buf, "JPEG", quality=q, optimize=True, progressive=True)
                data = buf.getvalue()
                if len(data) <= THUMB_MAX_BYTES:
                    break
            if len(data) <= THUMB_MAX_BYTES or side <= 96:
                break
            side = int(side * 0.8)
    tmp = dst + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, dst)


def _video_poster(src, dst_jpg):
    """Один кадр из видео через ffmpeg (для плитки в галерее)."""
    ff = shutil.which("ffmpeg")
    if not ff:
        raise RuntimeError("ffmpeg не найден")
    for ss in ("1", "0"):
        r = subprocess.run([ff, "-v", "error", "-y", "-ss", ss, "-i", src, "-frames:v", "1", "-q:v", "3", dst_jpg],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        if r.returncode == 0 and os.path.isfile(dst_jpg) and os.path.getsize(dst_jpg) > 0:
            return
    raise RuntimeError("не удалось достать кадр")


def _done_file(rid):
    """Абсолютный путь скачанного файла по номеру строки (фото или видео), либо None."""
    with dbc() as c:
        row = c.execute(
            "SELECT dl_status, dl_file, dl_root, vd_status, vd_file, vd_root FROM items WHERE rowid=?", (rid,)
        ).fetchone()
    if not row:
        return None
    if row["vd_status"] == "done" and row["vd_file"]:
        rel = row["vd_file"]
        # Для строк из старых баз, где корень ещё не хранился, используем путь по умолчанию.
        root = row["vd_root"] or DEFAULT_FILES_DIR
    elif row["dl_status"] == "done" and row["dl_file"]:
        rel = row["dl_file"]
        root = row["dl_root"] or DEFAULT_FILES_DIR
    else:
        return None
    root = os.path.realpath(os.path.abspath(root))
    src = os.path.realpath(os.path.join(root, rel))
    try:
        if os.path.commonpath((root, src)) != root:
            return None
    except ValueError:
        return None
    return src if os.path.isfile(src) else None


@app.get("/thumb/<int:rid>")
def thumb(rid):
    if not HAVE_PIL:
        return Response("Pillow не установлен: pip install pillow", status=501)
    src = _done_file(rid)
    if not src:
        return Response(status=404)

    key = hashlib.md5(f"{src}|{os.path.getmtime(src)}|{THUMB_SIDE}".encode("utf-8")).hexdigest()
    dst = os.path.join(THUMBS_DIR, key[:2], key + ".jpg")
    if not os.path.isfile(dst):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with _thumb_sem:
            if not os.path.isfile(dst):      # вдруг другой поток уже сделал
                poster = None
                try:
                    if os.path.splitext(src)[1].lstrip(".").lower() in VIDEO_EXT:
                        poster = dst + ".poster.jpg"
                        _video_poster(src, poster)
                        _encode_thumb(poster, dst)
                    else:
                        _encode_thumb(src, dst)
                except Exception as e:  # noqa: BLE001
                    print(f"[thumb] {src}: {type(e).__name__}: {e}")
                    return Response(status=415)
                finally:
                    if poster and os.path.exists(poster):
                        try:
                            os.remove(poster)
                        except OSError:
                            pass
    resp = send_file(dst, mimetype="image/jpeg", max_age=31536000, conditional=True)
    resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return resp


@app.get("/file/<int:rid>")
def open_file(rid):
    """Открывает скачанный файл (оригинал, либо лучшее из скачанного). HEIC/HEIF браузеры не показывают —
    их отдаём как большой JPEG (если установлен pillow-heif). Видео отдаётся с поддержкой Range (перемотка)."""
    src = _done_file(rid)
    if not src:
        return Response("файл не найден", status=404)
    if src.lower().endswith((".heic", ".heif")) and HAVE_PIL and HAVE_HEIF:
        key = hashlib.md5(f"view|{src}|{os.path.getmtime(src)}|{VIEW_MAX_SIDE}".encode("utf-8")).hexdigest()
        dst = os.path.join(THUMBS_DIR, "view", key + ".jpg")
        if not os.path.isfile(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            try:
                with Image.open(src) as im:
                    im = ImageOps.exif_transpose(im)
                    if im.mode != "RGB":
                        im = im.convert("RGB")
                    im.thumbnail((VIEW_MAX_SIDE, VIEW_MAX_SIDE), Image.LANCZOS)
                    tmp = dst + ".tmp"
                    im.save(tmp, "JPEG", quality=90)
                os.replace(tmp, dst)
            except Exception as e:  # noqa: BLE001
                print(f"[view] {src}: {type(e).__name__}: {e}")
                return send_file(src, conditional=True)   # хотя бы отдадим как есть
        return send_file(dst, mimetype="image/jpeg", conditional=True, max_age=3600)
    return send_file(src, conditional=True, max_age=3600)


if __name__ == "__main__":
    init_db()
    init_auth_db()
    load_files_dir_setting()
    os.makedirs(files_dir(), exist_ok=True)
    os.makedirs(THUMBS_DIR, exist_ok=True)
    print(f"Проверяю существующие файлы: {reconcile_existing_downloads()}, "
          f"из раздела «Файлы» уже на диске: {reconcile_existing_files()}")
    cleanup_part_files()
    DL.load_settings()
    VD.load_settings()
    FL.load_settings()
    print(f"DB: {DB_PATH}\nАвторизация: {AUTH_DB_PATH}")
    print(f"Файлы: {files_dir()}\nПревью: {THUMBS_DIR}" + ("" if HAVE_PIL else "  (!) нет Pillow: pip install pillow"))
    if not shutil.which("ffmpeg"):
        print("(!) ffmpeg не найден в PATH: повреждённые видео не соберутся, у видео не будет превью")
    print(f"Статус загрузки: http://{HOST}:{PORT}/status")
    print(f"Галерея: http://{HOST}:{PORT}/")
    app.run(host=HOST, port=PORT, threaded=True)