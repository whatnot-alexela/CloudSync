"""
Общие папки Яндекс Диска по ссылке (disk.yandex.ru/d/…, yadi.sk/d/…, файл — …/i/…) для vps/fetch.py: разобрать
ссылку и собрать список файлов через открытое API Диска (вход не нужен). Качает их уже программа — тем же
загрузчиком, что и свой Диск (докачка, спасение повреждённых файлов), fetch переносит их в облако частями.

Только stdlib.
"""
import hashlib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

API = os.environ.get("YDISK_PUBLIC_API", "https://cloud-api.yandex.net/v1/disk/public/resources")
FIELDS = "_embedded.total,_embedded.items.type,_embedded.items.size,_embedded.items.path"
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class Fail(Exception):
    pass


def parse_link(link):
    """«https://disk.yandex.ru/d/КОД/Подпапка?fbclid=…» -> (ключ «https://disk.yandex.ru/d/КОД», «/Подпапка»);
    не общая ссылка — None. Ссылку на подпапку браузер показывает как раз в таком виде."""
    u = urllib.parse.urlsplit((link or "").strip())
    parts = [urllib.parse.unquote(p) for p in u.path.split("/") if p]
    if u.scheme not in ("http", "https") or len(parts) < 2 or parts[0] not in ("d", "i"):
        return None
    key = urllib.parse.urlunsplit((u.scheme, u.netloc, "/" + parts[0] + "/" + urllib.parse.quote(parts[1]), "", ""))
    return key, "/" + "/".join(parts[2:])


def public_id(key):
    """Короткое имя ссылки для путей в базе программы (/public/<id>/…): её код, если он из букв и цифр."""
    code = key.rstrip("/").rsplit("/", 1)[-1]
    return code if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", code) else hashlib.md5(key.encode()).hexdigest()[:16]


def yapi(endpoint="", **params):
    """GET открытого API Диска; занятость и сбои сети — повтор через паузу, ошибка Диска — Fail с его текстом."""
    url = API + endpoint + "?" + urllib.parse.urlencode(params)
    why, code = None, None
    for attempt in range(1, 7):
        try:
            with _OPENER.open(url, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(e.read() or b"{}")
            except ValueError:
                body = {}
            why, code = body.get("description") or body.get("message") or f"HTTP {e.code}", e.code
            if e.code != 429 and e.code < 500:
                raise Fail(f"Диск: {why}")
        except (OSError, ValueError) as e:
            why, code = e, None
        time.sleep(5 * attempt)
    if code == 429:
        raise Fail(f"Яндекс ограничил запросы по этой ссылке ({why}) — обычно это проходит за час-сутки, "
                   "тогда повтори ту же команду")
    raise Fail(f"Диск не отвечает: {why}")


def walk(key, sub="/", progress=print):
    """Файлы общей папки по ссылке (или её подпапки sub): (имя папки, [(путь от корня ссылки, размер)], один ли это
    файл). Папки открываются параллельно, по уровням."""
    root = yapi(public_key=key, limit=0, fields="name,type,size")
    if root.get("type") == "file":
        return root.get("name") or "файл", [("/" + (root.get("name") or "файл"), root.get("size") or 0)], True
    files, lock = [], threading.Lock()

    def listing(path):
        subdirs, offset = [], 0
        while True:
            d = yapi(public_key=key, path=path, limit=1000, offset=offset, fields=FIELDS)
            emb = d.get("_embedded") or {}
            items = emb.get("items") or []
            with lock:
                for it in items:
                    if it.get("type") == "dir":
                        subdirs.append(it["path"])
                    else:
                        files.append((it["path"], it.get("size") or 0))
            offset += len(items)
            if not items or offset >= emb.get("total", 0):
                return subdirs

    pending, t = [sub], time.time()
    with ThreadPoolExecutor(6) as ex:
        while pending:
            futs = [ex.submit(listing, p) for p in pending]
            pending = []
            for f in futs:
                pending.extend(f.result())
                if time.time() - t > 30:
                    t = time.time()
                    progress(f"список: файлов {len(files)}, {sum(s for _, s in files) / 2**30:.1f} ГБ, "
                             f"ещё папок открыть: {len(pending)}+")
    files.sort()
    return root.get("name") or "папка", files, False
