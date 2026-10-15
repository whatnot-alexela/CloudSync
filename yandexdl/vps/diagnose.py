"""
Разбор ответов Диска, сохранённых перехватом (YDISK_CAPTURE_DUMP): в каком виде Диск отдаёт список папки.
Нужен, если после обхода найдено 0 файлов — по выводу видно, где в ответе лежат пути /disk/… и какие
у этих объектов поля; под это правится find_disk_resources() в app/capture_cdp.py и capture/addon.py.

    python diagnose.py /var/lib/yandexdl/capture-dump [сколько_разных_запросов]
"""
import json
import os
import re
import sys
from collections import OrderedDict
from urllib.parse import parse_qs, urlparse

JSON_SCRIPT_RE = re.compile(r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>', re.S | re.I)


def is_disk_path(s):
    return isinstance(s, str) and (s == "/disk" or s.startswith("/disk/") or s.startswith("disk:/"))


def walk(obj, where, out):
    """out: шаблон пути (индексы списков -> []) -> [пример объекта-владельца, ключ, сколько раз]."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if is_disk_path(v) or (isinstance(k, str) and is_disk_path(k) and isinstance(v, dict)):
                key = f"{where}.{k}" if is_disk_path(v) else f"{where}.<путь как ключ>"
                owner = obj if is_disk_path(v) else v
                if key not in out:
                    out[key] = [owner, k, 0]
                out[key][2] += 1
            walk(v, f"{where}.{'<путь как ключ>' if isinstance(k, str) and is_disk_path(k) else k}", out)
    elif isinstance(obj, list):
        for v in obj:
            walk(v, where + "[]", out)


def short(v, depth=0):
    if isinstance(v, str):
        return v if len(v) <= 70 else v[:67] + "..."
    if isinstance(v, dict):
        if depth >= 2:
            return "{" + ", ".join(list(v)[:12]) + ("…" if len(v) > 12 else "") + "}"
        return {k: short(x, depth + 1) for k, x in list(v.items())[:25]}
    if isinstance(v, list):
        return [short(x, depth + 1) for x in v[:2]] + ([f"…ещё {len(v) - 2}"] if len(v) > 2 else [])
    return v


def skeleton(obj, depth=0, max_depth=3):
    if depth >= max_depth:
        return "…"
    if isinstance(obj, dict):
        return {k: skeleton(v, depth + 1, max_depth) for k, v in list(obj.items())[:20]}
    if isinstance(obj, list):
        return [f"список из {len(obj)}", skeleton(obj[0], depth + 1, max_depth)] if obj else []
    return type(obj).__name__


def describe(name, d):
    u = urlparse(d.get("url") or "")
    q = parse_qs(u.query)
    model = (q.get("m") or q.get("_m") or [""])[0]
    post = d.get("post") or ""
    body = d.get("body") or ""
    print(f"--- {name}: {d.get('mode')} {u.path}{' [' + model + ']' if model else ''}  "
          f"(тело {len(body)} Б, распознано ресурсов: {d.get('found')})")
    if post:
        print(f"    запрос: {post[:300]}")
    docs = []
    if d.get("mode") == "html":
        for block in JSON_SCRIPT_RE.findall(body):
            try:
                docs.append(json.loads(block))
            except ValueError:
                pass
        hits = [m.start() for m in re.finditer(r'["\'](?:/disk/|disk:/)', body)]
        print(f"    JSON-блоков в <script>: {len(docs)}; упоминаний путей /disk/ в HTML: {len(hits)}")
        for pos in hits[:3]:
            print("    …" + body[max(0, pos - 120):pos + 120].replace("\n", " ") + "…")
    else:
        try:
            docs.append(json.loads(body))
        except ValueError:
            print(f"    не JSON: {body[:200]!r}")
    for doc in docs:
        found = OrderedDict()
        walk(doc, "$", found)
        if not found:
            continue
        print("    пути /disk/… найдены здесь:")
        for where, (owner, key, count) in list(found.items())[:6]:
            print(f"      {where}  ({count} шт.; поле {key!r}; поля объекта: {', '.join(map(str, list(owner)[:25]))})")
            print("        пример: " + json.dumps(short(owner), ensure_ascii=False)[:900])
        return True
    if docs and d.get("mode") != "html":
        print("    путей /disk/ нет; структура: " + json.dumps(skeleton(docs[0]), ensure_ascii=False)[:600])
    return False


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    dump_dir = argv[0] if argv else "/var/lib/yandexdl/capture-dump"
    limit = int(argv[1]) if len(argv) > 1 else 40
    try:
        names = sorted(n for n in os.listdir(dump_dir) if n.endswith(".json"))
    except OSError as e:
        print(f"Нет папки с ответами {dump_dir}: {e}")
        return 1
    if not names:
        print(f"В {dump_dir} пусто: перехват не видел ни одного ответа /models* от Диска. Проверь, что вкладка "
              "с Диском открыта на disk.yandex.ru (а не на 360.yandex.ru или странице входа), и «yandexdl logs».")
        return 1
    # по одному (последнему) ответу на каждый вид запроса: режим + путь + имя модели
    latest = OrderedDict()
    for name in reversed(names):
        try:
            with open(os.path.join(dump_dir, name), encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, ValueError) as e:
            print(f"--- {name}: не читается ({e})")
            continue
        u = urlparse(d.get("url") or "")
        q = parse_qs(u.query)
        path = "/client/…" if d.get("mode") == "html" else u.path
        kind = (d.get("mode"), path, (q.get("m") or q.get("_m") or [""])[0])
        if kind not in latest:
            latest[kind] = (name, d)
    print(f"Сохранено ответов: {len(names)}, разных запросов: {len(latest)}; разбираю до {limit}\n")
    with_paths = 0
    for name, d in list(latest.values())[:limit]:
        with_paths += bool(describe(name, d))
    print(f"\nОтветов, где встречаются пути /disk/…: {with_paths}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
