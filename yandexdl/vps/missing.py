"""
Что есть на Яндекс Диске, но чего программа не знает: обход папок в окне программы видит только то, что страница
Диска успела подгрузить при прокрутке, и в больших папках мог остановиться на первой порции (40 штук). Полный список
берётся у официального API Диска через rclone, сравнивается с базой программы; с --add недостающие файлы и папки
добавляются в базу, и дальше их скачивает обычный fetch (с --no-crawl — обход уже не нужен).

  missing.py ПАПКА [--yandex YANDEX:] [--exclude ПАПКА]… [--add]

ПАПКА — как у fetch («/» — весь Диск); YANDEX — remote rclone типа yandex (как у cleanup, по умолчанию «yandex:»).
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cleanup  # noqa: E402
import compare  # noqa: E402
import fetch  # noqa: E402
import public  # noqa: E402
from compare import Fail, human, log, rclone, remote_join  # noqa: E402

BATCH = 2000
SHOW = 15


def yandex_listing(top, filters):
    """Файлы и папки под top: [(путь от top, папка?, размер, md5, mime, mtime)]. rclone lsjson пишет по объекту
    на строку — читаем потоком, не держа весь JSON в памяти."""
    p = subprocess.Popen(["rclone", "lsjson", "-R", "--hash", "--hash-type", "md5", *filters, top],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out = []
    for line in p.stdout:
        line = line.strip().rstrip(",")
        if not line.startswith("{"):
            continue
        it = json.loads(line)
        h = {k.lower(): v for k, v in (it.get("Hashes") or {}).items()}.get("md5")
        m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d+)?(Z|[+-]\d\d:\d\d)$", it.get("ModTime") or "")
        mtime = int(datetime.fromisoformat(m.group(1) + ("+00:00" if m.group(2) == "Z" else m.group(2))).timestamp()) \
            if m else None
        out.append((it["Path"], bool(it.get("IsDir")), it.get("Size"), h if isinstance(h, str) and len(h) == 32
                    else None, it.get("MimeType"), mtime))
    err = p.stderr.read()
    if p.wait():
        raise Fail(f"rclone lsjson {top}: {err.strip()[-300:]}")
    return out


def mediatype(mime):
    top = (mime or "").split("/", 1)[0]
    return top if top in ("image", "video", "audio") else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="папка Диска («/» — весь)")
    ap.add_argument("--yandex", default="yandex:", metavar="REMOTE:", help="remote rclone с этим Яндекс Диском")
    ap.add_argument("--exclude", action="append", default=[], metavar="ПАПКА")
    ap.add_argument("--add", action="store_true", help="добавить недостающее в базу программы")
    ap.add_argument("--rclone", default="gdrive:Yandex", metavar="ОБЛАКО:ПАПКА", help="только для подсказки fetch")
    a = ap.parse_args()
    try:
        if public.parse_link(a.target):
            raise Fail("общую ссылку программа читает целиком через открытый API, обход там не нужен — "
                       "нужна папка своего Диска")
        yname = a.yandex.split(":", 1)[0]
        ybase = a.yandex if ":" in a.yandex else a.yandex + ":"
        t = cleanup.remote_type(yname)
        if t is None:
            raise Fail(f"в rclone нет «{yname}:» — сначала подключи к rclone Яндекс Диск (README, «Чистка Яндекс Диска»)")
        if t != "yandex" and not os.environ.get("YDISK_CLEANUP_ANY_REMOTE"):
            raise Fail(f"«{yname}:» в rclone — не Яндекс Диск (тип {t})")

        db = sqlite3.connect(compare.DB, timeout=60)
        scope = "/disk" if a.target.strip().strip("/") in ("", "disk") else compare.disk_folder(db, a.target)
        excl = [compare.disk_folder(db, x) for x in a.exclude]
        title = (scope[len("/disk/"):] if scope != "/disk" else "/ (весь Диск)") + (
            ", кроме " + ", ".join(f"«{x[len('/disk/'):]}»" for x in excl) if excl else "")
        print(f"Папка: {title}", flush=True)
        yrel_scope = scope[len("/disk"):].strip("/")
        ytop = remote_join(ybase, yrel_scope) if yrel_scope else ybase
        filters = []
        for x in excl:
            if x.startswith(scope + "/"):
                filters += ["--exclude", "/" + cleanup.glob_escape(x[len(scope) + 1:]) + "/**"]
        log(f"читаю полный список на Яндексе ({ytop}) …")
        items = yandex_listing(ytop, filters)
        known = {p for (p,) in db.execute("SELECT path FROM files WHERE path=? OR substr(path, 1, length(?))=?",
                                          (scope, scope + "/", scope + "/"))}
        full = lambda rel: scope + "/" + rel                     # noqa: E731
        excluded = lambda p: any(p == x or p.startswith(x + "/") for x in excl)   # noqa: E731
        items = [it for it in items if not excluded(full(it[0]))]
        # исключённые папки, которых программа не знает, — сами папки, без содержимого: fetch с тем же --exclude
        # проверяет, что такие папки на Диске есть
        skipped_dirs = []
        for x in excl:
            if x in known or not x.startswith(scope + "/"):
                continue
            parent, _, name = x[len(scope) + 1:].rpartition("/")
            try:
                there = json.loads(rclone("lsjson", "--dirs-only", "--no-mimetype", "--no-modtime",
                                          remote_join(ytop, parent) if parent else ytop) or "[]")
            except Fail:
                there = []
            if any(it.get("Name") == name for it in there):
                skipped_dirs.append((x[len(scope) + 1:],))
        files = [it for it in items if not it[1]]
        new_files = [it for it in files if full(it[0]) not in known]
        new_dirs = [it for it in items if it[1] and full(it[0]) not in known]
        print(f"\nНа Яндексе в этой папке файлов: {len(files)} ({human(sum(it[2] or 0 for it in files))}), "
              f"папок: {len(items) - len(files)}")
        print(f"Программа о них не знает (обход их не увидел): {len(new_files)} файлов, "
              f"{human(sum(it[2] or 0 for it in new_files))}; папок: {len(new_dirs)}")
        where = lambda k: (f"{yrel_scope}/{k}" if yrel_scope else k) if k else (yrel_scope or "(прямо в корне)")  # noqa: E731
        groups = cleanup.by_folder([(it[0], it[2]) for it in new_files])
        if groups:
            print("По папкам (больше всего места вверху):")
            for k, (n, b) in groups[:SHOW]:
                print(f"  {n:7d} файлов  {human(b):>9}  {where(k)}")
            if len(groups) > SHOW:
                print(f"  … и ещё {len(groups) - SHOW} папок")
        if not new_files and not new_dirs:
            print("\nПрограмма знает всё, что есть на Яндексе в этой папке.")
            return 0
        excl_args = "".join(f' --exclude "{x[len("/disk/"):]}"' for x in excl)
        target = "/" if scope == "/disk" else scope[len("/disk/"):]
        os.makedirs(cleanup.REPORT_DIR, exist_ok=True)
        stamp = time.strftime("%Y-%m-%d_%H%M%S")
        report = os.path.join(cleanup.REPORT_DIR, f"{stamp}_нет_в_программе.txt")
        with open(report, "w", encoding="utf-8") as f:
            f.write(f"Папка: {title}\nНа Яндексе есть, программа не знает: {len(new_files)} файлов\n\n== по папкам\n")
            f.write("".join(f"{n:7d} файлов  {human(b):>9}  {where(k)}\n" for k, (n, b) in groups))
            f.write("\n== файлы\n" + "".join(f"{where(it[0])}\n" for it in sorted(new_files)))
        print(f"\nПолный список: {report}")

        if not a.add:
            yarg = "" if a.yandex in ("yandex", "yandex:") else f" --yandex {a.yandex}"
            print(f"\nДобавить их в программу (ничего не скачивается) — та же команда с --add:")
            print(f'  yandexdl missing "{target}"{excl_args}{yarg} --add')
            return 0

        log(f"добавляю в программу {len(new_files)} файлов и {len(new_dirs)} папок …")
        res = [{"path": full(rel), "type": "dir", "name": rel.rsplit("/", 1)[-1]}
               for rel, *_ in new_dirs + skipped_dirs]
        res += [{"path": full(rel), "type": "file", "name": rel.rsplit("/", 1)[-1], "mtime": mt,
                 "md5": md5, "meta": {"size": size, "mimetype": mime, "mediatype": mediatype(mime), "md5": md5},
                 "source": "rclone"} for rel, _, size, md5, mime, mt in new_files]
        added = 0
        for i in range(0, len(res), BATCH):
            r = fetch.api("/api/ingest-files", {"resources": res[i:i + BATCH]})
            added += r.get("new_files", 0)
            if i and i % (BATCH * 10) == 0:
                log(f"добавлено {min(i + BATCH, len(res))} из {len(res)}")
        # все папки из списка Яндекса прочитаны целиком — обходу в окне программы их открывать незачем
        ts = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
        dirs = [scope] + [full(it[0]) for it in items if it[1]]
        db.executemany("UPDATE files SET listed_at=COALESCE(listed_at, ?), crawled_at=COALESCE(crawled_at, ?) "
                       "WHERE path=? AND kind='dir'", [(ts, ts, d) for d in dirs])
        db.commit()
        print(f"\nДобавлено в программу: {added} файлов. Скачать их (и перенести в облако) — обычный fetch без обхода:")
        print(f'  yandexdl fetch "{target}"{excl_args} --no-crawl --rclone {a.rclone} --part-gb 2')
        return 0
    except (Fail, fetch.Fail) as e:
        print(f"Ошибка: {e}", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
