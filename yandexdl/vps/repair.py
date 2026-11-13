"""
Повтор повреждённых файлов: скачанные с дырами (нечитаемые места заполнены нулями) качаются заново и склеиваются
с версией, уже лежащей в облаке, — где дыра в одной копии, байты берутся из другой. Хуже не становится: дыра
остаётся только там, где её нет ни в старой, ни в новой копии; если Яндекс со временем стал отдавать файл целиком —
файл становится целым.

  repair.py ПАПКА|ССЫЛКА --rclone ОБЛАКО [--exclude ПАПКА]… [--jobs 2]

ПАПКА — как у fetch («/» — весь Диск, «Творчество», адрес из браузера), ССЫЛКА — общая папка disk.yandex.ru/d/…;
ОБЛАКО — то же, что в --rclone у fetch этой папки (gdrive:Yandex для своего Диска, gdrive:Библиотека для ссылки).
Заново качает программа — тем же загрузчиком со спасением по кускам (POST /api/fl/fetch-copy), мимо очереди;
облако — через rclone; база программы — напрямую. Прерванный запуск можно повторить: сначала берутся файлы,
которые давно не пробовали.
"""
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compare  # noqa: E402
import fetch  # noqa: E402
import public  # noqa: E402
from compare import BLOCK, Fail, human, log, lost_of, merge, rclone, rclone_try, remote_join, zero_blocks  # noqa: E402

WORK = os.path.join(compare.WORK, "повтор")
FETCH_TIMEOUT = 6 * 3600      # сек: спасение большого битого файла по кускам бывает долгим
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def fetch_copy(path, dest):
    """Программа качает файл заново в dest. Возвращает её ответ: {ok, note} или {ok: False, auth|limit|unreadable}."""
    req = urllib.request.Request(fetch.API + "/api/fl/fetch-copy", method="POST",
                                 data=json.dumps({"path": path, "dest": dest}).encode("utf-8"),
                                 headers={"Content-Type": "application/json"})
    try:
        with _OPENER.open(req, timeout=FETCH_TIMEOUT) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return {**json.loads(e.read() or b"{}"), "ok": False}
        except ValueError:
            return {"ok": False, "error": f"HTTP {e.code}"}
    except (OSError, ValueError) as e:
        return {"ok": False, "error": f"программа не ответила: {e}"}


def file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def disk_md5(raw):
    """md5 оригинала из того, что Диск сообщил о файле при обходе (если сообщил)."""
    try:
        r = json.loads(raw or "{}")
    except ValueError:
        return None
    m = r.get("md5") or (r.get("meta") or {}).get("md5")
    return m if isinstance(m, str) and len(m) == 32 else None


def repair_one(n, row, remote, key):
    """Один файл: скачать заново, склеить со старой версией из облака, положить лучшее. -> (итог, заметка)."""
    path, size, note, fl_file, raw, rel = row
    cloud = remote_join(remote, fl_file.split("/", 2)[2] if key else fl_file.split("/", 1)[1])
    new, old, m = (os.path.join(WORK, f"{n}.{x}") for x in ("новая", "старая", "склейка"))
    try:
        # Сначала — есть ли старая версия там, где её ищем: нет — значит, папку качали в другое облако (--rclone не тот),
        # и класть туда ничего нельзя: рядом выросла бы вторая копия папки.
        if rclone_try("lsjson", "--stat", cloud)[0]:
            return "nocloud", f"в облаке нет {cloud}"
        free = shutil.disk_usage(WORK).free
        if free < 3 * (size or 0) + 2 * 1024 ** 3:
            return "error", f"мало места на сервере для {human(size)} (свободно {human(free)})"
        r = fetch_copy(path, new)
        if not r.get("ok"):
            kind = "auth" if r.get("auth") else "limit" if r.get("limit") else \
                "unreadable" if r.get("unreadable") else "error"
            return kind, r.get("error") or "без объяснения"
        want = compare.original_md5(key, rel) if key else disk_md5(raw)
        if r.get("note") is None and (not want or file_md5(new) == want):      # Яндекс отдал файл целиком
            rclone("copyto", new, cloud)
            return "whole", "восстановлен при повторе: скачан целиком" + (" (md5 совпал с оригиналом)" if want else "")
        rclone("copyto", cloud, old)
        got, conflicts = merge(new, old, m)
        if conflicts:
            return "error", f"новая и старая копии различаются в {conflicts} местах — это разные версии, не склеиваю"
        before, after = zero_blocks(old), zero_blocks(m)
        if want and got == want:
            rclone("copyto", m, cloud)
            return "whole", "восстановлен при повторе: склеен из старой и новой копий, md5 совпал с оригиналом"
        if after < before:
            rclone("copyto", m, cloud)
            if after == 0 and not want:
                return "whole", "восстановлен при повторе: дыр не осталось (сверить не с чем — md5 Диск не сообщил)"
            return "better", (f"повреждён: потеряно {after * BLOCK} из {size} байт (примерно) — дыр стало меньше "
                              f"при повторе, было {lost_of(note) or before * BLOCK}")
        return "same", None
    except Fail as e:
        return "error", str(e)
    finally:
        for x in (new, old, m):
            if os.path.exists(x):
                os.remove(x)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="папка Диска («/» — весь) или общая ссылка")
    ap.add_argument("--rclone", required=True, metavar="ОБЛАКО:ПАПКА", help="куда эту папку качал fetch")
    ap.add_argument("--exclude", action="append", default=[], metavar="ПАПКА")
    ap.add_argument("--jobs", type=int, default=2, help="сколько файлов качать заново одновременно")
    a = ap.parse_args()
    try:
        db = sqlite3.connect(compare.DB, timeout=60)
        link = public.parse_link(a.target)
        key = link[0] if link else None
        if link:
            root = f"/public/{public.public_id(key)}"
            scope = root + ("" if link[1] == "/" else link[1].rstrip("/"))
            excl = [root + "/" + x.strip("/") for x in a.exclude]
        else:
            scope = "/disk" if a.target.strip().strip("/") in ("", "disk") else compare.disk_folder(db, a.target)
            excl = [compare.disk_folder(db, x) for x in a.exclude]
        q = ("SELECT path, size, fl_note, fl_file, raw FROM files WHERE kind='file' AND fl_status='done' "
             "AND fl_note LIKE 'повреждён%' AND fl_file IS NOT NULL AND (path=? OR substr(path, 1, length(?))=?) "
             "ORDER BY fl_at, path")
        rows = [(p, s, nt, f, rw, p[len(root):] if key else p)
                for p, s, nt, f, rw in db.execute(q, (scope, scope + "/", scope + "/"))
                if not any(p == x or p.startswith(x + "/") for x in excl)]
        lost = sum(lost_of(r[2]) for r in rows)
        log(f"повреждённых файлов: {len(rows)}, в них не хватает {human(lost)} — качаю заново и склеиваю "
            f"с тем, что в {a.rclone}, по {a.jobs} сразу")
        if not rows:
            return 0
        os.makedirs(WORK, exist_ok=True)
        done = {"whole": 0, "better": 0, "same": 0, "unreadable": 0, "error": 0, "nocloud": 0}
        nocloud = []
        pending, running, auth_tries, stop, t, n = deque(rows), {}, 0, None, time.time(), 0
        with ThreadPoolExecutor(a.jobs) as ex:
            while (pending and not stop) or running:
                while pending and not stop and len(running) < a.jobs:
                    n += 1
                    row = pending.popleft()
                    running[ex.submit(repair_one, n, row, a.rclone, key)] = row
                finished, _ = wait(running, timeout=60, return_when=FIRST_COMPLETED)
                auth_hit = False
                for f in finished:
                    row = running.pop(f)
                    kind, note = f.result()
                    if kind == "auth":
                        pending.appendleft(row)
                        auth_hit = True
                        continue
                    if kind == "limit":
                        pending.appendleft(row)
                        stop = f"{note}. Повтори ту же команду через сутки — начнёт с непроверенных"
                        continue
                    done[kind] += 1
                    auth_tries = 0
                    if kind == "error":
                        log(f"(!) {row[0]}: {note}")
                    if kind == "nocloud":
                        nocloud.append(note)
                        if len(nocloud) <= 3:
                            log(f"(!) {note} — этот файл пропускаю; папку, видно, качали в другое облако")
                    ts = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
                    if kind in ("whole", "better"):
                        db.execute("UPDATE files SET fl_note=?, fl_at=? WHERE path=?", (note, ts, row[0]))
                    else:                                      # попробовали — в следующий раз этот файл в конце очереди
                        db.execute("UPDATE files SET fl_at=? WHERE path=?", (ts, row[0]))
                    db.commit()
                if auth_hit and not running:
                    auth_tries += 1
                    if auth_tries > fetch.AUTH_RESTARTS or not fetch.run_restart():
                        stop = ("Яндекс не принимает вход, перезапуск программы не помог: войди заново через "
                                "«yandexdl vnc» и повтори ту же команду")
                    else:
                        log("(!) Яндекс не принял вход — перезапустил программу, жду свежий ключ")
                        fetch.fresh_auth()
                if time.time() - t > 60:
                    t = time.time()
                    log(f"проверено {sum(done.values())} из {len(rows)}: стали целыми {done['whole']}, дыр стало меньше "
                        f"{done['better']}, без изменений {done['same']}, не читается сейчас {done['unreadable']}")
        print()
        print(f"Стали целыми:              {done['whole']}")
        print(f"Дыр стало меньше:          {done['better']}")
        print(f"Без изменений:             {done['same']}")
        print(f"Сейчас не читается вовсе:  {done['unreadable']} (сохранённая версия осталась как была)")
        if done["error"]:
            print(f"Не вышло (см. выше):       {done['error']}")
        if nocloud:
            print(f"Нет в облаке по этому пути: {len(nocloud)} — их папку fetch качал в другое облако: повтори для неё "
                  f"с тем --rclone, что был у fetch (например: {nocloud[0]})")
        if pending:
            print(f"Не проверены:              {len(pending)}")
        if stop:
            raise Fail(stop)
        return 0
    except Fail as e:
        print(f"Ошибка: {e}", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
