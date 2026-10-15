"""
Скачать одну папку Диска через API работающей программы (127.0.0.1:8001), без окна:
выбрать папку → включить «качать и фото/видео из папок» → обход → скачивание → отчёт.

    python fetch.py --folder Творчество --dest /root/yandex [--dump-dir DIR]
    python fetch.py --status

Только stdlib. Запускает yandexdl-vps.sh fetch / status.
"""
import argparse
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("YDISK_API", "http://127.0.0.1:8001")
FILES_SUBDIR = "Диск"
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class Fail(Exception):
    pass


def api(path, body=None, ok_codes=(200,)):
    data, headers = None, {}
    if body is not None:
        data, headers = json.dumps(body).encode("utf-8"), {"Content-Type": "application/json"}
    req = urllib.request.Request(API + path, data=data, headers=headers, method="POST" if body is not None else "GET")
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        payload = json.loads(e.read() or b"{}")
        if e.code in ok_codes:
            return payload
        raise Fail(f"{path}: HTTP {e.code} {payload.get('error') or payload}")


def human(n):
    n = float(n or 0)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if n < 1024 or unit == "ТБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def wait_idle(timeout=120):
    t0 = time.time()
    while api("/api/fl/status")["state"] != "idle":
        if time.time() - t0 > timeout:
            raise Fail("загрузка файлов не останавливается")
        time.sleep(1)


def check_auth():
    a = api("/api/auth/status")
    if not a.get("cookies") or not a.get("has_sk"):
        raise Fail("программа ещё не видела входа в Яндекс. Выполни «yandexdl vnc», войди в Яндекс во вкладке "
                   "«Яндекс Диск», дождись открытия Диска (disk.yandex.ru/client/disk) и повтори")
    if not a.get("has_session"):
        log("(!) в куках нет Session_id — похоже, вход не завершён; продолжаю, но скачивание может не пойти")
    return a


def crawl():
    api("/api/fl/crawl", {"run": True, "reset": True})
    log("обход папок начат")
    t0 = last_print = time.time()
    last_current = None
    agent_warned = False
    while True:
        time.sleep(3)
        st = api("/api/fl/status")
        cr, c = st["crawl"], st["counts"]
        if not cr["agent"] and time.time() - t0 > 15 and not agent_warned:
            log("(!) окно программы не отзывается на обход — проверь «yandexdl logs»")
            agent_warned = True
        if not cr["run"]:
            log(f"обход закончен: {cr.get('message') or '—'}")
            return cr.get("message") or ""
        if cr["current"] != last_current or time.time() - last_print > 30:
            last_current, last_print = cr["current"], time.time()
            log(f"папок пройдено {cr['visited']}, найдено файлов {c['files']}, папок {c['dirs']}; "
                f"сейчас: {cr['current'] or '—'}")


def download_round():
    res = api("/api/fl/start", {}, ok_codes=(200, 409))
    if not res.get("ok") and "уже запущено" not in (res.get("error") or ""):
        raise Fail(f"скачивание не запустилось: {res.get('error')}")
    last_print = 0
    while True:
        time.sleep(5)
        st = api("/api/fl/status")
        c, s = st["counts"], st["session"]
        if st["state"] == "idle":
            log(f"скачивание остановилось: {st.get('message') or '—'}")
            return st
        if time.time() - last_print > 30:
            last_print = time.time()
            eta = ""
            if st["speed"] > 0 and c["remaining_bytes"]:
                eta = f", осталось ~{int(c['remaining_bytes'] / st['speed'] / 60) + 1} мин"
            log(f"скачано {c['done']} ({human(c['done_bytes'])}), ошибок {c['failed']}, "
                f"осталось {c['remaining'] + c['queued'] + c['downloading']} ({human(c['remaining_bytes'])}), "
                f"скорость {human(st['speed'])}/с{eta}")


def local_folder(st):
    rel = st["scope"][len("/disk"):].strip("/")
    return os.path.join(st["files_dir"], *rel.split("/")) if rel else st["files_dir"]


def du(path):
    n = size = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            if f.endswith(".part"):
                continue
            try:
                size += os.path.getsize(os.path.join(root, f))
                n += 1
            except OSError:
                pass
    return n, size


def report(st):
    c = st["counts"]
    folder = local_folder(st)
    n, size = du(folder)
    print()
    print("=" * 70)
    print(f"Папка на Диске:  {st['scope']}")
    print(f"Найдено файлов:  {c['files']} (из них фото/видео {c['media']}), папок {c['dirs']}")
    print(f"Скачано:         {c['done']} файлов, {human(c['done_bytes'])}")
    print(f"Ошибок:          {c['failed']}")
    print(f"На диске сервера: {folder}")
    print(f"                 {n} файлов, {human(size)} ({size} байт)")
    if c["failed"]:
        print("Не скачались (последние):")
        for x in st["recent_failed"][:15]:
            print(f"  {x['path']}: {(x.get('error') or '')[:150]}")
    print("=" * 70)


def run(folder, dest, dump_dir):
    check_auth()
    api("/api/fl/stop", {})
    api("/api/fl/crawl", {"run": False})
    wait_idle()
    d = api("/api/settings/files-dir", {"dir": dest}, ok_codes=(200, 400))
    if not d.get("ok"):
        raise Fail(f"папка для загрузки {dest}: {d.get('error')}")
    sc = api("/api/fl/scope", {"folder": folder}, ok_codes=(200, 400))
    if not sc.get("ok"):
        raise Fail(f"папка на Диске «{folder}»: {sc.get('error')}")
    api("/api/fl/settings", {"include_media": True})
    log(f"папка на Диске: {sc['scope']}; качать в {d['dir']}; фото/видео из папок: да")

    msg = crawl()
    st = api("/api/fl/status")
    c = st["counts"]
    if msg.startswith("остановлен"):
        raise Fail("обход прервался: " + msg + "\nВойди заново через «yandexdl vnc» и повтори «yandexdl fetch»")
    log(f"найдено файлов {c['files']} (фото/видео {c['media']}), папок {c['dirs']}; "
        f"к скачиванию {c['remaining']} файлов, {human(c['remaining_bytes'])}")
    if not c["files"]:
        print("\nФайлов не найдено: похоже, Диск отдаёт список папки в непредусмотренном виде.")
        print("Ниже — разбор перехваченных ответов (его и нужно прислать, чтобы поправить find_disk_resources):\n")
        if dump_dir:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            import diagnose
            diagnose.main([dump_dir])
        return 2

    free = shutil.disk_usage(st["files_dir"] if os.path.isdir(st["files_dir"]) else dest).free
    if c["remaining_bytes"] > free - 200 * 1024 * 1024:
        raise Fail(f"не хватит места: нужно {human(c['remaining_bytes'])}, свободно {human(free)}")

    for rnd in (1, 2):
        st = download_round()
        if (st.get("message") or "").startswith("Остановился"):
            report(st)
            raise Fail("скачивание прервалось из-за входа в Яндекс: войди заново через «yandexdl vnc», "
                       "открой Диск и повтори «yandexdl fetch» — уже скачанное не перекачается")
        if not st["counts"]["failed"] or rnd == 2:
            break
        n = api("/api/fl/retry-failed", {}).get("reset")
        log(f"повторяю {n} файлов с ошибками")
    report(st)
    return 0 if not st["counts"]["failed"] else 1


def status():
    st = api("/api/fl/status")
    c, cr, a = st["counts"], st["crawl"], st["auth"]
    print(f"Вход в Яндекс: кук {a['cookies']}, sk {'есть' if a['has_sk'] else 'нет'}, "
          f"Session_id {'есть' if a['has_session'] else 'нет'}")
    print(f"Папка: {st['scope']}  →  {local_folder(st)}")
    print(f"Обход: {'идёт, сейчас ' + str(cr['current']) if cr['run'] else 'не идёт'}"
          f"{' — ' + cr['message'] if cr.get('message') else ''}")
    print(f"Найдено: файлов {c['files']} (фото/видео {c['media']}), папок {c['dirs']}")
    print(f"Скачивание: {st['state']}{' — ' + st['message'] if st.get('message') else ''}; "
          f"скачано {c['done']} ({human(c['done_bytes'])}), ошибок {c['failed']}, "
          f"осталось {c['remaining'] + c['queued'] + c['downloading']} ({human(c['remaining_bytes'])})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--folder")
    p.add_argument("--dest", default="/root/yandex")
    p.add_argument("--dump-dir")
    p.add_argument("--status", action="store_true")
    a = p.parse_args()
    try:
        if a.status:
            status()
            return 0
        if not a.folder:
            p.error("--folder обязателен")
        return run(a.folder, a.dest, a.dump_dir)
    except Fail as e:
        print(f"\nОшибка: {e}", flush=True)
        return 3
    except urllib.error.URLError as e:
        print(f"\nОшибка: API программы не отвечает ({e.reason}) — «yandexdl start»", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
