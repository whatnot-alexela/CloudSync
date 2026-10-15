"""
Скачать одну папку Диска через API работающей программы (127.0.0.1:8001), без окна:
выбрать папку → включить «качать и фото/видео из папок» → обход → скачивание → отчёт.
С --rclone скачанное частями переносится в облако, если всё сразу на сервер не помещается.

    python fetch.py --folder Творчество --dest /root/yandex [--dump-dir DIR] [--threads N]
                    [--no-crawl] [--retry-failed] [--rclone gdrive:Яндекс [--reserve-gb 2] [--part-gb 5]]
    python fetch.py --status

Только stdlib. Запускает yandexdl-vps.sh fetch / status.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("YDISK_API", "http://127.0.0.1:8001")
FILES_SUBDIR = "Диск"
GB = 1024 ** 3
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


def start_download():
    res = api("/api/fl/start", {}, ok_codes=(200, 409))
    if not res.get("ok") and "уже запущено" not in (res.get("error") or ""):
        raise Fail(f"скачивание не запустилось: {res.get('error')}")


def download_round(mover=None):
    start_download()
    last_print = 0
    while True:
        time.sleep(5)
        st = api("/api/fl/status")
        c = st["counts"]
        if st["state"] == "idle":
            log(f"скачивание остановилось: {st.get('message') or '—'}")
            return st
        why = mover.need(st) if mover else None
        if why:
            log(f"пауза: {why} — переношу скачанное в {mover.target(st)}")
            api("/api/fl/stop", {})
            wait_idle(600)
            mover.move(api("/api/fl/status"))
            start_download()
            continue
        if time.time() - last_print > 30:
            last_print = time.time()
            eta = ""
            if st["speed"] > 0 and c["remaining_bytes"]:
                eta = f", осталось ~{int(c['remaining_bytes'] / st['speed'] / 60) + 1} мин"
            log(f"скачано {c['done']} ({human(c['done_bytes'])}), ошибок {c['failed']}, "
                f"осталось {c['remaining'] + c['queued'] + c['downloading']} ({human(c['remaining_bytes'])}), "
                f"скорость {human(st['speed'])}/с{eta}")


def free_bytes(path):
    """Свободное место на диске, где лежит path (самой папки может ещё не быть)."""
    while not os.path.isdir(path) and os.path.dirname(path) != path:
        path = os.path.dirname(path)
    return shutil.disk_usage(path).free


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


class Mover:
    """
    Перенос скачанного на облако через rclone move — частями: загрузка встаёт на паузу, когда на сервере
    остаётся меньше reserve свободного места (или скачана часть в part байт), скачанное уезжает в облако,
    загрузка продолжается. Перенесённые файлы программа не качает повторно: в её базе они уже done.
    """

    def __init__(self, remote, reserve_gb, part_gb):
        self.remote = remote
        self.reserve = int(reserve_gb * GB)
        self.part = int(part_gb * GB) if part_gb else None
        self.mark = None      # done_bytes на момент последнего переноса
        self.moved = 0        # сколько раз переносили

    def check(self):
        if ":" not in self.remote:
            raise Fail("облако указывается как имя:папка, например gdrive:Яндекс")
        if not shutil.which("rclone"):
            raise Fail("не установлен rclone: curl https://rclone.org/install.sh | bash")
        name = self.remote.split(":", 1)[0] + ":"
        have = subprocess.run(["rclone", "listremotes"], capture_output=True, text=True).stdout.split()
        if name not in have:
            raise Fail(f"в rclone нет облака {name} (есть: {', '.join(have) or 'никаких'}) — настрой его: rclone config")

    def target(self, st):
        rel = os.path.relpath(local_folder(st), st["files_dir"]).replace(os.sep, "/")
        base = self.remote if self.remote.endswith(":") else self.remote.rstrip("/") + "/"
        return base + ("" if rel == "." else rel)

    def need(self, st):
        done = st["counts"]["done_bytes"]
        if self.mark is None:
            self.mark = done
        free = free_bytes(st["files_dir"])
        if free < self.reserve:
            return f"на сервере осталось {human(free)}"
        if self.part and done - self.mark >= self.part:
            return f"скачана очередная часть ({human(done - self.mark)})"
        return None

    def move(self, st):
        src = local_folder(st)
        n, size = du(src) if os.path.isdir(src) else (0, 0)
        if n:
            dst = self.target(st)
            log(f"rclone: переношу {n} файлов ({human(size)}) в {dst}")
            r = subprocess.run(["rclone", "move", src, dst, "--delete-empty-src-dirs", "--exclude", "*.part",
                                "--transfers", "4", "--stats", "60s", "--stats-one-line",
                                "--stats-log-level", "NOTICE"])
            left, _ = du(src) if os.path.isdir(src) else (0, 0)
            if r.returncode != 0 or left:
                raise Fail(f"rclone перенёс не всё (код {r.returncode}, на сервере осталось {left} файлов) — "
                           "смотри его сообщения выше; потом повтори fetch с --no-crawl")
            self.moved += 1
            log(f"rclone: перенесено, файлов {n}, {human(size)}")
        self.mark = st["counts"]["done_bytes"]
        free = free_bytes(st["files_dir"])
        if free < self.reserve:
            raise Fail(f"после переноса на сервере свободно только {human(free)} — освободи место и повтори")

    def remote_size(self, st):
        r = subprocess.run(["rclone", "size", self.target(st), "--json"], capture_output=True, text=True)
        try:
            return json.loads(r.stdout)
        except ValueError:
            return None


REPORT_NAME = "_повреждённые_файлы.txt"


def damage_report(st):
    """Список повреждённых (скачаны с дырами) и нечитаемых файлов — в файл рядом со скачанным
    (уедет в облако вместе с ними). Возвращает (повреждённых, потеряно байт, нечитаемых)."""
    items = api("/api/fl/damaged")["items"]
    damaged = [x for x in items if x["fl_status"] == "done"]
    failed = [x for x in items if x["fl_status"] == "failed"]
    lost = 0
    for x in damaged:
        try:
            lost += int(x["fl_note"].split("потеряно ", 1)[1].split(" ", 1)[0])
        except (IndexError, ValueError):
            pass
    if not items:
        return 0, 0, 0
    folder = local_folder(st)
    os.makedirs(folder, exist_ok=True)
    root = st["scope"].rstrip("/") + "/"
    with open(os.path.join(folder, REPORT_NAME), "w", encoding="utf-8") as f:
        f.write(f"Папка на Диске: {st['scope']}\nСоставлено: {time.strftime('%Y-%m-%d %H:%M')}\n\n"
                "Файлы, повреждённые на стороне Яндекс Диска. Скачано всё, что читалось; нечитаемые места\n"
                "заполнены нулями, смещения внутри файла сохранены (аудио обычно играется с паузами на месте дыр).\n\n")
        f.write(f"ПОВРЕЖДЕНЫ — {len(damaged)} файлов, потеряно {human(lost)}:\n")
        for x in damaged:
            f.write(f"  {x['path'][len(root):]}\n      {x['fl_note']}\n")
        f.write(f"\nНЕ ЧИТАЮТСЯ (не скачаны) — {len(failed)} файлов:\n")
        for x in failed:
            f.write(f"  {x['path'][len(root):]}  ({human(x['size'])})\n      {(x['fl_error'] or '')[:300]}\n")
    return len(damaged), lost, len(failed)


def report(st, mover=None, dmg=(0, 0, 0)):
    c = st["counts"]
    folder = local_folder(st)
    n, size = du(folder)
    print()
    print("=" * 70)
    print(f"Папка на Диске:  {st['scope']}")
    print(f"Найдено файлов:  {c['files']} (из них фото/видео {c['media']}), папок {c['dirs']}")
    print(f"Скачано:         {c['done']} файлов, {human(c['done_bytes'])}")
    if dmg[0]:
        print(f"  из них повреждены на Диске: {dmg[0]} (потеряно {human(dmg[1])}, дыры заполнены нулями)")
    print(f"Ошибок:          {c['failed']}" + (f" (из них не читаются совсем: {dmg[2]})" if dmg[2] else ""))
    if dmg[0] or dmg[2]:
        print(f"Список повреждённых: {REPORT_NAME} в папке со скачанным")
    print(f"На диске сервера: {folder}")
    print(f"                 {n} файлов, {human(size)} ({size} байт)")
    if mover:
        rs = mover.remote_size(st)
        print(f"В облаке:        {mover.target(st)}")
        print(f"                 " + (f"{rs['count']} файлов, {human(rs['bytes'])} ({rs['bytes']} байт)" if rs
                                      else "не удалось посчитать (rclone size)"))
    if c["failed"]:
        print("Не скачались (последние):")
        for x in st["recent_failed"][:15]:
            print(f"  {x['path']}: {(x.get('error') or '')[:150]}")
    print("=" * 70)


def run(folder, dest, dump_dir, no_crawl=False, mover=None, threads=None, retry_failed=False):
    if mover:
        mover.check()
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
    if retry_failed:
        log(f"файлы с ошибками — в очередь заново: {api('/api/fl/retry-failed', {}).get('reset')}")
    settings = {"include_media": True}
    if threads:
        settings["threads"] = threads
    fs = api("/api/fl/settings", settings)["settings"]
    log(f"папка на Диске: {sc['scope']}; качать в {d['dir']}; фото/видео из папок: да; потоков: {fs['threads']}"
        + (f"; переносить в {mover.remote}" if mover else ""))

    if no_crawl:
        log("обход пропускаю: беру список файлов, собранный раньше")
    else:
        msg = crawl()
        if msg.startswith("остановлен"):
            raise Fail("обход прервался: " + msg + "\nВойди заново через «yandexdl vnc» и повтори «yandexdl fetch»")
    st = api("/api/fl/status")
    c = st["counts"]
    log(f"найдено файлов {c['files']} (фото/видео {c['media']}), папок {c['dirs']}; "
        f"к скачиванию {c['remaining']} файлов, {human(c['remaining_bytes'])}")
    if not c["files"] and no_crawl:
        raise Fail(f"в собранном списке нет файлов в «{sc['scope']}» — проверь название или запусти без --no-crawl")
    if not c["files"]:
        print("\nФайлов не найдено: похоже, Диск отдаёт список папки в непредусмотренном виде.")
        print("Ниже — разбор перехваченных ответов (его и нужно прислать, чтобы поправить find_disk_resources):\n")
        if dump_dir:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            import diagnose
            diagnose.main([dump_dir])
        return 2

    free = free_bytes(st["files_dir"])
    if mover:
        if free < mover.reserve + 500 * 1024 * 1024:
            raise Fail(f"на сервере свободно {human(free)} — меньше запаса {human(mover.reserve)} + 500 МБ")
        if c["remaining_bytes"] > free - mover.reserve:
            log(f"всё сразу не поместится ({human(c['remaining_bytes'])}, свободно {human(free)}) — "
                f"качаю частями, каждую переношу в облако")
        mover.move(st)            # то, что уже лежит на сервере от прошлых запусков, — сразу в облако
    elif c["remaining_bytes"] > free - 200 * 1024 * 1024:
        raise Fail(f"не хватит места: нужно {human(c['remaining_bytes'])}, свободно {human(free)}. "
                   "Можно качать частями с переносом в облако: --rclone имя:папка")

    st = download_round(mover)
    dmg = damage_report(st)
    if mover:
        mover.move(st)
    report(st, mover, dmg)
    if (st.get("message") or "").startswith("Остановился"):
        raise Fail("скачивание прервалось из-за входа в Яндекс: войди заново через «yandexdl vnc», "
                   "открой Диск и повтори «yandexdl fetch … --no-crawl» — уже скачанное не перекачается")
    if st["counts"]["failed"]:
        print("Повторить файлы с ошибками: та же команда с --no-crawl --retry-failed")
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
    p.add_argument("--no-crawl", action="store_true", help="не обходить папки, взять список, собранный раньше")
    p.add_argument("--rclone", metavar="ИМЯ:ПАПКА", help="переносить скачанное в облако (rclone move) частями")
    p.add_argument("--reserve-gb", type=float, default=2.0, help="с --rclone: сколько места оставлять свободным")
    p.add_argument("--part-gb", type=float, help="с --rclone: переносить каждые столько скачанных ГБ")
    p.add_argument("--threads", type=int, help="сколько файлов качать одновременно (1–8)")
    p.add_argument("--retry-failed", action="store_true", help="ещё раз попробовать файлы, которые не скачались")
    a = p.parse_args()
    try:
        if a.status:
            status()
            return 0
        if not a.folder:
            p.error("--folder обязателен")
        mover = Mover(a.rclone, a.reserve_gb, a.part_gb) if a.rclone else None
        return run(a.folder, a.dest, a.dump_dir, no_crawl=a.no_crawl, mover=mover, threads=a.threads,
                   retry_failed=a.retry_failed)
    except Fail as e:
        print(f"\nОшибка: {e}", flush=True)
        return 3
    except urllib.error.URLError as e:
        print(f"\nОшибка: API программы не отвечает ({e.reason}) — «yandexdl start»", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
