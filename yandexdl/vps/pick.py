"""Оставить в папке Диска только выбранные подпапки: показать список, спросить номера, остальное пропустить,
обойти выбранное. Потом: yandexdl fetch "<папка>" --no-crawl --rclone gdrive:Yandex"""
import json, os, sqlite3, sys, time, urllib.error, urllib.request
API = "http://127.0.0.1:8001"
DB = os.environ.get("YDISK_DB", "/var/lib/yandexdl/yandexdl-desktop/gallery.db")
op = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def api(path, body=None):
    req = urllib.request.Request(API + path, method="POST" if body is not None else "GET",
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"} if body is not None else {})
    try:
        with op.open(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read() or b"{}")

def inside(p):
    return "(path = ? OR substr(path, 1, ?) = ?)", (p, len(p) + 1, p + "/")

folder = sys.argv[1] if len(sys.argv) > 1 else "ВЕРОНИКА_YandexDisc"
api("/api/fl/stop", {})
api("/api/fl/crawl", {"run": False})
while api("/api/fl/status")["state"] != "idle":
    time.sleep(1)
sc = api("/api/fl/scope", {"folder": folder})
if not sc.get("ok"):
    sys.exit("Папка «%s»: %s" % (folder, sc.get("error")))
scope = sc["scope"]
print("Папка на Диске:", scope, flush=True)
db = sqlite3.connect(DB, timeout=60)
kids = lambda: db.execute("SELECT path, kind FROM files WHERE parent = ? ORDER BY kind DESC, name", (scope,)).fetchall()

if not kids():
    db.execute("UPDATE files SET crawled_at = NULL WHERE path = ?", (scope,))
    db.commit()
    api("/api/fl/crawl", {"run": True})
    print("Открываю папку на Диске, жду список (до 5 минут)…", flush=True)
    t0 = time.time()
    while True:
        time.sleep(3)
        cr = api("/api/fl/crawl")
        if cr["visited"] >= 1 or not cr["run"] or time.time() - t0 > 300:
            break
    api("/api/fl/crawl", {"run": False})
    if (cr.get("message") or "").startswith("остановлен"):
        sys.exit("Обход: " + cr["message"])

dirs = [p for p, k in kids() if k == "dir"]
files = [p for p, k in kids() if k == "file"]
if not dirs:
    sys.exit("Подпапок не видно. Проверь название или передай адрес папки из браузера: python3 %s \"адрес\"" % sys.argv[0])
print()
for i, p in enumerate(dirs, 1):
    print("%4d  %s" % (i, p[len(scope) + 1:]))
if files:
    print("      (+ %d файлов прямо в папке — их пропущу)" % len(files))
ans = input("\nНомера нужных папок через пробел (можно 3-7): ")
keep = set()
for tok in ans.replace(",", " ").split():
    a, _, b = tok.partition("-")
    if a.isdigit() and (not b or b.isdigit()):
        for n in range(int(a), int(b or a) + 1):
            if 1 <= n <= len(dirs):
                keep.add(dirs[n - 1])
if not keep:
    sys.exit("Ничего не выбрано — ничего не меняю.")

ts = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
for d in dirs:
    if d in keep:
        continue
    cond, args = inside(d)
    db.execute("UPDATE files SET crawled_at = COALESCE(crawled_at, ?) WHERE kind = 'dir' AND " + cond, (ts,) + args)
    db.execute("UPDATE files SET fl_status = 'skipped' WHERE kind = 'file' AND fl_status IS NULL AND " + cond, args)
db.execute("UPDATE files SET fl_status = 'skipped' WHERE kind = 'file' AND fl_status IS NULL AND parent = ?", (scope,))
db.commit()
print("\nБеру %d папок:" % len(keep))
for d in dirs:
    if d in keep:
        print("   ", d[len(scope) + 1:])
print("Остальные %d папок и файлы прямо в папке пропущены.\n" % (len(dirs) - len(keep)), flush=True)

api("/api/fl/crawl", {"run": True})
print("Обхожу выбранные папки (можно закрыть SSH — обход идёт в программе; ход: yandexdl status)", flush=True)
last = 0
while True:
    time.sleep(5)
    st = api("/api/fl/status")
    cr = st["crawl"]
    if not cr["run"]:
        break
    if time.time() - last > 30:
        last = time.time()
        cur = cr["current"][len(scope) + 1:] if cr["current"] else "—"
        print("%s  папок пройдено %d, к скачиванию файлов %d; сейчас: %s" % (time.strftime("%H:%M:%S"), cr["visited"],
              st["counts"]["remaining"], cur), flush=True)
c = st["counts"]
print("\nОбход: %s" % (cr.get("message") or "закончен"))
print("К скачиванию: %d файлов, %.2f ГБ" % (c["remaining"], c["remaining_bytes"] / 2**30))
print('Дальше: yandexdl fetch "%s" --no-crawl --rclone gdrive:Yandex' % folder)
