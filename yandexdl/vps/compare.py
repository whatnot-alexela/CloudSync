"""
Сверка общей папки по ссылке со своей копией той же папки на Диске (например, сохранённой к себе
в «Загрузки/Библиотека иностранной литературы»), когда обе повреждены, но по-разному.

  compare.py ССЫЛКА ПАПКА
      Отчёт: какие файлы в общей копии повреждены или не скачались и что с ними в своей копии. Заодно готовит
      скачивание из своей копии только этих файлов (их строки — в базу программы, остальное в своей папке помечается
      пропущенным, обходить её не нужно). Потом: yandexdl fetch ПАПКА --no-crawl --rclone ОБЛАКО_СВОЕГО_ДИСКА.
  compare.py ССЫЛКА ПАПКА --merge КУДА ОТКУДА
      Для каждого такого файла кладёт в облако общей копии (КУДА — как в --rclone её fetch, например
      gdrive:Библиотека) лучшее, что есть: целый файл из своей копии (ОТКУДА — --rclone своего Диска, например
      gdrive:Yandex) или склейку двух повреждённых — дыры заполнены нулями, недостающее берётся из другой копии.
      Результат сверяется с md5 оригинала из открытого API Диска; в базе программы файл отмечается заново.

Базу программы читает и пишет напрямую (как pick.py), облако — через rclone. Только stdlib.
"""
import argparse
import hashlib
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import public  # noqa: E402

DB = os.environ.get("YDISK_DB", "/var/lib/yandexdl/yandexdl-desktop/gallery.db")
WORK = os.environ.get("YDISK_COMPARE_DIR", "/root/yandex/.сверка")
REPORT_NAME = "_сверка_со_своей_копией.txt"
BLOCK = 4096          # склейка и подсчёт дыр — такими кусками


class Fail(Exception):
    pass


def human(n):
    n = float(n or 0)
    for unit in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if n < 1024 or unit == "ТБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def disk_folder(db, value):
    """«Загрузки/Библиотека», «/disk/…» или адрес из браузера https://disk.yandex.ru/client/disk/… -> «/disk/…»
    (регистр букв — как у папки, которую программа уже видела)."""
    s = (value or "").strip()
    if "://" in s:
        p = unquote(urlparse(s).path)
        if not p.startswith("/client/disk/"):
            raise Fail("это не адрес папки своего Диска (нужен вида https://disk.yandex.ru/client/disk/Папка)")
        s = p[len("/client/disk"):]
    s = s[len("disk:"):] if s.startswith("disk:") else s
    parts = [x.strip() for x in s.strip().strip("/").split("/") if x.strip()]
    if parts[:1] == ["disk"]:
        parts = parts[1:]
    if not parts or any(x in (".", "..") for x in parts):
        raise Fail(f"«{value}»: нужна папка на Диске")
    folder = "/disk/" + "/".join(parts)
    low = [r[0] for r in db.execute("SELECT path FROM files WHERE kind='dir' AND lower(path)=lower(?)", (folder,))]
    return folder if folder in low or len(low) != 1 else low[0]


def kind(status, note):
    if status == "done":
        return "damaged" if (note or "").startswith("повреждён") else "ok"
    return "failed" if status == "failed" else "pending"


def lost_of(note):
    try:
        return int(note.split("потеряно ", 1)[1].split(" ", 1)[0])
    except (AttributeError, IndexError, ValueError):
        return 0


def rows_under(db, root):
    """{путь от root: (статус, заметка, размер, fl_file)} — файлы внутри root."""
    inside = root + "/"
    return {p[len(root):]: (st, note, size, f) for p, st, note, size, f in db.execute(
        "SELECT path, fl_status, fl_note, size, fl_file FROM files WHERE kind='file' "
        "AND substr(path, 1, length(?)) = ?", (inside, inside))}


def remote_join(remote, rel):
    return (remote if remote.endswith(":") else remote.rstrip("/") + "/") + rel.strip("/")


def rclone(*args):
    r = subprocess.run(["rclone", *args], capture_output=True, text=True)
    if r.returncode:
        raise Fail(f"rclone {args[0]}: {(r.stderr or r.stdout).strip()[-300:]}")
    return r.stdout


def cloud_md5(path):
    try:
        out = rclone("md5sum", path).split()
    except Fail:
        return None
    return out[0] if out and len(out[0]) == 32 else None


def original_md5(key, rel):
    try:
        return public.yapi(public_key=key, path=rel, fields="md5").get("md5")
    except public.Fail:
        return None


def zero_blocks(path):
    """Сколько кусков по BLOCK байт в файле целиком из нулей — так видно дыры (чужие нули попадаются редко)."""
    n, zero = 0, bytes(BLOCK)
    with open(path, "rb") as f:
        while True:
            b = f.read(BLOCK)
            if not b:
                return n
            n += b == zero[:len(b)]


def merge(a, b, out):
    """Склеить две копии одного файла: где в одной нули (дыра), берём байты другой. Возвращает (md5 склейки,
    мест, где копии различаются и обе не нули — значит, это разные версии файла)."""
    md5, conflicts = hashlib.md5(), 0
    with open(a, "rb") as fa, open(b, "rb") as fb, open(out, "wb") as fo:
        while True:
            x, y = fa.read(BLOCK * 256), fb.read(BLOCK * 256)
            if not x and not y:
                break
            if len(x) != len(y):
                raise Fail("копии разной длины")
            if x != y:
                parts = []
                for i in range(0, len(x), BLOCK):
                    p, q = x[i:i + BLOCK], y[i:i + BLOCK]
                    if p == q or not q.strip(b"\0"):
                        parts.append(p)
                    elif not p.strip(b"\0"):
                        parts.append(q)
                    else:                       # край дыры внутри куска — побайтно
                        m = bytearray(p)
                        for j, (u, v) in enumerate(zip(p, q)):
                            if u != v:
                                if u == 0:
                                    m[j] = v
                                elif v != 0:
                                    conflicts += 1
                        parts.append(bytes(m))
                x = b"".join(parts)
            md5.update(x)
            fo.write(x)
    return md5.hexdigest(), conflicts


def prepare(db, own_root, pub, own, bad):
    """Строки своей копии для файлов, которые в общей плохи; всё остальное в своей папке — пропустить."""
    ts = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    added, dirs = 0, {own_root}
    for rel in bad:
        path = own_root + rel
        parent = path.rsplit("/", 1)[0]
        if rel not in own:
            db.execute("INSERT OR IGNORE INTO files(path, parent, name, kind, size, is_media, first_seen, updated_at) "
                       "VALUES(?,?,?,'file',?,0,?,?)", (path, parent, path.rsplit("/", 1)[1], pub[rel][2], ts, ts))
            added += 1
        elif own[rel][0] == "skipped":
            db.execute("UPDATE files SET fl_status=NULL WHERE path=?", (path,))
        while parent != own_root and parent not in dirs:
            dirs.add(parent)
            parent = parent.rsplit("/", 1)[0]
    for d in dirs:
        db.execute("INSERT OR IGNORE INTO files(path, parent, name, kind, first_seen, updated_at) VALUES(?,?,?,'dir',?,?)",
                   (d, d.rsplit("/", 1)[0], d.rsplit("/", 1)[1], ts, ts))
    inside = own_root + "/"
    want = {own_root + rel for rel in bad}
    others = [p for (p,) in db.execute("SELECT path FROM files WHERE kind='file' AND fl_status IS NULL "
                                       "AND substr(path, 1, length(?)) = ?", (inside, inside)) if p not in want]
    db.executemany("UPDATE files SET fl_status='skipped' WHERE path=?", [(p,) for p in others])
    # обход своей папки не нужен: всё, что из неё качать, уже в базе
    db.execute("UPDATE files SET crawled_at=COALESCE(crawled_at, ?) WHERE kind='dir' AND "
               "(path=? OR substr(path, 1, length(?)) = ?)", (ts, own_root, inside, inside))
    db.commit()
    return added, len(others)


def report(link, own_root, pub, own, bad):
    cats = {}
    for rel in bad:
        o = own.get(rel)
        c = "missing" if not o else kind(o[0], o[1])
        cats.setdefault((kind(*pub[rel][:2]), c), []).append(rel)
    pending = sum(1 for v in pub.values() if kind(v[0], v[1]) == "pending")
    names = {"ok": "цел", "damaged": "повреждён", "failed": "не читается", "pending": "ещё не скачан",
             "missing": "ещё не скачан"}
    print(f"Общая копия: {link}\nСвоя копия:  {own_root}\n")
    print(f"В общей копии файлов {len(pub)}: целых {sum(1 for v in pub.values() if kind(*v[:2]) == 'ok')}, "
          f"повреждённых {sum(1 for v in pub.values() if kind(*v[:2]) == 'damaged')}, "
          f"не читаются {sum(1 for v in pub.values() if kind(*v[:2]) == 'failed')}"
          + (f", ещё не скачано {pending}" if pending else ""))
    if not bad:
        print("Сверять нечего: повреждённых и нечитаемых файлов в общей копии нет.")
        return cats
    print(f"\nПовреждённые и нечитаемые в общей копии ({len(bad)}) — что с ними в своей:")
    for (p, o), rels in sorted(cats.items()):
        print(f"  в общей {names[p]:<12} у тебя {names[o]:<14} {len(rels):6d} файлов")
    return cats


def rclone_try(*args):
    """rclone без исключения: (код выхода, stdout). Для пачек, где часть файлов может не пройти."""
    r = subprocess.run(["rclone", *args], capture_output=True, text=True)
    return r.returncode, r.stdout


def cloud_md5s(root, rels):
    """md5 файлов root/rel из облака одним вызовом rclone: {rel: md5}."""
    if not rels:
        return {}
    lst = os.path.join(WORK, "список.txt")
    with open(lst, "w", encoding="utf-8") as f:
        f.write("\n".join(rels) + "\n")
    _, out = rclone_try("md5sum", root, "--files-from-raw", lst, "--checkers", "16")
    res = {}
    for line in out.splitlines():
        h, _, rel = line.partition("  ")
        if len(h) == 32 and rel:
            res[rel] = h
    return res


def originals(key, rels):
    """md5 оригиналов из открытого API Диска — по 6 запросов сразу: {rel: md5 или None}."""
    res, t = {}, time.time()
    with ThreadPoolExecutor(6) as ex:
        for n, (rel, h) in enumerate(zip(rels, ex.map(lambda r: original_md5(key, r), rels)), 1):
            res[rel] = h
            if time.time() - t > 60:
                t = time.time()
                log(f"md5 оригиналов: {n} из {len(rels)}")
    return res


def mark(db, pid, rel, tail, size, note):
    db.execute("UPDATE files SET fl_status='done', fl_note=?, fl_error=NULL, fl_bytes=?, fl_file=?, fl_at=? WHERE path=?",
               (note, size, f"Ссылки/{pid}/{tail}", time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
                f"/public/{pid}{rel}"))


def do_merge(db, key, pid, own_root, to, frm, pub, own, bad, partial=False):
    """Лучшая версия каждого плохого файла общей копии — в облако общей копии. Целые файлы своей копии копируются
    пачкой (внутри облака, одним вызовом rclone), md5 берутся списком; склейка двух повреждённых — по одному файлу."""
    os.makedirs(WORK, exist_ok=True)
    own_rel = own_root[len("/disk/"):]
    unknown = [rel for rel in bad if rel not in own]
    waiting = [rel for rel in bad if rel in own and kind(*own[rel][:2]) == "pending"]
    if unknown and not partial:
        raise Fail(f"{len(unknown)} нужных файлов твоей копии программа не знает — сначала сверка без --merge: "
                   f'yandexdl compare "{key}" "{own_rel}"')
    if waiting and not partial:
        raise Fail(f"из твоей копии ещё не скачано {len(waiting)} нужных файлов: загрузка не закончена или прервалась "
                   f"(в yandexdl status — «осталось …»). Сначала докачай: yandexdl fetch \"{own_rel}\" --no-crawl "
                   f"--rclone {frm} --part-gb 2. Склеить уже скачанное сейчас, а остальное потом — добавь --partial")
    lines, done = [], {"restored": 0, "copied": 0, "better": 0, "same": 0, "skip": 0, "nowhere": 0, "fail": 0}
    copies, merges = [], []
    for rel in bad:
        p, o = pub[rel], own.get(rel)
        ok_ = kind(*o[:2]) if o else "missing"
        if ok_ == "failed":
            lines.append(f"не читается нигде: {rel}")
            done["nowhere"] += 1
            continue
        if ok_ in ("missing", "pending") or not o[3]:
            continue
        src_tail = o[3].split("/", 1)[1]                                     # fl_file: «Диск/<путь на Диске>»
        tail = p[3].split("/", 2)[2] if p[3] else src_tail[len(own_rel) + 1:]
        item = (rel, src_tail, tail, p[2] or 0, o)
        (copies if ok_ == "ok" or kind(*p[:2]) == "failed" else merges).append(item)
    log(f"к сверке: взять с твоего Диска {len(copies)}, склеить {len(merges)}, не читается нигде {done['nowhere']}"
        + (f"; ещё не скачано из твоей копии {len(waiting) + len(unknown)} — сверю в следующий раз" if waiting or unknown
           else ""))

    # --- целые (или единственные) копии со своего Диска: md5 списком, копирование пачкой
    src_root = remote_join(frm, own_rel)
    if copies:
        whole = [c for c in copies if kind(*c[4][:2]) == "ok"]
        want = originals(key, [c[0] for c in whole])
        rel_of = {c[0]: c[1][len(own_rel) + 1:] for c in copies if c[1].startswith(own_rel + "/")}
        got = cloud_md5s(src_root, list(rel_of.values()))
        ready = []
        for rel, src_tail, tail, size, o in copies:
            g, w = got.get(rel_of.get(rel)), want.get(rel)
            if kind(*o[:2]) == "ok":
                if w and g and w != g and kind(*pub[rel][:2]) == "damaged":
                    lines.append(f"у тебя другая версия (md5 не как у оригинала), не трогаю: {rel}")
                    done["skip"] += 1
                    continue
                note = "взят целым с твоего Диска" + (" (md5 совпал с оригиналом)" if w and w == g else
                                                       " (md5 не как у оригинала — возможно, другая версия)" if w and g
                                                       else "")
            else:
                note = (f"повреждён: потеряно {lost_of(o[1])} из {size} байт — взят с твоего Диска, "
                        "общая копия не читалась")
            ready.append((rel, src_tail, tail, size, note, g))
        batch = [r for r in ready if rel_of.get(r[0]) == r[2]]               # тот же путь в обоих облаках
        in_batch = {r[0] for r in batch}
        log(f"копирую с твоего Диска {len(ready)} файлов (внутри облака)…")
        if batch:
            lst = os.path.join(WORK, "список.txt")
            with open(lst, "w", encoding="utf-8") as f:
                f.write("\n".join(r[2] for r in batch) + "\n")
            rclone_try("copy", src_root, to, "--files-from-raw", lst, "--ignore-times", "--transfers", "8",
                       "--checkers", "16")
        for r in ready:
            if r[0] not in in_batch:
                rclone_try("copyto", remote_join(frm, r[1]), remote_join(to, r[2]))
        landed = cloud_md5s(to.rstrip("/") if not to.endswith(":") else to, [r[2] for r in ready])
        for rel, src_tail, tail, size, note, g in ready:
            if g and landed.get(tail) == g:                                 # на месте и тот самый файл
                mark(db, pid, rel, tail, size, note)
                lines.append(f"{note}: {rel}")
                done["copied"] += 1
            else:
                lines.append(f"не скопировалось — повтори --merge: {rel}")
                done["fail"] += 1
        db.commit()
        log(f"взято с твоего Диска {done['copied']}" + (f", не скопировалось {done['fail']}" if done["fail"] else ""))

    # --- обе копии с дырами: скачать обе, склеить, проверить по md5 оригинала
    a, b, m = (os.path.join(WORK, x) for x in ("общая", "своя", "склейка"))
    t = time.time()
    for n, (rel, src_tail, tail, size, o) in enumerate(merges, 1):
        if (o[2] or 0) != size:
            lines.append(f"копии разного размера ({o[2]} и {size} байт), не склеиваю: {rel}")
            done["skip"] += 1
            continue
        free = shutil.disk_usage(WORK).free
        if free < 3 * size + 2 * 1024 ** 3:
            raise Fail(f"для склейки «{rel}» ({human(size)}) мало места на сервере: свободно {human(free)}")
        dst = remote_join(to, tail)
        try:
            rclone("copyto", dst, a)
            rclone("copyto", remote_join(frm, src_tail), b)
            got, conflicts = merge(a, b, m)
            if conflicts:
                lines.append(f"копии различаются в {conflicts} местах — разные версии, не склеиваю: {rel}")
                done["skip"] += 1
                continue
            want = original_md5(key, rel)
            before, after = zero_blocks(a), zero_blocks(m)
            if want and got == want:
                note = "восстановлен: склеен из двух повреждённых копий, md5 совпал с оригиналом"
                done["restored"] += 1
            elif after < before:
                note = (f"повреждён: потеряно {after * BLOCK} из {size} байт (примерно) — склеен из двух копий, "
                        f"было около {before * BLOCK}")
                done["better"] += 1
            else:
                lines.append(f"склейка ничего не дала — дыры в одних и тех же местах: {rel}")
                done["same"] += 1
                continue
            rclone("copyto", m, dst)
        finally:
            for x in (a, b, m):
                if os.path.exists(x):
                    os.remove(x)
        mark(db, pid, rel, tail, size, note)
        db.commit()
        lines.append(f"{note}: {rel}")
        if time.time() - t > 60 or n == len(merges):
            t = time.time()
            log(f"склеено {n} из {len(merges)}: восстановлено {done['restored']}, дыр стало меньше {done['better']}, "
                f"не помогло {done['same']}")

    path = os.path.join(WORK, REPORT_NAME)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Сверка общей копии с твоей ({own_root}), {time.strftime('%Y-%m-%d %H:%M')}\n\n" + "\n".join(lines) + "\n")
    rclone("copyto", path, remote_join(to, REPORT_NAME))
    print()
    print(f"Восстановлено склейкой (md5 как у оригинала): {done['restored']}")
    print(f"Взято с твоего Диска:                        {done['copied']}")
    print(f"Склеено, дыр стало меньше:                   {done['better']}")
    print(f"Склейка не помогла (дыры в тех же местах):   {done['same']}")
    print(f"Не читается ни там, ни там:                  {done['nowhere']}")
    print(f"Пропущено (разные версии файла):             {done['skip']}")
    if done["fail"]:
        print(f"Не скопировалось (повтори --merge):          {done['fail']}")
    if waiting or unknown:
        print(f"Ещё не скачаны из твоей копии, не сверялись: {len(waiting) + len(unknown)} — докачай и повтори --merge")
    print(f"Подробно — {REPORT_NAME} в {to}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link")
    ap.add_argument("folder")
    ap.add_argument("--merge", nargs=2, metavar=("КУДА", "ОТКУДА"))
    ap.add_argument("--partial", action="store_true", help="с --merge: склеить уже скачанное, хоть своя копия и не докачана")
    a = ap.parse_args()
    try:
        parsed = public.parse_link(a.link)
        if not parsed:
            raise Fail("первым нужна общая ссылка disk.yandex.ru/d/…")
        key, pid = parsed[0], public.public_id(parsed[0])
        db = sqlite3.connect(DB, timeout=60)
        pub_root = f"/public/{pid}"
        pub = rows_under(db, pub_root)
        if not pub:
            raise Fail("эту ссылку программа ещё не качала — сначала yandexdl fetch с ней")
        own_root = disk_folder(db, a.folder)
        own = rows_under(db, own_root)
        bad = sorted(rel for rel, v in pub.items() if kind(v[0], v[1]) in ("damaged", "failed"))
        if a.merge:
            do_merge(db, key, pid, own_root, a.merge[0], a.merge[1], pub, own, bad, a.partial)
            return 0
        report(a.link, own_root, pub, own, bad)
        if bad:
            added, skipped = prepare(db, own_root, pub, own, bad)
            own = rows_under(db, own_root)
            need = sum(1 for rel in bad if kind(*own[rel][:2]) == "pending")
            print(f"\nИз твоей копии нужно скачать {need} файлов (в базу добавлено {added}; остальные {skipped} файлов "
                  "твоей папки помечены пропущенными — их качать не нужно). Дальше:")
            print(f'  yandexdl fetch "{own_root[len("/disk/"):]}" --no-crawl --rclone gdrive:Yandex --part-gb 2')
            print(f'  yandexdl compare "{key}" "{own_root[len("/disk/"):]}" --merge gdrive:Библиотека gdrive:Yandex')
            print("(облака — те же, что в --rclone у fetch библиотеки и своего Диска)")
        return 0
    except Fail as e:
        print(f"Ошибка: {e}", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
