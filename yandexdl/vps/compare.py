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


def do_merge(db, key, pid, own_root, to, frm, pub, own, bad):
    os.makedirs(WORK, exist_ok=True)
    own_rel = own_root[len("/disk/"):]
    lines, done = [], {"restored": 0, "copied": 0, "better": 0, "same": 0, "skip": 0}
    a, b, m = (os.path.join(WORK, x) for x in ("общая", "своя", "склейка"))
    for n, rel in enumerate(bad, 1):
        p, o = pub[rel], own.get(rel)
        pk, ok_ = kind(*p[:2]), (kind(*o[:2]) if o else "missing")
        if ok_ in ("missing", "pending") or not (o and o[3]):
            done["skip"] += 1
            continue
        if ok_ == "failed":
            lines.append(f"не читается нигде: {rel}")
            done["skip"] += 1
            continue
        src = remote_join(frm, o[3].split("/", 1)[1])                      # fl_file: «Диск/<путь на Диске>»
        tail = p[3].split("/", 2)[2] if p[3] else o[3].split("/", 1)[1][len(own_rel) + 1:]
        dst = remote_join(to, tail)
        size = p[2] or 0
        note = None
        if ok_ == "ok":
            want, got = original_md5(key, rel), cloud_md5(src)
            if want and got and want != got and pk == "damaged":
                lines.append(f"у тебя другая версия (md5 не как у оригинала), не трогаю: {rel}")
                done["skip"] += 1
                continue
            rclone("copyto", src, dst)
            note = "взят целым с твоего Диска" + (" (md5 совпал с оригиналом)" if want and want == got else
                                                   " (md5 не как у оригинала — возможно, другая версия)" if want and got
                                                   else "")
            done["copied"] += 1
        elif pk == "failed":                                                # общая не читается, своя с дырами
            rclone("copyto", src, dst)
            note = f"повреждён: потеряно {lost_of(o[1])} из {size} байт — взят с твоего Диска, общая копия не читалась"
            done["copied"] += 1
        else:                                                               # обе с дырами — склеить
            if (o[2] or 0) != size:
                lines.append(f"копии разного размера ({o[2]} и {size} байт), не склеиваю: {rel}")
                done["skip"] += 1
                continue
            free = shutil.disk_usage(WORK).free
            if free < 3 * size + 2 * 1024 ** 3:
                raise Fail(f"для склейки «{rel}» ({human(size)}) мало места на сервере: свободно {human(free)}")
            try:
                rclone("copyto", dst, a)
                rclone("copyto", src, b)
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
        lines.append(f"{note}: {rel}")
        db.execute("UPDATE files SET fl_status='done', fl_note=?, fl_error=NULL, fl_bytes=?, fl_file=?, fl_at=? "
                   "WHERE path=?", (note, size, f"Ссылки/{pid}/{tail}", time.strftime("%Y-%m-%dT%H:%M:%S+00:00",
                                                                                   time.gmtime()), f"/public/{pid}{rel}"))
        db.commit()
        if n % 50 == 0:
            log(f"сверено {n} из {len(bad)}: восстановлено {done['restored']}, взято с твоего Диска {done['copied']}, "
                f"дыр стало меньше {done['better']}")
    path = os.path.join(WORK, REPORT_NAME)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Сверка общей копии с твоей ({own_root}), {time.strftime('%Y-%m-%d %H:%M')}\n\n" + "\n".join(lines) + "\n")
    rclone("copyto", path, remote_join(to, REPORT_NAME))
    print()
    print(f"Восстановлено склейкой (md5 как у оригинала): {done['restored']}")
    print(f"Взято с твоего Диска:                        {done['copied']}")
    print(f"Склеено, дыр стало меньше:                   {done['better']}")
    print(f"Склейка не помогла (дыры в тех же местах):   {done['same']}")
    print(f"Пропущено (нечем заменить, разные версии…):  {done['skip']}")
    print(f"Подробно — {REPORT_NAME} в {to}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("link")
    ap.add_argument("folder")
    ap.add_argument("--merge", nargs=2, metavar=("КУДА", "ОТКУДА"))
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
            do_merge(db, key, pid, own_root, a.merge[0], a.merge[1], pub, own, bad)
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
