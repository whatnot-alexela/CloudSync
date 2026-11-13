"""
Проверка облака: все ли файлы, которые по базе программы скачаны (целыми или с дырами), действительно лежат там,
куда их переносил fetch, и того ли они размера.

  verify.py ПАПКА|ССЫЛКА --rclone ОБЛАКО [--exclude ПАПКА]… [--fix]

ПАПКА — как у fetch («/» — весь Диск), ССЫЛКА — общая папка disk.yandex.ru/d/…; ОБЛАКО — то же, что в --rclone
у fetch этой папки. Облако читается одним списком (rclone lsjson -R). --fix ставит пропавшие файлы на скачивание
заново (в базе — как ещё не скачанные): потом та же команда fetch докачает их и перенесёт. Папку, которой в облаке
нет целиком, --fix не трогает: скорее всего, её переносили в другое облако — проверь её с тем --rclone.
"""
import argparse
import json
import os
import sqlite3
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compare  # noqa: E402
import public  # noqa: E402
from compare import Fail, human, log, remote_join, rclone  # noqa: E402

EXAMPLES = 10


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="папка Диска («/» — весь) или общая ссылка")
    ap.add_argument("--rclone", required=True, metavar="ОБЛАКО:ПАПКА", help="куда эту папку переносил fetch")
    ap.add_argument("--exclude", action="append", default=[], metavar="ПАПКА")
    ap.add_argument("--fix", action="store_true", help="пропавшие поставить на скачивание заново")
    a = ap.parse_args()
    try:
        db = sqlite3.connect(compare.DB, timeout=60)
        link = public.parse_link(a.target)
        if link:
            root = f"/public/{public.public_id(link[0])}"
            scope = root + ("" if link[1] == "/" else link[1].rstrip("/"))
            excl = [root + "/" + x.strip("/") for x in a.exclude]
            cut = 2                                  # fl_file: «Ссылки/<id>/<путь от корня ссылки>»
        else:
            scope = "/disk" if a.target.strip().strip("/") in ("", "disk") else compare.disk_folder(db, a.target)
            excl = [compare.disk_folder(db, x) for x in a.exclude]
            cut = 1                                  # fl_file: «Диск/<путь на Диске>»
        rows = [(p, f.split("/", cut)[cut], b) for p, f, b in db.execute(
            "SELECT path, fl_file, COALESCE(fl_bytes, size) FROM files WHERE kind='file' AND fl_status='done' "
            "AND fl_file IS NOT NULL AND (path=? OR substr(path, 1, length(?))=?)", (scope, scope + "/", scope + "/"))
            if not any(p == x or p.startswith(x + "/") for x in excl)]
        if not rows:
            print("Скачанных по базе файлов в этой папке нет — сверять нечего.")
            return 0
        # облако читаем от корня папки: для своего Диска — её путь в облаке (как его записала программа)
        depth = 0 if link or scope == "/disk" else scope.count("/") - 1
        base = "/".join(rows[0][1].split("/")[:depth])
        top = remote_join(a.rclone, base) if base else a.rclone
        log(f"читаю, что лежит в {top} …")
        listing, absent = {}, False
        try:
            out = rclone("lsjson", "-R", "--files-only", "--fast-list", "--no-mimetype", "--no-modtime", top)
        except Fail as e:
            if "not found" not in str(e):
                raise
            # папки нет совсем: либо её переносили в другое облако (тогда --rclone не тот), либо её там удалили —
            # тогда, раз её назвали явно, --fix скачает её заново целиком
            absent, out = True, "[]"
            print(f"(!) Папки {top} в облаке нет совсем. Если её переносили в другое облако — проверь с тем --rclone; "
                  "если её там удалили — та же команда с --fix поставит её на скачивание заново целиком.")
        for it in json.loads(out or "[]"):
            listing[it["Path"]] = it.get("Size")
        dirs = {p.split("/", 1)[0] for p in listing if "/" in p}
        missing, other_size = [], []
        for path, tail, size in rows:
            rel = tail[len(base) + 1:] if base else tail
            if rel not in listing:
                missing.append((path, rel))
            elif size is not None and listing[rel] not in (None, -1, size):
                other_size.append((rel, size, listing[rel]))
        print(f"\nСкачано по базе: {len(rows)} файлов; в облаке на месте: {len(rows) - len(missing) - len(other_size)}, "
              f"нет в облаке: {len(missing)}, другого размера: {len(other_size)}")
        groups = Counter(rel.split("/", 1)[0] if "/" in rel else "(прямо в папке)" for _, rel in missing)
        gone = set() if absent else {g for g in groups if g != "(прямо в папке)" and g not in dirs}   # нет целиком
        if missing:
            print(f"\nНЕТ В ОБЛАКЕ ({top}) — по папкам:")
            for g, n in groups.most_common():
                print(f"  {n:7d}  {g}" + ("   ← этой папки в облаке нет целиком" if g in gone else ""))
            print("  например:")
            for _, rel in missing[:EXAMPLES]:
                print(f"    {rel}")
        if other_size:
            print(f"\nДРУГОГО РАЗМЕРА (по базе → в облаке):")
            for rel, want, got in other_size[:EXAMPLES]:
                print(f"    {rel}: {human(want)} → {human(got)}")
        if gone:
            print(f"\n(!) Папок {', '.join('«' + g + '»' for g in sorted(gone))} в {top} нет целиком, и --fix их не "
                  "трогает. Если их переносили в другое облако (или переименовали там) — проверь их с тем --rclone. "
                  "Если их там удалили — назови папку явно, и --fix скачает её заново, например:")
            g = sorted(gone)[0]
            print(f'  yandexdl verify "{(base + "/" if base else "") + g}" --rclone {a.rclone} --fix')
        fixable = [p for p, rel in missing if (rel.split("/", 1)[0] if "/" in rel else "") not in gone]
        fixable += [p for p, rel in ((p, t[len(base) + 1:] if base else t) for p, t, _ in rows)
                    if rel in {x[0] for x in other_size}]
        if not fixable:
            return 0
        if not a.fix:
            print(f"\nПоставить пропавшие и другого размера ({len(fixable)}) на скачивание заново — та же команда с --fix.")
            return 0
        db.executemany("UPDATE files SET fl_status=NULL, fl_note=NULL, fl_error=NULL, fl_file=NULL WHERE path=?",
                       [(p,) for p in fixable])
        db.commit()
        excl_args = "".join(f' --exclude "{x}"' for x in a.exclude)
        print(f"\nПоставил на скачивание заново: {len(fixable)} файлов. Теперь скачай их той же командой fetch:")
        print(f'  yandexdl fetch "{a.target}"{excl_args} --rclone {a.rclone} --part-gb 2')
        return 0
    except Fail as e:
        print(f"Ошибка: {e}", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
