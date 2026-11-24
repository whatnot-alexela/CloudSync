"""
Чистка Яндекс Диска: убрать с него то, что уже целым лежит в облаке. Файл удаляется, только если в облаке, куда его
перенёс fetch, лежит файл того же размера и с тем же md5, что у оригинала на Яндексе (md5 Яндекс помнит с загрузки
файла, поэтому совпадение значит: в облаке байт в байт оригинал). Повреждённые, не скачанные, не найденные в облаке и
файлы без md5 остаются на Яндексе.

  cleanup.py ПАПКА --rclone ОБЛАКО [--yandex YANDEX:] [--exclude ПАПКА]… [--delete]

ПАПКА — как у fetch («/» — весь Диск); ОБЛАКО — то же, что в --rclone у fetch; YANDEX — remote rclone типа yandex,
подключённый к этому же Диску (по умолчанию «yandex:», см. README). Без --delete — только проверка и отчёт.
С --delete файлы уходят в Корзину Яндекса: оттуда их можно вернуть, а место освобождается, когда Корзину очистят.
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import compare  # noqa: E402
import public  # noqa: E402
from compare import Fail, human, log, remote_join, rclone  # noqa: E402

REPORT_DIR = os.path.expanduser(os.environ.get("YDISK_CLEANUP_DIR", "~/yandexdl-cleanup"))
EXAMPLES = 10
PROGRESS_EVERY = 30   # сек


def glob_escape(s):
    return re.sub(r"([\\*?\[\]{}])", r"\\\1", s)


def remote_type(name):
    for line in rclone("listremotes", "--long").splitlines():
        n, _, t = line.partition(":")
        if n.strip() == name:
            return t.strip()
    return None


def listing(top, filters, fast=False):
    """{путь от top: [(размер, md5 или None), …]} — одним вызовом rclone lsjson."""
    out = rclone("lsjson", "-R", "--files-only", "--hash", "--hash-type", "md5", "--no-mimetype", "--no-modtime",
                 *(["--fast-list"] if fast else []), *filters, top)
    res = {}
    for it in json.loads(out or "[]"):
        h = {k.lower(): v for k, v in (it.get("Hashes") or {}).items()}.get("md5")
        res.setdefault(it["Path"], []).append((it.get("Size"), h.lower() if isinstance(h, str) and len(h) == 32 else None))
    return res


def delete(ytop, files, log_path):
    """Удалить files (пути от ytop) в Корзину Яндекса. Возвращает (удалённые, ошибки)."""
    lst = log_path + ".список"
    with open(lst, "w", encoding="utf-8") as f:
        f.write("\n".join(files) + "\n")
    env = dict(os.environ, RCLONE_YANDEX_HARD_DELETE="false")      # в Корзину, а не насовсем — как бы ни был настроен remote
    p = subprocess.Popen(["rclone", "delete", ytop, "--files-from-raw", lst, "-v", "--stats", "0",
                          "--checkers", "4", "--transfers", "4", "--retries", "3"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, env=env)
    done, errors, t = [], [], time.time()
    with open(log_path, "w", encoding="utf-8") as lf:
        for line in p.stderr:
            lf.write(line)
            m = re.search(r" INFO  : (.*): Deleted\s*$", line)
            if m:
                done.append(m.group(1))
            elif " ERROR " in line:
                errors.append(line.strip())
            if time.time() - t > PROGRESS_EVERY:
                t = time.time()
                log(f"удалено {len(done)} из {len(files)}" + (f", ошибок {len(errors)}" if errors else ""))
    p.wait()
    os.remove(lst)
    if p.returncode and not done and not errors:
        raise Fail(f"rclone delete завершился с кодом {p.returncode} — подробности в {log_path}")
    return done, errors


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="папка Диска («/» — весь)")
    ap.add_argument("--rclone", required=True, metavar="ОБЛАКО:ПАПКА", help="куда эту папку переносил fetch")
    ap.add_argument("--yandex", default="yandex:", metavar="REMOTE:", help="remote rclone с этим Яндекс Диском")
    ap.add_argument("--exclude", action="append", default=[], metavar="ПАПКА")
    ap.add_argument("--delete", action="store_true", help="удалить с Яндекса (в Корзину) то, что в облаке целое")
    a = ap.parse_args()
    try:
        if public.parse_link(a.target):
            raise Fail("это общая ссылка: она не на твоём Диске, удалять там нечего — нужна папка своего Диска")
        yname, cname = a.yandex.split(":", 1)[0], a.rclone.split(":", 1)[0]
        ybase = a.yandex if ":" in a.yandex else a.yandex + ":"
        if yname == cname:
            raise Fail(f"--yandex и --rclone — одно и то же облако «{cname}:»; --yandex — это Яндекс Диск")
        t = remote_type(yname)
        if t is None:
            raise Fail(f"в rclone нет «{yname}:» — сначала подключи к rclone Яндекс Диск (README, «Чистка Яндекс Диска»)")
        if t != "yandex" and not os.environ.get("YDISK_CLEANUP_ANY_REMOTE"):
            raise Fail(f"«{yname}:» в rclone — не Яндекс Диск (тип {t})")

        db = sqlite3.connect(compare.DB, timeout=60)
        scope = "/disk" if a.target.strip().strip("/") in ("", "disk") else compare.disk_folder(db, a.target)
        excl = [compare.disk_folder(db, x) for x in a.exclude]
        rows = [(p, f.split("/", 1)[1], note) for p, f, note in db.execute(
            "SELECT path, fl_file, fl_note FROM files WHERE kind='file' AND fl_status='done' AND fl_file LIKE 'Диск/%' "
            "AND substr(path, 1, length(?))=?", (scope + "/", scope + "/"))
            if not any(p == x or p.startswith(x + "/") for x in excl)]
        title = (scope[len("/disk/"):] if scope != "/disk" else "/ (весь Диск)") + (
            ", кроме " + ", ".join(f"«{x[len('/disk/'):]}»" for x in excl) if excl else "")
        print(f"Папка: {title}", flush=True)
        if not rows:
            print("Скачанных по базе файлов в этой папке нет — удалять нечего.")
            return 0

        # облако — от его папки, соответствующей scope (как в verify); Яндекс — от самой scope
        depth = 0 if scope == "/disk" else scope.count("/") - 1
        base = "/".join(rows[0][1].split("/")[:depth])
        ctop = remote_join(a.rclone, base) if base else a.rclone
        yrel_scope = scope[len("/disk"):].strip("/")
        ytop = remote_join(ybase, yrel_scope) if yrel_scope else ybase
        filters = []
        for x in excl:
            if x.startswith(scope + "/"):
                filters += ["--exclude", "/" + glob_escape(x[len(scope) + 1:]) + "/**"]
        log(f"читаю список файлов на Яндексе ({ytop}) …")
        ylist = listing(ytop, filters)
        log(f"читаю облако ({ctop}) …")
        clist = listing(ctop, filters, fast=True)

        ok, known = [], set()
        keep = {k: [] for k in ("damaged", "differ", "nocloud", "nomd5")}
        gone = 0
        for path, tail, note in rows:
            yrel = path[len(scope) + 1:]
            crel = tail[len(base) + 1:] if base else tail
            known.add(yrel)
            if yrel not in ylist:
                gone += 1                                    # на Яндексе уже нет (удалён раньше)
                continue
            ysize, ymd5 = ylist[yrel][0]
            cs = clist.get(crel)
            if not cs:
                keep["nocloud"].append((yrel, ysize))
            elif not ymd5 or all(h is None for _, h in cs):
                keep["nomd5"].append((yrel, ysize))
            elif any(s == ysize and h == ymd5 for s, h in cs):
                ok.append((yrel, ysize))
            else:
                keep["damaged" if (note or "").startswith("повреждён") else "differ"].append((yrel, ysize))
        other = [(p, v[0][0]) for p, v in ylist.items() if p not in known]
        size = lambda xs: human(sum(s or 0 for _, s in xs))     # noqa: E731

        def disk_path(rel):
            return f"{yrel_scope}/{rel}" if yrel_scope else rel

        print(f"\nНа Яндексе в этой папке файлов: {len(ylist)} ({size([(p, v[0][0]) for p, v in ylist.items()])})")
        print(f"\nМОЖНО УДАЛИТЬ — в облаке лежат целыми (размер и md5 как у оригинала): {len(ok)} файлов, {size(ok)}")
        print("Остаются на Яндексе:")
        labels = [("damaged", "повреждены (в облаке с дырами)"),
                  ("differ", "в облаке не такие, как оригинал (другой md5 или размер)"),
                  ("nocloud", f"не нашлись в облаке ({ctop})"),
                  ("nomd5", "нет md5, не проверить"),
                  ("other", "программа их не скачивала (не скачаны, пропущены или новые)")]
        groups = dict(keep, other=other)
        for k, text in labels:
            print(f"  {len(groups[k]):7d}  {text}" + (f", {size(groups[k])}" if groups[k] else ""))
        if gone:
            print(f"(уже нет на Яндексе: {gone} — удалены раньше)")
        for k, text in labels[1:3]:
            if groups[k]:
                print(f"\n{text[0].upper() + text[1:]}, например:")
                for rel, _ in groups[k][:EXAMPLES]:
                    print(f"    {disk_path(rel)}")

        os.makedirs(REPORT_DIR, exist_ok=True)
        stamp = time.strftime("%Y-%m-%d_%H%M%S")
        report = os.path.join(REPORT_DIR, f"{stamp}_проверка.txt")
        with open(report, "w", encoding="utf-8") as f:
            f.write(f"Папка: {title}\nОблако: {ctop}\n\n")
            for k, text in [("ok", "МОЖНО УДАЛИТЬ — в облаке целые")] + labels:
                xs = ok if k == "ok" else groups[k]
                f.write(f"== {text}: {len(xs)}\n" + "".join(f"{disk_path(r)}\n" for r, _ in sorted(xs)) + "\n")
        print(f"\nВсе списки: {report}")

        if not ok:
            print("Удалять нечего.")
            return 0
        if not a.delete:
            excl_args = "".join(f' --exclude "{x[len("/disk/"):]}"' for x in excl)
            target = "/" if scope == "/disk" else scope[len("/disk/"):]
            yarg = "" if a.yandex in ("yandex", "yandex:") else f" --yandex {a.yandex}"
            print(f"\nУдалить эти {len(ok)} файлов с Яндекса (в Корзину) — та же команда с --delete:")
            print(f'  yandexdl cleanup "{target}"{excl_args} --rclone {a.rclone}{yarg} --delete')
            return 0

        log(f"удаляю с Яндекса в Корзину {len(ok)} файлов ({size(ok)}) …")
        rlog = os.path.join(REPORT_DIR, f"{stamp}_удаление.log")
        done, errors = delete(ytop, [r for r, _ in ok], rlog)
        sizes = dict(ok)
        removed = os.path.join(REPORT_DIR, f"{stamp}_удалено.txt")
        with open(removed, "w", encoding="utf-8") as f:
            f.write(f"Удалено с Яндекс Диска в Корзину {time.strftime('%Y-%m-%d %H:%M')}: файлы, которые целыми "
                    f"(md5 как у оригинала) лежат в {ctop}\n\n" + "".join(f"{disk_path(r)}\n" for r in sorted(done)))
        cloud_copy = remote_join(ctop, f"_удалено_с_Яндекса_{stamp}.txt")
        try:
            rclone("copyto", removed, cloud_copy)
        except Fail as e:
            cloud_copy = None
            log(f"список удалённого в облако не положился: {e}")
        print(f"\nУдалено с Яндекса (в Корзину): {len(done)} файлов, {human(sum(sizes.get(r) or 0 for r in done))}")
        if len(done) < len(ok):
            print(f"Не удалилось: {len(ok) - len(done)} — повтори ту же команду (лишнего она не тронет). "
                  f"Ошибки — в {rlog}")
            for e in errors[:5]:
                print(f"    {e[-200:]}")
        print(f"Список удалённого: {removed}" + (f" и в облаке: {cloud_copy}" if cloud_copy else ""))
        print("Корзина Яндекса занимает место на Диске, пока её не очистят. Когда убедишься, что в облаке всё есть, "
              "очисти её на disk.yandex.ru (Корзина → Очистить корзину).")
        return 0 if len(done) == len(ok) else 1
    except Fail as e:
        print(f"Ошибка: {e}", flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
