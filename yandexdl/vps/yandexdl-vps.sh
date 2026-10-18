#!/usr/bin/env bash
# YandexDL Desktop на VPS без экрана: программа работает на виртуальном экране (Xvfb),
# в Яндекс входишь сам через noVNC в браузере, обход и скачивание — через API программы.
#
#   bash yandexdl-vps.sh install            поставить всё (Debian/Ubuntu, от root)
#   bash yandexdl-vps.sh update             поставить заново после git pull / scp новой версии
#   bash yandexdl-vps.sh vnc                включить noVNC: ссылка + одноразовый пароль
#   bash yandexdl-vps.sh vnc-off            выключить noVNC (после входа в Яндекс)
#   bash yandexdl-vps.sh fetch Творчество   обойти папку и скачать её (в /root/yandex/Диск/…); «/» — весь Диск;
#        [--no-crawl] [--rclone gdrive:Папка] [--threads 1]   без обхода; частями в облако; потоки
#        [--exclude Папка]…                  кроме этой подпапки (путь от корня Диска, можно несколько раз)
#        повтор той же команды продолжает с места остановки; [--recrawl] — обойти все папки заново
#   bash yandexdl-vps.sh pick Папка         оставить в папке только выбранные подпапки и обойти их
#   bash yandexdl-vps.sh status             что сейчас происходит
#   bash yandexdl-vps.sh diagnose           в каком виде Диск отдаёт списки папок (если файлов 0)
#   bash yandexdl-vps.sh start|stop|restart|logs
set -euo pipefail

SRC_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)"   # папка yandexdl/, где лежит скрипт
BASE=/opt/yandexdl
APP_DIR=$BASE/app
VENV=$BASE/venv
STATE=/var/lib/yandexdl                 # база, куки, профиль браузера (XDG_DATA_HOME программы)
DUMP_DIR=$STATE/capture-dump            # ответы Диска для diagnose
VNC_DIR=$STATE/vnc
DEST_DEFAULT=/root/yandex
SCREEN=":99"
WEB_PORT=6080
API=http://127.0.0.1:8001

say() { printf '\033[1m==> %s\033[0m\n' "$*"; }
die() { printf '\033[31mОшибка: %s\033[0m\n' "$*" >&2; exit 1; }
need_root() { [ "$(id -u)" = 0 ] || die "запускать от root"; }

public_ip() {
  local ip
  ip=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<NF;i++) if($i=="src") print $(i+1)}')
  case "$ip" in ""|10.*|192.168.*|172.1[6-9].*|172.2[0-9].*|172.3[01].*|100.6[4-9].*|100.[7-9][0-9].*|100.1[01][0-9].*|100.12[0-7].*)
    ip=$(curl -4 -fsS -m 5 https://api.ipify.org 2>/dev/null || echo "$ip") ;;
  esac
  echo "$ip"
}

# первый пакет из списка, который есть в репозитории (libasound2t64 на Ubuntu 24.04, libasound2 раньше)
pick() { local p; for p in "$@"; do apt-cache show "$p" >/dev/null 2>&1 && { echo "$p"; return; }; done; }

wait_api() {
  for _ in $(seq 1 "${1:-90}"); do
    curl -fsS -m 2 "$API/api/fl/status" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# Отпечаток кода программы (то, что запускается, и зависимости). Работает копия в $APP_DIR, и git pull или scp
# новой версии её не меняют: пока не выполнить install, идёт прежний код.
code_sum() {
  { (cd "$1" && find app server capture vps -type f \( -name '*.py' -o -name '*.sh' \) -not -path '*/__pycache__/*' \
      | LC_ALL=C sort | xargs -r -d '\n' sha1sum -- requirements.txt) 2>/dev/null || true; } | sha1sum | cut -c1-10
}

source_dir() {   # папка с исходниками: откуда запущен этот скрипт или откуда ставили программу в прошлый раз
  if [ "$SRC_DIR" != "$APP_DIR" ]; then echo "$SRC_DIR"; return 0; fi
  local src
  src=$(cat "$BASE/source" 2>/dev/null || true)
  [ -n "$src" ] && [ -f "$src/app/main.py" ] && echo "$src"
}

stale_src() {    # папка с исходниками, если код в ней не тот, что установлен
  local src
  src=$(source_dir) || return 1
  [ "$(code_sum "$src")" != "$(code_sum "$APP_DIR")" ] || return 1
  echo "$src"
}

cmd_install() {
  need_root
  command -v apt-get >/dev/null || die "скрипт рассчитан на Debian/Ubuntu (apt-get)"
  [ -f "$SRC_DIR/app/main.py" ] || die "не нашёл $SRC_DIR/app/main.py — скрипт должен лежать в yandexdl/vps/"
  # перезапуск программы оборвал бы задание старой версии; новая продолжит его с того же места
  systemctl is-active -q yandexdl-fetch 2>/dev/null \
    && die "идёт задание (yandexdl status). Останови его: systemctl stop yandexdl-fetch — после установки повтори ту же команду fetch, она продолжит"

  say "Система"
  . /etc/os-release && echo "ОС: $PRETTY_NAME"
  free -h | sed -n '1,3p'
  df -h / | tail -1

  say "Пакеты"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  local pkgs=(python3 python3-venv python3-pip rsync curl ca-certificates openssl ffmpeg
    xvfb x11vnc novnc websockify fonts-dejavu-core fonts-liberation
    libnss3 libnspr4 libdbus-1-3 libdrm2 libegl1 libgl1 libgbm1 libfontconfig1 libfreetype6
    libgssapi-krb5-2 libx11-xcb1 libxcb-cursor0 libxcb-dri3-0 libxcb-icccm4 libxcb-image0
    libxcb-keysyms1 libxcb-randr0 libxcb-render-util0 libxcb-render0 libxcb-shape0 libxcb-shm0
    libxcb-sync1 libxcb-util1 libxcb-xfixes0 libxcb-xinerama0 libxcb-xkb1 libxcomposite1 libxdamage1
    libxext6 libxfixes3 libxkbcommon-x11-0 libxkbcommon0 libxkbfile1 libxrandr2 libxrender1
    libxshmfence1 libxtst6)
  local alt p
  for alt in "libasound2t64 libasound2" "libglib2.0-0t64 libglib2.0-0" "libpng16-16t64 libpng16-16"; do
    p=$(pick $alt); if [ -n "$p" ]; then pkgs+=("$p"); fi
  done
  echo "ставлю ~60 пакетов, это несколько минут…"
  apt-get install -y -q "${pkgs[@]}" > >(grep --line-buffered '^Setting up' || true)

  # PySide6 нужен Python 3.9+; на Ubuntu 20.04 системный — 3.8, но в репозитории есть python3.9
  local py=python3 v
  if ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))'; then
    for v in 3.12 3.11 3.10 3.9; do
      if apt-cache show "python$v-venv" >/dev/null 2>&1; then
        echo "системный $(python3 -V) не подходит — ставлю python$v"
        apt-get install -y -qq "python$v" "python$v-venv" >/dev/null
        py=python$v
        break
      fi
    done
    "$py" -c 'import sys; sys.exit(sys.version_info < (3, 9))' \
      || die "нужен Python 3.9+ (PySide6), а тут $(python3 -V). Подойдёт Ubuntu 20.04–24.04 или Debian 12"
  fi

  # Chromium внутри Qt на 1 ГБ памяти без подкачки падает — добавим 2 ГБ swap
  local mem_mb swap_mb
  mem_mb=$(awk '/MemTotal/{print int($2/1024)}' /proc/meminfo)
  swap_mb=$(awk '/SwapTotal/{print int($2/1024)}' /proc/meminfo)
  if [ "$mem_mb" -lt 1900 ] && [ "$swap_mb" -lt 1000 ] && [ ! -e /swapfile ]; then
    say "Памяти ${mem_mb} МБ — добавляю swap 2 ГБ (/swapfile)"
    fallocate -l 2G /swapfile 2>/dev/null || dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
    chmod 600 /swapfile && mkswap /swapfile >/dev/null && swapon /swapfile
    grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
  fi

  say "Программа -> $APP_DIR"
  mkdir -p "$APP_DIR" "$STATE" "$DUMP_DIR" "$VNC_DIR"
  chmod 700 "$STATE"
  if [ "$SRC_DIR" != "$APP_DIR" ]; then
    rsync -a --delete --exclude '.venv' --exclude '__pycache__' --exclude '*.pyc' "$SRC_DIR/" "$APP_DIR/"
    # какая версия стоит (её пишут status и лог fetch) и откуда ставили (отсюда берёт update)
    local ver
    ver=$(git -C "$SRC_DIR" log -1 --format='коммит %h от %cd' --date=format-local:'%d.%m.%Y %H:%M' 2>/dev/null || true)
    echo "${ver:-код $(code_sum "$SRC_DIR")}, установлена $(date '+%d.%m.%Y %H:%M')" > "$APP_DIR/VERSION"
    echo "$SRC_DIR" > "$BASE/source"
    echo "версия: $(cat "$APP_DIR/VERSION")"
  fi
  chmod 755 "$APP_DIR/vps/yandexdl-vps.sh"
  ln -sfn "$APP_DIR/vps/yandexdl-vps.sh" /usr/local/bin/yandexdl

  say "Python-окружение (PySide6 качается ~300 МБ, пара минут)"
  if [ -x "$VENV/bin/python" ] && ! "$VENV/bin/python" -c 'import sys; sys.exit(sys.version_info < (3, 9))'; then
    rm -rf "$VENV"
  fi
  [ -x "$VENV/bin/python" ] || "$py" -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -r "$APP_DIR/requirements.txt"

  say "Службы systemd"
  cat > /etc/systemd/system/yandexdl-xvfb.service <<EOF
[Unit]
Description=YandexDL: виртуальный экран $SCREEN
[Service]
ExecStart=/usr/bin/Xvfb $SCREEN -screen 0 1400x900x24 -nolisten tcp
Restart=always
EOF
  cat > /etc/systemd/system/yandexdl-app.service <<EOF
[Unit]
Description=YandexDL Desktop (на виртуальном экране)
Requires=yandexdl-xvfb.service
After=yandexdl-xvfb.service network-online.target
[Service]
WorkingDirectory=$APP_DIR
Environment=DISPLAY=$SCREEN
Environment=QTWEBENGINE_DISABLE_SANDBOX=1
Environment=XDG_DATA_HOME=$STATE
Environment=YDISK_DEVTOOLS_PORT=9222
Environment=YDISK_CAPTURE_DUMP=$DUMP_DIR
Environment=PYTHONUNBUFFERED=1
ExecStartPre=/bin/sleep 1
ExecStart=$VENV/bin/python app/main.py
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
  cat > /etc/systemd/system/yandexdl-x11vnc.service <<EOF
[Unit]
Description=YandexDL: VNC-доступ к виртуальному экрану (только localhost)
Requires=yandexdl-xvfb.service
After=yandexdl-xvfb.service
[Service]
ExecStart=/usr/bin/x11vnc -display $SCREEN -rfbport 5900 -localhost -rfbauth $VNC_DIR/passwd -forever -shared -noxdamage -quiet
Restart=on-failure
EOF
  cat > /etc/systemd/system/yandexdl-novnc.service <<EOF
[Unit]
Description=YandexDL: noVNC (https, порт $WEB_PORT)
Requires=yandexdl-x11vnc.service
After=yandexdl-x11vnc.service
[Service]
ExecStart=/usr/bin/websockify --web /usr/share/novnc --cert $VNC_DIR/cert.pem --key $VNC_DIR/key.pem --ssl-only $WEB_PORT 127.0.0.1:5900
Restart=on-failure
EOF
  systemctl daemon-reload
  systemctl enable -q yandexdl-app.service

  cmd_start
  say "Готово. Дальше: yandexdl vnc  — ссылка для входа в Яндекс"
}

cmd_update() {
  need_root
  local src
  src=$(source_dir) || die "не знаю, откуда ставилась программа — запусти install из папки с новой версией: bash ПАПКА/yandexdl/vps/yandexdl-vps.sh install"
  exec bash "$src/vps/yandexdl-vps.sh" install
}

cmd_start() {
  need_root
  systemctl restart yandexdl-xvfb yandexdl-app
  wait_api 120 || { journalctl -u yandexdl-app -n 40 --no-pager; die "программа не поднялась (лог выше)"; }
  echo "Программа работает, API: $API"
}

cmd_stop() {
  need_root
  systemctl stop yandexdl-novnc yandexdl-x11vnc yandexdl-app yandexdl-xvfb 2>/dev/null || true
  echo "Остановлено."
}

firewall() {   # firewall open|close — открыть/закрыть порт noVNC, если включён ufw
  command -v ufw >/dev/null || return 0
  case "$(ufw status 2>/dev/null)" in "Status: active"*) ;; *) return 0 ;; esac
  if [ "$1" = open ]; then ufw allow "$WEB_PORT/tcp" >/dev/null; else ufw delete allow "$WEB_PORT/tcp" >/dev/null || true; fi
}

cmd_vnc() {
  need_root
  systemctl is-active -q yandexdl-app || cmd_start
  local ip pw
  ip=$(public_ip)
  mkdir -p "$VNC_DIR" && chmod 700 "$VNC_DIR"
  if [ ! -s "$VNC_DIR/cert.pem" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=$ip" -addext "subjectAltName=IP:$ip" \
      -keyout "$VNC_DIR/key.pem" -out "$VNC_DIR/cert.pem" >/dev/null 2>&1
  fi
  # VNC берёт не больше 8 символов. Не «tr </dev/urandom | head»: при pipefail tr падает от SIGPIPE
  pw=$(head -c 600 /dev/urandom | LC_ALL=C tr -dc 'A-HJ-NP-Za-km-z2-9')
  pw=${pw:0:8}
  x11vnc -storepasswd "$pw" "$VNC_DIR/passwd" >/dev/null 2>&1
  systemctl restart yandexdl-x11vnc yandexdl-novnc
  firewall open
  sleep 2
  systemctl is-active -q yandexdl-novnc || { journalctl -u yandexdl-novnc -n 20 --no-pager; die "noVNC не запустился"; }
  cat <<EOF

  Ссылка:  https://$ip:$WEB_PORT/vnc.html?autoconnect=1&resize=scale
  Пароль:  $pw   (действует, пока не выполнишь «yandexdl vnc-off»)

  Сертификат самодельный: браузер предупредит — «Дополнительно» → «Перейти на сайт».
  Во вкладке «Яндекс Диск» нажми «Войти», войди и дождись, пока откроется сам Диск
  (disk.yandex.ru/client/disk). Потом: yandexdl vnc-off

EOF
}

cmd_vnc_off() {
  need_root
  systemctl stop yandexdl-novnc yandexdl-x11vnc 2>/dev/null || true
  rm -f "$VNC_DIR/passwd"
  firewall close
  echo "noVNC выключен, пароль удалён."
}

cmd_fetch() {
  need_root
  [ $# -ge 1 ] || die "укажи папку (/ — весь Диск): yandexdl fetch Творчество [--no-crawl|--recrawl] [--rclone gdrive:Папка] [--threads 1] [--exclude Папка]"
  local folder=$1 src; shift
  if systemctl is-active -q yandexdl-fetch; then
    die "задание уже идёт (yandexdl status). Остановить его: systemctl stop yandexdl-fetch"
  fi
  if src=$(stale_src); then
    die "код в $src не совпадает с установленным в $APP_DIR, а работает установленный — после git pull или scp его нужно поставить: yandexdl update (или bash $src/vps/yandexdl-vps.sh install), потом повтори fetch"
  fi
  systemctl is-active -q yandexdl-app || cmd_start
  wait_api 60 || die "API программы не отвечает"
  systemctl reset-failed yandexdl-fetch.service 2>/dev/null || true
  # HOME — чтобы rclone нашёл свой конфиг (/root/.config/rclone/rclone.conf)
  systemd-run -q --unit=yandexdl-fetch --collect --setenv=HOME=/root \
    "$VENV/bin/python" -u "$APP_DIR/vps/fetch.py" --folder "$folder" --dest "$DEST_DEFAULT" --dump-dir "$DUMP_DIR" "$@"
  echo "Обход и скачивание идут в фоне (служба yandexdl-fetch) — можно закрыть SSH, работа не прервётся."
  echo "Ctrl+C только перестаёт показывать ход. Снова посмотреть: yandexdl status"
  echo
  journalctl -u yandexdl-fetch -f -o cat --since "-5s" &
  local jpid=$!
  trap 'kill $jpid 2>/dev/null; exit 0' INT
  while systemctl is-active -q yandexdl-fetch; do sleep 2; done
  sleep 1; kill $jpid 2>/dev/null || true
}

cmd_status() {
  wait_api 3 || die "программа не запущена: yandexdl start"
  "$VENV/bin/python" -u "$APP_DIR/vps/fetch.py" --status
  local src
  if src=$(stale_src); then
    echo "(!) Код в $src не совпадает с установленным, работает установленный — поставь новый: yandexdl update"
  fi
  echo
  if systemctl is-active -q yandexdl-fetch; then
    echo "Задание идёт (yandexdl-fetch), последние строки:"
  else
    echo "Задание не идёт. Чем закончился последний запуск fetch:"
  fi
  journalctl -u yandexdl-fetch -n 8 -o cat --no-pager -q || true
}

cmd_pick() {
  wait_api 5 || die "программа не запущена: yandexdl start"
  systemctl is-active -q yandexdl-fetch && die "сначала останови загрузку: systemctl stop yandexdl-fetch"
  "$VENV/bin/python" -u "$APP_DIR/vps/pick.py" "$@"
}

cmd_diagnose() { "$VENV/bin/python" -u "$APP_DIR/vps/diagnose.py" "$DUMP_DIR"; }

cmd_logs() { journalctl -u yandexdl-app -o cat --no-pager -n "${1:-80}" | grep -v '"GET /api/' || true; }

case "${1:-}" in
  install)  cmd_install ;;
  update)   cmd_update ;;
  start)    cmd_start ;;
  stop)     cmd_stop ;;
  restart)  cmd_stop; cmd_start ;;
  vnc)      cmd_vnc ;;
  vnc-off)  cmd_vnc_off ;;
  fetch)    shift; cmd_fetch "$@" ;;
  status)   cmd_status ;;
  pick)     shift; cmd_pick "$@" ;;
  diagnose) cmd_diagnose ;;
  logs)     shift; cmd_logs "$@" ;;
  *) sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
