#!/usr/bin/env bash
# YandexDL Desktop на VPS без экрана: программа работает на виртуальном экране (Xvfb),
# в Яндекс входишь сам через noVNC в браузере, обход и скачивание — через API программы.
#
#   bash yandexdl-vps.sh install            поставить всё (Debian/Ubuntu, от root)
#   bash yandexdl-vps.sh vnc                включить noVNC: ссылка + одноразовый пароль
#   bash yandexdl-vps.sh vnc-off            выключить noVNC (после входа в Яндекс)
#   bash yandexdl-vps.sh fetch Творчество   обойти папку и скачать её (в /root/yandex/Диск/…)
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

cmd_install() {
  need_root
  command -v apt-get >/dev/null || die "скрипт рассчитан на Debian/Ubuntu (apt-get)"
  [ -f "$SRC_DIR/app/main.py" ] || die "не нашёл $SRC_DIR/app/main.py — скрипт должен лежать в yandexdl/vps/"

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
  apt-get install -y -qq "${pkgs[@]}" >/dev/null

  python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))' \
    || die "нужен Python 3.9+ (PySide6), а тут $(python3 -V). Подойдёт Ubuntu 22.04/24.04 или Debian 12"

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
  rsync -a --delete --exclude '.venv' --exclude '__pycache__' --exclude '*.pyc' "$SRC_DIR/" "$APP_DIR/"
  chmod 755 "$APP_DIR/vps/yandexdl-vps.sh"
  ln -sfn "$APP_DIR/vps/yandexdl-vps.sh" /usr/local/bin/yandexdl

  say "Python-окружение (PySide6 качается ~300 МБ, пара минут)"
  [ -x "$VENV/bin/python" ] || python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q -r "$APP_DIR/requirements.txt"

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
  command -v ufw >/dev/null && ufw status 2>/dev/null | grep -q '^Status: active' || return 0
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
  pw=$(tr -dc 'A-HJ-NP-Za-km-z2-9' </dev/urandom | head -c 8)   # VNC берёт не больше 8 символов
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
  [ $# -ge 1 ] || die "укажи папку: yandexdl fetch Творчество [куда_качать]"
  systemctl is-active -q yandexdl-app || cmd_start
  wait_api 60 || die "API программы не отвечает"
  systemctl reset-failed yandexdl-fetch.service 2>/dev/null || true
  systemd-run -q --unit=yandexdl-fetch --collect \
    "$VENV/bin/python" -u "$APP_DIR/vps/fetch.py" --folder "$1" --dest "${2:-$DEST_DEFAULT}" --dump-dir "$DUMP_DIR"
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
  if systemctl is-active -q yandexdl-fetch; then
    echo; echo "Последние строки задания (yandexdl-fetch):"
    journalctl -u yandexdl-fetch -n 8 -o cat --no-pager
  fi
}

cmd_diagnose() { "$VENV/bin/python" -u "$APP_DIR/vps/diagnose.py" "$DUMP_DIR"; }

cmd_logs() { journalctl -u yandexdl-app -o cat --no-pager -n "${1:-80}" | grep -v '"GET /api/' || true; }

case "${1:-}" in
  install)  cmd_install ;;
  start)    cmd_start ;;
  stop)     cmd_stop ;;
  restart)  cmd_stop; cmd_start ;;
  vnc)      cmd_vnc ;;
  vnc-off)  cmd_vnc_off ;;
  fetch)    shift; cmd_fetch "$@" ;;
  status)   cmd_status ;;
  diagnose) cmd_diagnose ;;
  logs)     shift; cmd_logs "$@" ;;
  *) sed -n '2,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
