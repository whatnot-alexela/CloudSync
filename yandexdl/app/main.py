"""
YandexDL Desktop — прототип, v2: единое окно-браузер.

Вкладка 0 ("YandexDL") закреплена за нашим локальным сайтом — галереей/статусом
server.py, её нельзя закрыть. Остальные вкладки — обычный браузер: адресная строка,
назад/вперёд/обновить, "+" открывает новую вкладку, вкладки закрываются крестиком.
Пользователь просто открывает disk.yandex.ru (вкладка 2 открыта на нём по умолчанию)
и работает как в обычном браузере — можно хоть в нескольких вкладках сразу.

Все обычные вкладки используют один и тот же персистентный QWebEngineProfile
(общие куки/сессия), поэтому логин не слетает между вкладками и перезапусками.

Лог перехвата и статистика — в выезжающей снизу панели (QDockWidget), чтобы не
мешать "браузерности" основного окна; открывается кнопкой "Лог" на тулбаре.

ВАЖНО: QTWEBENGINE_REMOTE_DEBUGGING нужно выставить ДО импорта QtWebEngine*.

Это прототип: не запускался и не тестировался в песочнице (нет сети и PySide6).

--- Про сборку в exe/app/бинарник (PyInstaller) ---

server.py качает видео через ОТДЕЛЬНЫЙ подпроцесс recover_runner.py: вызывает
`[sys.executable, "-u", <путь к recover_runner.py>] + args`. В dev-режиме
sys.executable — это настоящий python3, всё работает само. В СОБРАННОМ приложении
sys.executable — это НАШ ЖЕ exe (другого питона рядом нет), так что такой вызов
на самом деле означает "запусти себя опять с этими аргументами". Шаг 0 ниже ловит
именно этот паттерн — ПЕРЕД импортом Qt, чтобы не поднимать GUI ради одного
скачивания видео — и выполняет recover_runner.py напрямую. Это сделано в main.py,
а не в server.py, именно чтобы server.py не пришлось трогать вообще.
"""
import json
import os
import socket
import sys
import time
import urllib.request
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import resource_dir, ffmpeg_dir  # noqa: E402

# --- 0. бандлed ffmpeg (если есть, см. yandexdl.spec) — в PATH до всего остального ---

_ff_dir = ffmpeg_dir()
if _ff_dir:
    os.environ["PATH"] = _ff_dir + os.pathsep + os.environ.get("PATH", "")


def _dispatch_recover_runner_if_needed():
    """См. пояснение в докстринге модуля. Если argv — это тот самый вызов от
    server.py, выполняем recover_runner.py и выходим, не трогая Qt вообще."""
    if len(sys.argv) >= 3 and sys.argv[1] == "-u" and os.path.basename(sys.argv[2]) == "recover_runner.py":
        import runpy
        real_runner = os.path.join(resource_dir(), "server", "recover_runner.py")
        sys.argv = [real_runner] + sys.argv[3:]
        runpy.run_path(real_runner, run_name="__main__")
        sys.exit(0)


_dispatch_recover_runner_if_needed()

# --- 1. свободный порт под remote-debugging, выставляем env ДО импорта Qt ---


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


DEVTOOLS_PORT = int(os.environ.get("YDISK_DEVTOOLS_PORT") or _free_port())
os.environ["QTWEBENGINE_REMOTE_DEBUGGING"] = str(DEVTOOLS_PORT)

# --- 2. теперь можно импортировать Qt ---

from PySide6.QtCore import Qt, Signal, QObject, QUrl, QTimer
from PySide6.QtGui import QIcon, QAction
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPlainTextEdit, QTabWidget, QToolBar, QLineEdit, QToolButton, QDockWidget,
)
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineProfile, QWebEnginePage

from server_runner import ServerThread  # noqa: E402
from capture_cdp import CDPCapture  # noqa: E402
from webauthn_dialog import attach_webauthn_handler  # noqa: E402
from paths import user_data_dir  # noqa: E402

DEFAULT_NEW_TAB_URL = "https://disk.yandex.ru"
DISK_FOLDER_URL = "https://disk.yandex.ru/client/disk"   # + путь папки без /disk

# Прокручивает до конца и окно, и все прокручиваемые блоки: список папки на Диске
# подгружается порциями по мере прокрутки, а в каком именно блоке он лежит — не важно.
SCROLL_JS = """
(function () {
  var els = [document.scrollingElement || document.documentElement];
  document.querySelectorAll('*').forEach(function (el) {
    if (el.scrollHeight > el.clientHeight + 20) {
      var oy = getComputedStyle(el).overflowY;
      if (oy === 'auto' || oy === 'scroll') els.push(el);
    }
  });
  els.forEach(function (el) { el.scrollTop = el.scrollHeight; });
  return els.length;
})();
"""

# к своему же localhost — напрямую, мимо системного прокси
_LOCAL_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class LogBridge(QObject):
    """CDPCapture и ServerThread живут в фоновых потоках — сигналы Qt безопасно
    перекидывают их сообщения в GUI-поток (Qt сам ставит slot-вызов в очередь)."""
    log = Signal(str)
    stats = Signal(dict)


class LogDock(QDockWidget):
    """Выезжающая снизу панель с логом перехвата и короткой строкой статуса."""

    def __init__(self, parent=None):
        super().__init__("Лог перехвата", parent)
        self.setAllowedAreas(Qt.BottomDockWidgetArea | Qt.RightDockWidgetArea)
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(4, 4, 4, 4)

        self.status_label = QLabel("Запускаюсь…")
        self.status_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(self.status_label)

        self.log_view = QPlainTextEdit(readOnly=True)
        self.log_view.setMaximumBlockCount(2000)
        layout.addWidget(self.log_view)

        self.setWidget(w)

    def append_log(self, text: str):
        self.log_view.appendPlainText(text)

    def update_stats(self, d: dict):
        parts = []
        if "active_tabs" in d:
            parts.append(f"вкладок с Диском под наблюдением: {d['active_tabs']}")
        if "total_clusters" in d:
            parts.append(f"кластеров: {d['total_clusters']}")
        if "total_resources" in d:
            parts.append(f"ресурсов: {d['total_resources']}")
        if "total_files" in d:
            parts.append(f"из списков папок: {d['total_files']}")
        if parts:
            self.status_label.setText(" · ".join(parts))


class FolderCrawler(QObject):
    """
    Обход папок для раздела «Файлы». Включается кнопкой на странице статуса (вкладка «Файлы»)
    или на тулбаре — флаг хранит сервер (/api/fl/crawl), окно его опрашивает. Пока флаг стоит,
    папки Диска по одной открываются в отдельной вкладке и прокручиваются до конца; списки файлов
    забирает обычный перехват (capture_cdp), как при ручном просмотре. Какую папку открыть следующей
    и что уже пройдено — решает сервер (/api/fl/next-folder, /api/fl/folder-done).

    Папка считается дочитанной, когда STABLE_TICKS тиков подряд с сервера не приходит ничего нового
    из её содержимого (прокрутка больше не подгружает порции).
    """
    TICK_MS = 1500
    STABLE_TICKS = 3        # тиков подряд без новых файлов папки — папка дочитана
    START_DELAY_TICKS = 3   # новой вкладке дать время: перехват подключается к ней раз в ~2 с
    LOAD_TIMEOUT = 60       # сек: страница так и не загрузилась — идём дальше
    FOLDER_TIMEOUT = 600    # сек: потолок на одну папку
    EMPTY_MIN_SECONDS = 15  # сек: папку без единого файла не считать дочитанной раньше — список мог ещё не прийти

    def __init__(self, window):
        super().__init__(window)
        self.win = window
        self.view = None
        self.running = False
        self._reset_folder()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._tick)

    def start(self):
        self.timer.start(self.TICK_MS)

    def stop(self):
        self.timer.stop()

    def _reset_folder(self):
        self.current = None
        self.since = None
        self.last_fresh = -1
        self.stable = 0
        self.started = 0.0
        self.loading = False
        self.wait_ticks = 0

    def _api(self, path, body=None):
        base = self.win.server_thread.base_url
        if not base:
            return None
        data, headers = None, {}
        if body is not None:
            data, headers = json.dumps(body).encode("utf-8"), {"Content-Type": "application/json"}
        req = urllib.request.Request(base + path, data=data, headers=headers,
                                     method="POST" if body is not None else "GET")
        try:
            with _LOCAL_OPENER.open(req, timeout=3) as resp:
                return json.loads(resp.read())
        except Exception:  # noqa: BLE001 — сервер ещё не поднялся или перезапускается
            return None

    def toggle(self):
        st = self._api("/api/fl/crawl")
        if st is not None:
            self._api("/api/fl/crawl", {"run": not st.get("run")})

    def _finish(self, message):
        self._api("/api/fl/crawl", {"run": False, "message": message})
        self.win._on_log(f"[обход] {message}")

    def _view_open(self):
        if self.view is None:
            return False
        try:
            return self.win.tabs.indexOf(self.view) >= 0
        except RuntimeError:     # C++-объект вкладки уже удалён
            return False

    def _tick(self):
        st = self._api("/api/fl/crawl?agent=1")   # заодно сообщает странице статуса, что обход есть кому выполнять
        if st is None:
            return
        run = bool(st.get("run"))
        self.win.crawl_btn.setText("Остановить обход" if run else "Обойти папки")
        if not run:
            if self.running:
                self.win._on_log("[обход] остановлен")
            self.running = False
            self._reset_folder()
            return
        if not self.running:
            self.running = True
            self.win._on_log("[обход] начинаю обход папок Диска")
            if self.view is not None and not self._view_open():
                self.view = None
        if self.view is None:
            self._open_tab()
            return
        if not self._view_open():
            self.view = None
            self._finish("остановлен: вкладку «Обход» закрыли")
            return
        if self.wait_ticks > 0:
            self.wait_ticks -= 1
            return

        if self.current is None:
            nxt = self._api("/api/fl/next-folder", {}) or {}
            if not nxt.get("path"):
                self._finish("готово: все найденные папки пройдены")
                return
            self._visit(nxt["path"], nxt.get("since"))
            return

        elapsed = time.monotonic() - self.started
        if self.loading and elapsed < self.LOAD_TIMEOUT:
            return
        url = self.view.url()
        if "disk.yandex" not in url.host() or not url.path().startswith("/client/"):
            self._finish("остановлен: Диск попросил войти или пройти проверку — сделай это во вкладке "
                         "«Обход» и запусти обход снова")
            return
        self.view.page().runJavaScript(SCROLL_JS)
        info = self._api(f"/api/fl/folder?path={quote(self.current)}&since={quote(self.since or '')}") or {}
        fresh = info.get("fresh", -1)
        if fresh == self.last_fresh:
            self.stable += 1
        else:
            self.stable, self.last_fresh = 0, fresh
        settled = self.stable >= self.STABLE_TICKS and (info.get("children") or elapsed >= self.EMPTY_MIN_SECONDS)
        if settled or elapsed > self.FOLDER_TIMEOUT:
            self._api("/api/fl/folder-done", {"path": self.current})
            self.win._on_log(f"[обход] {self.current}: в папке {info.get('children', '?')} файлов/папок")
            self._reset_folder()

    def _open_tab(self):
        self.view = self.win._open_tab(DISK_FOLDER_URL)
        self.view.loadFinished.connect(self._on_loaded)
        self._reset_folder()
        self.wait_ticks = self.START_DELAY_TICKS

    def _visit(self, path, since):
        self._reset_folder()
        self.current, self.since = path, since
        self.started = time.monotonic()
        self.loading = True
        self.view.load(QUrl(DISK_FOLDER_URL + quote(path[len("/disk"):])))
        self.win._on_log(f"[обход] открываю {path}")

    def _on_loaded(self, ok):
        self.loading = False


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("YandexDL Desktop (прототип)")
        self.resize(1400, 900)

        # --- Flask-сервер в потоке этого же процесса ---
        self.bridge = LogBridge()
        self.bridge.log.connect(self._on_log)
        self.bridge.stats.connect(self._on_stats)
        self.server_thread = ServerThread(on_log=self._log_from_thread)
        self.server_thread.start()

        # --- общий профиль для всех обычных вкладок (одна "банка" кук) ---
        profile_dir = os.path.join(user_data_dir(), "browser-profile")
        os.makedirs(profile_dir, exist_ok=True)
        self.browser_profile = QWebEngineProfile("yandexdl-browser", self)
        self.browser_profile.setPersistentStoragePath(profile_dir)
        self.browser_profile.setPersistentCookiesPolicy(
            QWebEngineProfile.PersistentCookiesPolicy.ForcePersistentCookies
        )

        # --- тулбар: назад/вперёд/обновить/адресная строка/новая вкладка/лог ---
        toolbar = QToolBar("Навигация")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        self.back_btn = QToolButton(text="◀")
        self.fwd_btn = QToolButton(text="▶")
        self.reload_btn = QToolButton(text="⟳")
        self.back_btn.clicked.connect(lambda: self._current_view() and self._current_view().back())
        self.fwd_btn.clicked.connect(lambda: self._current_view() and self._current_view().forward())
        self.reload_btn.clicked.connect(lambda: self._current_view() and self._current_view().reload())
        toolbar.addWidget(self.back_btn)
        toolbar.addWidget(self.fwd_btn)
        toolbar.addWidget(self.reload_btn)

        self.address_bar = QLineEdit()
        self.address_bar.returnPressed.connect(self._navigate_from_address_bar)
        toolbar.addWidget(self.address_bar)

        new_tab_btn = QToolButton(text="+ вкладка")
        new_tab_btn.clicked.connect(lambda: self._open_tab(DEFAULT_NEW_TAB_URL))
        toolbar.addWidget(new_tab_btn)

        # обход папок для раздела «Файлы» (то же, что кнопка на странице статуса)
        self.crawl_btn = QToolButton(text="Обойти папки")
        self.crawl_btn.setToolTip("Открыть все папки Диска по очереди, чтобы собрать список файлов для вкладки «Файлы»")
        toolbar.addWidget(self.crawl_btn)

        log_btn = QToolButton(text="Лог")
        toolbar.addWidget(log_btn)

        # --- вкладки ---
        self.tabs = QTabWidget()
        self.tabs.setTabsClosable(True)
        self.tabs.setMovable(True)
        self.tabs.tabCloseRequested.connect(self._close_tab)
        self.tabs.currentChanged.connect(self._on_current_tab_changed)
        self.setCentralWidget(self.tabs)

        # вкладка 0: наш сайт, закреплена (без крестика)
        self.site_view = self._make_view(profile=None)  # дефолтный off-the-record профиль — ок для своего же localhost
        self.site_view.load(QUrl("about:blank"))
        self.tabs.addTab(self.site_view, "YandexDL")
        self.tabs.tabBar().setTabButton(0, self.tabs.tabBar().ButtonPosition.RightSide, None)

        # вкладка 1: обычный браузер, сразу на Диске
        self._open_tab(DEFAULT_NEW_TAB_URL)
        self.tabs.setCurrentIndex(1)

        # --- лог-панель снизу, по умолчанию скрыта ---
        self.log_dock = LogDock(self)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.log_dock)
        self.log_dock.hide()
        log_btn.clicked.connect(lambda: self.log_dock.setVisible(not self.log_dock.isVisible()))

        # --- когда сервер поднялся, подставляем его адрес в закреплённую вкладку ---
        QTimer.singleShot(500, self._load_site_tab)

        # --- захват CDP: следит сразу за всеми вкладками с disk.yandex ---
        self.capture = CDPCapture(
            devtools_port=DEVTOOLS_PORT,
            on_log=self._log_from_thread,
            on_stats=self._stats_from_thread,
        )
        QTimer.singleShot(1500, self.capture.start)

        # --- обход папок: ждёт команды со страницы статуса или с тулбара ---
        self.crawler = FolderCrawler(self)
        self.crawl_btn.clicked.connect(self.crawler.toggle)
        QTimer.singleShot(2000, self.crawler.start)

    # ---------------------------------------------------------- вкладки

    def _make_view(self, profile):
        view = QWebEngineView()
        page = QWebEnginePage(profile, view) if profile is not None else QWebEnginePage(view)
        view.setPage(page)
        view.urlChanged.connect(lambda url, v=view: self._on_url_changed(v, url))
        view.titleChanged.connect(lambda title, v=view: self._on_title_changed(v, title))
        if profile is not None:
            # вход по Face ID/Touch ID/Windows Hello/ключу нужен только на "настоящих"
            # вкладках браузера (Диск), не на нашей служебной вкладке с localhost
            attach_webauthn_handler(page, parent_widget=self)
        return view

    def _open_tab(self, url: str):
        view = self._make_view(profile=self.browser_profile)
        view.load(QUrl(url))
        idx = self.tabs.addTab(view, "Новая вкладка")
        self.tabs.setCurrentIndex(idx)
        return view

    def _close_tab(self, index: int):
        if index == 0:
            return  # вкладку с нашим сайтом не закрываем
        widget = self.tabs.widget(index)
        self.tabs.removeTab(index)
        if widget:
            widget.deleteLater()

    def _current_view(self):
        return self.tabs.currentWidget()

    def _on_current_tab_changed(self, index):
        view = self.tabs.widget(index)
        if view is not None:
            self.address_bar.setText(view.url().toString())

    def _on_url_changed(self, view, url):
        if view is self._current_view():
            self.address_bar.setText(url.toString())

    def _on_title_changed(self, view, title):
        idx = self.tabs.indexOf(view)
        if idx > 0 and title:  # вкладку 0 не переименовываем
            crawler = getattr(self, "crawler", None)   # первая вкладка открывается раньше, чем он создан
            if crawler is not None and view is crawler.view:
                title = "Обход: " + title
            self.tabs.setTabText(idx, title[:24])

    def _navigate_from_address_bar(self):
        text = self.address_bar.text().strip()
        if not text:
            return
        if "://" not in text:
            text = "https://" + text
        view = self._current_view()
        if view is not None:
            view.load(QUrl(text))

    def _load_site_tab(self):
        if self.server_thread.base_url:
            self.site_view.load(QUrl(self.server_thread.base_url + "/status"))
        else:
            QTimer.singleShot(500, self._load_site_tab)  # сервер ещё не поднялся — ждём

    # ---- мосты из фоновых потоков в GUI-поток (через Qt-сигналы — thread-safe) ----

    def _log_from_thread(self, text: str):
        self.bridge.log.emit(text)

    def _stats_from_thread(self, **kw):
        self.bridge.stats.emit(kw)

    def _on_log(self, text: str):
        self.log_dock.append_log(text)
        print(text, flush=True)   # и в консоль/журнал — на сервере без экрана лог иначе не увидеть

    def _on_stats(self, d: dict):
        self.log_dock.update_stats(d)

    def closeEvent(self, event):
        self.crawler.stop()
        self.capture.stop()
        self.server_thread.stop()
        super().closeEvent(event)


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
