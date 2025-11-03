"""
TradingView 데이터 크롤링 + DB 저장 통합 자동화 시스템 (ver2: Auto-Recover / Progress / Summary)
- 기존 코드 기반 안정화 및 운영 편의 기능 추가
- 주요 추가 기능:
  1) 자동 재시도 (기본 3회) 및 예외 로깅 강화
  2) tqdm 진행률 표시 (전체 종목/남은 종목)
  3) 실행 요약 리포트 summary_report.csv 생성 (성공/실패/시도횟수/처리행수/에러메시지)
  4) 재시작 복구: 기존 summary_report.csv의 성공 종목은 스킵
  5) ChromeDriver 자동 설치 (webdriver_manager)로 버전 불일치 문제 최소화
  6) 드라이버/DB/SSH 자원 안전 종료 보장

필요 패키지(예):
pip install selenium pymysql sshtunnel python-dotenv webdriver-manager tqdm pandas

주의:
- TradingView DOM이 바뀌면 일부 XPATH가 동작하지 않을 수 있습니다.
- 실제 서비스 시 계정 보호를 위해 ID/PW/쿠키 관리에 유의하세요.
"""

from __future__ import annotations
import argparse
import os
import sys
import csv
import json
import time
import traceback
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import logging
import shutil
import tempfile

# ---- 추가 패키지 ----
try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pd = None

try:
    from tqdm import tqdm as _tqdm
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    def _tqdm(iterable, *args, **kwargs):
        """Fallback tqdm replacement that simply iterates."""
        for item in iterable:
            yield item

tqdm = _tqdm

# 누락 여부 추적 (필수 의존성)
CRITICAL_DEPENDENCIES: List[str] = []

# DB 관련 모듈
try:
    from sshtunnel import SSHTunnelForwarder
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    SSHTunnelForwarder = None
    CRITICAL_DEPENDENCIES.append("sshtunnel")

try:
    import pymysql
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pymysql = None
    CRITICAL_DEPENDENCIES.append("pymysql")

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    def load_dotenv(*_args, **_kwargs):
        """Fallback when python-dotenv is unavailable."""
        return False

# 웹 크롤링 관련 모듈
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.chrome.service import Service
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    webdriver = None
    By = Options = WebDriverWait = EC = TimeoutException = NoSuchElementException = ElementClickInterceptedException = ActionChains = None
    Service = None
    CRITICAL_DEPENDENCIES.append("selenium")

# ChromeDriver 자동 설치
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    ChromeDriverManager = None
    CRITICAL_DEPENDENCIES.append("webdriver-manager")

# -----------------------------
# SSH 및 DB 설정 (필요시 .env 로 오버라이드)
# -----------------------------
SSH_HOST = os.environ.get("SSH_HOST", "ahnbi2.suwon.ac.kr")
SSH_PORT = int(os.environ.get("SSH_PORT", "22"))
SSH_USER = os.environ.get("SSH_USER", "etf2")
SSH_PASS = os.environ.get("SSH_PASS", "deepdata")

# 로컬 포트 포워딩 설정
LOCAL_BIND_HOST = os.environ.get("LOCAL_BIND_HOST", "127.0.0.1")
LOCAL_BIND_PORT = int(os.environ.get("LOCAL_BIND_PORT", "3309"))

# 원격 DB 설정
DB_REMOTE_HOST = os.environ.get("DB_REMOTE_HOST", "127.0.0.1")
DB_REMOTE_PORT = int(os.environ.get("DB_REMOTE_PORT", "5100"))

# DB 접속 정보
DB_USER = os.environ.get("DB_USER", "etf2")
DB_PASS = os.environ.get("DB_PASS", "deepdata")
DB_NAME = os.environ.get("DB_NAME", "etf2_db")

# -----------------------------
# 크롤링 설정
# -----------------------------
COOKIES_FILE = os.environ.get("COOKIES_FILE", "tradingview_cookies.json")
DOWNLOAD_ROOT = Path(os.environ.get("TV_DOWNLOAD_ROOT", "./downloads")).resolve()
USER_PROFILE_DIR = Path(os.environ.get("TV_CHROME_PROFILE", "./chrome_profile")).resolve()

# 지표 설정 (필요시 환경변수로 커스터마이즈 가능)
INDICATORS = json.loads(os.environ.get("TV_INDICATORS", '["Relative Strength Index", "Moving Average Convergence Divergence"]'))

# 종목 리스트 (환경변수 TV_TICKERS_JSON 로 대체 가능)
_default_tickers = ["MSFT", "AAPL", "NVDA"]
TV_TICKERS = json.loads(os.environ.get("TV_TICKERS_JSON", json.dumps(_default_tickers)))

# 시간프레임 설정: (표시, 라벨, URL interval, 과거데이터 Lazy 로딩 필요 여부)
TIMEFRAMES = [
    ('12M', '12 months', '12M',   False),  # 연
    ('M',   '1 month',   '1M',    False),  # 월
    ('W',   '1 week',    '1W',    False),  # 주
    ('D',   '1 day',     '1D',    True),   # 일
    ('1h',  '1 hour',    '60',    True),   # 시
    ('10m', '10 minutes','10',    True),   # 10분
]

# -----------------------------
# 로깅 설정
# -----------------------------
LOG_FILE = Path("tv_task4_ver2.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# -----------------------------
# 유틸
# -----------------------------
def ensure_dir(p: Path) -> None:
    """디렉토리 생성 보장 (파일 경로를 넣어도 부모까지 안전 생성)"""
    try:
        if p.exists() and p.is_file():
            p.parent.mkdir(parents=True, exist_ok=True)
            return
        if p.suffix:
            p.parent.mkdir(parents=True, exist_ok=True)
            return
        p.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

# -----------------------------
# SSH 및 DB
# -----------------------------
def create_ssh_tunnel() -> SSHTunnelForwarder:
    """SSH 터널 생성"""
    if SSHTunnelForwarder is None:
        raise RuntimeError("sshtunnel 패키지가 설치되어야 합니다.")
    logger.info("DB: SSH 터널 연결 시도...")
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        local_bind_address=(LOCAL_BIND_HOST, LOCAL_BIND_PORT),
        remote_bind_address=(DB_REMOTE_HOST, DB_REMOTE_PORT)
    )
    tunnel.start()
    logger.info("DB: SSH 터널 연결 성공")
    return tunnel

def db_connect() -> pymysql.Connection:
    """MariaDB 연결"""
    if pymysql is None:
        raise RuntimeError("pymysql 패키지가 설치되어야 합니다.")
    logger.info("DB: MariaDB 연결 시도...")
    try:
        conn = pymysql.connect(
            host=LOCAL_BIND_HOST,
            port=LOCAL_BIND_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            autocommit=False,
            charset="utf8mb4"
        )
        logger.info("DB: MariaDB 연결 성공")
        return conn
    except Exception as e:
        logger.error(f"DB: 연결 실패: {e}")
        raise

# -----------------------------
# CSV 전처리
# -----------------------------
def process_csv_for_db(csv_path: Path, symbol: str, timeframe: str) -> list:
    """CSV → DB 저장용 레코드 리스트 변환"""
    logger.info(f"데이터 처리 시작 → {csv_path.name}")
    processed_rows: list = []

    def get_value(row: dict, names: list, default=None):
        for n in names:
            if n in row and row[n] not in (None, ''):
                return row[n]
        # case-insensitive fallback
        lower_map = {k.lower(): v for k, v in row.items()}
        for n in names:
            if n.lower() in lower_map and lower_map[n.lower()] not in (None, ''):
                return lower_map[n.lower()]
        return default

    def parse_float(x):
        if x is None or x == '':
            return None
        try:
            return float(str(x).replace(',', ''))
        except Exception:
            return None

    def parse_int(x):
        try:
            return int(float(str(x).replace(',', '')))
        except Exception:
            return None

    def parse_time(s: str) -> str:
        if s is None:
            raise ValueError('time is None')
        s = str(s).strip().replace('Z', '')
        fmts = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass
        # ISO fallback
        try:
            from datetime import datetime as _dt
            dt = _dt.fromisoformat(s)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            raise ValueError(f'날짜 형식 파싱 실패: {s}')

    with csv_path.open(encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            try:
                time_raw = get_value(row, ['time', 'timestamp', 'Date'])
                if not time_raw:
                    continue
                time_parsed = parse_time(time_raw)

                open_v = parse_float(get_value(row, ['open', 'Open']))
                high_v = parse_float(get_value(row, ['high', 'High']))
                low_v  = parse_float(get_value(row, ['low', 'Low']))
                close_v= parse_float(get_value(row, ['close', 'Close']))
                vol_v  = parse_int(get_value(row, ['volume', 'Volume', 'Volume USD'])) or 0
                rsi_v  = parse_float(get_value(row, ['rsi', 'RSI'])) or 0.0
                macd_v = parse_float(get_value(row, ['macd', 'MACD'])) or 0.0

                if None in (open_v, high_v, low_v, close_v):
                    continue

                processed_rows.append({
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'time': time_parsed,
                    'open': open_v, 'high': high_v, 'low': low_v, 'close': close_v,
                    'volume': vol_v, 'rsi': rsi_v, 'macd': macd_v
                })
            except Exception as e:
                logger.warning(f"행 처리 오류(무시): {e} (i={i})")
                continue

    logger.info(f"데이터 처리 완료: {len(processed_rows)}행")
    return processed_rows

# -----------------------------
# DB 저장
# -----------------------------
def save_to_db(conn: pymysql.Connection, data: list, symbol: str, timeframe: str) -> int:
    """DB 저장 (테이블 자동 생성 + upsert). 반환: 처리 행수"""
    cur = conn.cursor()
    table_name = f"{symbol.lower()}_{timeframe.lower()}"

    try:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            symbol        VARCHAR(32) NOT NULL,
            timeframe     VARCHAR(16) NOT NULL,
            time          DATETIME NOT NULL,
            open          DECIMAL(18, 8) NOT NULL,
            high          DECIMAL(18, 8) NOT NULL,
            low           DECIMAL(18, 8) NOT NULL,
            close         DECIMAL(18, 8) NOT NULL,
            volume        BIGINT NOT NULL,
            rsi           DECIMAL(10, 5),
            macd          DECIMAL(10, 5),
            PRIMARY KEY (symbol, timeframe, time)
        )
        """
        cur.execute(create_table_sql)
        logger.info(f"DB: 테이블 확인/생성 → {table_name}")

        insert_sql = f"""
        INSERT INTO `{table_name}`
        (symbol, timeframe, time, open, high, low, close, volume, rsi, macd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low),
            close=VALUES(close), volume=VALUES(volume),
            rsi=VALUES(rsi), macd=VALUES(macd)
        """

        batch_size = 1000
        total_rows = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            values = [(
                row['symbol'], row['timeframe'], row['time'],
                row['open'], row['high'], row['low'], row['close'],
                row['volume'], row['rsi'], row['macd']
            ) for row in batch]
            cur.executemany(insert_sql, values)
            total_rows += len(batch)
        conn.commit()
        logger.info(f"DB: 저장 완료 → {table_name}, {total_rows}행")
        return total_rows
    except Exception as e:
        logger.error(f"DB 저장 중 오류: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

# -----------------------------
# 드라이버
# -----------------------------
def setup_driver(download_dir: Path) -> webdriver.Chrome:
    """Chrome 드라이버 설정 (webdriver_manager 자동 설치)"""
    if webdriver is None or Options is None or ChromeDriverManager is None or Service is None:
        raise RuntimeError("selenium 및 webdriver-manager 패키지가 설치되어야 합니다.")
    logger.info("DRIVER: Chrome 드라이버 설정...")
    ensure_dir(USER_PROFILE_DIR)
    ensure_dir(download_dir)

    chrome_options = Options()
    chrome_options.add_argument(f"--user-data-dir={str(USER_PROFILE_DIR)}")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_window_size(1600, 1000)
    logger.info("DRIVER: 설정 완료")
    return driver

# -----------------------------
# 쿠키/로그인
# -----------------------------
def save_cookies(driver: webdriver.Chrome) -> None:
    logger.info(f"COOKIE: 저장 시도 → {COOKIES_FILE}")
    try:
        cookies = driver.get_cookies()
        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump(cookies, f)
        logger.info("COOKIE: 저장 완료")
    except Exception as e:
        logger.warning(f"COOKIE: 저장 실패: {e}")

def load_cookies(driver: webdriver.Chrome) -> bool:
    if not Path(COOKIES_FILE).exists():
        return False
    try:
        driver.get("https://www.tradingview.com/")
        time.sleep(5)
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            cookies = json.load(f)
            for cookie in cookies:
                # domain 필드 없이 저장된 경우를 대비
                if 'sameSite' in cookie and cookie['sameSite'] is None:
                    cookie.pop('sameSite', None)
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    # domain mismatch 시도 시 무시
                    pass
        logger.info("COOKIE: 로드 완료")
        return True
    except Exception as e:
        logger.warning(f"COOKIE: 로드 실패: {e}")
        return False

def manual_login(driver: webdriver.Chrome) -> None:
    logger.info("로그인 필요: 브라우저에서 수동 로그인 후 엔터")
    driver.get("https://www.tradingview.com/")
    input(">> TradingView에 로그인 완료 후 Enter를 누르세요... ")
    save_cookies(driver)

# -----------------------------
# 차트/지표/다운로드
# -----------------------------
def go_chart(driver: webdriver.Chrome, symbol: str, interval: str | None = None) -> None:
    base = f"https://www.tradingview.com/chart/?symbol={symbol}"
    url = base if interval is None else f"{base}&interval={interval}"
    logger.info(f"NAVIGATE: {url}")
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'canvas[data-name="pane-top-canvas"]'))
        )
        logger.info("NAVIGATE: 차트 로드 완료")
    except TimeoutException:
        logger.warning("NAVIGATE: 차트 로드 지연")

def lazy_load_short_tf(driver: webdriver.Chrome, tf_short: str, tf_label: str) -> None:
    logger.info(f"LOAD: 과거 데이터 로딩({tf_short}:{tf_label})")
    try:
        canvas = driver.find_element(By.CSS_SELECTOR, 'canvas[data-name="pane-top-canvas"]')
    except NoSuchElementException:
        logger.warning("LOAD: 캔버스 찾기 실패")
        return

    size = canvas.size
    center_x = size["width"] // 2
    center_y = size["height"] // 2

    # 축소
    for _ in range(30):
        wheel_script = f"""
        var canvas = arguments[0];
        var rect = canvas.getBoundingClientRect();
        var clientX = rect.left + {center_x};
        var clientY = rect.top + {center_y};
        canvas.dispatchEvent(new WheelEvent('wheel', {{
            clientX: clientX, clientY: clientY, deltaY: 200, bubbles: true
        }}));
        """
        driver.execute_script(wheel_script, canvas)
        time.sleep(0.05)

    # 드래그로 과거 로딩
    drag_count = 10 if tf_short == "D" else (30 if tf_short == "1h" else 50)
    for i in range(drag_count):
        start_x = size["width"] - 100
        end_x = 100
        drag_script = f"""
        var canvas = arguments[0];
        var rect = canvas.getBoundingClientRect();
        canvas.dispatchEvent(new MouseEvent('mousedown', {{
            clientX: rect.left + {start_x}, clientY: rect.top + {center_y}, bubbles: true
        }}));
        var steps = 10; var stepX = ({end_x} - {start_x}) / steps;
        for (var j = 1; j <= steps; j++) {{
            canvas.dispatchEvent(new MouseEvent('mousemove', {{
                clientX: rect.left + {start_x} + stepX * j, clientY: rect.top + {center_y}, bubbles: true
            }}));
        }}
        canvas.dispatchEvent(new MouseEvent('mouseup', {{
            clientX: rect.left + {end_x}, clientY: rect.top + {center_y}, bubbles: true
        }}));
        """
        driver.execute_script(drag_script, canvas)
        time.sleep(0.7)

def add_indicator(driver: webdriver.Chrome, keyword: str) -> None:
    logger.info(f"INDICATOR: 추가 → {keyword}")
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[data-name='pane-top-canvas']"))
    )
    time.sleep(2)
    for attempt in range(3):
        try:
            # 지표 버튼 찾기
            indicators_button = None
            button_selectors = [
                "//button[contains(@aria-label, 'Indicators')]",
                "//button[contains(@aria-label, '지표')]",
                "//button[contains(@data-name, 'indicators')]",
                "//button[.//span[contains(text(), 'Indicators')]]",
                "//div[contains(@data-name, 'header-toolbar')]//button"
            ]
            for selector in button_selectors:
                try:
                    indicators_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    if indicators_button:
                        break
                except Exception:
                    continue
            if not indicators_button:
                continue
            indicators_button.click()
            time.sleep(1)

            # 검색창 입력
            search_selectors = [
                "//div[@role='dialog']//input[@type='text']",
                "//input[contains(@placeholder, 'Search')]",
                "//input[contains(@placeholder, '검색')]",
                "//div[@data-dialog-name]//input"
            ]
            search_input = None
            for selector in search_selectors:
                try:
                    search_input = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    if search_input:
                        break
                except Exception:
                    continue
            if not search_input:
                continue
            search_input.clear()
            search_input.send_keys(keyword)
            time.sleep(1)

            # 결과 클릭
            result_selectors = [
                f"//div[@role='dialog']//span[contains(text(), '{keyword}')]",
                f"//div[contains(@class, 'search-results')]//span[contains(text(), '{keyword}')]",
                f"//div[@data-dialog-name]//span[contains(text(), '{keyword}')]",
                "//div[@role='dialog']//div[@role='button' or @role='option']//span[1]"
            ]
            clicked = False
            for selector in result_selectors:
                try:
                    result = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    result.click()
                    clicked = True
                    break
                except Exception:
                    continue
            if clicked:
                logger.info("INDICATOR: 선택 완료")
                return
        except Exception as e:
            logger.warning(f"INDICATOR: 시도 실패({attempt+1}/3) - {e}")
            try:
                driver.find_element(By.XPATH, "//button[contains(@aria-label, 'Close')]").click()
            except Exception:
                pass
            time.sleep(2)
    logger.warning(f"INDICATOR: 최종 실패 → {keyword}")

def _click_first(driver: webdriver.Chrome, selectors: List[str], wait: int = 15) -> None:
    """여러 XPATH 중 먼저 클릭 가능한 요소를 클릭한다."""
    last_exc: Optional[Exception] = None
    for selector in selectors:
        try:
            element = WebDriverWait(driver, wait).until(
                EC.element_to_be_clickable((By.XPATH, selector))
            )
            driver.execute_script("arguments[0].click();", element)
            return
        except Exception as exc:  # pragma: no cover - selenium runtime
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise TimeoutException("클릭 가능한 요소를 찾지 못했습니다.")


def export_csv(driver: webdriver.Chrome) -> None:
    logger.info("EXPORT: CSV 내보내기 시작")

    export_button_selectors = [
        "//button[contains(@aria-label, 'Export')]",
        "//button[contains(@data-name, 'export-chart-data')]",
        "//div[contains(@data-name, 'header-toolbar')]//button[.//span[contains(text(), 'Export')]]",
        "//button[contains(., '내보내기')]",
    ]
    _click_first(driver, export_button_selectors)
    time.sleep(1)

    export_menu_selectors = [
        "//div[@role='menuitem' or @role='option']//span[contains(text(), 'Export chart data')]",
        "//div[contains(@class, 'menu')]//div[contains(text(), 'Export chart data')]",
        "//div[@role='menuitem' or @role='option']//span[contains(text(), '내보내기')]",
        "//div[contains(@data-name, 'menu')]//span[contains(text(), 'Download data')]",
    ]
    _click_first(driver, export_menu_selectors)
    time.sleep(1)

    final_export_selectors = [
        "//button[contains(@class, 'button') and .//span[contains(text(), 'Export')]]",
        "//button[contains(@class, 'button') and contains(text(), 'Export')]",
        "//button[contains(@aria-label, 'export-data-dialog-export-button')]",
        "//button[contains(., '내보내기')]",
    ]
    _click_first(driver, final_export_selectors)
    logger.info("EXPORT: 실행 완료")

def wait_for_download(download_dir: Path, timeout: int = 90) -> Path:
    logger.info(f"DOWNLOAD: 대기({timeout}s)")
    ensure_dir(download_dir)
    end_time = time.time() + timeout
    last_file = None
    while time.time() < end_time:
        crs = list(download_dir.glob("*.crdownload"))
        if crs:
            time.sleep(0.5); continue
        csvs = list(download_dir.glob("*.csv"))
        if not csvs:
            time.sleep(0.5); continue
        latest = max(csvs, key=lambda x: x.stat().st_mtime)
        # 안정화 체크
        s1 = latest.stat().st_size
        time.sleep(1.0)
        s2 = latest.stat().st_size
        if s1 == s2 and s2 > 0:
            logger.info(f"DOWNLOAD: 완료 → {latest.name}")
            return latest
    raise TimeoutException("CSV 다운로드 시간 초과")

# -----------------------------
# 심볼 처리 with 재시도
# -----------------------------
def process_symbol(driver: webdriver.Chrome, symbol: str, download_dir: Path, db_conn: pymysql.Connection, progress_bar: Optional[object] = None) -> Dict[str, int]:
    """한 종목의 모든 TF 처리. 반환: {'inserted_rows': n}"""
    logger.info(f"{'='*18} [{symbol}] 처리 시작 {'='*18}")
    inserted_total = 0

    symbol_dir = download_dir / symbol
    ensure_dir(symbol_dir)

    for tf_short, tf_label, url_interval, requires_lazy in TIMEFRAMES:
        logger.info(f"[{symbol}] TF 처리 → {tf_short} ({tf_label})")

        tf_dir = symbol_dir / tf_short
        ensure_dir(tf_dir)

        # 1) 차트 이동
        go_chart(driver, symbol, url_interval)
        time.sleep(3)

        # 2) 지표 추가(있으면 좋고, 실패해도 계속 진행)
        for indicator in INDICATORS:
            try:
                add_indicator(driver, indicator)
                time.sleep(1)
            except Exception as e:
                logger.warning(f"[{symbol}] 지표 추가 실패({indicator}): {e}")

        # 3) 과거 로딩
        if requires_lazy:
            try:
                lazy_load_short_tf(driver, tf_short, tf_label)
            except Exception as e:
                logger.warning(f"[{symbol}] 과거 로딩 실패: {e}")

        # 4) CSV 내보내기 및 이동
        try:
            export_csv(driver)
            csv_file = wait_for_download(download_dir)
            target_file = tf_dir / f"{symbol}_{tf_short}_{csv_file.name}"
            shutil.move(str(csv_file), str(target_file))

            # 5) 전처리 & DB 저장
            data = process_csv_for_db(target_file, symbol, tf_short)
            if data:
                n = save_to_db(db_conn, data, symbol, tf_short)
                inserted_total += n
            logger.info(f"[{symbol}] {tf_short} 완료: 파일={target_file.name}, 저장행수={len(data)}")
        except Exception as e:
            logger.error(f"[{symbol}] {tf_short} 처리 오류: {e}")
        finally:
            # 전체 진행률 바가 전달되면 TF 단위로 진행률을 갱신
            if progress_bar is not None:
                try:
                    progress_bar.update(1)
                except Exception:
                    pass

    logger.info(f"{'='*18} [{symbol}] 처리 완료 (총 저장행수={inserted_total}) {'='*18}\n")
    return {"inserted_rows": inserted_total}

def process_symbol_with_retry(driver, symbol, download_dir, db_conn, retries=3, progress_bar: Optional[object] = None) -> Dict[str, int]:
    last_err = None
    for attempt in range(1, retries+1):
        try:
            logger.info(f"[{symbol}] 시도 {attempt}/{retries}")
            result = process_symbol(driver, symbol, download_dir, db_conn, progress_bar=progress_bar)
            return {"status": "success", "attempt": attempt, **result}
        except Exception as e:
            last_err = e
            logger.warning(f"[{symbol}] 재시도 예정 ({attempt}/{retries}) - {e}")
            time.sleep(2)
    # 최종 실패
    return {"status": "fail", "attempt": retries, "error": str(last_err) if last_err else "unknown"}

# -----------------------------
# 복구/요약
# -----------------------------
def get_completed_symbols(summary_file: Path) -> set:
    if not summary_file.exists():
        return set()
    try:
        if pd is not None:
            df = pd.read_csv(summary_file)
            completed = set(df.loc[df["status"] == "success", "symbol"])
            return completed
        # pandas 미설치 시 CSV 모드로 처리
        completed: set = set()
        with summary_file.open(encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("status") == "success" and row.get("symbol"):
                    completed.add(row["symbol"])
        return completed
    except Exception:
        return set()

def save_summary(summary: list, summary_file: Path) -> None:
    ensure_dir(summary_file)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if pd is not None:
        df = pd.DataFrame(summary)
        df["timestamp"] = timestamp
        df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    else:
        fieldnames = ["symbol", "status", "attempt", "inserted_rows", "error", "timestamp"]
        with summary_file.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in summary:
                row_copy = dict(row)
                row_copy["timestamp"] = timestamp
                writer.writerow({k: row_copy.get(k, "") for k in fieldnames})
    logger.info(f"📊 실행 결과 요약 저장 → {summary_file}")

# -----------------------------
# 진단 도구
# -----------------------------
def run_self_test() -> None:
    """외부 의존성 없이 핵심 전처리/요약 로직을 빠르게 점검."""
    print("\n" + "-" * 60)
    print(" Running self-test (no external services required) ")
    print("-" * 60)

    temp_dir = Path(tempfile.mkdtemp(prefix="tv_task4_test_"))
    try:
        sample_csv = temp_dir / "sample.csv"
        with sample_csv.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["time", "open", "high", "low", "close", "volume", "rsi", "macd"],
            )
            writer.writeheader()
            writer.writerow({
                "time": "2024-01-01 09:00:00",
                "open": "100",
                "high": "110",
                "low": "95",
                "close": "105",
                "volume": "1,000",
                "rsi": "55.5",
                "macd": "0.8",
            })
            writer.writerow({
                "time": "2024-01-02 09:00:00",
                "open": "105",
                "high": "112",
                "low": "101",
                "close": "108",
                "volume": "1,200",
                "rsi": "57.1",
                "macd": "0.9",
            })

        processed = process_csv_for_db(sample_csv, "TEST", "D")
        if not processed:
            raise RuntimeError("CSV processing self-test failed: no rows processed")

        summary_path = temp_dir / "summary.csv"
        summary_rows = [{
            "symbol": "TEST",
            "status": "success",
            "attempt": 1,
            "inserted_rows": len(processed),
            "error": "",
        }]
        save_summary(summary_rows, summary_path)
        completed = get_completed_symbols(summary_path)
        if "TEST" not in completed:
            raise RuntimeError("Summary read self-test failed: symbol not detected")

        print("Self-test completed successfully. All offline checks passed.\n")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

# -----------------------------
# 메인
# -----------------------------
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="TradingView 데이터 수집 자동화 스크립트")
    parser.add_argument("--self-test", action="store_true", help="외부 서비스 없이 핵심 로직을 점검")
    args = parser.parse_args(argv)

    if args.self_test:
        run_self_test()
        return

    if CRITICAL_DEPENDENCIES:
        missing = ", ".join(sorted(set(CRITICAL_DEPENDENCIES)))
        raise SystemExit(
            "필수 패키지가 누락되었습니다. pip install 로 설치 후 다시 실행하세요: "
            f"{missing}"
        )

    print("\n" + "="*60)
    print(" TradingView 데이터 수집 + DB 저장 자동화 (ver2 Auto-Recover) ")
    print("="*60 + "\n")

    load_dotenv()
    ensure_dir(DOWNLOAD_ROOT)

    summary_file = Path("summary_report.csv")
    completed = get_completed_symbols(summary_file)
    pending = [s for s in TV_TICKERS if s not in completed]

    if completed:
        logger.info(f"복구 모드: 완료된 종목 스킵 → {sorted(list(completed))}")
    logger.info(f"대상 종목 수: {len(pending)} / 전체 {len(TV_TICKERS)}")

    driver = None
    tunnel = None
    db_conn = None
    summary_rows = []
    task_bar = None

    try:
        driver = setup_driver(DOWNLOAD_ROOT)

        # 로그인 처리
        if not load_cookies(driver):
            manual_login(driver)

        tunnel = create_ssh_tunnel()
        db_conn = db_connect()

        # 전체 TF 기반 진행률 표시
        total_tasks = max(1, len(pending) * len(TIMEFRAMES))
        task_bar = tqdm(total=total_tasks, desc="전체 진행", unit="TF")
        try:
            for symbol in tqdm(pending, desc="📈 남은 종목 처리", unit="symbol"):
                result = process_symbol_with_retry(driver, symbol, DOWNLOAD_ROOT, db_conn, retries=3, progress_bar=task_bar)
                row = {
                    "symbol": symbol,
                    "status": result.get("status", "fail"),
                    "attempt": result.get("attempt", 0),
                    "inserted_rows": result.get("inserted_rows", 0),
                    "error": result.get("error", "")
                }
                summary_rows.append(row)
                # 즉시 저장(중간 저장) → 비정상 종료에도 복구 용이
                save_summary(summary_rows, summary_file)
        finally:
            try:
                if task_bar is not None:
                    task_bar.close()
            except Exception:
                pass

    except Exception as e:
        logger.error(f"치명적 오류: {e}\n{traceback.format_exc()}")
        # 그래도 지금까지의 요약은 저장
        if summary_rows:
            save_summary(summary_rows, summary_file)
        raise
    finally:
        try:
            if db_conn:
                db_conn.close()
                logger.info("DB 연결 종료")
        except Exception:
            pass
        try:
            if tunnel:
                tunnel.stop()
                logger.info("SSH 터널 종료")
        except Exception:
            pass
        try:
            if driver:
                driver.quit()
                logger.info("드라이버 종료")
        except Exception:
            pass
        print("\n프로그램 종료")


if __name__ == "__main__":
    main()
