import json
import logging
import sys
import time
import uuid
from functools import wraps
from pathlib import Path
from urllib.parse import urlencode
import pendulum
import polars as pl
import requests
import yaml
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm


# Логирование
LOG_FILE = "hh_search.log"
LOG_ENCODING = "utf8"
BASE_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(etl_id)s] %(message)s"

formatter = logging.Formatter(BASE_LOG_FORMAT)
file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding=LOG_ENCODING)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, console_handler])
logger = logging.getLogger("hh_search")
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("urllib3").propagate = False


# конфигурации
def load_config(config_path="conf/config.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


conf = load_config()

TELEGRAM_TOKEN = conf.get("telegram", {}).get("token")
TELEGRAM_CHAT_ID = conf.get("telegram", {}).get("chat_id")


# Функция отправки в ТГ, при ошибке пишет предупреждение в лог
def send_telegram_message(text: str):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token or chat_id not set, message skipped")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logger.info("Telegram message sent successfully")
    except Exception as e:
        logger.warning(f"Failed to send Telegram message (ignoring): {e}")


# Функция ретрая
def retry(max_attempts=5, wait_seconds=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    if attempt > 1:
                        logger.warning(f"Starting attempt #{attempt}")
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(
                        f"Attempt #{attempt} failed! Error: {e}. "
                        f"{'Retrying in ' + str(wait_seconds) + 's' if attempt < max_attempts else 'No more attempts.'}"
                    )
                    if attempt == max_attempts:
                        logger.error(f"All attempts failed. Last error: {e}")
                        raise
                    time.sleep(wait_seconds)
            return None
        return wrapper
    return decorator


@retry(max_attempts=5, wait_seconds=2)
def fetch_page(url: str) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp


# Основной ЕТЛ процесс
def run_etl() -> None:
    etl_id = str(uuid.uuid4())
    global logger
    logger = logging.LoggerAdapter(logger, {"etl_id": etl_id})

    start_time = pendulum.now("UTC")
    total_vacancies = 0
    pages_processed = 0
    error_occurred = False
    error_msg = ""

    try:
        db_engine = create_engine(conf["database"]["url"])

        # HWM
        with db_engine.connect() as conn:
            result = conn.execute(
                text("SELECT MAX(request_dttm) AS hwm FROM etl.hh_search")
            ).fetchone()
            hwm = result[0] if result else None

        if hwm is None:
            date_from = "01.01.2026 00:00:00"
        else:
            date_from = pendulum.instance(hwm).format("DD.MM.YYYY HH:mm:ss")

        logger.info(f"Searching vacancies from {date_from} (etl_id={etl_id})")

        # Параметры поиска
        base_params = {
            "area": conf["hh_api"]["area"],
            "text": conf["hh_api"]["text"],
            "search_field": conf["hh_api"]["search_field"],
            "date_from": date_from,
            "no_magic": "true",
            "items_on_page": conf["hh_api"].get("items_on_page", 20),
            "order_by": "publication_time",
            "enable_snippets": "true",
        }

        first_params = base_params.copy()
        first_params["page"] = 0
        first_url = f"https://hh.ru/search/vacancy?{urlencode(first_params)}"

        response = fetch_page(first_url)
        soup = BeautifulSoup(response.content, "html.parser")
        templates = soup.select("template#HH-Lux-InitialState")
        if not templates:
            raise RuntimeError("Template #HH-Lux-InitialState not found")
        initial_data = json.loads(templates[0].text)

        paging = initial_data["vacancySearchResult"]["paging"]
        if paging is None:
            logger.info("No new vacancies found.")
            total_pages = 0
        else:
            if paging.get("lastPage") is None:
                total_pages = paging["pages"][-1]["page"] + 1
            else:
                total_pages = paging["lastPage"]["page"] + 1

        # Parquet
        parquet_dir = Path(conf["storage"]["parquet_path"])
        parquet_dir.mkdir(parents=True, exist_ok=True)

        for page_num in tqdm(range(total_pages), desc="Processing pages"):
            logger.info(f"Processing page {page_num}")

            page_params = base_params.copy()
            page_params["page"] = page_num
            page_url = f"https://hh.ru/search/vacancy?{urlencode(page_params)}"

            request_dttm = pendulum.now("UTC")
            page_response = fetch_page(page_url)
            page_soup = BeautifulSoup(page_response.content, "html.parser")
            page_data = json.loads(page_soup.select("template#HH-Lux-InitialState")[0].text)

            vacancies = page_data["vacancySearchResult"]["vacancies"]
            rows = []
            for vac in vacancies:
                row = {
                    "request_dttm": request_dttm,
                    "vacancy_id": vac["vacancyId"],
                    "vacancy_title": vac["name"],
                    "company_id": vac["company"]["id"],
                    "company_title": vac["company"]["name"],
                    "company_visible_name": vac["company"]["visibleName"],
                    "publication_time": pendulum.parse(vac["publicationTime"]["$"]),
                    "last_change_time": pendulum.parse(vac["lastChangeTime"]["$"]),
                    "creation_time": pendulum.parse(vac["creationTime"]),
                    "is_adv": vac.get("@isAdv", "false"),
                    "snippet": json.dumps(vac["snippet"], ensure_ascii=False),
                    "responses_count": vac["responsesCount"],
                    "total_responses_count": vac["totalResponsesCount"],
                }
                rows.append(row)

            if rows:
                df = pl.DataFrame(rows)
                df = df.with_columns(pl.lit(etl_id).alias("etl_id"))

                df.write_database(
                    "etl.hh_search",
                    db_engine,
                    if_table_exists="append",
                )

                parquet_file = parquet_dir / f"hh_vacancies_{etl_id}_page{page_num}.parquet"
                df.write_parquet(parquet_file, compression=conf["storage"]["compression"])

                total_vacancies += len(rows)
                pages_processed += 1

        # Итог
        end_time = pendulum.now("UTC")
        duration = (end_time - start_time).in_words(locale="ru")
        success_message = (
            f"<b>Результаты поиска вакансий (etl_id={etl_id})</b>\n"
            f"Начало поиска: {start_time}\n"
            f"Обработано страниц: {pages_processed}\n"
            f"Новых вакансий: {total_vacancies}\n"
            f"Время выполнения: {duration}"
        )
        logger.info(success_message)
        send_telegram_message(success_message)

    except Exception as e:
        logger.exception("Critical error in ETL process")
        error_occurred = True
        error_msg = str(e)
        error_message = f"<b>Ошибка при выполнении ETL (etl_id={etl_id})</b>\n\n{error_msg}"
        send_telegram_message(error_message)

    finally:
        sys.exit(1 if error_occurred else 0)


if __name__ == "__main__":
    run_etl()