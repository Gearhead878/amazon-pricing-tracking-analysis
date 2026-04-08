import logging
import shutil
from lxml import etree
from src.db.sql_connector import get_mysql_connection
from datetime import datetime, date, timedelta
from src.crawler.scraper_helper import extract_text, sanitize_rows_for_sql
from collections import defaultdict
from decimal import Decimal
from src.crawler.amz_page_loader import AmazonPageFetcher
import sys
import tkinter as tk
from tkinter import messagebox
from pathlib import Path
import pandas as pd
import base64
from src.db.queries import (
    sql_fetch_asins,
    sql_reseller_written,
    sql_select_snapshot_ids,
    sql_select_resellers_base,
    sql_insert_amz_reseller_prices,
    sql_amz_product_snapshots_written,
    sql_fetch_asins_manual_base,
    sql_check_price_table_date,
    sql_export_latest_prices_by_date,
    sql_export_asin_suppressed
)
from src.crawler.amz_price_selectors import (
    PRODUCT_TITLE_XPATH,
    PINNED_OFFER_SELLER_LINK_XPATH,
    PINNED_OFFER_AMAZON_SELLER_TEXT_XPATH,
    PINNED_OFFER_DELIVERY_TIME_XPATH,
    PINNED_OFFER_PRICE_WHOLE_XPATH,
    PINNED_OFFER_PRICE_FRACTION_XPATH,
    BOTTOM_OFFERS_XPATH,
    BOTTOM_OFFER_PRICE_WHOLE_XPATH,
    BOTTOM_OFFER_PRICE_FRACTION_XPATH,
    BOTTOM_OFFER_SELLER_LINK_XPATH,
    BOTTOM_OFFER_AMAZON_SELLER_TEXT_XPATH,
    BOTTOM_OFFER_DELIVERY_TIME_XPATH,
)


logger = logging.getLogger(__name__)
AMAZON_OFFER_URL = (
    "https://www.amazon.com.au/dp/{asin}/ref=olp-opf-redir?"
    "aod=1&ie=UTF8&condition=NEW"
)
SCREENSHOT_DIR = Path(__file__).resolve().parent.parent.parent / "screenshot"

class AMZPriceCheck:
    def __init__(self, driver):
        self.driver = driver

    @staticmethod
    def _load_asins_for_date(td):
        with get_mysql_connection(dictionary=True) as (connection, cursor):
            cursor.execute(sql_fetch_asins, (td,))
            asins = cursor.fetchall()
            logger.info(f'Fetched {len(asins)} asins')
            return asins

    @staticmethod
    def _fetch_manual_asins(asins):
        if not isinstance(asins, list):
            raise TypeError ('Manual input ASINs must be a list')

        with get_mysql_connection(dictionary=True) as (connection, cursor):
            placeholders = ','.join(['%s'] * len(asins))
            sql_fetch_asins_manual = sql_fetch_asins_manual_base.format(placeholders=placeholders)
            cursor.execute(sql_fetch_asins_manual, asins)
            fetched_asins = cursor.fetchall()
            logger.info(f'Fetched {len(fetched_asins)} asins')
            if len(asins) != len(fetched_asins):
                logger.error("Some manual input ASINs can't be found in database")
                raise ValueError ("Some manual input ASINs can't be found in database")
            return fetched_asins


    @staticmethod
    def _persist_product_snapshots_batch(data):
        fields = [
            "asin_id",
            "snapshot_time",
            "snapshot_date",
            "title",
            "asin_suppressed",
            "is_carried"
        ]
        sql_data = sanitize_rows_for_sql(data, fields)
        try:
            with get_mysql_connection(dictionary=True) as (connection, cursor):
                cursor.executemany(sql_amz_product_snapshots_written, sql_data)
        except Exception as e:
            logger.exception(f"Can't persist product snapshots: {e}")
            raise

    @staticmethod
    def _convert_price(price):
        return Decimal(str(price).replace(',', ''))

    def _persist_reseller_offers_batch(self, snapshot_time, data):
        """
        Write the today's price into database
        :param snapshot_time: today's datetime object
        :param data: the data contains resellers infos and asin info
        :return: None
        """
        try:
            with get_mysql_connection(dictionary=True) as (connection, cursor):

                # write the reseller data
                unique_names = sorted({r['name'] for row in data for r in row['resellers'] if r.get('name')})

                if not unique_names:
                    logger.info(f'No resellers data in current batch{[row["asin_id"] for row in data]}')
                    return

                rows = [{'name': name} for name in unique_names]
                cursor.executemany(sql_reseller_written, rows)

                cursor.execute(sql_select_snapshot_ids, (snapshot_time,))
                snapshot_rows = cursor.fetchall()
                snapshot_id_map = {
                    row['asin_id']: row['id'] for row in snapshot_rows
                }

                placeholders = ", ".join(["%s"] * len(unique_names))
                sql_select_resellers_ids = sql_select_resellers_base.format(placeholders=placeholders)
                cursor.execute(sql_select_resellers_ids, unique_names)
                resellers_rows = cursor.fetchall()
                reseller_id_map = {
                    row['name']: row['id'] for row in resellers_rows
                }

                insert_data = []
                for row in data:
                    snapshot_id=snapshot_id_map[row['asin_id']]
                    offer_counter = defaultdict(int)
                    for seller in row['resellers']:
                        reseller_id = reseller_id_map[seller['name']]
                        key = (snapshot_id, reseller_id)
                        offer_counter[key] += 1
                        price = self._convert_price(seller['price'])
                        is_buybox = seller['is_buybox']
                        insert_data.append({
                            'snapshot_id': snapshot_id,
                            'reseller_id': reseller_id,
                            'offer_no': offer_counter[key],
                            'price': price,
                            'is_buybox': is_buybox
                        })

                if insert_data:
                    cursor.executemany(sql_insert_amz_reseller_prices, insert_data)
                    logger.info(
                        "Inserted reseller prices",
                        extra={
                            "snapshot_time": snapshot_time,
                            "asin_count": len(data),
                            "price_rows": len(insert_data),
                            "reseller_count": len(unique_names),
                        }
                    )

        except Exception as e:
            logger.exception(f"Can't upload scraped data: {e}")
            raise

    def _flush_batch(self, snapshot_time, batch):
        if not batch:
            return

        self._persist_product_snapshots_batch(batch)
        self._persist_reseller_offers_batch(snapshot_time, batch)

    @staticmethod
    def _normalize_reseller(name, price, is_buybox=False):
        """
        Normalized the reseller infos, and raise an error if and only if one of seller or price is None
        :param name: seller name
        :param price: seller pricing
        :return: None or dict of seller name and pricing
        """
        has_name = name is not None
        has_price = price is not None

        if has_name and has_price:
            return {
                'name': str(name).strip(),
                'price': price,
                'is_buybox': is_buybox
            }

        if not has_name and not has_price:
            return

        raise ValueError(f'Incomplete reseller data: name={name}, price={price}')

    @staticmethod
    def _screen_shot(driver, snapshot_date, asin):
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        child_folder = SCREENSHOT_DIR / snapshot_date.strftime("%Y-%m-%d")
        child_folder.mkdir(parents=True, exist_ok=True)

        file_path = child_folder / f'{asin}.png'

        result = driver.execute_cdp_cmd("Page.captureScreenshot", {
            "captureBeyondViewport": True,
            "fromSurface": True
        })

        with open(file_path, "wb") as f:
            f.write(base64.b64decode(result['data']))

        return file_path

    @staticmethod
    def _cleanup_old_screenshots(screenshot_root, current_date, keep_days = 28):
        """
        clean the old screenshot files
        :param screenshot_root: screenshot root folder
        :param current_date: snapshot date
        :param keep_days: screenshot kept days
        :return: None
        """
        cutoff = current_date - timedelta(days=keep_days)

        if not screenshot_root.exists():
            return

        for child in screenshot_root.iterdir():
            if not child.is_dir():
                continue

            try:
                folder_date = date.fromisoformat(child.name)
            except ValueError:
                logger.warning("Skip non-date screenshot folder: %s", child.name)
                continue

            if folder_date < cutoff:
                shutil.rmtree(child)

    def upper_section(self, page_source):
        """
        The function to extract the upper section data
        :param page_source: current page_source
        :return: product title, buy_box seller information
        """
        delivery_info = {}
        html = etree.HTML(page_source)
        product_title = extract_text(html, PRODUCT_TITLE_XPATH)
        seller = extract_text(html, PINNED_OFFER_SELLER_LINK_XPATH)

        if not seller:
            try:
                amz_au = extract_text(html, PINNED_OFFER_AMAZON_SELLER_TEXT_XPATH)
                if amz_au:
                    seller = amz_au
                    delivery_time = html.xpath(PINNED_OFFER_DELIVERY_TIME_XPATH)
                    delivery_info['Time'] = [str(item) for item in delivery_time]
            except Exception as e:
                logger.exception(f"Error parsing upper section {e}")

        price_whole = extract_text(html, PINNED_OFFER_PRICE_WHOLE_XPATH)
        price_fraction = extract_text(html, PINNED_OFFER_PRICE_FRACTION_XPATH)

        price = None
        if price_whole and price_fraction:
            price = price_whole + '.' + price_fraction

        return product_title, self._normalize_reseller(seller, price, is_buybox=True)

    def bottom_section(self, page_source):
        """
        The function to extract the bottom section data
        :param page_source: current page_source
        :return: the below section sellers' information, including sellers' names and prices
        """
        delivery_details = {}
        below_info = []
        html = etree.HTML(page_source)
        sellers_infos = html.xpath(BOTTOM_OFFERS_XPATH)

        for info in sellers_infos:
            price_dollars = extract_text(info, BOTTOM_OFFER_PRICE_WHOLE_XPATH)
            price_cents = extract_text(info, BOTTOM_OFFER_PRICE_FRACTION_XPATH)

            price = None
            if price_dollars and price_cents:
                price = price_dollars + '.' + price_cents

            seller = extract_text(info, BOTTOM_OFFER_SELLER_LINK_XPATH)

            if not seller:
                try:
                    amz_au = extract_text(info, BOTTOM_OFFER_AMAZON_SELLER_TEXT_XPATH)
                    if amz_au:
                        seller = amz_au
                        delivery_time = info.xpath(BOTTOM_OFFER_DELIVERY_TIME_XPATH)
                        delivery_details['Time'] = [str(item) for item in delivery_time]
                except Exception as e:
                    logger.exception(f"Error parsing bottom section {e}")

            normalized = self._normalize_reseller(seller, price)
            if normalized:
                below_info.append(normalized)

        return below_info, delivery_details

    @staticmethod
    def _confirm_old_price_table(price_date):
        root = tk.Tk()
        root.withdraw()

        msg = "An outdated price table has been detected.\n\nDo you want to continue?"
        if price_date:
            msg = f"An outdated price table has been detected.\nDate：{price_date}\n\nDo you want to continue?"

        result = messagebox.askyesno(
            title="Outdated price table reminder",
            message=msg
        )

        root.destroy()
        return result

    def _check_price_table_date(self, snapshot_date):
        with get_mysql_connection(dictionary=True) as (connection, cursor):
            cursor.execute(sql_check_price_table_date)
            latest = cursor.fetchone()['price_month']

            if (snapshot_date.year, snapshot_date.month) > (latest.year, latest.month):
                logger.info(f"Using old price table {latest}")
                status = self._confirm_old_price_table(latest)
                if not status:
                    sys.exit(1)
                return True
        return False

    def run(self, asins=None, screenshots_enabled=False):
        #todo: sleep time between each fetch
        snapshot_time = datetime.now()
        snapshot_date = snapshot_time.date()

        is_carried = self._check_price_table_date(snapshot_date)

        if not asins:
            asin_records = self._load_asins_for_date(snapshot_date)
        else:
            asin_records = self._fetch_manual_asins(asins)

        if screenshots_enabled:
            self._cleanup_old_screenshots(SCREENSHOT_DIR, snapshot_date)

        batch = []
        fetcher = AmazonPageFetcher(self.driver)
        for asin in asin_records:
            logger.debug(f"{asin['asin']} starts scraping")
            url = AMAZON_OFFER_URL.format(asin=asin['asin'])
            row_data = {
                **asin,
                'snapshot_time': snapshot_time,
                'snapshot_date': snapshot_date,
                'asin_suppressed': False,
                'resellers': [],
                'is_carried': is_carried
            }

            success, page_source = fetcher.fetch(url)

            if screenshots_enabled:
                self._screen_shot(self.driver, snapshot_date, asin['asin'])

            #todo: if the laptop is always on, this correct, but we can experience such as internet connection and so on
            if not success:
                row_data['title'] = None
                row_data['asin_suppressed'] = True

            else:
                product_title, seller_info = self.upper_section(page_source)
                row_data['title'] = product_title
                if seller_info and seller_info.get('name'):
                    row_data['resellers'].append(seller_info)
                additional_offers, _ = self.bottom_section(page_source)
                row_data['resellers'].extend(additional_offers)

            batch.append(row_data)

            if len(batch) >= 100:
                self._flush_batch(snapshot_time, batch)
                batch = []

        if batch:
            self._flush_batch(snapshot_time, batch)

    @staticmethod
    def export_daily_price(file_path=None, snapshot_date=None):
        if snapshot_date is None:
            snapshot_date = datetime.now().date()
        elif isinstance(snapshot_date, datetime):
            snapshot_date = snapshot_date.date()
        elif not isinstance(snapshot_date, date):
            raise TypeError(
                "snapshot_date must be a datetime.date, datetime.datetime, or None"
            )

        if not file_path:
            parent_folder = Path(__file__).resolve().parent.parent.parent
            result_dir = parent_folder / 'result'
            result_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"new_amazon_check_{snapshot_date:%Y%m%d}.xlsx"
            output_path = result_dir / file_name
        else:
            output_path = Path(file_path)
            if output_path.is_dir() or output_path.suffix == "":
                file_name = f"new_amazon_check_{snapshot_date:%Y%m%d}.xlsx"
                output_path = output_path / file_name

        with get_mysql_connection(dictionary=True) as (connection, cursor):
            cursor.execute(sql_export_latest_prices_by_date, (snapshot_date,))
            daily_price = cursor.fetchall()

            cursor.execute(sql_export_asin_suppressed, (snapshot_date,))
            asin_suppressed = cursor.fetchall()

            df_price = pd.DataFrame(daily_price)
            df_suppressed = pd.DataFrame(asin_suppressed)

            with pd.ExcelWriter(output_path) as writer:
                df_price.to_excel(writer, sheet_name='Price', index=False)
                df_suppressed.to_excel(writer, sheet_name='Suppressed ASINs', index=False)