import logging
from pathlib import Path
import pandas as pd
from src.config import PRICE_FILE_VALIDATION
import re
from src.db.sql_connector import get_mysql_connection
import hashlib
from mysql.connector import Error
from mysql.connector.errorcode import ER_DUP_ENTRY
from datetime import datetime
import warnings
from src.db.queries import (
    sql_price_temp_table,
    sql_price_written,
    sql_asin_update,
    sql_products_update,
    sql_price_updates,
    sql_eol_update,
    sql_price_imports
)


logger = logging.getLogger(__name__)

class UploadProductPrice:
    def __init__(self, file_path):
        self.file_path = Path(file_path)

    def _read_file(self):
        """
        Read files
        :return: df
        """
        if self.file_path.exists():
            file_name = self.file_path.stem
            match = re.search(PRICE_FILE_VALIDATION['date_regex'], file_name)
            if match:
                file_date = match.group(0)
            else:
                logger.warning('Can\'t find the price file effective from date')
                raise ValueError('Can\'t find the price file effective from date')

            if self.file_path.suffix == '.csv':
                df = pd.read_csv(self.file_path)
                return df, file_date
            elif self.file_path.suffix == '.xlsx':
                df = pd.read_excel(self.file_path)
                return df, file_date
            else:
                logger.warning('File type not supported')
                raise TypeError('File type not supported')

        else:
            logger.warning('File does not exist')
            raise TypeError('File does not exist')

    @staticmethod
    def _calculate_df_hash(df):
        row_hashes = pd.util.hash_pandas_object(df, index=False)
        combined_hash = hashlib.sha256(row_hashes.values.tobytes()).hexdigest()
        return combined_hash

    @staticmethod
    def _convert_to_str(df, column):
        new_df = df.copy()
        new_df[column] = new_df[column].astype(str)
        return new_df

    @staticmethod
    def _remove_dollar_sign(df, column):
        new_df = df.copy()
        new_df[column] = (
            new_df[column].astype(str)
            .str.replace('$', '')
            .str.replace(',', '')
            .astype(float)
        )
        return new_df

    def _validate_data_and_filter(self, df, required_columns):
        """
        Validate df, ASIN and Price
        :param df: input df
        :param required_columns: required columns
        :return: cleaned df, hash
        """
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            logger.warning(f'Column {missing} not available')
            raise ValueError(f'Column {missing} not found in price file')

        nan_cols = [c for c in required_columns if df[c].isna().any()]
        if nan_cols:
            logger.warning(f'Columns {nan_cols} contains nan values')
            raise ValueError(f'Column {nan_cols} contains nan values')

        filtered_df = df[required_columns].copy()

        filtered_df.rename(columns={'ASIN#': 'ASIN', 'Model No.': 'Model', 'RRP\nAUD': 'RRP'}, inplace=True)

        filtered_df = self._convert_to_str(filtered_df, 'Model')

        filtered_df = self._convert_to_str(filtered_df, 'ASIN')
        invalid_asins = filtered_df[filtered_df['ASIN'].astype(str).str.len() != 10]
        if len(invalid_asins) > 0:
            logger.warning(f'There are {len(invalid_asins)} invalid asins')
            raise ValueError(f"ASIN's length must be 10 chars. There are {len(invalid_asins)} Error. \n "
                             f"They are {invalid_asins['ASIN'].tolist()}")

        filtered_df = self._convert_to_str(filtered_df, 'RRP')
        filtered_df = self._remove_dollar_sign(filtered_df, 'RRP')

        hash_value = self._calculate_df_hash(filtered_df)
        return filtered_df, hash_value

    def upload_file(self):
        """
        TODO: Need to add logger to record the sql process, like what data written in for each cursor
        :return:
        """
        df, file_date = self._read_file()
        required_columns = PRICE_FILE_VALIDATION["required_columns"]
        new_df, hash_value = self._validate_data_and_filter(df, required_columns)
        row_number = len(new_df)
        date_time = str(file_date).replace('.', '-') + "-01"
        price_month = datetime.strptime(date_time, "%Y-%m-%d").date()
        try:
            with get_mysql_connection() as (connection, cursor):

                cursor.execute(sql_price_imports, (price_month, hash_value, row_number))

                imported_id = cursor.lastrowid

                cursor.execute(sql_price_temp_table)

                price_data = [(row['ASIN'], row['Model'], row['RRP']) for _, row in new_df.iterrows()]
                cursor.executemany(sql_price_written, price_data)

                cursor.execute(sql_asin_update)

                cursor.execute(sql_products_update)

                cursor.execute(sql_price_updates, (price_month, imported_id))

                cursor.execute(sql_eol_update, (imported_id,))

        except Error as e:
            if e.errno == ER_DUP_ENTRY:
                logger.info(f"Duplicate upload ignored for {date_time} hash={hash_value}")
                warnings.warn(
                    f"Duplicate upload ignored for {price_month} hash={hash_value}",
                    UserWarning
                )
                return
            logger.warning(f"Can't upload price data: {e}")
            raise ValueError(f"Can't upload price data: {e}") from e






