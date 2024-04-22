import datetime
import enum
import json
import math
import os
import random
import re
import string
import time

import pandas as pd
from websocket import create_connection

import requests
import json

import logging

logger = logging.getLogger(__name__)
logger.basicConfig = logging.basicConfig(level=logging.INFO)


# websocket_logger = logging.getLogger('websocket')
# websocket_logger.setLevel(logging.WARNING)


class Interval(enum.Enum):
    in_10_seconds = "10S"
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"


class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://fr.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
            self,
            username: str = None,
            password: str = None,
            auth_token: str = None
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): tradingview username. Defaults to None.
            password (str, optional): tradingview password. Defaults to None.
        """

        self.ws_debug = True
        if auth_token is not None:
            self.token = auth_token
        else:
            self.token = self.__auth(username, password)
        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning(
                "you are using nologin method, data you access may be limited"
            )

        self.ws = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):

        if (username is None or password is None):
            token = None

        else:
            data = {"username": username,
                    "password": password,
                    "remember": "on"}
            try:
                response = requests.post(
                    url=self.__sign_in_url, data=data, headers=self.__signin_headers)
                token = response.json()['user']['auth_token']
            except Exception as e:
                logger.error('error while signin')
                token = None
        return token

    def __create_connection(self):
        logger.debug("creating websocket connection")
        self.ws = create_connection(
            "wss://prodata.tradingview.com/socket.io/websocket", headers=self.__ws_headers, timeout=self.__ws_timeout
        )

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)

            return found, found2
        except AttributeError:
            logger.error("error in filter_raw_message")

    @staticmethod
    def __generate_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session():
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters)
                                for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st):
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, paramList):
        return self.__prepend_header(self.__construct_message(func, paramList))

    def __send_message(self, func, args):
        m = self.__create_message(func, args)
        if self.ws_debug:
            logging.debug(f'Message sent: {m}')
        self.ws.send(m)

    def __parse_messages(self, message):
        # Pattern to find the message starts
        pattern = r'(~m~\d+~m~)'

        # Split the data at each found pattern, retaining the delimiter in the result
        parts = re.split(pattern, message)

        # The first element is usually an empty string if the data starts with the delimiter; remove it
        if parts[0] == '':
            parts = parts[1:]

        # Combine the delimiters with the messages because the split removes them from the main text
        messages = parts

        return messages

    @staticmethod
    def __create_df(raw_data, symbol):
        try:
            datasets = re.findall('"s":\[(.+?)\}\]', raw_data)
            data = list()
            volume_data = True
            for dataset in datasets:
                x = dataset.split(',{"')

                for xi in x:
                    xi = re.split("\[|:|,|\]", xi)
                    ts = datetime.datetime.fromtimestamp(float(xi[4]))

                    row = [ts]

                    for i in range(5, 10):

                        # skip converting volume data if does not exists
                        if not volume_data and i == 9:
                            row.append(0.0)
                            continue
                        try:
                            row.append(float(xi[i]))

                        except ValueError:
                            volume_data = False
                            row.append(0.0)
                            logger.debug('no volume data')

                    data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open",
                               "high", "low", "close", "volume"]
            ).set_index("datetime")
            data.insert(0, "symbol", value=symbol)
            data = data[~data.index.duplicated(keep='first')]
            logger.debug(f'Unique rows in df: {data.index.nunique()}')
            return data
        except AttributeError:
            logger.error("no data, please check the exchange and symbol")

    @staticmethod
    def __format_symbol(symbol, exchange, contract: int = None):
        if ":" in symbol:
            pass
        elif contract is None:
            symbol = f"{exchange}:{symbol}"

        elif isinstance(contract, int):
            symbol = f"{exchange}:{symbol}{contract}!"

        else:
            raise ValueError("not a valid contract")

        return symbol

    def get_hist(
            self,
            symbol: str,
            exchange: str = "NSE",
            interval: Interval = Interval.in_daily,
            n_bars: int = 10,
            fut_contract: int = None,
            extended_session: bool = False,
    ) -> pd.DataFrame:
        """get historical data

        Args:
            exchange (str, optional): exchange, not required if symbol is in format EXCHANGE:SYMBOL. Defaults to None.
            interval (str, optional): chart interval. Defaults to 'D'.
            n_bars (int, optional): no of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for continuous current contract in front, 2 for continuous next contract in front . Defaults to None.
            extended_session (bool, optional): regular session if False, extended session if True, Defaults to False.

        Returns:
            pd.Dataframe: dataframe with sohlcv as columns
        """
        if n_bars <= 0:
            raise ValueError('n_bars must be greater than 0')
        symbol = self.__format_symbol(
            symbol=symbol, exchange=exchange, contract=fut_contract
        )

        data_bucket_size = 1000
        remaining_data_size = n_bars
        interval = interval.value

        self.__create_connection()
        self.__send_message("set_auth_token", [self.token])
        self.__send_message("chart_create_session", [self.chart_session, ""])
        self.__send_message("quote_create_session", [self.session])
        self.__send_message(
            "quote_set_fields",
            [
                self.session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        )

        self.__send_message(
            "quote_add_symbols", [self.session, symbol]
        )
        self.__send_message("quote_fast_symbols", [self.session, symbol])

        self.__send_message(
            "resolve_symbol",
            [
                self.chart_session,
                "sds_sym_1",
                '={"symbol":"'
                + symbol
                + '","adjustment":"splits","session":'
                + ('"regular"' if not extended_session else '"extended"')
                + "}",
            ],
        )
        self.__send_message(
            "create_series",
            [self.chart_session, "sds_1", "s1", "sds_sym_1", interval, data_bucket_size, ""],
        )
        self.__send_message("switch_timezone", [
            self.chart_session, "exchange"])

        df = pd.DataFrame()
        logger.info(f"Getting data for {symbol}...")
        max_bars = n_bars
        series_completed = False
        while True:
            try:
                result = self.ws.recv()
                messages = self.__parse_messages(result)
                for message in messages:
                    # Parse to dataframe messages containing price data
                    if 'timescale_update' in message:
                        df_temp = self.__create_df(message, symbol)
                        if len(df_temp) > 0:
                            df = pd.concat([df, df_temp])
                            remaining_data_size = remaining_data_size - len(df_temp)

                            # If we fetched all the data we need, we break the loop
                            if remaining_data_size <= 0:
                                series_completed = True
                                # We need to trimm the excess data
                                max_bars = n_bars
                                break

                    # Check if we reached the maximum available data
                    elif '"data_completed":"end"' in message:
                        logger.info('Reached the maximum available data')
                        max_bars = len(df)
                        series_completed = True
                    # If its a ping, ping back
                    elif re.match(r'~m~\d+~m~~h~\d+', message.strip()):
                        logger.debug('Ping received')
                        self.ws.send(message)
                        logger.debug(f'Ping sent: {message}')
                    else:
                        logger.debug(f'Received message not containing price data {result}')

                # If we fetched all the data we need or we reached max historical data, we break the loop
                if series_completed:
                    logger.info(f'Done for {symbol}')
                    break
                logger.debug('Request more data')
                self.__send_message("request_more_data", [self.chart_session, "sds_1", data_bucket_size])

            except Exception as e:
                logger.error(e)
                break

        # We return the filtered df with max_bars
        return df.sort_index(ascending=False)[:max_bars]

    def search_symbol(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)

        symbols_list = []
        try:
            resp = requests.get(url)

            symbols_list = json.loads(resp.text.replace(
                '</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(e)

        return symbols_list


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    tv = TvDatafeed()
    print(
        tv.get_hist(
            "XBTUSD.P",
            "BITMEX",
            interval=Interval.in_4_hour,
            n_bars=6000,
            extended_session=False,
        )
    )
