from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

from pgportfolio.marketdata.coinlist2 import CoinList2
import numpy as np
import pandas as pd
from pgportfolio.tools.data import panel_fillna
from pgportfolio.constants2 import *
#import sqlite3
from datetime import datetime
import logging


class HistoryManager2:
    # if offline ,the coin_list could be None
    # NOTE: return of the sqlite results is a list of tuples, each tuple is a row
    def __init__(self, online=True):
        #self.initialize_db()
        self.__storage_period = FIVE_MINUTES  # keep this as 300
        self._online = online
        # if self._online:
        #     self._coin_list = CoinList2(end, volume_average_days, volume_forward)
        # self.__volume_forward = volume_forward
        # self.__volume_average_days = volume_average_days
        self.__coins = ['DLA'] #'SQAL','DLM','SQCU','SQRU']
        self._coin_number = len(self.__coins)

    @property
    def coins(self):
        return self.__coins

    # def initialize_db(self):
    #     with sqlite3.connect(DATABASE_DIR) as connection:
    #         cursor = connection.cursor()
    #         cursor.execute('CREATE TABLE IF NOT EXISTS History (date INTEGER,'
    #                        ' coin varchar(20), high FLOAT, low FLOAT,'
    #                        ' open FLOAT, close FLOAT, volume FLOAT, '
    #                        ' quoteVolume FLOAT, weightedAverage FLOAT,'
    #                        'PRIMARY KEY (date, coin));')
    #         connection.commit()

    def get_global_data_matrix(self, start, end, period=300, features=('close',)):
        """
        :return a numpy ndarray whose axis is [feature, coin, time]
        """
        return self.get_global_panel(start, end, period, features).values

    def get_global_panel(self, start, end, period=300, features=('close',)):
        """
        :param start/end: linux timestamp in seconds
        :param period: time interval of each data access point
        :param features: tuple or list of the feature names
        :return a panel, [feature, coin, time]
        """
        coins = self.__coins

        logging.info("feature type list is %s" % str(features))
        self.__checkperiod(period)

        time_index = pd.read_sql_query("SELECT distinct trantime FROM data_5m WHERE  trantime>='{start}' and trantime<='{end}' ORDER BY trantime".format(start=start, end=end), con=DATABASE_CONN)
        time_index = pd.to_datetime(time_index['trantime'])
        panel = pd.Panel(items=features, major_axis=coins, minor_axis=time_index, dtype=np.float32)

        try:
            for row_number, coin in enumerate(coins):
                for feature in features:
                    # NOTE: transform the start date to end date
                    if feature == "close":
                        sql = ("SELECT trantime AS date_norm, pclose FROM data_5m WHERE"
                               " trantime>='{start}' and trantime<='{end}'" 
                               " and asset=\"{coin}\"".format(start=start, end=end, coin=coin))
                    elif feature == "open":
                        sql = ("SELECT trantime AS date_norm, popen FROM data_5m WHERE"
                               " trantime>='{start}' and trantime<='{end}'" 
                               " and asset=\"{coin}\"".format(start=start, end=end, coin=coin))
                    elif feature == "volume":
                        sql = ("SELECT trantime AS date_norm, vol FROM data_5m WHERE"
                               " trantime>='{start}' and trantime<='{end}'" 
                               " and asset=\"{coin}\"".format(start=start, end=end, coin=coin))
                    elif feature == "high":
                        sql = ("SELECT trantime AS date_norm, phigh FROM data_5m WHERE"
                               " trantime>='{start}' and trantime<='{end}'" 
                               " and asset=\"{coin}\"".format(start=start, end=end, coin=coin))
                    elif feature == "low":
                        sql = ("SELECT trantime AS date_norm, plow FROM data_5m WHERE"
                               " trantime>='{start}' and trantime<='{end}'" 
                               " and asset=\"{coin}\"".format(start=start, end=end, coin=coin))
                    else:
                        msg = ("The feature %s is not supported" % feature)
                        logging.error(msg)
                        raise ValueError(msg)
                    serial_data = pd.read_sql_query(sql, con=DATABASE_CONN,
                                                    parse_dates=["date_norm"],
                                                    index_col="date_norm")
                    panel.loc[feature, coin, serial_data.index] = serial_data.squeeze()
                    #panel = panel_fillna(panel, "both")
        except Exception as e:
            logging.error('access database error!')
            logging.error(e)
        return panel

    def __checkperiod(self, period):
        if period == FIVE_MINUTES:
            return
        elif period == FIFTEEN_MINUTES:
            return
        elif period == HALF_HOUR:
            return
        elif period == TWO_HOUR:
            return
        elif period == FOUR_HOUR:
            return
        elif period == DAY:
            return
        else:
            raise ValueError('peroid has to be 5min, 15min, 30min, 2hr, 4hr, or a day')

    def __fill_data(self, start, end, coin, cursor):
        chart = self._coin_list.get_chart_until_success(
            pair=self._coin_list.allActiveCoins.at[coin, 'pair'],
            start=start,
            end=end,
            period=self.__storage_period)
        logging.info("fill %s data from %s to %s"%(coin, datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M'),
                                            datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M')))
        for c in chart:
            if c["date"] > 0:
                if c['weightedAverage'] == 0:
                    weightedAverage = c['close']
                else:
                    weightedAverage = c['weightedAverage']

                #NOTE here the USDT is in reversed order
                if 'reversed_' in coin:
                    cursor.execute('INSERT INTO History VALUES (?,?,?,?,?,?,?,?,?)',
                        (c['date'],coin,1.0/c['low'],1.0/c['high'],1.0/c['open'],
                        1.0/c['close'],c['quoteVolume'],c['volume'],
                        1.0/weightedAverage))
                else:
                    cursor.execute('INSERT INTO History VALUES (?,?,?,?,?,?,?,?,?)',
                                   (c['date'],coin,c['high'],c['low'],c['open'],
                                    c['close'],c['volume'],c['quoteVolume'],
                                    weightedAverage))
