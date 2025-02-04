# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module provides a connection to the PostgreSQL database for benchmarking"""
import psycopg
from connectors.connector import DBConnector
import configparser
import time
import os
import json
from utils.custom_logging import logger


class PostgresConnector(DBConnector):
    """This class handles the connection to the tested PostgreSQL database"""

    def __init__(self, config):
        super().__init__()
        # get connection config from config-file
        self.config = configparser.ConfigParser()
        self.config.read(config)
        defaults = self.config['DEFAULT']
        user = defaults['DB_USER']
        database = defaults['DB_NAME']
        password = defaults['DB_PASSWORD']
        host = defaults['DB_HOST']
        port = defaults['DB_PORT']
        self.timeout = defaults['TIMEOUT_MS']
        self.postgres_connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        self.connect()

    def connect(self) -> None:
        self.connection = psycopg.connect(
            self.postgres_connection_string,
            autocommit=True,
            prepare_threshold=None,
        )
        self.cursor = self.connection.cursor()
        self.cursor.execute(f'set statement_timeout to {self.timeout}; commit;')

    def close(self) -> None:
        self.cursor.close()
        self.connection.close()

    def set_disabled_knobs(self, knobs: list) -> None:
        # todo enable all others rules before
        all_knobs = set(PostgresConnector.get_knobs())
        assert "no_forceidx" not in knobs and "no_forceidx" not in all_knobs
        statements = ''
        for knob in all_knobs:
            if knob not in knobs:
                statements += f'SET {knob} to ON;'
        for knob in knobs:
            statements += f'SET {knob} to OFF;'

        while True:
            try:
                self.cursor.execute(statements)
                break
            except Exception as e:
                logger.warn(f"Error with setting knobs: {e}")

    def explain(self, query: str) -> str:
        """Explain a query and return the json query plan"""
        self.cursor.execute(f'EXPLAIN (FORMAT JSON) {query}')
        return json.dumps(self.cursor.fetchone()[0][0]['Plan'])

    def execute(self, query: str) -> DBConnector.TimedResult:
        """Execute the query and return its result"""
        begin = time.time_ns()
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        elapsed_time_usec = int((time.time_ns() - begin) / 1_000)

        return DBConnector.TimedResult(result, elapsed_time_usec)

    @staticmethod
    def get_name() -> str:
        return 'postgres'

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        with open(os.path.dirname(__file__) + '/../knobs/postgres.txt', 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]
