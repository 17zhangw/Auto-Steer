# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Run AutoSteer's training mode to explore alternative query plans"""
from typing import Type
import storage
import os
import sys

import connectors.connector
from connectors import postgres_connector, postgres_prior_connector
from utils.arguments_parser import get_parser
from utils.custom_logging import logger
from autosteer.dp_exploration import explore_optimizer_configs
from autosteer.query_span import run_get_query_span
from inference.train import train_tcnn


def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], config, benchmark: str, query: str):
    run_get_query_span(connector, config, benchmark, query)
    connector = connector(config)
    explore_optimizer_configs(connector, f'{benchmark}/{query}')


def inference_mode(connector, benchmark: str, retrain: bool, create_datasets: bool):
    train_tcnn(connector, benchmark, retrain, create_datasets)


def get_connector_type(connector: str) -> Type[connectors.connector.DBConnector]:
    if connector == 'postgres':
        return postgres_connector.PostgresConnector
    elif connector == 'postgres_prior':
        return postgres_prior_connector.PostgresConnector
    elif connector == 'mysql':
        return mysql_connector.MySqlConnector
    elif connector == 'spark':
        return spark_connector.SparkConnector
    elif connector == 'presto':
        return presto_connector.PrestoConnector
    elif connector == 'duckdb':
        return duckdb_connector.DuckDBConnector
    logger.fatal('Unknown connector %s', connector)


def run(args):
    # Set the output directory.
    storage.RESULTS_DIR = args.output_dir

    ConnectorType = get_connector_type(args.database)
    if args.output_name is not None:
        storage.TESTED_DATABASE = args.output_name
    else:
        storage.TESTED_DATABASE = ConnectorType.get_name()

    if args.benchmark is None or not os.path.isdir(args.benchmark):
        logger.fatal('Cannot access the benchmark directory containing the sql files with path=%s', args.benchmark)
        sys.exit(1)

    storage.BENCHMARK_ID = storage.register_benchmark(args.benchmark)

    if (args.inference and args.training) or (not args.inference and not args.training):
        logger.fatal('Specify either training or inference mode')
        sys.exit(1)
    if args.inference:
        logger.info('Run AutoSteer\'s inference mode')
        inference_mode(ConnectorType, args.benchmark, args.retrain, args.create_datasets)
    elif args.training:
        logger.info('Run AutoSteer\'s training mode')
        queries = sorted(list(filter(lambda q: q.endswith('.sql'), os.listdir(args.benchmark))))
        logger.info('Found the following SQL files: %s', queries)
        for query in queries:
            logger.info('run Q%s...', query)
            approx_query_span_and_run(ConnectorType, args.config, args.benchmark, query)


if __name__ == '__main__':
    args = get_parser().parse_args()
    run(args)
