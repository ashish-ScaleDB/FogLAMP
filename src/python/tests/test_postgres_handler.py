import pytest
import logging
import asyncio
import sqlalchemy as sa
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session
from foglamp.configurator import Configurator
from foglamp.coap.postgres_handler import *
from foglamp.coap.uri_handlers.sensor_values import DEBUG_SENSOR1, DEBUG_SENSOR2, DEBUG_SENSOR3, \
                                                    DEBUG_SENSOR4, DEBUG_SENSOR5, set_custom_log_levels

# noinspection PyClassHasNoInit


class TestPostgresHandler:
    connection = engine = conf = meta = None

    @classmethod
    def setup_class(cls):
        cls.conf = Configurator()
        cls.conf.FOGLAMP_DEPLOYMENT = 'test'
        cls.conf.initialize_dbconfig(cls.conf.FOGLAMP_DEPLOYMENT)

        def check_for_tables():
            # Connect to the database and create the schema within a transaction
            engine = sa.create_engine(cls.conf.db_conn_str)
            connection = engine.connect()

            meta = sa.MetaData(bind=connection, reflect=True)
            with connection  as conn:
                if 'log_t' not in meta.tables:
                    __log__ = sa.Table(
                        'log_t'
                        , cls.meta
                        , sa.Column('log_level', sa.types.INT)
                        , sa.Column('log_levelname', sa.types.VARCHAR(10))
                        , sa.Column('log', sa.types.VARCHAR(100))
                        , sa.Column('created_at', sa.types.DATE)
                        , sa.Column('created_by', sa.types.VARCHAR(50)))
                    __log__.create()

        check_for_tables()

        cls.engine = sa.create_engine(cls.conf.db_conn_str)
        cls.connection = cls.engine.connect()
        cls.meta = sa.MetaData(bind=cls.connection, reflect=True)

        # Add Custom level to logging
        set_custom_log_levels()

    @classmethod
    def teardown_class(cls):
        cls.connection.close()
        cls.engine.dispose()

    def setup_method(self, method):
        self.__transaction = TestPostgresHandler.connection.begin()
        self.session = Session(TestPostgresHandler.connection)

    def teardown_method(self, method):
        # TODO: Investigate why rollback is not working
        self.__transaction.rollback()

        logs = TestPostgresHandler.meta.tables['log_t']
        self.session.execute(logs.delete())
        self.session.commit()
        self.session.close()

    def test_log(self):
        logdb = PostgresHandler()
        my_logger = logging.getLogger("my_logger")
        my_logger.addHandler(logdb)
        my_logger.sensor1('This is a test error')

        logs = TestPostgresHandler.meta.tables['log_t']
        for row in TestPostgresHandler.connection.execute(logs.select()):
            assert row.log_level == 51
            assert row.log_levelname == 'SENSOR1'
            assert row.log == 'This is a test error'
            assert row.created_by == "my_logger"

    def test_async_log(self):
        logdb = AsyncPostgresHandler()
        my_async_logger = logging.getLogger("my_async_logger")
        my_async_logger.addHandler(logdb)
        my_async_logger.sensor5('This is a async test error')

        logs = TestPostgresHandler.meta.tables['log_t']
        for row in TestPostgresHandler.connection.execute(logs.select()):
            assert row.log_level == 55
            assert row.log_levelname == 'SENSOR5'
            assert row.log == 'This is a async test error'
            assert row.created_by == "my_async_logger"
