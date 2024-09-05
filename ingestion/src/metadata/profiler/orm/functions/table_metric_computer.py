#  pylint: disable=protected-access,attribute-defined-outside-init
#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Run profiler metrics on the table
"""

import traceback
from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Tuple

from sqlalchemy import Column, MetaData, Table, func, inspect, literal, select
from sqlalchemy.sql.expression import ColumnOperators, and_, cte
from sqlalchemy.types import String
from sqlalchemy.engine.row import Row

from metadata.generated.schema.entity.data.table import Table as OMTable
from metadata.generated.schema.entity.data.table import TableType
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.orm.registry import Dialects
from metadata.profiler.processor.runner import QueryRunner
from metadata.utils.logger import profiler_interface_registry_logger

logger = profiler_interface_registry_logger()

COLUMN_COUNT = "columnCount"
COLUMN_NAMES = "columnNames"
ROW_COUNT = Metrics.ROW_COUNT().name()
SIZE_IN_BYTES = "sizeInBytes"
CREATE_DATETIME = "createDateTime"
TABLE_ACCURACY_PROPORTION = "tableAccuracyProportion"

ERROR_MSG = (
    "Schema/Table name not found in table args. Falling back to default computation"
)


class AbstractTableMetricComputer(ABC):
    """Base table computer"""

    def __init__(
        self, runner: QueryRunner, metrics: List[Metrics], conn_config, entity: OMTable
    ):
        """Instantiate base table computer"""
        self._runner = runner
        self._metrics = metrics
        self._conn_config = conn_config
        self._database = self._runner._session.get_bind().url.database
        self._table = self._runner.table
        self._entity = entity

    @property
    def database(self):
        return self._database

    @property
    def table(self):
        return self._table

    @property
    def runner(self):
        return self._runner

    @property
    def metrics(self):
        return self._metrics

    @property
    def table_name(self):
        return self._table_name

    @property
    def schema_name(self):
        return self._schema_name

    @property
    def conn_config(self):
        return self._conn_config

    def _set_table_and_schema_name(self):
        """get table and schema name from table args

        Args:
            table (DeclarativeMeta): _description_
        """
        try:
            self._schema_name = self.table.__table_args__.get("schema")
            self._table_name = self.table.__tablename__
        except AttributeError:
            raise AttributeError(ERROR_MSG)

    def _build_table(self, table, schema) -> Table:
        """build table object from table name and schema name

        Args:
            table_name (str): table name
            schema_name (str): schema name

        Returns:
            Table
        """
        if schema:
            return Table(table, MetaData(), schema=schema)
        return Table(table, MetaData())

    def _get_col_names_and_count(self) -> Tuple[str, int]:
        """get column names and count from table

        Args:
            table (DeclarativeMeta): table object

        Returns:
            Tuple[str, int]
        """
        col_names = literal(",".join(inspect(self.table).c.keys()), type_=String).label(
            COLUMN_NAMES
        )
        col_count = literal(len(inspect(self.table).c)).label(COLUMN_COUNT)
        return col_names, col_count
    
    def _to_int(self, val):
        if val is None:
            return 0
        
        try:
            return int(val)
        except Exception as e:
            logger.warning(f'Cannot convert to int for value {val} and type {type(val)}, set as 0')
            return 0
    
    def _get_accuracy_ratio(self, col_name: str) -> Optional[float]:
        col = Column(col_name)
        total_count = self.runner.select_first_from_table(Metrics.COUNT(col=col).fn())[0]
        counts = [
            self.runner.select_first_from_table(Metrics.ACCURACY_BANKCARDNUMBER_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_CHINESENAME_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_DATE_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_EMAIL_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_IDNUMBER_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_IPADDRESS_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_PHONE_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_POSTCODE_COUNT(col=col).fn())[0],
            self.runner.select_first_from_table(Metrics.ACCURACY_URL_COUNT(col=col).fn())[0]
        ]
        logger.debug(f"counts: {counts}")

        total_count = self._to_int(total_count)
        max_count = 0
        if total_count is None or total_count == 0:
            return 0
        for cur_count in counts:
            cur_count_int = self._to_int(cur_count)
            if cur_count_int > max_count:
                max_count = cur_count_int
        max_ratio = max_count / total_count
        logger.debug(f"col_name = {col_name}, total_count = {total_count}, accuracy count = {max_count}, accuracy ratio = {max_ratio}")

        return max_ratio
    
    def _get_table_accuracy_proportion(self, table_name, col_names) -> float:
        """get accuracy proportion from table"""
        if not col_names:
            accuracy_ratio = 0
        else:
            col_names = col_names.split(",")
            all_ratio = []
            for col_name in col_names:
                cur_ratio = self._get_accuracy_ratio(col_name)
                if cur_ratio is not None:
                    all_ratio.append(cur_ratio)
            if len(all_ratio) == 0:
                accuracy_ratio = 0
            else:
                accuracy_ratio = sum(all_ratio) / len(all_ratio)
        logger.debug(f"Table = {table_name}, Accuracy proportion = {accuracy_ratio}")
        # format as percent
        return f"{accuracy_ratio:.2%}"

    def _build_query(
        self,
        columns: List[Column],
        table: Table,
        where_clause: Optional[List[ColumnOperators]] = None,
    ):
        query = select(*columns).select_from(table)
        if where_clause:
            query = query.where(*where_clause)

        return query

    @abstractmethod
    def compute(self):
        """Default compute behavior for table metrics"""
        raise NotImplementedError


class BaseTableMetricComputer(AbstractTableMetricComputer):
    """Base table computer"""

    def compute(self):
        """Default compute behavior for table metrics"""
        return self.runner.select_first_from_table(
            *[metric().fn() for metric in self.metrics]
        )


class SnowflakeTableMetricComputer(BaseTableMetricComputer):
    """Snowflake Table Metric Computer"""

    def compute(self):
        """Compute table metrics for snowflake"""
        columns = [
            Column("ROW_COUNT").label("rowCount"),
            Column("BYTES").label("sizeInBytes"),
            Column("CREATED").label("createDateTime"),
            *self._get_col_names_and_count(),
        ]
        where_clause = [
            func.lower(Column("TABLE_CATALOG")) == self.database.lower(),
            func.lower(Column("TABLE_SCHEMA")) == self.schema_name.lower(),
            func.lower(Column("TABLE_NAME")) == self.table_name.lower(),
        ]
        query = self._build_query(
            columns,
            self._build_table("TABLES", f"{self.database}.INFORMATION_SCHEMA"),
            where_clause,
        )

        rest = self._runner._session.execute(query).first()
        if rest.rowCount is None:
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        return rest


class OracleTableMetricComputer(BaseTableMetricComputer):
    """Oracle Table Metric Computer"""

    def compute(self):
        """Compute table metrics for oracle"""
        create_date = cte(
            self._build_query(
                [
                    Column("owner"),
                    Column("object_name").label("table_name"),
                    Column("created"),
                ],
                self._build_table("DBA_OBJECTS", None),
                [
                    func.lower(Column("owner")) == self.schema_name.lower(),
                    func.lower(Column("object_name")) == self.table_name.lower(),
                ],
            )
        )

        row_count = cte(
            self._build_query(
                [
                    Column("owner"),
                    Column("table_name"),
                    Column("NUM_ROWS"),
                ],
                self._build_table("DBA_TABLES", None),
                [
                    func.lower(Column("owner")) == self.schema_name.lower(),
                    func.lower(Column("table_name")) == self.table_name.lower(),
                ],
            )
        )

        columns = [
            Column("NUM_ROWS").label("rowCount"),
            Column("created").label("createDateTime"),
            *self._get_col_names_and_count(),
        ]
        query = self._build_query(columns, row_count).join(
            create_date,
            and_(
                row_count.c.table_name == create_date.c.table_name,
                row_count.c.owner == create_date.c.owner,
            ),
        )

        res = self.runner._session.execute(query).first()
        if not res:
            return None
        if res.rowCount is None or (
            res.rowCount == 0 and self._entity.tableType == TableType.View
        ):
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        return res


class ClickHouseTableMetricComputer(BaseTableMetricComputer):
    """ClickHouse Table Metric Computer"""

    def compute(self):
        """compute table metrics for clickhouse"""
        columns = [
            Column("total_rows").label("rowCount"),
            Column("total_bytes").label("sizeInBytes"),
            *self._get_col_names_and_count(),
        ]

        where_clause = [
            Column("database") == self.schema_name,
            Column("name") == self.table_name,
        ]

        query = self._build_query(
            columns, self._build_table("tables", "system"), where_clause
        )

        res = self.runner._session.execute(query).first()
        if not res:
            return None
        if res.rowCount is None or (
            res.rowCount == 0 and self._entity.tableType == TableType.View
        ):
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        return res


class BigQueryTableMetricComputer(BaseTableMetricComputer):
    """BigQuery Table Metric Computer"""

    def compute(self):
        """compute table metrics for bigquery"""
        try:
            return self.tables()
        except Exception as exc:
            # if an error occurs fetching data from `__TABLES__`, fallback to `TABLE_STORAGE`
            logger.debug(f"Error retrieving table metadata from `__TABLES__`: {exc}")
            return self.table_storage()

    def table_storage(self):
        """Fall back method if retrieving table metadata from`__TABLES__` fails"""
        columns = [
            Column("total_rows").label("rowCount"),
            Column("total_logical_bytes").label("sizeInBytes"),
            Column("creation_time").label("createDateTime"),
            *self._get_col_names_and_count(),
        ]

        where_clause = [
            Column("project_id")
            == self.conn_config.credentials.gcpConfig.projectId.root,
            Column("table_schema") == self.schema_name,
            Column("table_name") == self.table_name,
        ]

        query = self._build_query(
            columns,
            self._build_table(
                "TABLE_STORAGE",
                f"region-{self.conn_config.usageLocation}.INFORMATION_SCHEMA",
            ),
            where_clause,
        )

        res = self.runner._session.execute(query).first()
        if not res:
            return None
        if res.rowCount is None or (
            res.rowCount == 0 and self._entity.tableType == TableType.View
        ):
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        return res

    def tables(self):
        """retrieve table metadata from `__TABLES__`"""
        columns = [
            Column("row_count").label("rowCount"),
            Column("size_bytes").label("sizeInBytes"),
            func.TIMESTAMP_MILLIS(Column("creation_time")).label(CREATE_DATETIME),
            *self._get_col_names_and_count(),
        ]
        where_clause = [
            Column("project_id")
            == self.conn_config.credentials.gcpConfig.projectId.root,
            Column("dataset_id") == self.schema_name,
            Column("table_id") == self.table_name,
        ]
        query = self._build_query(
            columns,
            self._build_table(
                "__TABLES__",
                f"{self.conn_config.credentials.gcpConfig.projectId.root}.{self.schema_name}",
            ),
            where_clause,
        )
        res = self.runner._session.execute(query).first()
        if not res:
            return None
        if res.rowCount is None or (
            res.rowCount == 0 and self._entity.tableType == TableType.View
        ):
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        return res


class MySQLTableMetricComputer(BaseTableMetricComputer):
    """MySQL Table Metric Computer"""

    def compute(self):
        """compute table metrics for mysql"""
        logger.info("Computing table metrics for MySQL")
        col_names_and_count = self._get_col_names_and_count()
        columns = [
            Column("TABLE_ROWS").label(ROW_COUNT),
            (Column("data_length") + Column("index_length")).label(SIZE_IN_BYTES),
            Column("CREATE_TIME").label(CREATE_DATETIME),
            *col_names_and_count,
        ]
        where_clause = [
            Column("TABLE_SCHEMA") == self.schema_name,
            Column("TABLE_NAME") == self.table_name,
        ]
        query = self._build_query(
            columns, self._build_table("tables", "information_schema"), where_clause
        )

        res = self.runner._session.execute(query).first()
        if not res:
            return None
        if res.rowCount is None or (
            res.rowCount == 0 and self._entity.tableType == TableType.View
        ):
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        res = res._asdict()
        # innodb row count is an estimate we need to patch the row count with COUNT(*)
        # https://dev.mysql.com/doc/refman/8.3/en/information-schema-innodb-tablestats-table.html
        row_count = self.runner.select_first_from_table(Metrics.ROW_COUNT().fn())
        res.update({ROW_COUNT: row_count.rowCount})
        columnNames = res.get(COLUMN_NAMES, '')
        try:
            res.update({TABLE_ACCURACY_PROPORTION: self._get_table_accuracy_proportion(self.table_name, columnNames)})
        except Exception as exc:
            logger.error(f"Error computing table accuracy proportion: {str(exc)}", exc_info=True)
        return res


class RedshiftTableMetricComputer(BaseTableMetricComputer):
    """Redshift Table Metric Computer"""

    def compute(self):
        """compute table metrics for redshift"""
        columns = [
            Column("estimated_visible_rows").label(ROW_COUNT),
            Column("size").label(SIZE_IN_BYTES),
            Column("create_time").label(CREATE_DATETIME),
            *self._get_col_names_and_count(),
        ]

        where_clause = [
            Column("schema") == self.schema_name,
            Column("table") == self.table_name,
        ]

        query = self._build_query(
            columns, self._build_table("svv_table_info", "pg_catalog"), where_clause
        )
        res = self.runner._session.execute(query).first()
        if not res:
            return None
        if res.rowCount is None or (
            res.rowCount == 0 and self._entity.tableType == TableType.View
        ):
            # if we don't have any row count, fallback to the base logic
            return super().compute()
        return res


class TableMetricComputer:
    """Table Metric Construct"""

    def __init__(
        self,
        dialect: str,
        runner: QueryRunner,
        metrics: List[Metrics],
        conn_config,
        entity: OMTable,
    ):
        """Instantiate table metric computer with a dialect computer"""
        self._entity = entity
        self._dialect = dialect
        self._runner = runner
        self._metrics = metrics
        self._conn_config = conn_config
        self.table_metric_computer: AbstractTableMetricComputer = (
            table_metric_computer_factory.construct(
                self._dialect,
                runner=self._runner,
                metrics=self._metrics,
                conn_config=self._conn_config,
                entity=self._entity,
            )
        )

    def compute(self):
        """Compute table metrics"""
        return self.table_metric_computer.compute()


class TableMetricComputerFactory:
    """Factory returning the correct construct for the table metrics based on dialect"""

    def __init__(self):
        self._constructs = {}

    def register(self, dialect: str, construct: Callable):
        """Register a construct for a dialect"""
        self._constructs[dialect] = construct

    def construct(self, dialect, **kwargs):
        """Construct the query"""

        # check if we have registered a construct for the dialect
        construct = self._constructs.get(dialect)
        if not construct:
            construct = self._constructs["base"]
            return construct(**kwargs)

        try:
            construct_instance: AbstractTableMetricComputer = construct(**kwargs)
            construct_instance._set_table_and_schema_name()
            return construct_instance
        except Exception:
            # if an error occurs, fallback to the base construct
            logger.debug(traceback.format_exc())
            return self._constructs["base"](**kwargs)


table_metric_computer_factory = TableMetricComputerFactory()
table_metric_computer_factory.register("base", BaseTableMetricComputer)
table_metric_computer_factory.register(Dialects.Redshift, RedshiftTableMetricComputer)
table_metric_computer_factory.register(Dialects.MySQL, MySQLTableMetricComputer)
table_metric_computer_factory.register(Dialects.BigQuery, BigQueryTableMetricComputer)
table_metric_computer_factory.register(
    Dialects.ClickHouse, ClickHouseTableMetricComputer
)
table_metric_computer_factory.register(Dialects.Oracle, OracleTableMetricComputer)
table_metric_computer_factory.register(Dialects.Snowflake, SnowflakeTableMetricComputer)
