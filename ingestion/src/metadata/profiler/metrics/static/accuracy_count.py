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
Accuracy Count Metric definition
"""
# pylint: disable=duplicate-code

from sqlalchemy import case, column

from metadata.generated.schema.configuration.profilerConfiguration import MetricType
from metadata.profiler.metrics.core import StaticMetric, _label
from metadata.profiler.orm.functions.sum import SumFn
from metadata.profiler.orm.registry import is_concatenable


class AccuracyCount(StaticMetric):
    """
    ACCURACY_COUNT Metric
    """

    REG_LIST = [
        ('phone', r"^[0-9]{10}$"),
        ('email', r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"),
        ('postcode', r"^[0-9]{6}$"),
        ('address', r'^(?P<province>[^省]+省|[^市]+市|[^区]+区|[^县]+县)?(?P<city>[^市]+市|[^区]+区|[^县]+县)?(?P<district>[^区]+区|[^县]+县)?(?P<county>[^镇]+镇|[^乡]+乡)?[^省市区县]*$'),
        ('idnumder', r"^\d{6}(19|20)\d{2}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\d{3}(\d|X|x)$"),
        ('bankcardnumber', r"^\d{6,20}$"),
        ('date', r"^(19|20)\d{2}[-/年](0[1-9]|1[012])[-/月](0[1-9]|[12][0-9]|3[01])$|^19\d{2}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])$|^19\d{2}年(0[1-9]|1[012])月(0[1-9]|[12][0-9]|3[01])日.*$"),
        ('ipaddress', r"^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"),
        ('url', r"^(http|https|ftp)://[a-zA-Z0-9]+([\-\.]{1}[a-zA-Z0-9]+)*\.[a-zA-Z]{2,}(:[0-9]{1,5})?(/.*)?$"),
        ('currency', r"^(¥|CNY|US$|USD|HK\$|HKD|€|EUR|£|GBP|JP¥|JPY)?(\d+|\d{1,3}(,\d{3})*)(\.\d{1,2})?$"),
        ('chinesename', r"^[\u4e00-\u9fa5]{1,3}$"),
    ]

    @classmethod
    def name(cls):
        return MetricType.accuracyCount.value

    @property
    def metric_type(self):
        return int

    def _is_concatenable(self):
        return is_concatenable(self.col.type)

    @_label
    def fn(self):
        """sqlalchemy function"""
        max_of_sum = 0
        for reg in self.REG_LIST:
            expression = reg[1]
            this_sum = SumFn(
                case(
                    [
                        (
                            column(self.col.name, self.col.type).regexp_match(expression),
                            1,
                        )
                    ],
                    else_=0,
                )
            )
            if this_sum > max_of_sum:
                max_of_sum = this_sum
        return max_of_sum

    def df_fn(self, dfs):
        """pandas function"""

        if self._is_concatenable():
            max_of_sum = 0
            for reg in self.REG_LIST:
                expression = reg[1]
                this_sum = sum(
                    df[self.col.name][
                        df[self.col.name].astype(str).str.contains(expression)
                    ].count()
                    for df in dfs
                )
                if this_sum > max_of_sum:
                    max_of_sum = this_sum
            return max_of_sum
        raise TypeError(
            f"Don't know how to process type {self.col.type} when computing Accuracy Match Count"
        )
