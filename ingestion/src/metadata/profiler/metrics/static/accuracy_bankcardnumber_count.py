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
from metadata.utils.logger import profiler_logger

logger = profiler_logger()

class AccuracyBankCardNumberCount(StaticMetric):
    """
    ACCURACY_COUNT Metric
    """

    expression = r"^(62[0-5]\d{13,16})|(4[0-9]{12}(?:[0-9]{3})?)|(5[1-5][0-9]{14})$"

    @classmethod
    def name(cls):
        
        return MetricType.accuracyBankCardNumberCount.value

    @property
    def metric_type(self):
        return int

    def _is_concatenable(self):
        return is_concatenable(self.col.type)

    @_label
    def fn(self):
        """sqlalchemy function"""
        return SumFn(
            case(
                [
                    (
                        column(self.col.name, self.col.type).regexp_match(
                            self.expression
                        ),
                        1,
                    )
                ],
                else_=0,
            )
        )

    def df_fn(self, dfs):
        """pandas function"""

        if self._is_concatenable():
            return sum(
                df[self.col.name][
                    df[self.col.name].astype(str).str.contains(self.expression)
                ].count()
                for df in dfs
            )
        raise TypeError(
            f"Don't know how to process type {self.col.type} when computing Bank Card Number Match Count"
        )
