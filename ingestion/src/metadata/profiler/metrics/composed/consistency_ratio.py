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
Accuracy Ratio Composed Metric definition
"""
# pylint: disable=duplicate-code

from typing import Any, Dict, Optional, Tuple

from metadata.generated.schema.configuration.profilerConfiguration import MetricType
from metadata.profiler.metrics.core import ComposedMetric
from metadata.profiler.metrics.static.count import Count

from metadata.utils.logger import profiler_logger

logger = profiler_logger()

class ConsistencyRatio(ComposedMetric):
    """
    Given the total count and consistency count,
    compute the consistency ratio
    """

    @classmethod
    def name(cls):
        return MetricType.consistencyProportion.value

    @classmethod
    def required_metrics(cls) -> Tuple[str, ...]:
        return (Count.name(),)

    @property
    def metric_type(self):
        """
        Override default metric_type definition as
        we now don't care about the column
        """
        return float

    def fn(self, res: Dict[str, Any]) -> Optional[float]:
        """
        Safely compute consistency ratio based on the profiler
        results of other Metrics
        """
        return 1

