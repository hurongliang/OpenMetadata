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
from metadata.profiler.metrics.static.accuracy_address_count import AccuracyAddressCount
from metadata.profiler.metrics.static.accuracy_bankcardnumber_count import AccuracyBankCardNumberCount
from metadata.profiler.metrics.static.accuracy_chinesename_count import AccuracyChineseNameCount
from metadata.profiler.metrics.static.accuracy_date_count import AccuracyDateCount
from metadata.profiler.metrics.static.accuracy_email_count import AccuracyEmailCount
from metadata.profiler.metrics.static.accuracy_idnumder_count import AccuracyIdNumberCount
from metadata.profiler.metrics.static.accuracy_ipaddress_count import AccuracyIpAddressCount
from metadata.profiler.metrics.static.accuracy_phone_count import AccuracyPhoneCount
from metadata.profiler.metrics.static.accuracy_postcode_count import AccuracyPostCodeCount
from metadata.profiler.metrics.static.accuracy_url_count import AccuracyUrlCount

from metadata.utils.logger import profiler_logger

logger = profiler_logger()

class AccuracyRatio(ComposedMetric):
    """
    Given the total count and accuracy count,
    compute the accuracy ratio
    """

    @classmethod
    def name(cls):
        return MetricType.accuracyProportion.value

    @classmethod
    def required_metrics(cls) -> Tuple[str, ...]:
        return (Count.name(), AccuracyAddressCount.name(), AccuracyBankCardNumberCount.name(),
            AccuracyChineseNameCount.name(), AccuracyDateCount.name(), AccuracyEmailCount.name(),
            AccuracyIdNumberCount.name(), AccuracyIpAddressCount.name(), AccuracyPhoneCount.name(),
            AccuracyPostCodeCount.name(), AccuracyUrlCount.name())

    @property
    def metric_type(self):
        """
        Override default metric_type definition as
        we now don't care about the column
        """
        return float

    def fn(self, res: Dict[str, Any]) -> Optional[float]:
        """
        Safely compute accuracy ratio based on the profiler
        results of other Metrics
        """
        total_count = res.get(Count.name())
        count1 = 'address', res.get(AccuracyAddressCount.name())
        count2 = 'bankcardnumber', res.get(AccuracyBankCardNumberCount.name())
        count3 = 'chinesename', res.get(AccuracyChineseNameCount.name())
        count4 = 'date', res.get(AccuracyDateCount.name())
        count5 = 'email', res.get(AccuracyEmailCount.name())
        count6 = 'idnumber', res.get(AccuracyIdNumberCount.name())
        count7 = 'ipaddress', res.get(AccuracyIpAddressCount.name())
        count8 = 'phone', res.get(AccuracyPhoneCount.name())
        count9 = 'postcode', res.get(AccuracyPostCodeCount.name())
        count10 = 'url', res.get(AccuracyUrlCount.name())
        if not total_count:
            logger.info(f"total_count is not valid: {total_count}")
            return None
        
        max_count = 0
        max_type = None
        counts = [count1, count2, count3, count4, count5, count6, count7, count8, count9, count10]
        for type, a_count in counts:
            if a_count is not None and a_count > max_count:
                max_count = a_count
                max_type = type
        
        ratio = max_count / total_count
        if ratio >= 0.5:
            logger.info(f"ratio: {ratio}, total count: {total_count}, max_count: {max_count}, max_type: {max_type}")
            return ratio
        return 0

