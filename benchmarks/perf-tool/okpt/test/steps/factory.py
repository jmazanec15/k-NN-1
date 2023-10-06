# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Factory for creating steps."""

from okpt.io.config.parsers.base import ConfigurationError
from okpt.test.steps.base import Step, StepConfig

from okpt.test.steps.steps import QueryStep


def create_step(step_config: StepConfig) -> Step:
    if step_config.step_name == QueryStep.label:
        return QueryStep(step_config)

    raise ConfigurationError(f'Invalid step {step_config.step_name}')
