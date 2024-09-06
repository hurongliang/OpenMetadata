/*
 *  Copyright 2024 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import test, { expect } from '@playwright/test';
import { POSTGRES, REDSHIFT } from '../../constant/service';
import { GlobalSettingOptions } from '../../constant/settings';
import AirflowIngestionClass from '../../support/entity/ingestion/AirflowIngestionClass';
import BigQueryIngestionClass from '../../support/entity/ingestion/BigQueryIngestionClass';
import KafkaIngestionClass from '../../support/entity/ingestion/KafkaIngestionClass';
import MetabaseIngestionClass from '../../support/entity/ingestion/MetabaseIngestionClass';
import MlFlowIngestionClass from '../../support/entity/ingestion/MlFlowIngestionClass';
import MysqlIngestionClass from '../../support/entity/ingestion/MySqlIngestionClass';
import PostgresIngestionClass from '../../support/entity/ingestion/PostgresIngestionClass';
import RedshiftWithDBTIngestionClass from '../../support/entity/ingestion/RedshiftWithDBTIngestionClass';
import S3IngestionClass from '../../support/entity/ingestion/S3IngestionClass';
import SnowflakeIngestionClass from '../../support/entity/ingestion/SnowflakeIngestionClass';
import SupersetIngestionClass from '../../support/entity/ingestion/SupersetIngestionClass';
import { INVALID_NAMES, redirectToHomePage } from '../../utils/common';
import { settingClick, SettingOptionsType } from '../../utils/sidebar';

const services = [
  S3IngestionClass,
  MetabaseIngestionClass,
  MysqlIngestionClass,
  BigQueryIngestionClass,
  KafkaIngestionClass,
  MlFlowIngestionClass,
  SnowflakeIngestionClass,
  SupersetIngestionClass,
  PostgresIngestionClass,
  RedshiftWithDBTIngestionClass,
];

if (process.env.PLAYWRIGHT_IS_OSS) {
  services.push(AirflowIngestionClass);
}

// use the admin user to login
test.use({
  storageState: 'playwright/.auth/admin.json',
  trace: process.env.PLAYWRIGHT_IS_OSS ? 'off' : 'on-first-retry',
});

services.forEach((ServiceClass) => {
  const service = new ServiceClass();

  test.describe.configure({
    // 11 minutes max for ingestion tests
    timeout: 11 * 60 * 1000,
  });

  test.describe.serial(service.serviceType, { tag: '@ingestion' }, async () => {
    test.beforeEach('Visit entity details page', async ({ page }) => {
      await redirectToHomePage(page);
      await settingClick(
        page,
        service.category as unknown as SettingOptionsType
      );
    });

    test(`Create & Ingest ${service.serviceType} service`, async ({ page }) => {
      await service.createService(page);
    });

    test(`Update description and verify description after re-run`, async ({
      page,
    }) => {
      await service.updateService(page);
    });

    test(`Update schedule options and verify`, async ({ page }) => {
      await service.updateScheduleOptions(page);
    });

    if (
      [POSTGRES.serviceType, REDSHIFT.serviceType].includes(service.serviceType)
    ) {
      test(`Service specific tests`, async ({ page }) => {
        await service.runAdditionalTests(page, test);
      });
    }

    test(`Delete ${service.serviceType} service`, async ({ page }) => {
      await service.deleteService(page);
    });
  });
});

test.describe('Service form', () => {
  test('name field should throw error for invalid name', async ({ page }) => {
    await redirectToHomePage(page);
    await settingClick(page, GlobalSettingOptions.DATABASES);
    await page.click('[data-testid="add-service-button"]');
    await page.click('[data-testid="Mysql"]');
    await page.click('[data-testid="next-button"]');

    await page.waitForSelector('[data-testid="service-name"]');
    await page.click('[data-testid="next-button"]');

    await expect(page.locator('#name_help')).toBeVisible();
    await expect(page.locator('#name_help')).toHaveText('Name is required');

    await page.fill(
      '[data-testid="service-name"]',
      INVALID_NAMES.WITH_SPECIAL_CHARS
    );

    await expect(page.locator('#name_help')).toBeVisible();
    await expect(page.locator('#name_help')).toHaveText(
      'Name must contain only letters, numbers, underscores, hyphens, periods, parenthesis, and ampersands.'
    );

    await page.fill('[data-testid="service-name"]', 'test-service');

    await page.click('[data-testid="next-button"]');

    await expect(page.getByTestId('step-icon-3')).toHaveClass(/active/);
  });
});
