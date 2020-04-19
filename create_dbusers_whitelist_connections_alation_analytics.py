#!/opt/alation/env/bin/python
# coding=utf-8
"""Create user accounts on Alation Analytics Postgres after backup and restore.

This script assumes that the feature Alation Analytics is already turned on and this instance is
restored from a backup of a different Alation instance.

CAUTION: Once this script is executed all the database users on Alation Analytics will
have to reset their credentials.

Usage:
    python create_aa_user_accounts_on_postgres
"""
# Special import required to add django apps to the path.
# coding=utf-8
import os
import sys

# Set up the environment.
# Special import required to add django models to the python path.
from rosemeta.one_off_scripts import bootstrap_rosemeta

from django.core.exceptions import MultipleObjectsReturned

from db_metadata.enums import BuiltinDataSource
from rosemeta.models import DataSource

path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(path, '../../'))


import sys
import logging
import uuid

from db_management.tasks.database_external_connection_tasks import sync_all_db_connection_and_reload
from db_management.models import DBUser
from db_management.utils.db_utils import UserAccountAlreadyExistsOnDBException


logger = logging.getLogger(__name__)


def _create_db_users_on_alation_analytics_db():
    number_of_accounts_created = 0
    number_of_accounts_already_existing = 0
    for db_user in DBUser.objects.all():
        try:
            temp_password = uuid.uuid4().hex
            db_user.create_db_account(temp_password)
            number_of_accounts_created += 1
        except UserAccountAlreadyExistsOnDBException:
            number_of_accounts_already_existing += 1
            continue
        except Exception as e:
            logger.exception(
                "Unexpected exception occurred. This script will abort now! Please contact "
                "Alation support to resolve the following error: %s", e.message)
            return -1

    logger.info(
        "Successfully finished the script execution. Number of user accounts created: %d, "
        "user accounts already existing: %d", number_of_accounts_created,
        number_of_accounts_already_existing)


def _get_alation_analytics_ds():
    # Check if the DS is already in the catalog.
    ds = None
    try:
        ds = DataSource.objects.get(
            builtin_datasource=BuiltinDataSource.ALATION_ANALYTICS, deleted=False)
    except MultipleObjectsReturned as e:
        logger.exception(e)
        ds = DataSource.objects.filter(
            builtin_datasource=BuiltinDataSource.ALATION_ANALYTICS, deleted=False).last()
    except DataSource.DoesNotExist:
        pass
    return ds


def main():
    _create_db_users_on_alation_analytics_db()
    print("Please note that ALL the users will have to reset their credentials from the UI.")
    analytics_ds = _get_alation_analytics_ds()
    if not analytics_ds:
        print("Could not retrieve the Alation Analytics Datasource in the catalog. Please "
              "contact support@alation.com for further assistance.")
        return -1

    sync_all_db_connection_and_reload.apply_async(kwargs={
        'ds_id': analytics_ds.id,
        'force_reload': True
    })
    print("Scheduled the celery task to whitelist external connections on the Alation Analytics "
          "Database. ")
    return 0


if __name__ == '__main__':
    sys.exit(main())
