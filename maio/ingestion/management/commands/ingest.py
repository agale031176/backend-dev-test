import os
import json
import time
import logging

from django.core.management.base import BaseCommand
from django_redis import get_redis_connection

logger = logging.getLogger('root')


class Command(BaseCommand):
    help = 'Ingest file'

    @classmethod
    def _get_message_from_edge(cls, input_file: str) -> dict:
        """
        Returns data from super complex undocumented system. Consider it
        non mutable (you can rename it, move it, but don't change the contents).
        :return: json converted to dictionary
        """
        logger.info('Processing input file: %s', input_file)
        yield json.load(open(input_file))

    @classmethod
    def _store_message_in_database(cls, timestamp: str, message: str) -> bool:
        """
        Stores given CSV string in proprietary database. Consider it non mutable
        (you can rename it, move it, but don't change the contents).
        :param timestamp: Datetime in format: "2020-01-21T09:32:34" as string
        :param message: CSV represented as string
        :return: boolean result of the call
        """
        time.sleep(0.5)
        r = get_redis_connection('default')
        r.set(timestamp, message)

    def add_arguments(self, parser):
        parser.add_argument("input_file", type=str, help="json file to process")

    def handle(self, *args, **options):
        # Ensure file exists
        if not os.path.exists(options['input_file']):
            raise IOError('File does not exists %s'.format(options['input_file']))

        for message in self._get_message_from_edge(options['input_file']):
            values = message["Values"]
            csv_message = "%s,%s,%s,%s,%s,%s" % (
                values["FACTORY"],
                values["ZONE"],
                values["CELL"],
                values["MACHINE_GROUP"],
                values["MACHINE"],
                values["MACHINE_ID"],
            )
            logger.info('Insert value: %s', csv_message)
            self._store_message_in_database(values["TIMESTAMP"], csv_message)

        logger.info("Finished processing: %s", options['input_file'])
