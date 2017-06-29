#!/usr/bin/env python3

import argparse
import datetime
import json
import sched
import time
import csv
import sys

from dateutil import parser
from dateutil.tz import tzlocal

from google.cloud import pubsub


def main():
    arg_parser = argparse.ArgumentParser(description='Inject CSV or JSON files to Pub/Sub.')
    arg_parser.add_argument('--filename', type=str, nargs=1, required=True, help='Filename to inject.')
    arg_parser.add_argument('--headers', type=str, nargs=1, required=False,
                            help='Comma-delimited list of headers. Required if CSV does not include headers.')
    arg_parser.add_argument('--timestamp_field_name', type=str, default='Timestamp', required=False)
    arg_parser.add_argument('--normalize_to_now', action='store_true', default=False, required=False,
                            help="Use this option if you want to replay data as though it occurred today, in real time.")

    args = arg_parser.parse_args(sys.argv[1:])

    filename = args.filename[0]
    headers = None

    if args.headers:
        headers = [fieldName.strip() for fieldName in args.headers[0].split(',')]

    previous_timestamp = None
    timestampFieldName = args.timestamp_field_name
    normalizeToNow = args.normalize_to_now

    if filename.endswith("sv"):
        lines = (csvToJson(filename, headers))
    else:
        lines = iter(open(filename, "r").readlines())

    scheduler = sched.scheduler(time.time, time.sleep)
    start_time = None
    line_count = 0
    for line in lines:
        d = json.loads(line)
        try:
            timestamp = d[timestampFieldName]
        except KeyError as e:
            print("The specified timestamp key '{}' does not exist in this line: {}".format(
                timestampFieldName, line))
            raise

        # current_timestamp = None
        try:
            current_timestamp = parser.parse(timestamp)
            # print("TS: {}, TZ: {}".format(current_timestamp, current_timestamp.tzinfo))
        except ValueError as e:
            print(e)
            raise

        if start_time is None:
            start_time = datetime.datetime.now(current_timestamp.tzinfo).time()

        if previous_timestamp is None:
            previous_timestamp = current_timestamp

        delta = current_timestamp - previous_timestamp
        previous_timestamp = current_timestamp

        # print("{}, {}".format(current_timestamp.astimezone(current_timestamp.tzinfo).time(), start_time))

        if normalizeToNow:
            now = datetime.datetime.now(current_timestamp.tzinfo)
            current_timestamp = current_timestamp.replace(year=now.year, month=now.month, day=now.day)
            d[timestampFieldName] = current_timestamp.isoformat()

        if normalizeToNow and current_timestamp.astimezone(tzlocal()).time() < start_time:
            continue

        scheduler.enterabs(time=current_timestamp.timestamp(), priority=1, action=emitFunction,
                        argument=[json.dumps(d, sort_keys=True, separators=(',', ':'))])

        line_count+=1
        if line_count % 1000 == 0:
            scheduler.run()
        # emitRecord(json.dumps(d, sort_keys=True, separators=(',', ':')), delay=delta.total_seconds())


def csvToJson(inputFilename, headers=None):
    if not inputFilename.endswith("sv"):
        raise ValueError("The supplied filename does not have a .*sv extension.")

    with open(inputFilename, "r") as f:
        sample = f.read(10240)
        dialect = csv.Sniffer().sniff(sample)
        hasHeaders = csv.Sniffer().has_header(sample)

    csvFile = open(inputFilename, "r")
    reader = csv.reader(csvFile, dialect)

    if hasHeaders:
        headers = reader.next()

    if hasHeaders is False and not headers:
        raise ValueError(
            "Please provide a list of headers since the file does not appear to include them.",
            "Column headers are needed to create JSON keys.")

    return iter(json.dumps(dict((zip(headers, fields))), sort_keys=True, separators=(',',':')) for fields in reader)


def emitFunction(jsonString):
    print(jsonString)


if __name__ == "__main__":
    main()
