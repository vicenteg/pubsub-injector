#!/usr/bin/env python2

import argparse
import datetime
import json
import sched
import time
import sys
import decimal
import dataconverters.commas as commas

from dateutil import parser

from google.cloud import pubsub


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.time):
            return obj.isoformat()
        if isinstance(obj, str) and obj == "":
            return None
        return json.JSONEncoder.default(self, obj)


def main():
    arg_parser = argparse.ArgumentParser(
        description='Inject CSV or JSON files to Pub/Sub.')
    arg_parser.add_argument('--filename', type=str, nargs=1, required=True,
                            help='Filename to inject.')
    arg_parser.add_argument('--timestamp-field-name', type=str,
                            default='Timestamp', required=True,
                            help='Attempt to parse timestamps in this column.')
    arg_parser.add_argument('--normalize-to-now', action='store_true',
                            default=False, required=False,
                            help='Use this option to replay data as though' +
                                 'it occurred today, in real time.')
    arg_parser.add_argument('--pubsub-topic', type=str, required=False,
                            help='Specify a Google Pub/Sub topic to write ' +
                                 'events to.')

    args = arg_parser.parse_args(sys.argv[1:])

    filename = args.filename[0]

    timestampFieldName = args.timestamp_field_name
    normalizeToNow = args.normalize_to_now

    topic = None
    if args.pubsub_topic:
        client = pubsub.Client()
        topic = client.topic(args.pubsub_topic)

    if filename.endswith("json"):
        lines = iter(open(filename, "r").readlines())
    else:
        lines = (csvToJson(filename))

    scheduler = sched.scheduler(time.time, time.sleep)
    start_time = None
    line_count = 0
    for line in lines:
        try:
            d = json.loads(line)
        except ValueError as e:
            print(e)
            continue
        try:
            timestampFields = timestampFieldName.split(',', 2)
            t = tuple([(d.get(f)) for f in timestampFields])
            current_timestamp = parser.parse(t[0])
            if normalizeToNow:
                if len(t) == 2:
                    d[timestampFields[0]] = current_timestamp.date().strftime('%Y-%m-%d')
                else:
                    d[timestampFields[0]] = current_timestamp.isoformat()
            if len(t) == 2:
                tt = parser.parse(t[1]).time()
                current_timestamp = current_timestamp.replace(
                    hour=tt.hour, minute=tt.minute, second=tt.second)
                if normalizeToNow:
                    d[timestampFields[1]] = current_timestamp.time().isoformat()
        except KeyError as e:
            print("The specified timestamp key '{}' " +
                  "does not exist in this line: {}"
                  .format(timestampFieldName, line))
            raise

        if start_time is None:
            start_time = datetime.datetime.now(current_timestamp.tzinfo).time()

        if not normalizeToNow:
            current_timestamp = datetime.datetime.now()
        else:
            now = datetime.datetime.now(current_timestamp.tzinfo)
            current_timestamp = current_timestamp.replace(
                year=now.year, month=now.month, day=now.day)

        if normalizeToNow and current_timestamp.time() < start_time:
            continue

        scheduleMessage(int(current_timestamp.strftime("%s")),
                        [json.dumps(d, sort_keys=True, separators=(',', ':'))],
                        topic=topic, scheduler=scheduler)

        line_count += 1
        if line_count % 1000 == 0:
            scheduler.run()
        scheduler.run()


def scheduleMessage(timestamp, message, topic=None, scheduler=None):
    if topic:
        fn = topic.publish
    else:
        fn = emitFunction

    print("Scheduling for {}\n{}".format(
        datetime.datetime.fromtimestamp(timestamp), message))
    scheduler.enterabs(time=timestamp, priority=1, action=fn, argument=message)


def csvToJson(inputFilename):
    with open(inputFilename, "rb") as f:
        records, metadata = commas.parse(f, guess_types=True)
    return iter([json.dumps(r, cls=JSONEncoder) for r in records])


def emitFunction(jsonString):
    print(jsonString)


if __name__ == "__main__":
    main()
