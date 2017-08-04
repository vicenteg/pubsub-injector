#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
from dateutil import parser as dateparser
import decimal
import datetime
import json
import random
import re
import sched
import sys
import tempfile
import time

from google.cloud import pubsub
from google.cloud import storage

from tabulator import Stream
from tableschema import infer
from tableschema import Schema
from tableschema import exceptions

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

def download_gcs_file_and_open(gcs_filename):
    local_download = None

    def __download_gcs_file_and_open(gcs_filename):
        blob_path = re.sub('gs://', '', gcs_filename)
        first_slash = blob_path.index('/')
        bucket_name = blob_path[0:first_slash]
        blob_filename = blob_path[first_slash+1:]
        client = storage.Client()
        bucket = storage.Bucket(client,bucket_name)

        blob = bucket.get_blob(blob_filename)
        f = tempfile.NamedTemporaryFile()
        blob.download_to_file(f)
        f.seek(0)
        return f

    if not local_download:
        local_download = __download_gcs_file_and_open(gcs_filename)

    return local_download

def open_local_or_gcs(filename):
    if filename.startswith("gs://"):
        return download_gcs_file_and_open(filename)
    else:
        return open(filename)


def sample_source(iterable, samplesize):
    results = []
    iterator = iter(iterable)
    # Fill in the first samplesize elements:
    try:
        for _ in range(samplesize):
            results.append(iterator.next())
    except StopIteration:
        pass
#        raise ValueError("Sample larger than population.")

    for i, v in enumerate(iterator):
        r = random.randint(0, i)

        if r < samplesize:
            results[r] = v  # at a decreasing rate, replace random items

        #if i > samplesize * 1000:
        #    break

    return results


def pubsub_emit_fn(data, arguments):
    kwargs = arguments
    try:
        topic = kwargs["topic"]
    except KeyError as e:
        raise ValueError("No topic available in keyword arguments.")

    message = json.dumps(data, sort_keys=True, cls=JSONEncoder)
    return topic.publish(message)


def default_emit_fn(data, arguments):
    print(json.dumps(data, sort_keys=True, cls=JSONEncoder))


class emitter:
    def __init__(self, **kwargs):
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.last_timestamp = None
        self.topic = kwargs.get("topic", None)
        self.emit_fn = kwargs.get("fn", default_emit_fn)
        self.realtime = kwargs.get("realtime", False)
        self.replay = kwargs.get("replay", False)
        self.timestamp_field = kwargs.get("timestamp_field")

        self.kwargs = kwargs
        self.start_time = datetime.datetime.now()

    def emit(self, data):
        try:
            # In case date and time are split across columns
            timestamp_fields = self.timestamp_field.split(',', 2)

            if len(timestamp_fields) == 1:
                timestamp_string = data[self.timestamp_field]
            else:
                timestamp_string = " ".join([data[f] for f in timestamp_fields])
        except KeyError as e:
            raise ValueError("Timestamp column '{}' was not found.".format(self.timestamp_field))

        try:
            timestamp = dateparser.parse(timestamp_string)
        except ValueError as e:
            print("Timestamp string '{}' could not be parsed.".format(timestamp_string), file=sys.stderr)
            return

        now = datetime.datetime.now()
        new_timestamp = timestamp.replace(year=now.year, month=now.month, day=now.day)

        if self.replay:
            if len(timestamp_fields) == 1:
                data[self.timestamp_field] = new_timestamp
            else:
                data[timestamp_fields[0]] = new_timestamp.date().isoformat()
                data[timestamp_fields[1]] = new_timestamp.time().isoformat()

        if self.replay:
            if new_timestamp < datetime.datetime.now():
                # If we are replaying events, skip ones that occurred before the current time.
                return

            t = time.mktime((new_timestamp.year, new_timestamp.month, new_timestamp.day,
                             new_timestamp.hour, new_timestamp.minute, new_timestamp.second,
                                 -1, -1, -1)) + new_timestamp.microsecond / 1e6
            s = self.scheduler.enterabs
        else:
            if self.realtime and self.last_timestamp:
                t = (timestamp - self.last_timestamp).total_seconds()
            else:
                t = 0
            s = self.scheduler.enter

        # print("Emit: {}".format(data), file=sys.stderr)
        s(t, 0, self.emit_fn, [data, self.kwargs])
        self.last_timestamp = timestamp


    def run(self):
        self.scheduler.run()


def generate_headers(filename, passed_headers=None, generate_dummy_headers=False):
    with open_local_or_gcs(filename) as f:
        sample = f.read(10240).decode('utf-8')
        has_headers = csv.Sniffer().has_header(sample)

    with Stream(open_local_or_gcs(filename), format="csv", headers=has_headers) as f:
        if passed_headers:
            headers = passed_headers.split(",")
        elif has_headers:
            headers = f.headers
        elif generate_dummy_headers:
            headers = ["column_{}".format(i) for i,c in enumerate(f.iter()[0])]
        else:
            print("No headers found. You should pass some in, " +
                  "or let generate them using --generate-dummy-headers.", file=sys.stderr)
            raise ValueError("No headers found.")
    return headers


def infer_file_schema(filename, headers=[], infer_row_limit=100):
    with Stream(filename, format="csv") as f:
        sample = sample_source(f, infer_row_limit)
        return infer(headers, sample, row_limit=infer_row_limit)

def json_injector(filename, timestamp_column=None, topic=None, realtime=False, replay=False):
    with open(filename, "r") as f:
        emitter_args = {
            "realtime": realtime,
            "replay": replay,
        }

        if topic:
            emitter_args.update({"topic": topic, "fn": pubsub_emit_fn})
        else:
            emitter_args.update({"fn": default_emit_fn})
        e = emitter(timestamp_field=timestamp_column, **emitter_args)

        for i, row in enumerate(f):
            e.emit(data=json.loads(row))
            if i % 1000 == 0:
                e.run()
        e.run()


def injector(filename, inferred_schema=None, headers=None, timestamp_column=None, topic=None, realtime=False, replay=False):
    with Stream(open_local_or_gcs(filename), format="csv") as f:
        if inferred_schema:
            try:
                schema_obj = Schema(inferred_schema)
            except exceptions.SchemaValidationError:
                print("Something went wrong inferring the schema. "
                      "Try not inferring the schema with --no-infer-schema.", file=sys.stderr)
                print("I'll exit now, since there's nothing more I can do.", file=sys.stderr)
                sys.exit(1)
        else:
            schema_obj = None

        emitter_args = {
            "realtime": realtime,
            "replay": replay,
        }

        if topic:
            emitter_args.update({"topic": topic, "fn": pubsub_emit_fn})
        else:
            emitter_args.update({"fn": default_emit_fn})
        e = emitter(timestamp_field=timestamp_column, **emitter_args)

        for i, row in enumerate(iter(f.iter())):
            if schema_obj:
                row = schema_obj.cast_row(row)

            d = dict(zip(headers, row))
            e.emit(data=d)
            if i % 1000 == 0:
                e.run()
        e.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--headers", type=str, required=False,
                        help="List of headers, if they're not in the file or you want to override them.")
    parser.add_argument("--infer-row-limit", type=int, required=False, default=100,
                        help="Set the number of rows to sample for inferring schema.")
    parser.add_argument("--generate-dummy-headers", action='store_true',
                        help="Generate column names. Use this if your CSV has no headers.")
    parser.add_argument("--timestamp-column", type=str, required=True,
                        help="Set this to the column name that contains the timestamp for the row.")
    parser.add_argument("--topic", type=str, required=False,
                        help="Emit rows to Google Cloud Pubsub.")
    parser.add_argument("--realtime", action='store_true', required=False,
                        help="Emit rows using actual deltas between events.")
    parser.add_argument("--replay", action='store_true', required=False,
                        help="If set, replace the date portion of the timestamp with today, "
                             "and emit events occurring after the current time.")
    parser.add_argument("--no-infer-schema", action='store_true', required=False, default=False,
                        help="If schema inference is failing, disable it with this option.")
    parser.add_argument("--file", type=str, required=True,
                        help="Filename of file to inject.")

    args = parser.parse_args(sys.argv[1:])

    filename = args.file
    timestamp_column = args.timestamp_column

    topic = None
    if args.topic:
        try:
            client = pubsub.Client()
            topic = client.topic(args.topic)
        except Exception as e:
            print("Exception caught when connecting to PubSub: {}".format(e.message))
            sys.exit(1)


    headers = None
    inferred_schema = None
    if not filename.endswith('.json'):
        headers = generate_headers(filename,
                                   passed_headers=args.headers,
                                   generate_dummy_headers=args.generate_dummy_headers)

        if args.no_infer_schema:
            inferred_schema = None
        else:
            inferred_schema = infer_file_schema(filename,
                                                headers=headers,
                                                infer_row_limit=args.infer_row_limit)

    if filename.endswith('.json'):
        json_injector(filename,timestamp_column=timestamp_column,topic=topic,replay=args.replay,realtime=args.realtime)
    else:
        injector(filename,
             inferred_schema=inferred_schema,
             headers=headers,
             timestamp_column=timestamp_column,
             topic=topic,
             replay=args.replay,
             realtime=args.realtime)


if __name__ == "__main__": main()
