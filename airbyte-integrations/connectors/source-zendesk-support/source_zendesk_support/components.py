# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from typing import Any, Iterable, List, Mapping

import requests

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor


class ZendeskSupportExtractorEvents(RecordExtractor):
    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        try:
            records = response.json().get("ticket_events") or []
        except requests.exceptions.JSONDecodeError:
            records = []

        events = []
        for record in records:
            for event in record.get("child_events", []):
                if event.get("event_type") == "Comment":
                    for prop in ["via_reference_id", "ticket_id", "timestamp"]:
                        event[prop] = record.get(prop)

                    # https://github.com/airbytehq/oncall/issues/1001
                    if not isinstance(event.get("via"), dict):
                        event["via"] = None
                    events.append(event)
        return events


class ZendeskSupportExtractorEventsWithEndTime(RecordExtractor):
    """
    Custom extractor for ticket comments that adds the response-level end_time
    field to each child comment record for use as cursor field in incremental sync.
    """
    def extract_records(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        def is_comment_event(maybe_comment_event):
            return maybe_comment_event.get("event_type") == "Comment"

        response_data = response.json()
        records = response_data.get("ticket_events") or []
        end_time = response_data.get("end_time")

        # Combine both loops: iterate over records and their comment events
        for record, event in (
            (record, event)
            for record in records
            for event in filter(is_comment_event, record.get("child_events", []))
        ):
            # Add parent properties
            for prop in ["via_reference_id", "ticket_id", "timestamp"]:
                event[prop] = record.get(prop)

            # Add response-level end_time for cursor field
            event["end_time_cursor"] = end_time

            yield event


class ZendeskSupportAttributeDefinitionsExtractor(RecordExtractor):
    def extract_records(self, response: requests.Response) -> List[Mapping[str, Any]]:
        try:
            records = []
            for definition in response.json()["definitions"]["conditions_all"]:
                definition["condition"] = "all"
                records.append(definition)
            for definition in response.json()["definitions"]["conditions_any"]:
                definition["condition"] = "any"
                records.append(definition)
        except requests.exceptions.JSONDecodeError:
            records = []
        return records
