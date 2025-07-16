#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import csv
from dataclasses import asdict
from functools import lru_cache
import json
from abc import ABC, abstractmethod
from io import StringIO
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Union

import pendulum
import requests
from requests import HTTPError
from requests.exceptions import ChunkedEncodingError

from airbyte_cdk.models import SyncMode, AirbyteMessage
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.core import (
    CheckpointMixin,
    package_name_from_class,
    StreamData,
)
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.exceptions import (
    DefaultBackoffException,
    UserDefinedBackoffException,
)
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader
from source_iterable.slice_generators import (
    AdjustableSliceGenerator,
    RangeSliceGenerator,
    IterableStreamSlice,
)
from source_iterable.utils import dateutil_parse


EVENT_ROWS_LIMIT = 200
CAMPAIGNS_PER_REQUEST = 20


class IterableStream(HttpStream, ABC):
    # in case we get a 401 error (api token disabled or deleted) on a stream slice, do not make further requests within the current stream
    # to prevent 429 error on other streams
    ignore_further_slices = False

    _url_base = "https://api.iterable.com/api/"
    _primary_key = "id"

    def __init__(self, authenticator, config=None, **kwargs):
        self._cred = authenticator
        self._slice_retry = 0
        self._config = config
        super().__init__(authenticator)

    @property
    def primary_key(self) -> Optional[str]:
        return self._primary_key

    @property
    def url_base(self) -> str:
        return self._url_base

    @property
    def retry_factor(self) -> int:
        return 20

    # With factor 20 it would be from 20 to 400 seconds delay
    @property
    def max_retries(self) -> Union[int, None]:
        return 10

    @property
    @abstractmethod
    def data_field(self) -> str:
        """
        :return: Default field name to get data from response
        """

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        Iterable API does not support pagination
        """
        return None

    def check_generic_error(self, response: requests.Response) -> bool:
        """
        https://github.com/airbytehq/oncall/issues/1592#issuecomment-1499109251
        https://github.com/airbytehq/oncall/issues/1985
        """
        codes = ["Generic Error", "GenericError"]
        msg_pattern = "Please try again later"

        if response.status_code == 500:
            # I am not sure that all 500 errors return valid json
            try:
                response_json = json.loads(response.text)
            except ValueError:
                return False
            if response_json.get("code") in codes and msg_pattern in response_json.get(
                "msg", ""
            ):
                return True

        return False  # not a generic error

    def request_kwargs(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
        https://github.com/airbytehq/oncall/issues/1985#issuecomment-1559276465
        """
        return {"timeout": (60, 300)}

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        response_json = response.json() or {}
        records = response_json.get(self.data_field, [])
        for record in records:
            yield record

    def should_retry(self, response: requests.Response) -> bool:
        if self.check_generic_error(response):
            self._slice_retry += 1
            if self._slice_retry < 3:
                return True
            return False
        return response.status_code == 429 or 500 <= response.status_code < 600

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        self._slice_retry = 0
        if self.ignore_further_slices:
            return

        try:
            yield from super().read_records(
                sync_mode,
                cursor_field=cursor_field,
                stream_slice=stream_slice,
                stream_state=stream_state,
            )
        except (HTTPError, UserDefinedBackoffException, DefaultBackoffException) as e:
            response = e.response
            if self.check_generic_error(response):
                return
            raise e


class IterableExportStream(IterableStream, CheckpointMixin, ABC):
    """
    This stream utilize "export" Iterable api for getting large amount of data.
    It can return data in form of new line separater strings each of each
    representing json object.
    Data could be windowed by date ranges by applying startDateTime and
    endDateTime parameters.  Single request could return large volumes of data
    and request rate is limited by 4 requests per minute.

    Details: https://api.iterable.com/api/docs#export_exportDataJson
    """

    _cursor_field = "createdAt"
    _primary_key = None

    @property
    def cursor_field(self) -> str:
        return self._cursor_field

    @property
    def primary_key(self) -> Optional[str]:
        return self._primary_key

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = value

    def __init__(
        self, start_date: str | None = None, end_date=None, config=None, **kwargs
    ):
        super().__init__(config=config, **kwargs)
        self._start_date = pendulum.parse(start_date) if start_date else None
        if end_date:
            maybe_end_date = pendulum.parse(end_date)
            if not isinstance(maybe_end_date, pendulum.DateTime):
                raise ValueError(
                    f"End date should be a DateTime or Date, got {type(maybe_end_date)}"
                )
            self._end_date = maybe_end_date
        else:
            self._end_date = None

        self.stream_params = {"dataTypeName": self.data_field}

    def path(self, **kwargs) -> str:
        return "export/data.json"

    @staticmethod
    def _field_to_datetime(value: Union[int, str]) -> pendulum.DateTime:
        if isinstance(value, int):
            return pendulum.from_timestamp(value / 1000.0)
        elif isinstance(value, str):
            return dateutil_parse(value)

        raise ValueError(f"Unsupported type of datetime field {type(value)}")

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        for record in super().read_records(
            sync_mode=sync_mode,
            cursor_field=cursor_field,
            stream_slice=stream_slice,
            stream_state=stream_state,
        ):
            self.state = self._get_updated_state(self.state, record)
            yield record

    def _get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: StreamData,
    ) -> MutableMapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        if isinstance(latest_record, AirbyteMessage):
            print(f"Latest record is AirbyteMessage: {latest_record}")
            raise ValueError(
                "Latest record should not be an AirbyteMessage, it should be a dictionary."
            )
            # latest_benchmark = latest_record.state.data[self.cursor_field]
        else:
            latest_benchmark = latest_record[self.cursor_field]

        if current_stream_state.get(self.cursor_field):
            return {
                self.cursor_field: str(
                    max(
                        self._field_to_datetime(latest_benchmark),
                        self._field_to_datetime(
                            current_stream_state[self.cursor_field]
                        ),
                    )
                )
            }
        return {self.cursor_field: str(latest_benchmark)}

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state)
        if not stream_slice:
            return params

        # raise Exception(
        #     f"IterableExportStream is an abstract class and should not be instantiated directly. Params are: {self.stream_params}"
        # )

        # convert to a IterableStreamSlice
        local_stream_slice = IterableStreamSlice(**stream_slice)

        params.update(
            {
                "startDateTime": local_stream_slice.start_date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "endDateTime": local_stream_slice.end_date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            },
            **self.stream_params,
        )

        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        for obj in response.iter_lines():
            record = json.loads(obj)
            record[self.cursor_field] = self._field_to_datetime(
                record[self.cursor_field]
            ).to_iso8601_string()
            yield record

    def request_kwargs(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """
        https://api.iterable.com/api/docs#export_exportDataJson
        Sending those type of requests could download large piece of json
        objects splitted with newline character.
        Passing stream=True argument to requests.session.send method to avoid
        loading whole analytics report content into memory.
        """
        return {
            **super().request_kwargs(stream_state, stream_slice, next_page_token),
            "stream": True,
        }

    def get_start_date(
        self, stream_state: Mapping[str, Any] | None
    ) -> pendulum.DateTime:
        stream_state = stream_state or {}
        start_datetime = self._start_date
        if stream_state.get(self.cursor_field):
            start_datetime = pendulum.parse(stream_state[self.cursor_field])

        if not isinstance(start_datetime, pendulum.DateTime):
            raise ValueError(
                f"Start date should be a DateTime or Date, got {type(start_datetime)}"
            )
        return start_datetime

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:

        start_datetime = self.get_start_date(stream_state)

        return [
            asdict(
                IterableStreamSlice(
                    start_datetime, self._end_date or pendulum.now("UTC")
                )
            )
        ]


class IterableExportStreamRanged(IterableExportStream, ABC):
    """
    This class use RangeSliceGenerator class to break single request into
    ranges with same (or less for final range) number of days. By default it 90
    days.
    """

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_datetime = self.get_start_date(stream_state)

        return RangeSliceGenerator(start_datetime, self._end_date)


class IterableExportStreamAdjustableRange(IterableExportStream, ABC):
    """
    For streams that could produce large amount of data in single request so we
    cant just use IterableExportStreamRanged to split it in even ranges. If
    request processing takes a lot of time API server could just close
    connection and connector code would fail with ChunkedEncodingError.

    To solve this problem we use AdjustableSliceGenerator that able to adjust
    next slice range based on two factor:
    1. Previous slice range / time to process ratio.
    2. Had previous request failed with ChunkedEncodingError

    In case of slice processing request failed with ChunkedEncodingError (which
    means that API server closed connection cause of request takes to much
    time) make CHUNKED_ENCODING_ERROR_RETRIES (6) retries each time reducing
    slice length.

    See AdjustableSliceGenerator description for more details on next slice length adjustment alghorithm.
    """

    _adjustable_generator: AdjustableSliceGenerator
    CHUNKED_ENCODING_ERROR_RETRIES = 6

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_datetime = self.get_start_date(stream_state)
        self._adjustable_generator = AdjustableSliceGenerator(
            start_datetime, self._end_date, self._config
        )
        return self._adjustable_generator

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        # wtaf
        start_time = pendulum.now()

        in_loop_stream_slice: IterableStreamSlice = IterableStreamSlice(**stream_slice)  # type: ignore

        for _ in range(self.CHUNKED_ENCODING_ERROR_RETRIES):
            try:
                maybe_slice_diff: pendulum.Period = (
                    in_loop_stream_slice.end_date - in_loop_stream_slice.start_date
                )
                if not isinstance(maybe_slice_diff, pendulum.Period):
                    raise ValueError(
                        f"Expected maybe_slice_diff to be pendulum.Period, got {type(maybe_slice_diff)}"
                    )

                self.logger.info(
                    f"Processing slice of {maybe_slice_diff.total_days()} days for stream {self.name}"
                )
                for record in super().read_records(
                    sync_mode=sync_mode,
                    cursor_field=cursor_field,
                    stream_slice=stream_slice,
                    stream_state=stream_state,
                ):
                    now = pendulum.now()
                    self._adjustable_generator.adjust_range(now - start_time)
                    yield record
                    start_time = now
                break
            except ChunkedEncodingError:
                self.logger.warning(
                    "ChunkedEncodingError occurred, decrease days range and try again"
                )
                in_loop_stream_slice = self._adjustable_generator.reduce_range()
        else:
            raise Exception(
                f"ChunkedEncodingError: Reached maximum number of retires: {self.CHUNKED_ENCODING_ERROR_RETRIES}"
            )


class IterableExportEventsStreamAdjustableRange(
    IterableExportStreamAdjustableRange, ABC
):
    @lru_cache(maxsize=None)
    def get_json_schema(self) -> Mapping[str, Any]:
        """All child stream share the same 'events' schema"""
        return ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema(
            "events"
        )


class Campaigns(IterableStream):
    _data_field = "campaigns"

    @property
    def data_field(self) -> str:
        return self._data_field

    def path(self, **kwargs) -> str:
        return "campaigns"


class CampaignsMetrics(IterableStream):

    def __init__(self, start_date: str, end_date: Optional[str] = None, **kwargs):
        """
        https://api.iterable.com/api/docs#campaigns_metrics
        """
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[str]:
        return None

    @property
    def data_field(self) -> str:
        return "campaigns_metrics"

    def path(self, **kwargs) -> str:
        return "campaigns/metrics"

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )

        if not stream_slice:
            return params

        params["campaignId"] = stream_slice.get("campaign_ids")
        params["startDateTime"] = self.start_date
        if self.end_date:
            params["endDateTime"] = self.end_date
        return params

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        lists = Campaigns(authenticator=self._cred)
        campaign_ids = []
        for list_record in lists.read_records(
            sync_mode=kwargs.get("sync_mode", SyncMode.full_refresh)
        ):
            if isinstance(list_record, AirbyteMessage):
                self.logger.warning(
                    f"Received AirbyteMessage instead of campaign record: {list_record}"
                )
                raise ValueError("Expected campaign record, got AirbyteMessage.")

            campaign_ids.append(list_record["id"])

            if len(campaign_ids) == CAMPAIGNS_PER_REQUEST:
                yield {"campaign_ids": campaign_ids}
                campaign_ids = []

        if campaign_ids:
            yield {"campaign_ids": campaign_ids}

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        content = response.content.decode()
        records = self._parse_csv_string_to_dict(content)

        for record in records:
            yield {"data": record}

    @staticmethod
    def _parse_csv_string_to_dict(csv_string: str) -> List[Dict[str, Any]]:
        """
        Parse a response with a csv type to dict object
        Example:
            csv_string = "a,b,c,d
                          1,2,,3
                          6,,1,2"

            output = [{"a": 1, "b": 2, "d": 3},
                      {"a": 6, "c": 1, "d": 2}]


        :param csv_string: API endpoint response with csv format
        :return: parsed API response

        """

        reader = csv.DictReader(StringIO(csv_string), delimiter=",")
        result = []

        for row in reader:
            for key, value in row.items():
                if value == "":
                    continue
                try:
                    row[key] = int(value)
                except ValueError:
                    row[key] = float(value)
            row = {k: v for k, v in row.items() if v != ""}

            result.append(row)

        return result


class EmailBounce(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailBounce"


class EmailClick(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailClick"


class EmailComplaint(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailComplaint"


class EmailOpen(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailOpen"


class EmailSend(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailSend"


class EmailSendSkip(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailSendSkip"


class EmailSubscribe(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailSubscribe"


class EmailUnsubscribe(IterableExportStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "emailUnsubscribe"


class PushSend(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "pushSend"


class PushSendSkip(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "pushSendSkip"


class PushOpen(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "pushOpen"


class PushUninstall(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "pushUninstall"


class PushBounce(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "pushBounce"


class WebPushSend(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "webPushSend"


class WebPushClick(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "webPushClick"


class WebPushSendSkip(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "webPushSendSkip"


class InAppSend(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppSend"


class InAppOpen(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppOpen"


class InAppClick(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppClick"


class InAppClose(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppClose"


class InAppDelete(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppDelete"


class InAppDelivery(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppDelivery"


class InAppSendSkip(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inAppSendSkip"


class InboxSession(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inboxSession"


class InboxMessageImpression(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "inboxMessageImpression"


class SmsSend(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "smsSend"


class SmsBounce(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "smsBounce"


class SmsClick(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "smsClick"


class SmsReceived(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "smsReceived"


class SmsSendSkip(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "smsSendSkip"


class SmsUsageInfo(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "smsUsageInfo"


class Purchase(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "purchase"


class CustomEvent(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "customEvent"


class HostedUnsubscribeClick(IterableExportEventsStreamAdjustableRange):
    @property
    def data_field(self) -> str:
        return "hostedUnsubscribeClick"


class Templates(IterableExportStreamRanged):
    @property
    def data_field(self) -> str:
        return "templates"

    template_types = ["Base", "Blast", "Triggered", "Workflow"]
    message_types = ["Email", "Push", "InApp", "SMS"]

    def path(self, **kwargs) -> str:
        return "templates"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        for template in self.template_types:
            for message in self.message_types:
                self.stream_params = {
                    "templateType": template,
                    "messageMedium": message,
                }
                yield from super().read_records(
                    stream_slice=stream_slice,
                    stream_state=stream_state,
                    sync_mode=sync_mode,
                    cursor_field=cursor_field,
                )

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json.get(self.data_field, [])

        for record in records:
            record[self.cursor_field] = self._field_to_datetime(
                record[self.cursor_field]
            ).to_iso8601_string()
            yield record
