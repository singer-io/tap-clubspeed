import os
import json
import singer
from tap_clubspeed.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def _apply_access_checks(client, catalog_entries: list) -> list:
    """Remove streams that return HTTP 403 during an access probe.

    Raises Exception if no streams are accessible.
    """
    accessible_streams = []
    for entry in catalog_entries:
        stream_name = entry['tap_stream_id']
        instance = STREAMS[stream_name](client)
        if instance.check_access():
            accessible_streams.append(entry)
        else:
            LOGGER.warning(
                "Stream '%s' is not accessible with the provided credentials "
                "(HTTP 403). Excluding from catalog.",
                stream_name,
            )

    if not accessible_streams:
        raise Exception(
            "No streams are accessible with the provided credentials. "
            "Discovery cannot produce a usable catalog."
        )

    return accessible_streams


def discover_streams(client):
    streams = []

    for s in STREAMS.values():
        s = s(client)
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata()})

    streams = _apply_access_checks(client, streams)
    return streams




