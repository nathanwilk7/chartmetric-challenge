import logging

import typer

from chartmetric_challenge import VALID_INGESTORS


def main(
    source_path: str,
    ingestor_type: str,
    output_directory: str,
    pg_connection_string: str,
    granularity: str = "day",
    log_level: str = "INFO",
):
    '''
    Given a source_path to the input file, an ingestor_type (see `constants.py`),
    then outputs CSVs to the output_directory which are then loaded into the
    database specified by pg_connection_string.

    Currently, the only supported granularity is "day", which means it loads only the
    latest record for each day if there are multiple records for the same entity on
    the same day.
    '''
    logging.basicConfig(level=log_level)

    if ingestor_type not in VALID_INGESTORS:
        raise ValueError(f"ingestor_type must be one of {VALID_INGESTORS}")

    ingestor = VALID_INGESTORS[ingestor_type](granularity, source_path, output_directory, pg_connection_string)
    logging.info(f"Starting ingestor {ingestor_type}")
    ingestor.ingest()
    logging.info(f"Finished ingestor {ingestor_type}")


if __name__ == "__main__":
    typer.run(main)
