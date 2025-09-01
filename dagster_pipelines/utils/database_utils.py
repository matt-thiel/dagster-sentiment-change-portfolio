"""
Database utilities for managing ArcticDB operations and maintenance.

This module provides utility functions for database maintenance tasks such as
fragmentation checking and defragmentation of ArcticDB symbols.
"""

import os
import glob
from arcticdb.exceptions import ArcticNativeException
import pandas as pd


def check_db_fragmentation(
    db_symbol: str,
    db_lib: object,
    logger: object,
    frag_threshold: int | None = None,
) -> None:
    """
    Checks if an ArcticDB symbol is fragmented and defragments if necessary.

    This function examines the segment count for a given symbol in ArcticDB and
    performs defragmentation if the count exceeds the specified threshold.
    Defragmentation helps optimize read performance and reduce storage overhead.

    Args:
        db_symbol (str): The symbol name to check for fragmentation.
        db_lib (object): ArcticDB library instance containing the symbol.
        logger (object): Logger object for logging messages and warnings.
        frag_threshold (int | None, optional): Maximum allowed segment count before
            defragmentation is triggered. If None, uses ArcticDB's default threshold.

    Returns:
        None

    Raises:
        Exception: If there are issues checking fragmentation or performing defragmentation.
    """
    try:
        if db_lib.is_symbol_fragmented(db_symbol, segment_size=frag_threshold):
            logger.warning(
                "Segment count for symbol '%s' is above threshold, defragmenting...",
                db_symbol,
            )
            db_lib.defragment_symbol(db_symbol)
        else:
            logger.info(
                "No defragmentation required for symbol '%s'.",
                db_symbol,
            )
    except ArcticNativeException as e:
        logger.error(
            "Error checking database fragmentation for symbol '%s': %s",
            db_symbol,
            e,
        )


# Debugging info function requires a lot of locals and branches
# pylint: disable=too-many-locals, too-many-branches, too-many-nested-blocks
def print_arcticdb_summary(store: object, logger: object) -> None:
    """
    Print a summary of the ArcticDB database structure, including libraries, symbols,
    shape of each symbol (rows/columns), and storage size if available.

    Args:
        store (arcticdb.Arctic): The ArcticDB store object.
        logger (object): Logger object for logging messages and warnings.

    Returns:
        None
    """
    try:
        libraries = store.list_libraries()
        logger.info("ArcticDB Libraries: %s", libraries)
        logger.info("ArcticDB Structure:")
        for lib_name in libraries:
            lib = store[lib_name]
            symbols = lib.list_symbols()
            if not symbols:
                logger.info("Library: %s", lib_name)
                logger.info("  (No symbols)")
                continue
            for symbol in symbols:
                logger.info("Library: %s", lib_name)
                logger.info("  └─ Symbol: %s", symbol)
                try:
                    metadata = lib.read_metadata(symbol).metadata
                    # Print symbol description details (do not print storage size)
                    desc = lib.get_description(symbol)
                    logger.info("      Column Count: %s", len(desc.columns))
                    logger.info("      Row count: %s", desc.row_count)
                    logger.info(
                        "      Last update time (UTC): %s",
                        desc.last_update_time,
                    )
                    if metadata:
                        logger.info("      Metadata:")
                        for k, v in metadata.items():
                            logger.info("        %s: %s", k, v)
                    # Print all versions of the symbol
                    versions = lib.list_versions(symbol)
                    logger.info("      Versions:")
                    for symver, vinfo in versions.items():
                        logger.info("      ├─ Version %s:", symver.version)
                        logger.info("      │   Date (UTC): %s", vinfo.date)
                        logger.info("      │   Deleted: %s", vinfo.deleted)
                        logger.info("      │   Snapshots: %s", vinfo.snapshots)
                except ArcticNativeException as symbol_exc:
                    logger.warning(
                        "    Could not read symbol '%s': %s", symbol, symbol_exc
                    )
    except ArcticNativeException as exc:
        logger.error("Error summarizing ArcticDB: %s", exc)


def print_arcticdb_symbol(
    symbol: str, library: str, arctic_object: object, logger: object
) -> None:
    """
    Print the dataframe info for a given symbol in ArcticDB.

    Args:
        symbol (str): The symbol to print the dataframe info for.
        library (str): The library to read the symbol from.
        arctic_object (object): The ArcticDB object to read the symbol from.
        logger (object): Logger object for logging messages and warnings.
    """
    try:
        symbol_data = arctic_object[library].read(symbol)
        logger.info("Symbol dataframe info: %s", symbol_data.data.info())
        logger.info("Symbol dataframe head: %s", symbol_data.data.head())
        logger.info("Symbol dataframe tail: %s", symbol_data.data.tail())
    except ArcticNativeException as exc:
        logger.error("Error reading symbol '%s': %s", symbol, exc)


# Needs many arguments for flexibility.
# pylint: disable=too-many-arguments
def arctic_db_write_or_append(
    symbol: str,
    arctic_library: object,
    data: pd.DataFrame,
    metadata: dict,
    prune_previous_versions: bool = True,
    force_write: bool = False,
) -> None:
    """
    Write data to ArcticDB if the symbol does not exist, otherwise append to the exisitng symbol.

    Args:
        symbol (str): The symbol to write or append to.
        arctic_library (object): ArcticDB library instance.
        data (pd.DataFrame): The data to write or append.
        metadata (dict): The metadata to write or append.
        prune_previous_versions (bool): Whether to prune previous versions.
        force_write (bool): Whether to write the symbol even if it already exists.
    Returns:
        None
    """
    if arctic_library.has_symbol(symbol) and not force_write:
        arctic_library.append(
            symbol=symbol,
            data=data,
            metadata=metadata,
            prune_previous_versions=prune_previous_versions,
        )
    else:
        arctic_library.write(
            symbol=symbol,
            data=data,
            metadata=metadata,
            prune_previous_versions=prune_previous_versions,
        )


# Needs many arguments for flexibility.
# pylint: disable=too-many-arguments
def arctic_db_batch_update(
    symbol: str,
    arctic_library: object,
    new_data: pd.DataFrame,
    new_metadata: dict,
    logger: object,
    batch_size: int = 100,
    prune_previous_versions: bool = True,
    allow_mismatched_indices: bool = False,
) -> None:
    """
    Batch update a symbol in ArcticDB.

    Args:
        symbol (str): The symbol to update.
        arctic_library (object): ArcticDB library instance.
        new_data (pd.DataFrame): The new data to update symbol with.
        new_metadata (dict): The new metadata to update symbol with.
        logger (object): Logger object for logging messages and warnings.
        batch_size (int): Number of rows for each batch
        prune_previous_versions (bool): Whether to prune previous symbol versions.
        allow_mismatched_indices (bool): If true, add mismatched rows to existing data.
    """

    batch_start = 0
    rows_to_update = len(new_data)
    while batch_start < rows_to_update:
        batch = new_data.iloc[batch_start : batch_start + batch_size]
        date_range = batch.index[[0, -1]]
        existing_batch = arctic_library.read(symbol, date_range=date_range).data
        if not batch.index.equals(existing_batch.index):
            if not allow_mismatched_indices:
                logger.warning(
                    "Index mismatch between update batch and exisiting data. "
                    "Rows not in original data will be dropped."
                )
                logger.info(
                    "Mismatch occured between %s and %s in new data.",
                    date_range[0],
                    date_range[-1],
                )
            else:
                logger.warning(
                    "Index mismatch between update batch and exisiting data. "
                    "Rows not in original data will be added."
                )
                logger.info(
                    "Mismatch occured between %s and %s in new data.",
                    date_range[0],
                    date_range[-1],
                )

        if allow_mismatched_indices:
            # Overwrites exsiting with batch and adds new rows/columns.
            update_df = batch.combine_first(existing_batch)
        else:
            # assign() efficiently handles both updating existing columns and adding new ones
            # Create a copy to defragment the DataFrame and avoid performance warnings
            update_df = existing_batch.copy().assign(**batch)

        batch_start += batch_size

        # Only update metadata on last batch for efficiency
        if batch_start >= rows_to_update:
            arctic_library.update(
                symbol,
                update_df,
                metadata=new_metadata,
                prune_previous_versions=prune_previous_versions,
            )
        else:
            arctic_library.update(
                symbol, update_df, prune_previous_versions=prune_previous_versions
            )


def compare_files_to_timestamp(
    output_dir: str, current_timestamp: str, prefix: str, precision: int
) -> bool:
    """
    Check if a sentiment file exists up to the hour timestamp.

    This function checks if there's already a sentiment file saved within the same hour
    as the current timestamp. For example, if current time is 1:02 PM and there's already
    a file from 1:01 PM, it will return True to indicate the file should be skipped.

    Args:
        output_dir (str): Directory where sentiment files are saved.
        current_timestamp (str): Current timestamp in YYYYMMDDHHMMSS format.
        prefix (str): Prefix of the file name.
        precision (int): Number of characters to match in the timestamp.

    Returns:
        bool: True if a file exists from the same hour, False otherwise.
    """
    if not os.path.exists(output_dir):
        return False

    # Extract hour timestamp (YYYYMMDDHH) from current timestamp
    hour_timestamp = current_timestamp[:precision]  # YYYYMMDDHH

    # Pattern to match files from the same hour
    pattern = os.path.join(output_dir, f"{prefix}{hour_timestamp}*.csv")

    # Check if any files exist matching this pattern
    existing_files = glob.glob(pattern)

    return len(existing_files) > 0
