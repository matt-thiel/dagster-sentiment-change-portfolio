"""
Database utilities for managing ArcticDB operations and maintenance.

This module provides utility functions for database maintenance tasks such as
fragmentation checking and defragmentation of ArcticDB symbols.
"""

from arcticdb.exceptions import ArcticNativeException


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
                "Segment count for symbol '%s' is below threshold, no defragmentation required.",
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
                    metadata = lib.read_metadata(symbol)
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
