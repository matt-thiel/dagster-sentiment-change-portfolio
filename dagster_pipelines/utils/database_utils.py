"""
Database utilities for managing ArcticDB operations and maintenance.

This module provides utility functions for database maintenance tasks such as
fragmentation checking and defragmentation of ArcticDB symbols.
"""

def check_db_fragmentation(db_symbol: str, db_lib: object, logger: object, frag_threshold: int | None = None) -> None:
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
            logger.warning(f"Segment count for symbol '{db_symbol}' is above threshold, defragmenting...")
            db_lib.defragment_symbol(db_symbol)
        else:
            logger.info(f"Segment count for symbol '{db_symbol}' is below threshold, no defragmentation required.")
    except Exception as e:
        logger.error(f"Error checking database fragmentation for symbol '{db_symbol}': {e}")

def print_arcticdb_summary(store, logger):
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
            #logger.info("Library: %s", lib_name)
            lib = store[lib_name]
            symbols = lib.list_symbols()
            if not symbols:
                logger.info("  (No symbols)")
            for symbol in symbols:
                logger.info("Library: %s", lib_name)
                logger.info("  └─ Symbol: %s", symbol)
                try:
                    item = lib.read(symbol)
                    data = item.data
                    metadata = item.metadata
                    shape = getattr(data, 'shape', None)
                    if shape:
                        logger.info(
                            "      Shape: %s rows x %s cols", shape[0], shape[1]
                        )
                    else:
                        logger.info("      Data type: %s", type(data))
                    # Print symbol description details (do not print storage size)
                    desc = lib.get_description(symbol)
                    logger.info("      Column Count: %s", len(desc.columns))
                    logger.info("      Row count: %s", desc.row_count)
                    logger.info("      Last update time (UTC): %s", desc.last_update_time)
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
                except Exception as symbol_exc:
                    logger.warning("    Could not read symbol '%s': %s", symbol, symbol_exc)
    except Exception as exc:
        logger.error("Error summarizing ArcticDB: %s", exc)