"""Process logging defaults that exclude callback URLs and HTTP wire data."""

import logging


def configure_logging() -> None:
    """Keep application diagnostics while suppressing HTTP client request/wire logs."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)
