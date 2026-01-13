import logging
import sys
import time

def setup_logging(log_level: str = "INFO"):
    """
    Configures the root logger with a consistent format.
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Invalid log level: {log_level}. Defaulting to INFO.")
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Use GMT/UTC for timestamps in logs
    logging.Formatter.converter = time.gmtime
    
    # Silence noisy libraries if necessary
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("aiobotocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
