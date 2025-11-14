import logging

logging.basicConfig(
    filename="/var/log/openai-data-hub/etl.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logger = logging.getLogger("etl")
