""" handlers.py

Custom logging handlers module, including
one that produces messages to Discord channel.

# Requirements:
    - backoff: for managing timeouts/retries
    - requests: for sending the message

Example usage: See main and run_example() below

$ python3 handlers.py [DISCORD_URL] --username=[USERNAME]

That emits an ERROR-level message to the logging console and to Discord.

"""
import argparse
import backoff
import logging
import requests
from typing import Any, Dict, Optional, Union


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException)
def send_discord_msg(webhook_url: str, data: Dict[str, str], timeout=None):
    """
    Send message to Discord channel via webhook.
    Wrapped in an exponential backoff strategy

    Args:
        webhook_url: Discord webhook URL
        data: JSON payload to send, conforming to API spec.
            See: https://discord.com/developers/docs/resources/webhook#execute-webhook
        timeout: Time to wait in seconds until timeout

    Returns:
        None

    Raises:
        RequestException: Any request exception, after backoff complete
    """  # NOQA
    result = requests.post(webhook_url, json=data)
    try:
        result.raise_for_status()
    except Exception as e:
        # Printing since logging can result in infinite loop
        print(f"Unable to send message to Discord. Error: {e}")


class DiscordHandler(logging.StreamHandler):
    """
    Python custom logging handler that will send logs to a Discord channel
    via webhooks
    """

    def __init__(
        self,
        webhook_url: str,
        level: Union[int, str] = logging.ERROR,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Set up which broker(s) and topic to produce to upon logging.

        Args:
            webhook_url: Discord webhook URL
            level: Optional default logging level to assign as a logging.level
            options: Any additional arguments to pass to payload of Discord
                webhook message. The handler will pack these in with 'content'
                (i.e. the message). Examples: username, avatar_url, etc.
                Ref: https://discord.com/developers/docs/resources/webhook#execute-webhook

        Returns:
            None
        """  # NOQA
        super().__init__()
        self._webhook_url = webhook_url
        self._loglevel = level
        self._options = options
        self._MSG_LIMIT = 2000  # Character limit imposed in API

        # Initialize
        logging.debug(
            f"Intitializing Discord logging handler at level {level} with "
            f"configuration options {options}"
        )

        # Set handler logging level
        self.setLevel(level)

    def emit(self, record) -> None:
        """
        Required method when subclassing StreamHandler. Defines what to do when
        handler is triggered

        Args:
            record (str): Message that will be emitted

        Returns:

        """
        # Formats the message if we have a formatter assigned
        msg = self.format(record)

        # Honor Discord API character limit
        msg = msg[: self._MSG_LIMIT]

        # Pack message with other config
        payload = self._options.copy()
        payload["content"] = msg

        # Sends it off
        logging.debug(f"Sending message to discord: {msg}")
        send_discord_msg(self._webhook_url, payload)


def run_example(
    webhook_url: str, username: Optional[str] = None, level: int = logging.ERROR
) -> None:
    """
    Example application. Shows how to set up the logger handler
    and executes with an example message and username.

    Args:
        webhook_url: URL of discord webhook
        username: Optional username to send message with
        level: Optional default logging level to assign

    Returns:
        None
    """
    # Example logging configuration
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Add the Discord handler
    options = {"username": username}
    logging.getLogger().addHandler(DiscordHandler(webhook_url, level, options))

    # Log some examples. Depending on `level` argument, one of these
    # should go to discord
    logging.debug("Logging test message at DEBUG level.")
    logging.info("Logging test message at INFO level.")
    logging.error("Logging test message at ERROR level.")


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(dest="url", type=str, help="Discord webhook URL")
    parser.add_argument("--username", type=str, required=False, help="Username to use")
    parser.add_argument(
        "--level",
        default="error",
        type=str,
        required=False,
        help="log level to trigger message at",
    )
    args = parser.parse_args()

    # Run the example
    run_example(args.url, args.username, args.level.upper())
