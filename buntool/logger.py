import logging
from typing import Literal


class ColorFormatter(logging.Formatter):
    """A custom log formatter that adds color to the log prefix."""

    BLUE = '\033[94m'
    RESET = '\033[0m'

    def __init__(self, fmt: str, datefmt: str | None = None, style: Literal['%', '{', '$'] = '%'):
        super().__init__(fmt, datefmt, style=style)
        # Split the format string into prefix and message parts
        if '%(message)s' in fmt:
            self.prefix_fmt = fmt.split('%(message)s')[0]
        else:
            self.prefix_fmt = fmt

    def format(self, record):
        # Format the prefix part
        prefix = logging.Formatter(self.prefix_fmt).format(record)
        # Return the colored prefix and the original message
        return f"{self.BLUE}{prefix}{self.RESET}{record.getMessage()}"
