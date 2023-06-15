from datetime import datetime


def log(_message: str):
    """Logs a message to STDOUT with a timestamp.

    :param _message: str, a message to print
    """
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, {_message}")
