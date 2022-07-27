"""Class to raise exceptions with custom messages."""

class Error(Exception):
    """
    Error class to raise exceptions with custom messages. 
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg