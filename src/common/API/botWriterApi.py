from dataclasses import dataclass

@dataclass(eq=True, frozen=True)
class MessageWriteRequest:
    """
    Represents a request to write a message.
    Attributes:
        chat_id (int): The ID of the chat where the message will be sent.
        text (str): The content of the message to be sent.
    """
    chat_id: int
    text: str
    