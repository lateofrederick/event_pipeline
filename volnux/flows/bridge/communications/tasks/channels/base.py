import asyncio
import multiprocessing as mp
from typing import Optional, Union, Dict, List
from abc import ABC, abstractmethod

QueueType = Union[asyncio.Queue, mp.Queue]


class CommandChannelBase(ABC):
    """
    bidirectional channel for coordinator ↔ task communication.
    """

    def __init__(self, task_id: str):
        self.task_id = task_id

        self._command_queue: Optional[QueueType] = None
        self._message_queue: Optional[QueueType] = None

    @abstractmethod
    async def send_command(self, command: TaskCommand) -> None:
        """Send command from coordinator to task (async)"""
        raise NotImplementedError("send_command must be implemented by subclasses")

    @abstractmethod
    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional[TaskCommand]:
        """Receive command in task (async)"""
        raise NotImplementedError("receive_command must be implemented by subclasses")

    @abstractmethod
    async def send_message(self, message: TaskMessage) -> None:
        """Send message from task to coordinator (async)"""
        raise NotImplementedError("send_message must be implemented by subclasses")

    @abstractmethod
    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional[TaskMessage]:
        """Receive message in coordinator (async)"""
        raise NotImplementedError("receive_message must be implemented by subclasses")
