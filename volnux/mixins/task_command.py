import logging
import asyncio
from typing import Any, Dict, Optional, Tuple
from volnux.flows.bridge.communications.tasks import (
    CommandType,
    MessageType,
    TaskMessage,
)

logger = logging.getLogger(__name__)


class EventCommandMixin:
    """Mixin for handling commands from the coordinator"""

    async def _process_commands(self, *args, **kwargs) -> None:
        if self._command_channel:
            try:
                command = await self._command_channel.receive_command(timeout=0.01)
                if command:
                    await self._handle_command(command)
            except Exception as e:
                logger.debug(f"No command received: {e}")

    async def _handle_command(self, command: TaskCommand) -> None:
        """Handle incoming commands from coordinator"""

        if command.command_type == CommandType.PAUSE:
            self._paused = True
            logger.info(f"Task {self._task_id} paused")

            # Send status update
            if self._command_channel:
                await self._command_channel.send_message(
                    TaskMessage(
                        task_id=self._task_id,
                        message_type=MessageType.STATUS_UPDATE,
                        payload={"state": "PAUSED"},
                    )
                )

            while self._paused:
                await asyncio.sleep(0.1)

                # Check for resume/cancel command
                if self._command_channel:
                    cmd = await self._command_channel.receive_command(timeout=0.1)
                    if cmd:
                        if cmd.command_type == CommandType.RESUME:
                            self._paused = False
                            logger.info(f"Task {self._task_id} resumed")
                            # Send status update
                            await self._command_channel.send_message(
                                TaskMessage(
                                    task_id=self._task_id,
                                    message_type=MessageType.STATUS_UPDATE,
                                    payload={"state": "RUNNING"},
                                )
                            )
                        elif cmd.command_type == CommandType.CANCEL:
                            raise asyncio.CancelledError("Task cancelled")

        elif command.command_type == CommandType.CANCEL:
            logger.warning(f"Task {self._task_id} cancelled")
            raise asyncio.CancelledError("Task cancelled by coordinator")
