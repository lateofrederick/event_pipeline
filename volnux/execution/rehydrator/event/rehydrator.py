class EventRehydrator:
    """Reconstructs an event from an EventCheckpointSnapshot.

    Uses StateDeserializer for all deserialization. The rehydrator
    orchestrates the restoration process but delegates data conversion
    to the deserializer.
    """

    def __init__(self, deserializer: StateDeserializer = StateDeserializer):
        self.deserializer = deserializer

    async def rehydrate(self, snapshot: EventCheckpointSnapshot) -> "EventBase":
        """Reconstruct an event from its checkpoint snapshot.

        Steps:
        1. Import the event class
        2. Deserialize init args and instantiate
        3. Restore internal state (phase, retry count, exec result)
        4. Restore external resources
        5. Deserialize call args for potential process() re-execution
        """
        # 1. Import the event class
        event_class = import_class(snapshot.class_path)

        # 2. Deserialize init args and instantiate
        init_kwargs = self.deserializer.deserialize_init_args(snapshot.init_args)
        event = event_class(**init_kwargs)

        # 3. Restore internal state
        event._phase = snapshot.phase
        event._retry_count = snapshot.retry_count
        event._exec_status = snapshot.exec_status

        if snapshot.exec_result is not None:
            event._exec_result = self.deserializer.deserialize_exec_result(
                snapshot.exec_result
            )

        # 4. Restore external resources
        if snapshot.external_resources:
            event._external_resources = (
                self.deserializer.deserialize_external_resources(
                    snapshot.external_resources
                )
            )

        # 5. Deserialize call args
        event._call_args = self.deserializer.deserialize_call_args(snapshot.call_args)

        # 6. Restore retry configuration
        if hasattr(event, "retry_policy") and event.retry_policy:
            event.retry_policy.max_attempts = snapshot.max_retry_attempts

        return event

    async def resume_execution(self, event: "EventBase") -> None:
        """Resume event execution from its restored state.

        Skips phases that were already completed based on the
        checkpointed phase value.
        """
        current_phase = event.get_phase()

        if current_phase < EventPhase.COMMUNICATING:
            await event.communicate()

        if current_phase < EventPhase.PRE_PROCESS:
            await event.pre_process()

        if current_phase < EventPhase.PROCESSING:
            args = event._call_args.get("args", [])
            kwargs = event._call_args.get("kwargs", {})
            event._exec_result = await event.process(*args, **kwargs)
            event._exec_status = True

        if current_phase < EventPhase.POST_PROCESS:
            await event.post_process()

        if current_phase < EventPhase.COMPLETED:
            await event.complete()
