from pydantic_mini import Attrib, BaseModel, MiniAnnotated


class ScalingConfig(BaseModel):
    """Configuration for the adaptive scaling engine"""

    max_cpu_quota: MiniAnnotated[
        float,
        Attrib(
            default=4.0, help_text="Total CPU cores available (Q_max)"  # noqa: F722
        ),
    ]
    max_memory_quota: MiniAnnotated[
        float, Attrib(default=8.0, help_text="Total memory in GB")  # noqa: F722
    ]
    cpu_per_worker: MiniAnnotated[
        float,
        Attrib(
            default=1.0, help_text="Estimated CPU cores per worker (R_w)"  # noqa: F722
        ),
    ]
    memory_per_worker: MiniAnnotated[
        float,
        Attrib(default=0.5, help_text="Estimated memory GB per worker"),  # noqa: F722
    ]
    parallelism_multiplier: MiniAnnotated[
        int,
        Attrib(
            default=2, help_text="Multiplier for batch size calculation"  # noqa: F722
        ),
    ]
    scale_up_threshold: MiniAnnotated[
        float,
        Attrib(
            default=0.7, help_text="Queue utilization to trigger scale up"  # noqa: F722
        ),
    ]
    scale_down_timeout: MiniAnnotated[
        float,
        Attrib(
            default=10.0,
            help_text="Idle time before scaling down (seconds)",  # noqa: F722
        ),
    ]
    min_workers: MiniAnnotated[
        int, Attrib(default=1, help_text="Minimum number of workers")  # noqa: F722
    ]
    monitoring_interval: MiniAnnotated[
        float,
        Attrib(
            default=1.0, help_text="How often to check metrics (seconds)"  # noqa: F722
        ),
    ]
    cpu_threshold_scale_down: MiniAnnotated[
        float,
        Attrib(
            default=0.3,
            help_text="CPU usage below this triggers scale down (as % of quota)",  # noqa: F722
        ),
    ]
    cpu_threshold_scale_up: MiniAnnotated[
        float,
        Attrib(
            default=0.85,
            help_text="CPU usage above this prevents scale up (as % of quota)",  # noqa: F722
        ),
    ]
    memory_threshold: MiniAnnotated[
        float,
        Attrib(
            default=0.9, help_text="Memory usage threshold (90% of quota)"  # noqa: F722
        ),
    ]
    aggressive_scaling: MiniAnnotated[
        bool,
        Attrib(
            default=True, help_text="Enable aggressive real-time scaling"  # noqa: F722
        ),
    ]
