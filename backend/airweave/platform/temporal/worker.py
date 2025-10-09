"""Temporal worker for Airweave."""

import asyncio
import signal
import time
from datetime import timedelta
from typing import Any, Set

from aiohttp import web
from temporalio.worker import Worker

from airweave.core.config import settings
from airweave.core.logging import logger
from airweave.platform.entities._base import ensure_file_entity_models
from airweave.platform.temporal.activities import (
    create_sync_job_activity,
    mark_sync_job_cancelled_activity,
    run_sync_activity,
)
from airweave.platform.temporal.client import temporal_client
from airweave.platform.temporal.workflows import RunSourceConnectionWorkflow


class TemporalWorker:
    """Temporal worker for processing workflows and activities."""

    def __init__(self) -> None:
        """Initialize the Temporal worker."""
        self.worker: Worker | None = None
        self.running = False
        self.draining = False
        self.active_activities: Set[str] = set()
        self.drain_started_at: float | None = None
        self.metrics_server = None

    async def start(self) -> None:
        """Start the Temporal worker."""
        try:
            # Ensure all FileEntity subclasses have their parent and chunk models created
            ensure_file_entity_models()

            # Start metrics/control server
            await self._start_control_server()

            client = await temporal_client.get_client()
            task_queue = settings.TEMPORAL_TASK_QUEUE
            logger.info(f"Starting Temporal worker on task queue: {task_queue}")

            # Get the appropriate sandbox configuration
            sandbox_config = self._get_sandbox_config()

            self.worker = Worker(
                client,
                task_queue=task_queue,
                workflows=[RunSourceConnectionWorkflow],
                activities=[
                    self._wrap_activity(run_sync_activity),
                    self._wrap_activity(mark_sync_job_cancelled_activity),
                    self._wrap_activity(create_sync_job_activity),
                ],
                workflow_runner=sandbox_config,
                max_concurrent_workflow_task_polls=8,
                max_concurrent_activity_task_polls=16,
                sticky_queue_schedule_to_start_timeout=timedelta(seconds=0.5),
                nonsticky_to_sticky_poll_ratio=0.5,
                # Speed up cancel delivery by flushing heartbeats frequently
                default_heartbeat_throttle_interval=timedelta(seconds=2),
                max_heartbeat_throttle_interval=timedelta(seconds=2),
                # Critical: Configure graceful shutdown
                graceful_shutdown_timeout=timedelta(hours=2),
            )

            self.running = True
            await self.worker.run()

        except Exception as e:
            logger.error(f"Error starting Temporal worker: {e}")
            raise

    async def stop(self) -> None:
        """Stop the Temporal worker."""
        if self.worker and self.running:
            logger.info(f"Stopping worker with {len(self.active_activities)} active activities")
            self.running = False
            await self.worker.shutdown()

        # Cleanup metrics server
        if self.metrics_server:
            await self.metrics_server.cleanup()

        # Always close temporal client to prevent resource leaks
        await temporal_client.close()

    def _wrap_activity(self, activity_fn):
        """Wrap activity to track execution."""
        from temporalio import activity

        # Create a new function with the original name
        async def wrapped_activity(*args, **kwargs):
            activity_id = f"{activity_fn.__name__}_{time.time()}"
            self.active_activities.add(activity_id)
            try:
                # Check if we're draining and should reject
                if self.draining:
                    raise Exception("Worker is draining, rejecting new activity")
                return await activity_fn(*args, **kwargs)
            finally:
                self.active_activities.discard(activity_id)

        # Set the function name to the original activity name
        wrapped_activity.__name__ = activity_fn.__name__
        wrapped_activity.__qualname__ = activity_fn.__qualname__

        # Apply the activity decorator
        return activity.defn(wrapped_activity)

    async def _start_control_server(self):
        """Start HTTP server for metrics and control."""
        app = web.Application()
        app.router.add_post("/drain", self._handle_drain)
        app.router.add_get("/metrics", self._handle_metrics)
        app.router.add_get("/health", self._handle_health)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", 8888)
        await site.start()
        self.metrics_server = runner
        logger.info("Control server started on localhost:8888")

    async def _handle_drain(self, request):
        """Handle drain request from PreStop hook."""
        self.draining = True
        self.drain_started_at = time.monotonic()
        logger.info("Drain initiated - stopping activity polling")

        # Tell Temporal SDK to stop polling for new activities
        if self.worker:
            asyncio.create_task(self._initiate_graceful_shutdown())

        return web.Response(text="Drain initiated")

    async def _initiate_graceful_shutdown(self):
        """Gracefully shutdown Temporal worker."""
        logger.info("Initiating graceful shutdown - stopping activity polling")
        # This tells Temporal to:
        # 1. Stop polling for new activities
        # 2. Complete current activities
        # 3. Send activity heartbeats during shutdown
        if self.worker:
            await self.worker.shutdown()

    async def _handle_metrics(self, request):
        """Expose metrics for monitoring."""
        metrics = []
        metrics.append(f"temporal_activity_active {len(self.active_activities)}")
        metrics.append(f"temporal_worker_draining {1 if self.draining else 0}")
        if self.drain_started_at:
            drain_duration = time.monotonic() - self.drain_started_at
            metrics.append(f"temporal_drain_duration_seconds {drain_duration}")
        return web.Response(text="\n".join(metrics))

    async def _handle_health(self, request):
        """Health check endpoint."""
        if self.draining:
            # Return unhealthy when draining so K8s stops routing traffic
            return web.Response(text="DRAINING", status=503)
        return web.Response(text="OK")

    def _get_sandbox_config(self):
        """Determine the appropriate sandbox configuration."""
        should_disable = settings.TEMPORAL_DISABLE_SANDBOX

        if should_disable:
            from temporalio.worker import UnsandboxedWorkflowRunner

            logger.warning("⚠️  TEMPORAL SANDBOX DISABLED - Use only for debugging!")
            return UnsandboxedWorkflowRunner()

        # Default production sandbox
        from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

        logger.info("Using default sandboxed workflow runner")
        return SandboxedWorkflowRunner()


async def main() -> None:
    """Main function to run the worker."""
    worker = TemporalWorker()

    # Handle shutdown signals
    def signal_handler(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(worker.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
