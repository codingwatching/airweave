#!/usr/bin/env python3
"""
Test script for graceful shutdown in dev environment.

This script tests the graceful shutdown behavior by:
1. Starting long-running sync activities
2. Monitoring metrics during rollout
3. Verifying activities complete gracefully
"""

import asyncio
import time
import subprocess
from datetime import datetime
from typing import Dict, List


class GracefulShutdownDevTester:
    def __init__(self, namespace: str = "airweave", environment: str = "dev"):
        self.namespace = namespace
        self.environment = environment
        self.worker_pods = []
        self.test_results = {}

    async def get_worker_pods(self) -> List[str]:
        """Get list of temporal worker pods."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    self.namespace,
                    "-l",
                    "app.kubernetes.io/component=sync-worker",
                    "-o",
                    "jsonpath={.items[*].metadata.name}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            pods = result.stdout.strip().split()
            print(f"Found worker pods: {pods}")
            return pods
        except subprocess.CalledProcessError as e:
            print(f"Error getting pods: {e}")
            return []

    async def get_pod_metrics(self, pod_name: str) -> Dict[str, str]:
        """Get metrics from a specific pod."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "exec",
                    "-n",
                    self.namespace,
                    pod_name,
                    "--",
                    "curl",
                    "-s",
                    "http://localhost:8888/metrics",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode != 0:
                return {"error": f"Failed to get metrics: {result.stderr}"}

            metrics = {}
            for line in result.stdout.strip().split("\n"):
                if " " in line:
                    key, value = line.split(" ", 1)
                    metrics[key] = value

            return metrics
        except subprocess.TimeoutExpired:
            return {"error": "Timeout getting metrics"}
        except Exception as e:
            return {"error": str(e)}

    async def get_pod_health(self, pod_name: str) -> str:
        """Get health status from a specific pod."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "exec",
                    "-n",
                    self.namespace,
                    pod_name,
                    "--",
                    "curl",
                    "-s",
                    "-o",
                    "/dev/null",
                    "-w",
                    "%{http_code}",
                    "http://localhost:8888/health",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )

            return result.stdout.strip()
        except Exception as e:
            return f"Error: {e}"

    async def trigger_drain(self, pod_name: str) -> bool:
        """Trigger drain on a specific pod."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "exec",
                    "-n",
                    self.namespace,
                    pod_name,
                    "--",
                    "curl",
                    "-X",
                    "POST",
                    "http://localhost:8888/drain",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            return result.returncode == 0 and "Drain initiated" in result.stdout
        except Exception as e:
            print(f"Error triggering drain: {e}")
            return False

    async def start_test_sync(self, sync_id: str) -> bool:
        """Start a test sync activity."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "exec",
                    "-n",
                    self.namespace,
                    f"deployment/airweave-{self.environment}-backend",
                    "--",
                    "python",
                    "-c",
                    f"""
from airweave.api.v1.endpoints.syncs import trigger_sync
trigger_sync('{sync_id}', source='test-source-{sync_id}')
print('Sync started: {sync_id}')
""",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            return result.returncode == 0
        except Exception as e:
            print(f"Error starting sync: {e}")
            return False

    async def monitor_rollout(self, duration_seconds: int = 300):
        """Monitor the rollout process."""
        print(f"🔍 Monitoring rollout for {duration_seconds} seconds...")

        start_time = time.time()
        while time.time() - start_time < duration_seconds:
            pods = await self.get_worker_pods()

            if not pods:
                print("❌ No worker pods found")
                break

            print(
                f"\n⏰ {datetime.now().strftime('%H:%M:%S')} - Monitoring {len(pods)} pods"
            )

            for pod in pods:
                health = await self.get_pod_health(pod)
                metrics = await self.get_pod_metrics(pod)

                status = "🟢" if health == "200" else "🔴" if health == "503" else "⚪"
                active = metrics.get("temporal_activity_active", "?")
                draining = metrics.get("temporal_worker_draining", "?")

                print(
                    f"  {status} {pod}: health={health}, active={active}, draining={draining}"
                )

                if "error" in metrics:
                    print(f"    Error: {metrics['error']}")

            await asyncio.sleep(10)

        print("✅ Monitoring completed")

    async def test_graceful_shutdown(self):
        """Test graceful shutdown behavior."""
        print("=" * 80)
        print("🧪 TESTING GRACEFUL SHUTDOWN IN DEV ENVIRONMENT")
        print("=" * 80)

        # Step 1: Get initial state
        print("\n1️⃣ Getting initial pod state...")
        pods = await self.get_worker_pods()
        if not pods:
            print("❌ No worker pods found. Make sure the deployment is running.")
            return

        # Step 2: Start test syncs
        print("\n2️⃣ Starting test sync activities...")
        sync_ids = [f"test-sync-{i}" for i in range(3)]
        for sync_id in sync_ids:
            success = await self.start_test_sync(sync_id)
            if success:
                print(f"✅ Started sync: {sync_id}")
            else:
                print(f"❌ Failed to start sync: {sync_id}")

        # Wait for syncs to start
        print("\n⏳ Waiting for syncs to start...")
        await asyncio.sleep(10)

        # Step 3: Check initial metrics
        print("\n3️⃣ Checking initial metrics...")
        for pod in pods:
            metrics = await self.get_pod_metrics(pod)
            active = metrics.get("temporal_activity_active", "0")
            print(f"  {pod}: {active} active activities")

        # Step 4: Trigger rolling update
        print("\n4️⃣ Triggering rolling update...")
        try:
            subprocess.run(
                [
                    "kubectl",
                    "rollout",
                    "restart",
                    f"deployment/airweave-{self.environment}-sync-worker",
                    "-n",
                    self.namespace,
                ],
                check=True,
            )
            print("✅ Rolling update triggered")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to trigger rolling update: {e}")
            return

        # Step 5: Monitor the rollout
        print("\n5️⃣ Monitoring rollout process...")
        await self.monitor_rollout(duration_seconds=300)

        # Step 6: Check final state
        print("\n6️⃣ Checking final state...")
        final_pods = await self.get_worker_pods()
        print(f"Final pod count: {len(final_pods)}")

        for pod in final_pods:
            metrics = await self.get_pod_metrics(pod)
            active = metrics.get("temporal_activity_active", "0")
            print(f"  {pod}: {active} active activities")

        print("\n✅ Graceful shutdown test completed!")

    async def test_manual_drain(self):
        """Test manual drain functionality."""
        print("=" * 80)
        print("🧪 TESTING MANUAL DRAIN FUNCTIONALITY")
        print("=" * 80)

        pods = await self.get_worker_pods()
        if not pods:
            print("❌ No worker pods found")
            return

        pod = pods[0]  # Test with first pod

        print(f"\n1️⃣ Testing pod: {pod}")

        # Check initial state
        health = await self.get_pod_health(pod)
        metrics = await self.get_pod_metrics(pod)
        print(f"Initial state - Health: {health}, Metrics: {metrics}")

        # Trigger drain
        print(f"\n2️⃣ Triggering drain on {pod}...")
        success = await self.trigger_drain(pod)
        if success:
            print("✅ Drain triggered successfully")
        else:
            print("❌ Failed to trigger drain")
            return

        # Check state after drain
        print("\n3️⃣ Checking state after drain...")
        await asyncio.sleep(2)

        health = await self.get_pod_health(pod)
        metrics = await self.get_pod_metrics(pod)
        print(f"After drain - Health: {health}, Metrics: {metrics}")

        # Monitor for a bit
        print("\n4️⃣ Monitoring for 30 seconds...")
        for i in range(6):
            await asyncio.sleep(5)
            health = await self.get_pod_health(pod)
            metrics = await self.get_pod_metrics(pod)
            active = metrics.get("temporal_activity_active", "?")
            draining = metrics.get("temporal_worker_draining", "?")
            print(f"  {i * 5}s: health={health}, active={active}, draining={draining}")

        print("\n✅ Manual drain test completed!")


async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Test graceful shutdown in dev environment"
    )
    parser.add_argument(
        "--mode",
        choices=["rollout", "manual"],
        default="rollout",
        help="Test mode: rollout or manual drain",
    )
    parser.add_argument("--namespace", default="airweave", help="Kubernetes namespace")
    parser.add_argument("--environment", default="dev", help="Environment (dev/prd)")

    args = parser.parse_args()

    tester = GracefulShutdownDevTester(args.namespace, args.environment)

    print(f"🧪 Graceful Shutdown Dev Test - Mode: {args.mode}")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📍 Namespace: {args.namespace}, Environment: {args.environment}")
    print()

    if args.mode == "rollout":
        await tester.test_graceful_shutdown()
    elif args.mode == "manual":
        await tester.test_manual_drain()

    print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
