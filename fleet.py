"""Laptop-scale model of version-pinned fleet configuration rollouts.

The gateway routes every request at one epoch. Nodes retain their current
manifest while preparing the next one, so a failed prepare leaves traffic on
the previous version. This models coordination; it is not a consensus system.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Manifest:
    version: int
    config: dict[str, Any]

    @property
    def digest(self) -> str:
        canonical = json.dumps(
            {"version": self.version, "config": self.config},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(canonical).hexdigest()


@dataclass
class Node:
    node_id: int
    active: Manifest
    staged: Manifest | None = None
    failure: str | None = None

    async def prepare(self, target: Manifest) -> str:
        if self.failure == "timeout":
            await asyncio.sleep(0.1)
        if self.failure == "reject":
            raise RuntimeError(f"node {self.node_id} rejected prepare")
        if self.failure == "corrupt":
            self.staged = Manifest(target.version, {**target.config, "tampered": True})
        else:
            self.staged = target
        return self.staged.digest

    def can_serve(self, manifest: Manifest) -> bool:
        return self.active.digest == manifest.digest or (
            self.staged is not None and self.staged.digest == manifest.digest
        )

    def serve(self, version: int) -> Manifest | None:
        """Return the actual local manifest a request for this epoch would see."""
        if self.active.version == version:
            return self.active
        if self.staged is not None and self.staged.version == version:
            return self.staged
        return None

    def promote(self, manifest: Manifest) -> None:
        if self.staged is None or self.staged.digest != manifest.digest:
            raise RuntimeError(f"node {self.node_id} lacks prepared manifest")
        self.active = self.staged
        self.staged = None

    def discard_stage(self) -> None:
        self.staged = None


@dataclass
class RolloutResult:
    committed: bool
    reason: str
    served_version_sets: list[set[int]] = field(default_factory=list)

    @property
    def mixed_samples(self) -> int:
        return sum(len(versions) > 1 for versions in self.served_version_sets)


class Fleet:
    def __init__(self, size: int, initial: Manifest):
        if size < 1:
            raise ValueError("fleet size must be positive")
        self.nodes = [Node(index, initial) for index in range(size)]
        self.routed = initial

    def served_versions(self) -> set[int]:
        """Versions available to requests through the gateway's single epoch."""
        versions = set()
        for node in self.nodes:
            response = node.serve(self.routed.version)
            if response is not None and response.digest == self.routed.digest:
                versions.add(response.version)
        return versions

    def drifted_nodes(self) -> list[int]:
        """Find nodes that cannot serve the routed manifest's exact hash."""
        return [node.node_id for node in self.nodes if not node.can_serve(self.routed)]

    async def rollout(self, target: Manifest, timeout: float = 0.05) -> RolloutResult:
        if target.version <= self.routed.version:
            raise ValueError("target version must increase")
        observations = [self.served_versions()]
        tasks = [asyncio.create_task(node.prepare(target)) for node in self.nodes]
        try:
            acknowledgments = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=timeout
            )
        except asyncio.TimeoutError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for node in self.nodes:
                node.discard_stage()
            observations.append(self.served_versions())
            return RolloutResult(False, "prepare timeout", observations)

        if any(not isinstance(ack, str) or ack != target.digest for ack in acknowledgments):
            for node in self.nodes:
                node.discard_stage()
            observations.append(self.served_versions())
            return RolloutResult(False, "prepare rejected or hash mismatch", observations)

        # The model's single gateway epoch switch is the commit point. Every
        # node has the exact new manifest staged before this assignment.
        observations.append(self.served_versions())
        self.routed = target
        observations.append(self.served_versions())
        for node in self.nodes:
            node.promote(target)
        observations.append(self.served_versions())
        return RolloutResult(True, "committed", observations)


async def detect_drift(fleet: Fleet, interval: float = 0.01, limit: float = 2.0) -> list[int]:
    """Poll content hashes until drift appears or the real-time limit expires."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit
    while loop.time() < deadline:
        found = fleet.drifted_nodes()
        if found:
            return found
        await asyncio.sleep(interval)
    return []


def naive_rollout_mixed_samples(size: int) -> int:
    """Baseline: independent node activation with no gateway epoch barrier."""
    if size < 2:
        return 0
    active_versions = [0] * size
    mixed = 0
    for node_id in range(size):
        active_versions[node_id] = 1
        if len(set(active_versions)) > 1:
            mixed += 1
    return mixed
