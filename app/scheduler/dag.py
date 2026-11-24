import uuid
from typing import Dict, List, Optional, Set, Tuple
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.models.job import Job, JobDependency, JobStatus, utcnow
from app.scheduler.priority_queue import PriorityQueue
from app.core.logging import get_logger

logger = get_logger(__name__)


class CycleDetectedError(Exception):
    """Raised when a circular dependency is detected in the DAG."""
    def __init__(self, message: str, cycle: Optional[List[str]] = None) -> None:
        super().__init__(message)
        self.cycle = cycle or []


def find_cycle_in_graph(adjacency_list: Dict[str, List[str]]) -> Optional[List[str]]:
    """Detects if a directed graph contains any cycle using DFS graph coloring.
    
    Args:
        adjacency_list: Map of node -> list of destination nodes (directed edges).
        
    Returns:
        List representing a cycle path if found (e.g. ['A', 'B', 'C', 'A']), or None.
    """
    # 0 = unvisited, 1 = visiting (in recursion stack), 2 = visited (completed)
    visited: Dict[str, int] = {node: 0 for node in adjacency_list}
    parent_map: Dict[str, str] = {}
    cycle_nodes: Optional[List[str]] = None

    def dfs(current: str) -> bool:
        nonlocal cycle_nodes
        visited[current] = 1

        for neighbor in adjacency_list.get(current, []):
            if neighbor not in visited:
                visited[neighbor] = 0

            if visited[neighbor] == 1:
                # Cycle found! Reconstruct cycle path
                cycle = [neighbor, current]
                curr = current
                while curr in parent_map and parent_map[curr] != neighbor:
                    curr = parent_map[curr]
                    cycle.append(curr)
                cycle.append(neighbor)
                cycle.reverse()
                cycle_nodes = cycle
                return True
            elif visited[neighbor] == 0:
                parent_map[neighbor] = current
                if dfs(neighbor):
                    return True

        visited[current] = 2
        return False

    for node in list(adjacency_list.keys()):
        if visited[node] == 0:
            if dfs(node):
                return cycle_nodes

    return None


class DAGManager:
    """Orchestrates job dependency resolution, cycle validation, and cascading execution/failure."""

    @staticmethod
    def validate_batch_dag(nodes: List[str], edges: Dict[str, List[str]]) -> None:
        """Validates that a batch graph has no cycles and all referenced dependencies exist.
        
        Args:
            nodes: List of node identifiers.
            edges: Dict mapping child -> list of parent nodes.
        """
        node_set = set(nodes)
        adj: Dict[str, List[str]] = {n: [] for n in nodes}

        for child, parents in edges.items():
            for parent in parents:
                if parent == child:
                    raise CycleDetectedError(
                        f"Self-dependency detected: node '{child}' depends on itself.",
                        cycle=[child, child],
                    )
                if parent not in node_set:
                    raise ValueError(f"Referenced parent '{parent}' does not exist in DAG nodes.")
                # Directed edge: parent -> child (parent must finish before child runs)
                adj[parent].append(child)

        cycle = find_cycle_in_graph(adj)
        if cycle:
            cycle_str = " -> ".join(cycle)
            raise CycleDetectedError(f"Cycle detected in dependency graph: {cycle_str}", cycle=cycle)

    @staticmethod
    async def validate_submission_dependencies(
        session: AsyncSession,
        candidate_job_id: uuid.UUID,
        parent_ids: List[uuid.UUID],
    ) -> List[Job]:
        """Validates parent job existence and checks for potential cycles before job submission.
        
        Args:
            session: Active database session.
            candidate_job_id: UUID of the candidate job.
            parent_ids: List of parent job UUIDs.
            
        Returns:
            List of existing parent Job entities.
            
        Raises:
            ValueError: If a parent does not exist.
            CycleDetectedError: If a cycle or self-dependency is detected.
        """
        if not parent_ids:
            return []

        # 1. Check self-dependency
        if candidate_job_id in parent_ids:
            raise CycleDetectedError(
                f"Job cannot depend on itself: {candidate_job_id}",
                cycle=[str(candidate_job_id), str(candidate_job_id)],
            )

        # 2. Check parents existence in database
        stmt = (
            select(Job)
            .where(Job.id.in_(parent_ids))
            .options(selectinload(Job.dependencies))
        )
        res = await session.execute(stmt)
        parents = list(res.scalars().all())

        found_parent_ids = {p.id for p in parents}
        missing_ids = [p_id for p_id in parent_ids if p_id not in found_parent_ids]
        if missing_ids:
            raise ValueError(f"Parent job(s) do not exist: {', '.join(str(m) for m in missing_ids)}")

        # 3. Graph reachability cycle check:
        # Candidate job depends on parent P. If candidate_job_id is already an ancestor of P,
        # then adding candidate -> P would form a cycle.
        # Follow ancestors of each parent:
        visited: Set[uuid.UUID] = set()
        queue: List[uuid.UUID] = list(parent_ids)

        while queue:
            curr_id = queue.pop(0)
            if curr_id == candidate_job_id:
                raise CycleDetectedError(
                    f"Cycle detected: Job {candidate_job_id} is already an ancestor of parent {curr_id}",
                    cycle=[str(candidate_job_id), str(curr_id), str(candidate_job_id)],
                )
            if curr_id in visited:
                continue
            visited.add(curr_id)

            # Find dependencies of curr_id (its parents)
            dep_stmt = select(JobDependency.depends_on_job_id).where(JobDependency.job_id == curr_id)
            dep_res = await session.execute(dep_stmt)
            ancestor_ids = dep_res.scalars().all()
            for anc in ancestor_ids:
                if anc not in visited:
                    queue.append(anc)

        return parents

    @staticmethod
    async def on_job_succeeded(
        session: AsyncSession,
        redis: Redis,
        parent_job_id: uuid.UUID,
    ) -> List[uuid.UUID]:
        """Triggered when a job transitions to SUCCEEDED. Checks all dependent children.
        
        If all parent dependencies of a child are SUCCEEDED, transitions child to QUEUED
        and enqueues it to the Redis priority queue.
        
        Returns:
            List of child job UUIDs that were unblocked and enqueued.
        """
        priority_queue = PriorityQueue(redis)

        # Find all downstream child jobs that depend on parent_job_id
        child_dep_stmt = (
            select(JobDependency.job_id)
            .where(JobDependency.depends_on_job_id == parent_job_id)
        )
        child_res = await session.execute(child_dep_stmt)
        child_job_ids = list(child_res.scalars().all())

        if not child_job_ids:
            return []

        unblocked_jobs: List[uuid.UUID] = []

        for child_id in child_job_ids:
            child_stmt = (
                select(Job)
                .where(Job.id == child_id)
                .with_for_update()
            )
            child_result = await session.execute(child_stmt)
            child = child_result.scalar_one_or_none()

            if not child or child.status != JobStatus.PENDING:
                continue

            # Query all parent dependencies for this child
            parents_stmt = (
                select(Job.id, Job.status)
                .join(JobDependency, JobDependency.depends_on_job_id == Job.id)
                .where(JobDependency.job_id == child_id)
            )
            parents_res = await session.execute(parents_stmt)
            parents = parents_res.all()

            # Check if any parent failed
            terminal_failures = {JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.DEAD}
            failed_parent = next((p for p in parents if p.status in terminal_failures), None)

            if failed_parent:
                # Fail fast!
                child.status = JobStatus.FAILED
                child.error = f"Dependency failure: parent job [{failed_parent.id}] ended with status '{failed_parent.status}'"
                child.finished_at = utcnow()
                await session.commit()
                logger.warning(
                    f"Child job [{child.id}] failed fast: parent [{failed_parent.id}] status is '{failed_parent.status}'."
                )
                await DAGManager.on_job_failed(session, child.id, JobStatus.FAILED)
                continue

            # Check if ALL parents are SUCCEEDED
            all_succeeded = all(p.status == JobStatus.SUCCEEDED for p in parents)
            if all_succeeded:
                child.status = JobStatus.QUEUED
                child.scheduled_at = utcnow()
                await session.commit()

                # Enqueue into Redis Priority Queue
                await priority_queue.enqueue(str(child.id), priority=child.priority)
                unblocked_jobs.append(child.id)
                logger.info(
                    f"DAG unblocked: child job [{child.id}] (priority: {child.priority}) "
                    f"promoted PENDING -> QUEUED after parent [{parent_job_id}] succeeded."
                )

        return unblocked_jobs

    @staticmethod
    async def on_job_failed(
        session: AsyncSession,
        failed_job_id: uuid.UUID,
        failure_status: JobStatus,
    ) -> List[uuid.UUID]:
        """Triggered when a job permanently fails or is cancelled.
        
        Cascades failure fast to all dependent children in PENDING or QUEUED status.
        
        Returns:
            List of child job UUIDs that were failed fast.
        """
        # Find all direct downstream child jobs
        child_dep_stmt = (
            select(JobDependency.job_id)
            .where(JobDependency.depends_on_job_id == failed_job_id)
        )
        child_res = await session.execute(child_dep_stmt)
        child_job_ids = list(child_res.scalars().all())

        if not child_job_ids:
            return []

        cascaded_failed_jobs: List[uuid.UUID] = []

        for child_id in child_job_ids:
            child_stmt = (
                select(Job)
                .where(Job.id == child_id)
                .with_for_update()
            )
            child_result = await session.execute(child_stmt)
            child = child_result.scalar_one_or_none()

            if not child or child.status in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD, JobStatus.CANCELLED}:
                continue

            child.status = JobStatus.FAILED
            child.error = f"Dependency failure: parent job [{failed_job_id}] terminated with status '{failure_status}'"
            child.finished_at = utcnow()
            await session.commit()

            cascaded_failed_jobs.append(child.id)
            logger.warning(
                f"DAG cascade: child job [{child.id}] failed fast due to parent [{failed_job_id}] failure."
            )

            # Recurse downstream
            downstream = await DAGManager.on_job_failed(session, child.id, JobStatus.FAILED)
            cascaded_failed_jobs.extend(downstream)

        return cascaded_failed_jobs
