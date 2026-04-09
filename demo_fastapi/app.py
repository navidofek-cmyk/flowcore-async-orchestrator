from __future__ import annotations

import asyncio
import os
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from flowcore.brokers.redis import RedisBroker
from flowcore.cli import _run_tree_demo


StepStatus = Literal["pending", "queued", "running", "retrying", "done"]
ActorStatus = Literal["waiting", "moving", "arrived"]
RunStatus = Literal["idle", "running", "finished"]


@dataclass(frozen=True, slots=True)
class Cell:
    x: int
    y: int


@dataclass(frozen=True, slots=True)
class LevelPlan:
    level: int
    name: str
    reward_name: str
    prince_start: Cell
    princess_start: Cell
    goal: Cell
    exit_gate: Cell
    prince_retry_index: int | None = None


@dataclass(slots=True)
class ActorState:
    name: str
    role: str
    status: ActorStatus = "waiting"
    position: Cell = field(default_factory=lambda: Cell(1, 1))
    carrying: str | None = None
    note: str = ""


@dataclass(slots=True)
class DemoStep:
    id: str
    title: str
    note: str
    level: int
    status: StepStatus = "pending"
    attempts: int = 0
    detail: str = ""


@dataclass(slots=True)
class LevelProgress:
    level: int
    name: str
    reward_name: str
    unlocked: bool = False
    completed: bool = False
    flowcore_trace_id: str | None = None
    flowcore_status: str = "not-started"


@dataclass(slots=True)
class DemoState:
    run_id: str | None = None
    run_status: RunStatus = "idle"
    active_level: int = 1
    steps: list[DemoStep] = field(default_factory=list)
    levels: list[LevelProgress] = field(default_factory=list)
    prince: ActorState = field(default_factory=lambda: ActorState(name="Prince Arin", role="Explorer"))
    princess: ActorState = field(
        default_factory=lambda: ActorState(
            name="Princess Elin",
            role="Gate Keeper",
            position=Cell(5, 5),
        )
    )
    visited_cells: list[dict[str, int]] = field(default_factory=list)
    reward_position: Cell | None = None
    reward_name: str | None = None
    current_story: str = ""
    log: list[str] = field(default_factory=list)
    active_flowcore_trace_id: str | None = None
    agent_name: str = "Maze Agent Orin"
    agent_status: str = "idle"
    agent_thought: str = "Scanning the maze for the safest route."


MAZE_ROWS: tuple[str, ...] = (
    "###########",
    "#...#.....#",
    "#.#.#.###.#",
    "#.#...#...#",
    "#.#####.#.#",
    "#...#...#.#",
    "###.#.###.#",
    "#...#.....#",
    "#.#######.#",
    "#.........#",
    "###########",
)

LEVELS: tuple[LevelPlan, ...] = (
    LevelPlan(
        level=1,
        name="Rose Maze",
        reward_name="crystal rose",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(5, 3),
        exit_gate=Cell(9, 9),
        prince_retry_index=8,
    ),
    LevelPlan(
        level=2,
        name="Lantern Maze",
        reward_name="sun lantern",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(9, 1),
        exit_gate=Cell(7, 7),
        prince_retry_index=12,
    ),
    LevelPlan(
        level=3,
        name="Crown Maze",
        reward_name="golden crown key",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(1, 9),
        exit_gate=Cell(1, 9),
        prince_retry_index=10,
    ),
    LevelPlan(
        level=4,
        name="Mirror Maze",
        reward_name="silver mirror shard",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(7, 5),
        exit_gate=Cell(9, 9),
        prince_retry_index=15,
    ),
    LevelPlan(
        level=5,
        name="River Maze",
        reward_name="moon pearl",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(9, 7),
        exit_gate=Cell(9, 9),
        prince_retry_index=14,
    ),
    LevelPlan(
        level=6,
        name="Storm Maze",
        reward_name="thunder feather",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(3, 9),
        exit_gate=Cell(9, 9),
        prince_retry_index=11,
    ),
    LevelPlan(
        level=7,
        name="Star Maze",
        reward_name="royal star seal",
        prince_start=Cell(1, 1),
        princess_start=Cell(9, 9),
        goal=Cell(7, 1),
        exit_gate=Cell(1, 9),
        prince_retry_index=16,
    ),
)


app = FastAPI(title="FlowCore Maze Demo")
state = DemoState()
run_lock = asyncio.Lock()
REDIS_URL = os.getenv("FLOWCORE_DEMO_REDIS_URL", "redis://flowcore-demo-redis:6379/0")
QUEUE_NAME = os.getenv("FLOWCORE_DEMO_QUEUE", "flowcore")


def _is_walkable(cell: Cell) -> bool:
    if not (0 <= cell.y < len(MAZE_ROWS) and 0 <= cell.x < len(MAZE_ROWS[cell.y])):
        return False
    return MAZE_ROWS[cell.y][cell.x] == "."


def _neighbors(cell: Cell) -> tuple[Cell, ...]:
    candidates = (
        Cell(cell.x + 1, cell.y),
        Cell(cell.x - 1, cell.y),
        Cell(cell.x, cell.y + 1),
        Cell(cell.x, cell.y - 1),
    )
    return tuple(candidate for candidate in candidates if _is_walkable(candidate))


def _find_path(start: Cell, goal: Cell) -> tuple[Cell, ...]:
    if start == goal:
        return (start,)

    queue: deque[Cell] = deque([start])
    previous: dict[Cell, Cell | None] = {start: None}

    while queue:
        current = queue.popleft()
        for neighbor in _neighbors(current):
            if neighbor in previous:
                continue
            previous[neighbor] = current
            if neighbor == goal:
                path: list[Cell] = [goal]
                cursor: Cell | None = current
                while cursor is not None:
                    path.append(cursor)
                    cursor = previous[cursor]
                path.reverse()
                return tuple(path)
            queue.append(neighbor)

    raise ValueError(f"No walkable path from {start} to {goal}.")


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _build_steps() -> list[DemoStep]:
    return [
        DemoStep(
            id=f"level-{plan.level}-prince-search",
            title=f"Level {plan.level}: Prince Crosses {plan.name}",
            note=f"Prince Arin walks through the maze to find the {plan.reward_name}.",
            level=plan.level,
        )
        for plan in LEVELS
    ] + [
        DemoStep(
            id=f"level-{plan.level}-handoff",
            title=f"Level {plan.level}: Princess Receives Reward",
            note=f"Princess Elin receives the {plan.reward_name} at the exit gate.",
            level=plan.level,
        )
        for plan in LEVELS
    ] + [
        DemoStep(
            id=f"level-{plan.level}-unlock-next",
            title=f"Level {plan.level}: Next Gate Opens",
            note="The next part of the story unlocks.",
            level=plan.level,
        )
        for plan in LEVELS
    ]


def _build_levels() -> list[LevelProgress]:
    levels = [
        LevelProgress(level=plan.level, name=plan.name, reward_name=plan.reward_name)
        for plan in LEVELS
    ]
    if levels:
        levels[0].unlocked = True
    return levels


def _reset_state() -> None:
    state.run_id = None
    state.run_status = "idle"
    state.active_level = 1
    state.steps = _build_steps()
    state.levels = _build_levels()
    state.prince = ActorState(
        name="Prince Arin",
        role="Explorer",
        position=LEVELS[0].prince_start,
        note="Ready at the entrance.",
    )
    state.princess = ActorState(
        name="Princess Elin",
        role="Gate Keeper",
        position=LEVELS[0].princess_start,
        note="Waiting by the first gate.",
    )
    state.visited_cells = [{"x": state.prince.position.x, "y": state.prince.position.y}]
    state.reward_position = LEVELS[0].goal
    state.reward_name = LEVELS[0].reward_name
    state.current_story = "The prince is about to enter the first maze."
    state.log = ["Demo ready. Press Start to watch the prince cross the mazes."]
    state.active_flowcore_trace_id = None
    state.agent_status = "idle"
    state.agent_thought = "Scanning the maze for the safest route."


def _append_log(message: str) -> None:
    stamp = datetime.now(UTC).strftime("%H:%M:%S")
    state.log.insert(0, f"{stamp} | {message}")
    del state.log[30:]


def _cell_dict(cell: Cell | None) -> dict[str, int] | None:
    if cell is None:
        return None
    return {"x": cell.x, "y": cell.y}


def _set_step(step_id: str, status: StepStatus, *, detail: str = "", attempts: int | None = None) -> None:
    for step in state.steps:
        if step.id == step_id:
            step.status = status
            step.detail = detail
            if attempts is not None:
                step.attempts = attempts
            return


def _serialize_state() -> dict[str, object]:
    maze = []
    visited = {(cell["x"], cell["y"]) for cell in state.visited_cells}
    prince_pos = (state.prince.position.x, state.prince.position.y)
    princess_pos = (state.princess.position.x, state.princess.position.y)
    reward_pos = None if state.reward_position is None else (state.reward_position.x, state.reward_position.y)

    for y, row in enumerate(MAZE_ROWS):
        for x, char in enumerate(row):
            cell_type = "wall" if char == "#" else "path"
            if (x, y) in visited and cell_type == "path":
                cell_type = "visited"
            if reward_pos == (x, y):
                cell_type = "reward"
            if princess_pos == (x, y):
                cell_type = "princess"
            if prince_pos == (x, y):
                cell_type = "prince"
            maze.append({"x": x, "y": y, "kind": cell_type})

    return {
        "run_id": state.run_id,
        "run_status": state.run_status,
        "active_level": state.active_level,
        "steps": [asdict(step) for step in state.steps],
        "levels": [asdict(level) for level in state.levels],
        "prince": {
            "name": state.prince.name,
            "role": state.prince.role,
            "status": state.prince.status,
            "position": _cell_dict(state.prince.position),
            "carrying": state.prince.carrying,
            "note": state.prince.note,
        },
        "princess": {
            "name": state.princess.name,
            "role": state.princess.role,
            "status": state.princess.status,
            "position": _cell_dict(state.princess.position),
            "carrying": state.princess.carrying,
            "note": state.princess.note,
        },
        "reward_name": state.reward_name,
        "reward_position": _cell_dict(state.reward_position),
        "maze": maze,
        "story": state.current_story,
        "log": list(state.log),
        "active_flowcore_trace_id": state.active_flowcore_trace_id,
        "agent": {
            "name": state.agent_name,
            "status": state.agent_status,
            "thought": state.agent_thought,
        },
    }


async def _fetch_flowcore_status() -> dict[str, object]:
    broker = RedisBroker.from_url(REDIS_URL, queue_name=QUEUE_NAME)
    try:
        status = await broker.status()
        return {
            "queue_name": status["queue_name"],
            "queue_depth": status["queue_depth"],
            "processing_depth": status["processing_depth"],
            "dlq_depth": status["dlq_depth"],
            "workers": status["workers"],
            "recent_runs": status["recent_runs"],
        }
    finally:
        await broker.close()


async def _run_real_flowcore_level(level: int) -> None:
    state.agent_status = "dispatching"
    state.agent_thought = f"Sending level {level} into FlowCore so 9100 can monitor the real run."
    result = await _run_tree_demo(
        executor="distributed",
        delay_scale=0.25,
        redis_url=REDIS_URL,
        queue_name=QUEUE_NAME,
        workers=1,
    )
    state.active_flowcore_trace_id = result.trace_id
    level_progress = state.levels[level - 1]
    level_progress.flowcore_trace_id = result.trace_id
    statuses = {item.status.value for item in result.metadata.values()}
    level_progress.flowcore_status = "failed" if "FAILED" in statuses else "succeeded"
    state.agent_status = "watching"
    state.agent_thought = f"FlowCore trace {result.trace_id} completed for level {level}."
    _append_log(f"FlowCore level {level} run finished with trace {result.trace_id}.")


async def _walk_path(plan: LevelPlan) -> None:
    step_id = f"level-{plan.level}-prince-search"
    _set_step(step_id, "queued", detail="The prince is preparing to enter the maze.", attempts=0)
    await asyncio.sleep(0.4)
    _set_step(step_id, "running", detail="The prince is moving through the maze.", attempts=1)
    state.levels[plan.level - 1].flowcore_status = "running"
    flowcore_task = asyncio.create_task(_run_real_flowcore_level(plan.level))
    prince_path = _find_path(plan.prince_start, plan.goal)
    state.prince.status = "moving"
    state.prince.note = f"Crossing {plan.name}."
    state.agent_status = "planning"
    state.agent_thought = (
        f"Computed a {len(prince_path) - 1}-step route through {plan.name}. "
        f"I will keep the prince off every wall tile."
    )
    state.current_story = f"Prince Arin enters {plan.name} to find the {plan.reward_name}."
    _append_log(f"Prince Arin entered {plan.name}.")
    for index, cell in enumerate(prince_path):
        state.prince.position = cell
        state.visited_cells.append({"x": cell.x, "y": cell.y})
        state.prince.note = f"Exploring tile ({cell.x}, {cell.y})."
        state.agent_status = "guiding"
        state.agent_thought = (
            f"Guiding the prince across tile ({cell.x}, {cell.y}); "
            f"{len(prince_path) - index - 1} safe steps remain."
        )
        if plan.prince_retry_index is not None and index == plan.prince_retry_index:
            _set_step(
                step_id,
                "retrying",
                detail="A moving wall forced the prince to pause and try again.",
                attempts=1,
            )
            state.current_story = f"A moving wall blocked the path in {plan.name}, but the prince tries again."
            state.agent_status = "rerouting"
            state.agent_thought = (
                f"A moving wall shifted in {plan.name}. Rechecking the corridor and keeping the prince on valid path tiles."
            )
            _append_log(f"{plan.name}: a moving wall blocked the corridor. Retrying.")
            await asyncio.sleep(1.0)
            _set_step(
                step_id,
                "running",
                detail="The prince found a safer route and continues forward.",
                attempts=2,
            )
        await asyncio.sleep(0.65)

    state.prince.carrying = plan.reward_name
    state.reward_position = None
    state.prince.status = "arrived"
    state.prince.note = f"Holding the {plan.reward_name}."
    state.agent_status = "watching"
    state.agent_thought = f"The prince reached the reward chamber in {plan.name}. Waiting for FlowCore completion."
    state.current_story = f"Prince Arin found the {plan.reward_name} at the heart of {plan.name}."
    await flowcore_task
    _set_step(
        step_id,
        "done",
        detail=f"The prince reached the goal and picked up the {plan.reward_name}.",
        attempts=2 if plan.prince_retry_index is not None else 1,
    )
    _append_log(f"Prince Arin found the {plan.reward_name}.")


async def _handoff_reward(plan: LevelPlan) -> None:
    step_id = f"level-{plan.level}-handoff"
    _set_step(step_id, "queued", detail="The princess is waiting by the gate.", attempts=0)
    await asyncio.sleep(0.3)
    _set_step(step_id, "running", detail="The reward is being handed over.", attempts=1)
    state.princess.status = "moving"
    state.princess.note = f"Walking to meet the prince at level {plan.level}."
    state.agent_status = "guiding"
    state.agent_thought = f"Princess Elin is following the safe corridor to collect the {plan.reward_name}."
    _append_log(f"Princess Elin walks to receive the {plan.reward_name}.")

    princess_path = _find_path(plan.princess_start, plan.goal)
    for cell in princess_path:
        state.princess.position = cell
        await asyncio.sleep(0.55)

    state.princess.carrying = plan.reward_name
    state.princess.status = "arrived"
    state.princess.note = f"Holding the {plan.reward_name}."
    state.prince.carrying = None
    state.agent_status = "confirming"
    state.agent_thought = f"Handoff complete. The {plan.reward_name} can now unlock the next gate."
    state.current_story = f"The prince handed the {plan.reward_name} to Princess Elin."
    _set_step(
        step_id,
        "done",
        detail=f"Princess Elin now holds the {plan.reward_name}.",
        attempts=1,
    )
    _append_log(f"Princess Elin received the {plan.reward_name}.")


async def _unlock_next_level(plan: LevelPlan) -> None:
    step_id = f"level-{plan.level}-unlock-next"
    _set_step(step_id, "queued", detail="The gate is glowing and ready to open.", attempts=0)
    await asyncio.sleep(0.3)
    _set_step(step_id, "running", detail="The princess is opening the next gate.", attempts=1)
    state.agent_status = "unlocking"
    state.agent_thought = f"Using the {plan.reward_name} to open the passage to the next level."
    await asyncio.sleep(0.9)

    level_progress = state.levels[plan.level - 1]
    level_progress.completed = True
    state.current_story = f"Level {plan.level} is complete."

    if plan.level < len(LEVELS):
        next_level = state.levels[plan.level]
        next_level.unlocked = True
        next_plan = LEVELS[plan.level]
        state.active_level = next_plan.level
        state.prince.status = "waiting"
        state.prince.position = next_plan.prince_start
        state.prince.note = f"Ready for {next_plan.name}."
        state.princess.status = "waiting"
        state.princess.position = next_plan.princess_start
        state.princess.note = f"Waiting by the gate of {next_plan.name}."
        state.princess.carrying = None
        state.reward_position = next_plan.goal
        state.reward_name = next_plan.reward_name
        state.visited_cells = [{"x": state.prince.position.x, "y": state.prince.position.y}]
        state.agent_status = "planning"
        state.agent_thought = f"Next mission ready: charting the safest route through {next_plan.name}."
        state.current_story = f"Level {next_plan.level} unlocked: {next_plan.name}."
        _append_log(f"Level {next_plan.level} unlocked.")
    else:
        state.active_level = plan.level
        state.reward_name = None
        state.agent_status = "complete"
        state.agent_thought = "All seven mazes are solved. The castle route is secure."
        state.current_story = "All levels complete. The princess opens the final castle gate."
        _append_log("The final gate opened. The story is complete.")

    _set_step(
        step_id,
        "done",
        detail="The next gate opened successfully.",
        attempts=1,
    )


async def _run_demo() -> None:
    async with run_lock:
        if state.run_status == "running":
            return

        _reset_state()
        state.run_id = uuid4().hex[:10]
        state.run_status = "running"
        _append_log(f"Run {state.run_id} started.")

        for plan in LEVELS:
            await _walk_path(plan)
            await _handoff_reward(plan)
            await _unlock_next_level(plan)

        state.run_status = "finished"
        state.prince.status = "arrived"
        state.prince.note = "The prince reached the royal hall."
        state.princess.status = "arrived"
        state.princess.note = "The princess opened the final level."
        state.agent_status = "complete"
        state.agent_thought = "Story complete. Every route stayed inside the maze and every FlowCore run was dispatched."
        _append_log(f"Run {state.run_id} finished. The castle is safe.")


@app.on_event("startup")
async def startup_event() -> None:
    _reset_state()


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FlowCore Maze Story</title>
  <style>
    :root {
      --bg: #f7efe2;
      --panel: rgba(255, 252, 248, 0.95);
      --ink: #1f2933;
      --muted: #667085;
      --line: #dfd2be;
      --brand: #9a3412;
      --brand-soft: #ffedd5;
      --ok: #166534;
      --ok-bg: #dcfce7;
      --run: #92400e;
      --run-bg: #ffedd5;
      --retry: #a16207;
      --retry-bg: #fef3c7;
      --idle: #475467;
      --idle-bg: #e4e7ec;
      --wall: #433324;
      --path: #f8fafc;
      --visited: #dbeafe;
      --prince: #2563eb;
      --princess: #db2777;
      --reward: #f59e0b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(154, 52, 18, 0.16), transparent 24rem),
        linear-gradient(180deg, #faf4eb 0%, #efe4d3 100%);
    }
    .shell {
      max-width: 1240px;
      margin: 0 auto;
      padding: 2rem 1.2rem 3rem;
      display: grid;
      gap: 1rem;
    }
    .hero, .panel {
      border: 1px solid rgba(90, 61, 31, 0.14);
      border-radius: 26px;
      background: var(--panel);
      box-shadow: 0 18px 48px rgba(53, 35, 12, 0.10);
    }
    .hero {
      padding: 1.5rem 1.6rem;
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: flex-start;
    }
    .eyebrow {
      display: inline-block;
      padding: 0.25rem 0.65rem;
      border-radius: 999px;
      background: var(--brand-soft);
      color: var(--brand);
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0.7rem 0 0.5rem;
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
      font-size: 2rem;
    }
    p {
      margin: 0;
      line-height: 1.6;
      color: var(--muted);
    }
    .actions {
      display: flex;
      gap: 0.75rem;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    button {
      border: 0;
      border-radius: 14px;
      padding: 0.9rem 1rem;
      font-weight: 700;
      cursor: pointer;
    }
    .primary { background: #111827; color: white; }
    .secondary { background: #efe4d1; color: #6b4f2c; }
    .grid {
      display: grid;
      grid-template-columns: 1.25fr 1fr;
      gap: 1rem;
    }
    .panel h2 {
      margin: 0;
      font-size: 1rem;
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
    }
    .head {
      padding: 1rem 1.2rem;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
    }
    .body { padding: 1rem 1.2rem 1.2rem; }
    .maze {
      display: grid;
      grid-template-columns: repeat(11, 1fr);
      gap: 0.28rem;
      max-width: 34rem;
    }
    .cell {
      aspect-ratio: 1;
      border-radius: 14px;
      border: 1px solid rgba(90, 61, 31, 0.08);
    }
    .wall { background: var(--wall); }
    .path { background: var(--path); }
    .visited { background: var(--visited); }
    .prince { background: var(--prince); box-shadow: inset 0 0 0 3px rgba(255,255,255,0.8); }
    .princess { background: var(--princess); box-shadow: inset 0 0 0 3px rgba(255,255,255,0.8); }
    .reward { background: var(--reward); box-shadow: inset 0 0 0 3px rgba(255,255,255,0.8); }
    .legend, .levels, .steps, .actors, .log {
      display: grid;
      gap: 0.8rem;
    }
    .agent-card {
      border: 1px solid rgba(223, 211, 194, 0.92);
      border-radius: 18px;
      padding: 0.9rem;
      background: rgba(255,255,255,0.78);
      margin-bottom: 1rem;
    }
    .legend {
      grid-template-columns: repeat(5, minmax(0, 1fr));
    }
    .legend-item, .level, .step, .actor, .log-item {
      border: 1px solid rgba(223, 211, 194, 0.92);
      border-radius: 18px;
      padding: 0.9rem;
      background: rgba(255,255,255,0.72);
    }
    .legend-item { display: flex; align-items: center; gap: 0.55rem; }
    .swatch {
      width: 1rem;
      height: 1rem;
      border-radius: 999px;
    }
    .step-top, .level-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
      margin-bottom: 0.35rem;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 0.25rem 0.55rem;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 700;
    }
    .pending { color: var(--idle); background: var(--idle-bg); }
    .queued, .running { color: var(--run); background: var(--run-bg); }
    .retrying { color: var(--retry); background: var(--retry-bg); }
    .done { color: var(--ok); background: var(--ok-bg); }
    .story {
      margin-top: 1rem;
      padding: 0.95rem 1rem;
      border-radius: 18px;
      background: rgba(255, 248, 237, 0.82);
      border: 1px solid rgba(223, 211, 194, 0.92);
    }
    .muted { color: var(--muted); }
    .mono { font-family: "IBM Plex Mono", monospace; }
    .log-item {
      font-family: "IBM Plex Mono", monospace;
      font-size: 0.84rem;
    }
    @media (max-width: 1040px) {
      .grid { grid-template-columns: 1fr; }
      .hero { display: grid; }
      .actions { justify-content: flex-start; }
      .legend { grid-template-columns: 1fr 1fr; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div>
        <span class="eyebrow">Maze Demo</span>
        <h1>The prince crosses the maze, the princess opens the next level.</h1>
        <p>This separate demo on port 9101 tells a clearer story than the operational dashboard. Prince Arin enters each maze, finds a reward, hands it to Princess Elin, and she unlocks the next gate.</p>
      </div>
      <div class="actions">
        <button class="primary" onclick="startDemo()">Start Story</button>
        <button class="secondary" onclick="resetDemo()">Reset</button>
      </div>
    </section>

    <section class="grid">
      <article class="panel">
        <div class="head">
          <h2>Maze View</h2>
          <span class="muted" id="run-status">idle</span>
        </div>
        <div class="body">
          <div class="legend">
            <div class="legend-item"><span class="swatch" style="background: var(--prince)"></span><span>Prince</span></div>
            <div class="legend-item"><span class="swatch" style="background: var(--princess)"></span><span>Princess</span></div>
            <div class="legend-item"><span class="swatch" style="background: var(--reward)"></span><span>Reward</span></div>
            <div class="legend-item"><span class="swatch" style="background: var(--visited)"></span><span>Visited</span></div>
            <div class="legend-item"><span class="swatch" style="background: var(--wall)"></span><span>Wall</span></div>
          </div>
          <div style="margin-top: 1rem" class="maze" id="maze"></div>
          <div class="story">
            <strong>Story</strong>
            <div class="muted" id="story">The prince is waiting at the entrance.</div>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="head">
          <h2>Actors And Levels</h2>
          <span class="mono muted" id="run-id">run: n/a</span>
        </div>
        <div class="body">
          <article class="agent-card">
            <strong id="agent-name">Maze Agent Orin</strong>
            <div class="muted" id="agent-status">idle</div>
            <div class="muted" id="agent-thought">Scanning the maze for the safest route.</div>
          </article>
          <div class="actors" id="actors"></div>
          <div style="height: 1rem"></div>
          <div class="levels" id="levels"></div>
        </div>
      </article>
    </section>

    <section class="panel">
      <div class="head">
        <h2>FlowCore Monitor Link</h2>
        <span class="muted">same backend family as 9100</span>
      </div>
      <div class="body">
        <div class="levels" id="flowcore-status"></div>
      </div>
    </section>

    <section class="grid">
      <article class="panel">
        <div class="head">
          <h2>Workflow Steps</h2>
          <span class="muted">how the story moves</span>
        </div>
        <div class="body">
          <div class="steps" id="steps"></div>
        </div>
      </article>

      <article class="panel">
        <div class="head">
          <h2>Event Log</h2>
          <span class="muted">latest first</span>
        </div>
        <div class="body">
          <div class="log" id="log"></div>
        </div>
      </article>
    </section>
  </main>

  <script>
    function badgeClass(status) {
      return status || "pending";
    }

    function renderMaze(cells) {
      const target = document.getElementById("maze");
      target.innerHTML = cells.map((cell) => `<div class="cell ${cell.kind}"></div>`).join("");
    }

    function renderActors(payload) {
      const target = document.getElementById("actors");
      const actors = [payload.prince, payload.princess];
      target.innerHTML = actors.map((actor) => `
        <article class="actor">
          <strong>${actor.name}</strong>
          <div class="muted">${actor.role}</div>
          <div class="muted">Status: ${actor.status}</div>
          <div class="muted">Position: (${actor.position.x}, ${actor.position.y})</div>
          <div class="muted">Carrying: ${actor.carrying || "nothing yet"}</div>
          <div class="muted">${actor.note}</div>
        </article>
      `).join("");
    }

    function renderAgent(agent) {
      document.getElementById("agent-name").textContent = agent.name;
      document.getElementById("agent-status").textContent = `Agent status: ${agent.status}`;
      document.getElementById("agent-thought").textContent = agent.thought;
    }

    function renderLevels(levels, activeLevel) {
      const target = document.getElementById("levels");
      target.innerHTML = levels.map((level) => `
        <article class="level">
          <div class="level-top">
            <strong>Level ${level.level}: ${level.name}</strong>
            <span class="badge ${level.completed ? "done" : (level.unlocked ? "running" : "pending")}">
              ${level.completed ? "completed" : (level.unlocked ? "open" : "locked")}
            </span>
          </div>
          <div class="muted">Reward: ${level.reward_name}</div>
          <div class="muted">${level.level === activeLevel ? "Current level" : "Waiting in the story"}</div>
          <div class="muted">FlowCore status: ${level.flowcore_status}</div>
          <div class="muted">Trace: ${level.flowcore_trace_id || "not started yet"}</div>
        </article>
      `).join("");
    }

    function renderSteps(steps) {
      const target = document.getElementById("steps");
      target.innerHTML = steps.map((step) => `
        <article class="step">
          <div class="step-top">
            <strong>${step.title}</strong>
            <span class="badge ${badgeClass(step.status)}">${step.status}</span>
          </div>
          <div class="muted">${step.note}</div>
          <div class="muted">Attempts: ${step.attempts}</div>
          <div class="muted">${step.detail || "Waiting..."}</div>
        </article>
      `).join("");
    }

    function renderFlowcoreStatus(flowcore) {
      const target = document.getElementById("flowcore-status");
      const runs = Array.isArray(flowcore.recent_runs) ? flowcore.recent_runs.slice(0, 4) : [];
      const workers = Array.isArray(flowcore.workers) ? flowcore.workers : [];
      target.innerHTML = `
        <article class="level">
          <div class="level-top">
            <strong>Queue ${flowcore.queue_name}</strong>
            <span class="badge running">live</span>
          </div>
          <div class="muted">Queued: ${flowcore.queue_depth}</div>
          <div class="muted">Processing: ${flowcore.processing_depth}</div>
          <div class="muted">DLQ: ${flowcore.dlq_depth}</div>
          <div class="muted">Workers seen: ${workers.length}</div>
          <div class="muted">Active trace from story: ${flowcore.active_trace || "none"}</div>
        </article>
      ` + runs.map((run) => `
        <article class="level">
          <div class="level-top">
            <strong>${run.run_id}</strong>
            <span class="badge ${run.status === "failed" ? "retrying" : "done"}">${run.status}</span>
          </div>
          <div class="muted">${run.dag_name}</div>
          <div class="muted">Started: ${new Date(run.started_at).toLocaleString()}</div>
          <div class="muted">Tasks: ${run.node_count} total / ${run.failed} failed / ${run.skipped} skipped</div>
        </article>
      `).join("");
    }

    function renderLog(items) {
      const target = document.getElementById("log");
      target.innerHTML = items.map((item) => `<div class="log-item">${item}</div>`).join("");
    }

    async function refreshState() {
      const response = await fetch("/api/state");
      const payload = await response.json();
      document.getElementById("run-status").textContent = payload.run_status;
      document.getElementById("run-id").textContent = payload.run_id ? `run: ${payload.run_id}` : "run: n/a";
      document.getElementById("story").textContent = payload.story;
      renderMaze(payload.maze);
      renderAgent(payload.agent);
      renderActors(payload);
      renderLevels(payload.levels, payload.active_level);
      renderSteps(payload.steps);
      payload.flowcore.active_trace = payload.active_flowcore_trace_id;
      renderFlowcoreStatus(payload.flowcore);
      renderLog(payload.log);
    }

    async function startDemo() {
      await fetch("/api/start", { method: "POST" });
      await refreshState();
    }

    async function resetDemo() {
      await fetch("/api/reset", { method: "POST" });
      await refreshState();
    }

    refreshState();
    setInterval(refreshState, 800);
  </script>
</body>
</html>"""


@app.get("/api/state")
async def get_state() -> dict[str, object]:
    payload = _serialize_state()
    payload["flowcore"] = await _fetch_flowcore_status()
    return payload


@app.post("/api/start")
async def start_demo() -> dict[str, object]:
    if state.run_status != "running":
        asyncio.create_task(_run_demo())
    return {"ok": True}


@app.post("/api/reset")
async def reset_demo() -> dict[str, object]:
    if state.run_status != "running":
        _reset_state()
    return {"ok": True}
