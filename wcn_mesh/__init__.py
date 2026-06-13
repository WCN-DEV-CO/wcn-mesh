"""wcn-mesh — a tiny typed actor runtime for Python.

Actors are isolated units of state that communicate only by messages. Each actor has
a mailbox; the runtime delivers messages one at a time, so an actor never races with
itself. Supervision trees restart failed actors per a policy. Addressing is location-
transparent (an ActorRef hides where the actor lives), so the same code runs locally
today and across nodes when you plug in a transport.

Extension seams:
  - Supervisor: decides what happens when an actor raises (default: restart).
  - Resolver:   resolves an address to a delivery target (default: local registry).
The private WCN layer plugs swarm orchestration / cross-node routing behind these.

Zero dependencies. Pure standard library. MIT licensed. Original implementation.
"""
from __future__ import annotations
import queue
import threading
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

__version__ = "0.1.0"
__all__ = ["Actor", "ActorRef", "ActorSystem", "Supervisor", "RestartSupervisor",
           "StopSupervisor", "Resolver", "LocalResolver", "Message"]


@dataclass
class Message:
    sender: Optional["ActorRef"]
    payload: Any


class Actor:
    """Subclass and implement receive(message). Access self.state freely — the runtime
    guarantees single-threaded message processing per actor."""
    def __init__(self) -> None:
        self.ref: Optional[ActorRef] = None
        self.system: Optional[ActorSystem] = None

    def receive(self, message: Message) -> None:  # override
        raise NotImplementedError

    def on_start(self) -> None: ...
    def on_restart(self, error: Exception) -> None: ...
    def on_stop(self) -> None: ...


class ActorRef:
    """Location-transparent handle. You send to a ref; you never touch the actor."""
    def __init__(self, address: str, system: "ActorSystem") -> None:
        self.address = address
        self._system = system

    def tell(self, payload: Any, sender: Optional["ActorRef"] = None) -> None:
        self._system._deliver(self.address, Message(sender, payload))

    def __repr__(self) -> str:
        return f"<ActorRef {self.address}>"


class Supervisor(Protocol):
    """SEAM: on actor failure, return one of 'restart' | 'stop' | 'resume'."""
    def decide(self, error: Exception, restarts: int) -> str: ...


class RestartSupervisor:
    def __init__(self, max_restarts: int = 3) -> None:
        self.max_restarts = max_restarts
    def decide(self, error: Exception, restarts: int) -> str:
        return "restart" if restarts < self.max_restarts else "stop"


class StopSupervisor:
    def decide(self, error: Exception, restarts: int) -> str:
        return "stop"


class Resolver(Protocol):
    """SEAM: map an address to a local mailbox (or, in the private layer, a remote node)."""
    def is_local(self, address: str) -> bool: ...


class LocalResolver:
    def is_local(self, address: str) -> bool:
        return True


@dataclass
class _Cell:
    actor: Actor
    factory: Callable[[], Actor]
    mailbox: "queue.Queue"
    thread: threading.Thread
    restarts: int = 0
    alive: bool = True


class ActorSystem:
    """Hosts actors, delivers messages, supervises failures."""

    def __init__(self, supervisor: Optional[Supervisor] = None,
                 resolver: Optional[Resolver] = None) -> None:
        self._cells: dict[str, _Cell] = {}
        self._lock = threading.Lock()
        self._supervisor: Supervisor = supervisor or RestartSupervisor()
        self._resolver: Resolver = resolver or LocalResolver()
        self._ids = itertools.count(1)
        self._dead_letters: list[Message] = []

    def spawn(self, factory: Callable[[], Actor], name: Optional[str] = None) -> ActorRef:
        address = name or f"actor-{next(self._ids)}"
        with self._lock:
            if address in self._cells:
                raise ValueError(f"address {address!r} already in use")
            mb: queue.Queue = queue.Queue()
            actor = factory()
            ref = ActorRef(address, self)
            actor.ref = ref; actor.system = self
            th = threading.Thread(target=self._run, args=(address,), daemon=True)
            cell = _Cell(actor=actor, factory=factory, mailbox=mb, thread=th)
            self._cells[address] = cell
        actor.on_start()
        th.start()
        return ref

    def _deliver(self, address: str, msg: Message) -> None:
        with self._lock:
            cell = self._cells.get(address)
        if cell is None or not cell.alive:
            self._dead_letters.append(msg)   # at-least-once: undeliverable -> dead letters
            return
        cell.mailbox.put(msg)

    def _run(self, address: str) -> None:
        while True:
            with self._lock:
                cell = self._cells.get(address)
            if cell is None or not cell.alive:
                return
            msg = cell.mailbox.get()
            if msg is _STOP:
                return
            try:
                cell.actor.receive(msg)
            except Exception as e:  # noqa: BLE001 — supervision boundary
                cell.restarts += 1
                decision = self._supervisor.decide(e, cell.restarts)
                if decision == "restart":
                    new_actor = cell.factory()
                    new_actor.ref = ActorRef(address, self); new_actor.system = self
                    cell.actor.on_restart(e)
                    cell.actor = new_actor
                    new_actor.on_start()
                    # at-least-once: retry the message that triggered the crash.
                    # a transient failure recovers; a permanent one consumes the
                    # restart budget until the supervisor decides to stop.
                    cell.mailbox.put(msg)
                elif decision == "stop":
                    self.stop(address)
                    return
                # "resume" -> just continue

    def stop(self, address: str) -> None:
        with self._lock:
            cell = self._cells.get(address)
            if cell is None:
                return
            cell.alive = False
        try:
            cell.actor.on_stop()
        finally:
            cell.mailbox.put(_STOP)

    @property
    def dead_letters(self) -> list:
        return list(self._dead_letters)

    def count(self) -> int:
        with self._lock:
            return sum(1 for c in self._cells.values() if c.alive)


_STOP = object()
