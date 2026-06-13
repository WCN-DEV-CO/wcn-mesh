# wcn-mesh

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen.svg)](#)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#)

A tiny **typed actor runtime**. Actors are isolated units of state that talk only by
messages — the runtime delivers one message at a time per actor, so you mutate state
**without locks and without races**. Supervision trees restart failed actors. Addresses
are location-transparent, so the same code runs locally now and across nodes later.
Zero dependencies.

## Install
```bash
pip install wcn-mesh
```

## Quick start
```python
from wcn_mesh import Actor, ActorSystem

class Counter(Actor):
    def __init__(self):
        super().__init__()
        self.state = 0
    def receive(self, msg):
        self.state += 1          # safe: one message at a time, no locks

sys = ActorSystem()
ref = sys.spawn(Counter)
ref.tell("inc")                  # fire-and-forget message
```

## Supervision (self-healing)
```python
from wcn_mesh import ActorSystem, RestartSupervisor

sys = ActorSystem(supervisor=RestartSupervisor(max_restarts=3))
# transient failures recover on retry; permanent ones stop after the budget
```

## Actor-to-actor
```python
class Ping(Actor):
    def __init__(self, target):
        super().__init__(); self.target = target
    def receive(self, msg):
        self.target.tell("pong", sender=self.ref)
```

## Extension seams
Bring your own brain without forking the engine:

- **Supervisor** — `decide(error, restarts) -> 'restart' | 'stop' | 'resume'`
- **Resolver** — map an address to a local mailbox *or a remote node* (your transport)

The local runtime is solid standalone; the `Resolver` seam is where distributed,
cross-node routing plugs in.

## Features
- ✅ Isolated actors, single-threaded delivery per actor (no locks needed)
- ✅ Supervision: restart / stop / resume policies
- ✅ At-least-once delivery (crash-triggering message is retried; dead letters captured)
- ✅ Location-transparent `ActorRef` addressing
- ✅ Pluggable Supervisor + Resolver seams
- ✅ **Zero dependencies**

## License
MIT © WCN Development Co
