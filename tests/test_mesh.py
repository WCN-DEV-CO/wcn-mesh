import time, threading
import pytest
from wcn_mesh import (Actor, ActorSystem, RestartSupervisor, StopSupervisor,
                      LocalResolver, Message)

def wait_for(cond, timeout=2.0):
    t0=time.monotonic()
    while time.monotonic()-t0<timeout:
        if cond(): return True
        time.sleep(0.005)
    return False

def test_spawn_and_tell():
    got=[]
    class Echo(Actor):
        def receive(self, m): got.append(m.payload)
    sys=ActorSystem()
    ref=sys.spawn(Echo)
    ref.tell("hello")
    assert wait_for(lambda: got==["hello"])

def test_single_threaded_per_actor():
    class Counter(Actor):
        def __init__(self): super().__init__(); self.state=0
        def receive(self, m):
            cur=self.state; time.sleep(0.0001); self.state=cur+1
    c=Counter()
    sys=ActorSystem()
    ref=sys.spawn(lambda: c)
    for _ in range(200): ref.tell("inc")
    assert wait_for(lambda: c.state==200)
    assert c.state==200

def test_named_actor_collision():
    class A(Actor):
        def receive(self,m): pass
    sys=ActorSystem()
    sys.spawn(A, name="dup")
    with pytest.raises(ValueError):
        sys.spawn(A, name="dup")

def test_transient_failure_recovers_on_retry():
    # message that crashes the FIRST instance succeeds after a restart (transient fault)
    state={"fail":True}
    events=[]
    class Flaky(Actor):
        def on_restart(self, e): events.append("restart")
        def receive(self, m):
            if m.payload=="work" and state["fail"]:
                state["fail"]=False     # heal after first crash
                raise RuntimeError("transient")
            events.append(("ok", m.payload))
    sys=ActorSystem(supervisor=RestartSupervisor(max_restarts=5))
    ref=sys.spawn(Flaky)
    ref.tell("work")
    assert wait_for(lambda: ("ok","work") in events)
    assert "restart" in events           # it did restart, then the retry succeeded

def test_supervisor_stops_after_max():
    class Boom(Actor):
        def receive(self, m): raise RuntimeError("always")
    sys=ActorSystem(supervisor=RestartSupervisor(max_restarts=2))
    ref=sys.spawn(Boom)
    ref.tell("x")
    assert wait_for(lambda: sys.count()==0)   # permanent failure -> budget consumed -> stop

def test_stop_supervisor():
    class Boom(Actor):
        def receive(self, m): raise RuntimeError("nope")
    sys=ActorSystem(supervisor=StopSupervisor())
    ref=sys.spawn(Boom)
    ref.tell("x")
    assert wait_for(lambda: sys.count()==0)

def test_dead_letters_for_stopped_actor():
    class A(Actor):
        def receive(self,m): pass
    sys=ActorSystem()
    ref=sys.spawn(A, name="gone")
    sys.stop("gone")
    assert wait_for(lambda: sys.count()==0)
    ref.tell("undeliverable")
    assert wait_for(lambda: any(m.payload=="undeliverable" for m in sys.dead_letters))

def test_actor_to_actor_messaging():
    inbox=[]
    class Pong(Actor):
        def receive(self,m): inbox.append(m.payload)
    class Ping(Actor):
        def __init__(self, target): super().__init__(); self.t=target
        def receive(self,m): self.t.tell("pong", sender=self.ref)
    sys=ActorSystem()
    pong=sys.spawn(Pong)
    ping=sys.spawn(lambda: Ping(pong))
    ping.tell("go")
    assert wait_for(lambda: inbox==["pong"])

def test_resolver_seam_default_local():
    sys=ActorSystem(resolver=LocalResolver())
    assert sys._resolver.is_local("anything") is True
