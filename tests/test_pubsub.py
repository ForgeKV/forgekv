#!/usr/bin/env python3
"""
Comprehensive pub/sub functional tests for ForgeKV.
Tests: direct subscribe, pattern subscribe, multiple channels,
       PUBSUB introspection, message ordering, cleanup.
"""
import threading
import time
import redis
import sys

HOST = "127.0.0.1"
PORT = 6379
PASSED = []
FAILED = []


def make_client(**kw):
    return redis.Redis(host=HOST, port=PORT, decode_responses=True, **kw)


def check(name, got, expected):
    if got == expected:
        PASSED.append(name)
        print(f"  PASS  {name}")
    else:
        FAILED.append(name)
        print(f"  FAIL  {name}: expected={expected!r}  got={got!r}")


def test_direct_subscribe():
    """Subscriber receives messages published to its channel."""
    received = []
    def subscriber():
        r = make_client()
        ps = r.pubsub()
        ps.subscribe("ch1")
        ps.get_message()  # consume subscribe confirmation
        deadline = time.time() + 3
        while time.time() < deadline and len(received) < 2:
            msg = ps.get_message(timeout=0.1)
            if msg and msg["type"] == "message":
                received.append(msg["data"])
        ps.unsubscribe()
        ps.close()

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    time.sleep(0.3)  # let subscriber register

    pub = make_client()
    n1 = pub.publish("ch1", "hello")
    n2 = pub.publish("ch1", "world")
    t.join(timeout=3)

    check("direct_subscribe:publish_count_1", n1, 1)
    check("direct_subscribe:publish_count_2", n2, 1)
    check("direct_subscribe:messages", received, ["hello", "world"])


def test_pattern_subscribe():
    """Subscriber with PSUBSCRIBE receives messages on matching channels."""
    received = []
    def subscriber():
        r = make_client()
        ps = r.pubsub()
        ps.psubscribe("news.*")
        ps.get_message()  # consume psubscribe confirmation
        deadline = time.time() + 3
        while time.time() < deadline and len(received) < 2:
            msg = ps.get_message(timeout=0.1)
            if msg and msg["type"] == "pmessage":
                received.append((msg["pattern"], msg["channel"], msg["data"]))
        ps.punsubscribe()
        ps.close()

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    time.sleep(0.3)

    pub = make_client()
    pub.publish("news.sport", "goal!")
    pub.publish("news.tech", "ai")
    pub.publish("weather", "sunny")  # should NOT be received
    t.join(timeout=3)

    check("pattern_subscribe:count", len(received), 2)
    if len(received) >= 2:
        check("pattern_subscribe:pattern", received[0][0], "news.*")
        check("pattern_subscribe:channel1", received[0][1], "news.sport")
        check("pattern_subscribe:data1", received[0][2], "goal!")
        check("pattern_subscribe:channel2", received[1][1], "news.tech")


def test_multiple_channels():
    """One subscriber on multiple channels receives all messages."""
    received = []
    def subscriber():
        r = make_client()
        ps = r.pubsub()
        ps.subscribe("a", "b", "c")
        for _ in range(3):  # consume subscribe confirmations
            ps.get_message()
        deadline = time.time() + 3
        while time.time() < deadline and len(received) < 3:
            msg = ps.get_message(timeout=0.1)
            if msg and msg["type"] == "message":
                received.append((msg["channel"], msg["data"]))
        ps.unsubscribe()
        ps.close()

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    time.sleep(0.3)

    pub = make_client()
    pub.publish("a", "msg_a")
    pub.publish("b", "msg_b")
    pub.publish("c", "msg_c")
    t.join(timeout=3)

    check("multi_channel:count", len(received), 3)
    channels = {ch for ch, _ in received}
    check("multi_channel:all_channels", channels, {"a", "b", "c"})


def test_multiple_subscribers():
    """Multiple subscribers on same channel all receive the message."""
    results = [[], []]
    def make_sub(idx):
        def sub():
            r = make_client()
            ps = r.pubsub()
            ps.subscribe("shared")
            ps.get_message()
            deadline = time.time() + 3
            while time.time() < deadline and not results[idx]:
                msg = ps.get_message(timeout=0.1)
                if msg and msg["type"] == "message":
                    results[idx].append(msg["data"])
            ps.unsubscribe()
            ps.close()
        return sub

    threads = [threading.Thread(target=make_sub(i), daemon=True) for i in range(2)]
    for t in threads: t.start()
    time.sleep(0.3)

    pub = make_client()
    n = pub.publish("shared", "broadcast")
    for t in threads: t.join(timeout=3)

    check("multi_subscriber:publish_count", n, 2)
    check("multi_subscriber:sub0_received", results[0], ["broadcast"])
    check("multi_subscriber:sub1_received", results[1], ["broadcast"])


def test_pubsub_introspection():
    """PUBSUB CHANNELS, NUMSUB, NUMPAT return correct values."""
    r = make_client()
    r.execute_command("FLUSHALL")
    time.sleep(0.1)

    ps1 = make_client().pubsub()
    ps2 = make_client().pubsub()
    ps3 = make_client().pubsub()

    ps1.subscribe("chan_x")
    ps2.subscribe("chan_x", "chan_y")
    ps3.psubscribe("chan_*")
    time.sleep(0.3)  # let subscriptions register

    # PUBSUB CHANNELS
    channels = set(r.execute_command("PUBSUB", "CHANNELS", "*"))
    check("introspection:channels_contains_x", "chan_x" in channels, True)
    check("introspection:channels_contains_y", "chan_y" in channels, True)

    # PUBSUB NUMSUB
    result = r.execute_command("PUBSUB", "NUMSUB", "chan_x", "chan_y")
    numsub = dict(zip(result[::2], result[1::2]))
    check("introspection:numsub_chan_x", numsub.get("chan_x", 0), 2)
    check("introspection:numsub_chan_y", numsub.get("chan_y", 0), 1)

    # PUBSUB NUMPAT
    numpat = r.execute_command("PUBSUB", "NUMPAT")
    check("introspection:numpat", numpat >= 1, True)

    ps1.unsubscribe(); ps1.close()
    ps2.unsubscribe(); ps2.close()
    ps3.punsubscribe(); ps3.close()


def test_publish_no_subscribers():
    """PUBLISH returns 0 when no subscribers exist."""
    r = make_client()
    r.execute_command("FLUSHALL")
    time.sleep(0.1)
    n = r.publish("empty_channel", "nobody_home")
    check("publish_no_subscribers", n, 0)


def test_unsubscribe_stops_messages():
    """After UNSUBSCRIBE, no more messages are received."""
    received_before = []
    received_after = []

    r_sub = make_client()
    ps = r_sub.pubsub()
    ps.subscribe("leaving")
    time.sleep(0.3)  # let subscription establish
    # drain all pending control messages
    for _ in range(5):
        m = ps.get_message(timeout=0.1)
        if not m:
            break

    pub = make_client()
    pub.publish("leaving", "first")
    time.sleep(0.5)  # give message time to arrive
    msg = ps.get_message(timeout=1.0)
    if msg and msg["type"] == "message":
        received_before.append(msg["data"])

    ps.unsubscribe("leaving")
    time.sleep(0.5)  # let unsubscribe propagate

    pub.publish("leaving", "second")
    time.sleep(0.5)
    # drain anything in the buffer
    for _ in range(5):
        msg = ps.get_message(timeout=0.2)
        if msg and msg["type"] == "message":
            received_after.append(msg["data"])
        if not msg:
            break

    ps.close()

    check("unsub_stops:received_before", received_before, ["first"])
    check("unsub_stops:received_after", received_after, [])


def test_ping_in_subscriber_mode():
    """PING in subscriber mode returns pong array response."""
    import socket as _socket
    # Use raw socket to avoid redis-py buffering surprises
    s = _socket.socket()
    s.settimeout(3)
    s.connect((HOST, PORT))
    # SUBSCRIBE pingchan
    s.sendall(b"*2\r\n$9\r\nSUBSCRIBE\r\n$8\r\npingchan\r\n")
    time.sleep(0.2)
    s.recv(1024)  # consume subscribe confirmation
    # PING with no message
    s.sendall(b"*1\r\n$4\r\nPING\r\n")
    time.sleep(0.2)
    resp = s.recv(1024).decode()
    s.close()
    # Should contain "pong" in the response array
    check("ping_in_sub_mode:response_contains_pong", "pong" in resp, True)


def run_all():
    tests = [
        test_publish_no_subscribers,
        test_direct_subscribe,
        test_pattern_subscribe,
        test_multiple_channels,
        test_multiple_subscribers,
        test_pubsub_introspection,
        test_unsubscribe_stops_messages,
        test_ping_in_subscriber_mode,
    ]
    for test in tests:
        print(f"\n[{test.__name__}]")
        try:
            test()
        except Exception as e:
            FAILED.append(test.__name__)
            print(f"  ERROR  {test.__name__}: {e}")

    print(f"\n{'='*50}")
    print(f"Results: {len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        print(f"FAILED: {FAILED}")
        sys.exit(1)
    else:
        print("ALL TESTS PASSED")


if __name__ == "__main__":
    run_all()
