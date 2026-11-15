//! Loom models of the cross-task protocols in `shared.rs`.
//!
//! Run: `RUSTFLAGS="--cfg matier_loom" cargo test -p matier-transport --lib --release loom_`
//! (use a separate `CARGO_TARGET_DIR`; `--cfg matier_loom` rebuilds the dependency graph).

use std::future::poll_fn;
use std::task::Poll;

use loom::future::block_on;
use loom::thread;
use matier_core::Error;
use tokio::sync::mpsc;

use super::shared::{Command, ConnShared, Dirty, EVENT_OVERHEAD, RecvEvent, StreamShared};
use super::sync::{Arc, Ordering};

fn stream(threshold: u32, credit: i64) -> (Arc<StreamShared>, mpsc::UnboundedReceiver<Command>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let origin = matier_core::OriginKey::from_uri(&"http://x/".parse().unwrap()).unwrap();
    let conn = Arc::new(ConnShared::new(origin, 1, 100, 1, false));
    (Arc::new(StreamShared::new(tx, conn, threshold, credit)), rx)
}

/// Bounded-preemption model for the larger state spaces (loom's default is unbounded).
fn bounded(f: impl Fn() + Sync + Send + 'static) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(f);
}

fn drain(dirty: &mut Dirty) {
    for d in dirty.drain(..) {
        d.wake_now();
    }
}

fn data(len: usize, end: bool) -> RecvEvent {
    RecvEvent::Data {
        view: matier_core::buf::copy_from_slice(&vec![0u8; len]),
        flow: len as u32,
        end,
    }
}

fn parts() -> http::response::Parts {
    http::Response::new(()).into_parts().0
}

/// Actor publishes the head while the consumer registers its waker: the consumer must always get it.
#[test]
fn loom_head_slot_handoff() {
    loom::model(|| {
        let (s, _rx) = stream(1, 0);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || {
            let mut dirty = Dirty::new();
            StreamShared::publish_head_deferred(&s2, Ok(parts()), &mut dirty);
            drain(&mut dirty);
        });
        let r = block_on(poll_fn(|cx| s.poll_head(cx)));
        assert!(r.is_ok());
        t.join().unwrap();
    });
}

/// Terminal error vs. head: whichever the actor publishes is delivered exactly once.
#[test]
fn loom_head_error_handoff() {
    loom::model(|| {
        let (s, _rx) = stream(1, 0);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || {
            let mut dirty = Dirty::new();
            StreamShared::fail(&s2, Error::closed(), &mut dirty);
            drain(&mut dirty);
        });
        let r = block_on(poll_fn(|cx| s.poll_head(cx)));
        assert!(r.is_err());
        t.join().unwrap();
    });
}

/// Receive queue + waker: an event pushed concurrently with the consumer's poll is never lost.
#[test]
fn loom_recv_queue_and_waker() {
    loom::model(|| {
        let (s, _rx) = stream(1, 0);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || {
            let mut dirty = Dirty::new();
            StreamShared::push_recv(&s2, RecvEvent::Trailers(Box::default()), &mut dirty);
            StreamShared::push_recv(&s2, RecvEvent::End, &mut dirty);
            // One deferred wake covers both events.
            assert_eq!(dirty.len(), 1);
            drain(&mut dirty);
        });
        let mut seen = 0;
        block_on(poll_fn(|cx| {
            loop {
                match s.poll_recv(cx) {
                    Poll::Ready(RecvEvent::End) => return Poll::Ready(()),
                    Poll::Ready(_) => seen += 1,
                    Poll::Pending => return Poll::Pending,
                }
            }
        }));
        assert_eq!(seen, 1);
        t.join().unwrap();
    });
}

/// QUEUED-flag dedup: two consumers report consumption while the actor handles `Ready` commands
/// (clear QUEUED, then read state). After everything drains, all consumption must have been seen and no
/// state may be left behind without a pending `Ready`.
#[test]
fn loom_queued_flag_dedup_never_loses_updates() {
    bounded(|| {
        let (s, rx) = stream(1, 0);
        let p1 = {
            let s = Arc::clone(&s);
            thread::spawn(move || StreamShared::consumed(&s, 1))
        };
        let p2 = {
            let s = Arc::clone(&s);
            thread::spawn(move || StreamShared::consumed(&s, 1))
        };
        let actor = thread::spawn(move || {
            let mut rx = rx;
            let mut total = 0;
            for _ in 0..3 {
                match rx.try_recv() {
                    Ok(Command::Ready(s)) => {
                        s.begin_ready();
                        total += s.take_consumed();
                    }
                    _ => thread::yield_now(),
                }
            }
            (total, rx)
        });
        p1.join().unwrap();
        p2.join().unwrap();
        let (mut total, mut rx) = actor.join().unwrap();
        while let Ok(Command::Ready(s)) = rx.try_recv() {
            s.begin_ready();
            total += s.take_consumed();
        }
        assert_eq!(total, 2, "a consumption update was lost");
        assert_eq!(s.take_consumed(), 0);
    });
}

/// Cancel racing with the actor pushing data: every pushed byte is accounted to the connection's `consumed`
/// counter exactly once between the consumer's drain and the actor's own drain.
#[test]
fn loom_cancel_vs_push() {
    loom::model(|| {
        let (s, rx) = stream(1 << 20, 0);
        let conn = Arc::clone(&s.conn);
        let s2 = Arc::clone(&s);
        let pusher = thread::spawn(move || {
            let mut dirty = Dirty::new();
            for _ in 0..2 {
                StreamShared::push_recv(&s2, data(10, false), &mut dirty);
            }
            drain(&mut dirty);
        });
        let s3 = Arc::clone(&s);
        let canceller = thread::spawn(move || StreamShared::cancel(&s3));
        pusher.join().unwrap();
        canceller.join().unwrap();
        // Actor side: handle the Ready, then drain whatever the consumer's drain missed.
        let mut rx = rx;
        while let Ok(Command::Ready(sh)) = rx.try_recv() {
            let f = sh.begin_ready();
            assert!(f & super::shared::CANCELED != 0);
            sh.drain_recv();
        }
        assert_eq!(
            conn.consumed.load(Ordering::SeqCst),
            2 * (10 + EVENT_OVERHEAD)
        );
        assert_eq!(s.flags() & super::shared::CANCELED, super::shared::CANCELED);
    });
}

/// FIFO across the inline ring and the spill queue (ring of 2, three events), with the consumer racing the
/// producer; END is folded into the last DATA event and seen last.
#[test]
fn loom_ring_spill_fifo_and_end() {
    bounded(|| {
        let (s, _rx) = stream(1 << 20, 0);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || {
            let mut dirty = Dirty::new();
            for i in 0..3usize {
                StreamShared::push_recv(&s2, data(i + 1, i == 2), &mut dirty);
            }
            drain(&mut dirty);
        });
        let mut next = 1usize;
        block_on(poll_fn(|cx| {
            loop {
                match s.poll_recv(cx) {
                    Poll::Ready(RecvEvent::Data { view, end, .. }) => {
                        assert_eq!(view.len(), next, "out of order");
                        next += 1;
                        if end {
                            return Poll::Ready(());
                        }
                    }
                    Poll::Ready(_) => panic!("unexpected event"),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }));
        assert_eq!(next, 4);
        t.join().unwrap();
    });
}

/// Dropping a stream with events still queued (ring and spill) drops them and charges the connection counter.
#[test]
fn loom_drop_with_queued_events() {
    loom::model(|| {
        let (s, _rx) = stream(1 << 20, 0);
        let conn = Arc::clone(&s.conn);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || {
            let mut dirty = Dirty::new();
            for _ in 0..3 {
                StreamShared::push_recv(&s2, data(1, false), &mut dirty);
            }
            drain(&mut dirty);
        });
        t.join().unwrap();
        drop(s);
        assert_eq!(
            conn.consumed.load(Ordering::SeqCst),
            3 * (1 + EVENT_OVERHEAD)
        );
    });
}

/// Deferred wake dedup: two pushes before a drain owe one wake; a push after the drain owes another, and the
/// consumer is never left sleeping with an event queued.
#[test]
fn loom_deferred_wake_vs_register_then_check() {
    bounded(|| {
        let (s, _rx) = stream(1 << 20, 0);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || {
            let mut dirty = Dirty::new();
            StreamShared::push_recv(&s2, data(1, false), &mut dirty);
            StreamShared::push_recv(&s2, data(2, false), &mut dirty);
            assert_eq!(dirty.len(), 1);
            drain(&mut dirty);
            StreamShared::push_recv(&s2, data(3, true), &mut dirty);
            assert_eq!(dirty.len(), 1);
            drain(&mut dirty);
        });
        let mut got = 0;
        block_on(poll_fn(|cx| {
            loop {
                match s.poll_recv(cx) {
                    Poll::Ready(RecvEvent::Data { end, .. }) => {
                        got += 1;
                        if end {
                            return Poll::Ready(());
                        }
                    }
                    Poll::Ready(_) => panic!("unexpected event"),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }));
        assert_eq!(got, 3);
        t.join().unwrap();
    });
}

/// Handshake between the actor parking connection-window credit (`CONSUMED_BLOCKED`, an RMW on the consumed
/// word) and a consumer reporting consumption: either the actor's RMW result includes the consumption, or the
/// consumer's `fetch_add` sees the bit and sends a `Ready`.
#[test]
fn loom_recv_blocked_handshake() {
    use super::shared::CONSUMED_BLOCKED;
    loom::model(|| {
        let (s, rx) = stream(1 << 20, 0);
        let conn = Arc::clone(&s.conn);
        let c2 = Arc::clone(&s);
        let consumer = thread::spawn(move || StreamShared::consumed(&c2, 10));
        // Actor: publish "blocked" and look at the consumption in the same RMW.
        let word = conn.consumed.fetch_or(CONSUMED_BLOCKED, Ordering::AcqRel);
        consumer.join().unwrap();
        let mut rx = rx;
        let readied = rx.try_recv().is_ok();
        assert!(
            (word & !CONSUMED_BLOCKED) >= 10 + EVENT_OVERHEAD || readied,
            "consumption was neither seen nor signalled"
        );
    });
}

/// Streaming-upload credit: the pump registers for credit while the actor returns it.
#[test]
fn loom_send_credit_wakeup() {
    loom::model(|| {
        let (s, _rx) = stream(1, 0);
        let s2 = Arc::clone(&s);
        let t = thread::spawn(move || s2.add_credit(10));
        let open = block_on(poll_fn(|cx| s.poll_credit(cx)));
        assert!(open);
        assert!(s.flags() & super::shared::SEND_CLOSED == 0);
        t.join().unwrap();
        let _ = Ordering::SeqCst;
    });
}

/// Slot accounting: concurrent reservers never exceed the limit and releases balance.
#[test]
fn loom_slot_accounting() {
    use super::shared::ConnShared;
    loom::model(|| {
        let origin = matier_core::OriginKey::from_uri(&"http://x/".parse().unwrap()).unwrap();
        let c = Arc::new(ConnShared::new(origin, 1, 1, 1, false));
        let a = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.try_reserve())
        };
        let b = {
            let c = Arc::clone(&c);
            thread::spawn(move || c.try_reserve())
        };
        let (ra, rb) = (a.join().unwrap(), b.join().unwrap());
        assert!(!(ra && rb), "limit 1 exceeded");
        assert_eq!(
            c.in_flight.load(Ordering::SeqCst),
            usize::from(ra) + usize::from(rb)
        );
    });
}
