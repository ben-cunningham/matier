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

use super::shared::{Command, RecvEvent, StreamShared};
use super::sync::{Arc, Ordering};

fn stream(threshold: u32, credit: i64) -> (Arc<StreamShared>, mpsc::UnboundedReceiver<Command>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(StreamShared::new(tx, threshold, credit)), rx)
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
        let t = thread::spawn(move || s2.publish_head(Ok(parts())));
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
        let t = thread::spawn(move || s2.fail(Error::closed()));
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
            s2.push_recv(RecvEvent::Trailers(Box::default()));
            s2.push_recv(RecvEvent::End);
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
    loom::model(|| {
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

/// Cancel racing with the actor pushing data: queued bytes are accounted exactly once between the consumer's
/// drain and the actor's own drain.
#[test]
fn loom_cancel_vs_push() {
    loom::model(|| {
        let (s, rx) = stream(1 << 20, 0);
        let s2 = Arc::clone(&s);
        let pusher = thread::spawn(move || {
            for _ in 0..2 {
                s2.push_recv(RecvEvent::Data(Box::new(
                    matier_core::buf::copy_from_slice(&[0u8; 10]),
                )));
            }
        });
        let s3 = Arc::clone(&s);
        let canceller = thread::spawn(move || StreamShared::cancel(&s3));
        pusher.join().unwrap();
        canceller.join().unwrap();
        // Actor side: handle the Ready, then drain whatever the consumer's drain missed.
        let mut rx = rx;
        let mut accounted = 0u64;
        while let Ok(Command::Ready(sh)) = rx.try_recv() {
            let f = sh.begin_ready();
            assert!(f & super::shared::CANCELED != 0);
            accounted += u64::from(sh.take_consumed());
            while let Some(ev) = sh.pop_recv() {
                if let RecvEvent::Data(d) = ev {
                    accounted += d.len() as u64;
                }
            }
        }
        assert_eq!(accounted, 20);
        assert_eq!(s.flags() & super::shared::CANCELED, super::shared::CANCELED);
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
