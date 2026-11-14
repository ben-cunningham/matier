# Go HTTP client internals (net/http + HTTP/2), read from source

**Sources fetched 2026-11-14** via raw.githubusercontent.com:

- `golang/go` master, HEAD `2eaf98346955ce01a7046efeef63f8a775df6ea4` (committed 2026-11-14 09:35 -0800). Tree listing for `src/net/http/internal/http2` was taken from go.googlesource.com (tree id `f2b280fb6bab`).
- `golang/net` master, HEAD `e44ac12c0d40f5de4834f3f6f73aeed26f066bca` (2026-11-11), used for comparison only.

Local copies are in `scratchpad/go-src/std/` (std files; HTTP/2 files have an `h2_` prefix) and `scratchpad/go-src/xnet/`. All `file:line` references below point at those files. Std HTTP/2 paths drop the prefix: `h2_transport.go` is `src/net/http/internal/http2/transport.go`.

> **Where HTTP/2 lives now.** `src/net/http/h2_bundle.go` is gone (404). HTTP/2 is a real std package at `src/net/http/internal/http2/` (transport.go 2908 lines, frame.go, flow.go, pipe.go, databuffer.go, writesched*.go including `writesched_priority_rfc9218.go`, server.go…). Under Go ≥1.27, `golang.org/x/net/http2` is now a thin wrapper around `net/http`: see `xnet/transport_wrap.go:5` `//go:build go1.27 && !http2legacy`. The old standalone x/net transport and its `clientConnPool` are only built with `!(go1.27 && !http2legacy)` (`xnet/transport.go:5`, `xnet/client_conn_pool.go`).
>
> **The connection pool was rewritten in 2026.** `src/net/http/transport_pool.go` (copyright 2026) is a single merged pool for H1, H2 and H3 connections. It replaces the old `idleConn` / `idleLRU` / `wantConn` / `connsPerHostWait` design and the HTTP/2 package's own `clientConnPool`. Several things the question assumed (`idleConn` map, `wantConn`) no longer exist on master. The legacy design is still described in §5 for contrast.

---

## 1. HTTP/2 client connection pool

### 1.1 Layering (new design)

- `net/http.Transport.roundTrip` (transport.go:580-737) calls `getConn` (1136), then `poolGetConn` (transport_pool.go:642), then `pconn.cc.RoundTrip(req)` (transport.go:679). For H2, `pconn.cc` is `http2ClientConn{*http2.ClientConn}` (http2client.go:156-178, 222). Each H2 TCP connection is now an individual pool member. The old design had one opaque "alt conn" per host that hid its own pool (transport_pool.go:135-141, `altConn` is now legacy only).
- Every connection implements `externalClientConn` (clientconn.go:84-93): `Reserve()`, `Release()`, `Available() int`, `InFlight() int`, `Err()`, plus `Proto()` and `CloseWithError` (96-112). The H2 implementation is at h2_transport.go:2776-2848.
- Connections report asynchronous state changes (request finished, GOAWAY, SETTINGS changed MAX_CONCURRENT_STREAMS, ping ack) through an `internalStateHook` (clientconn.go:60-77). On H2 this is `cc.maybeCallStateHook()` (h2_transport.go:2867). It calls `Transport.poolConnStateHook` (transport_pool.go:445), which takes the **Transport-global `t.pool.mu`** and recomputes the conn's state.

### 1.2 Pool data structures (transport_pool.go)

- `clientConnPool{mu, ents map[connectMethodKey]*poolEntry, lru intrusiveList[poolConn], idleTimer, dirty intrusiveList[poolEntry], closeIdle}` (34-58). There is one mutex for the whole Transport, and it "should never be held during blocking operations" (32-33). Lock order is pool → persistConn / cc.mu (30).
- `poolConn` (99-119) has `state` ∈ {Broken, Idle, Active, Exclusive, Draining, Alt} (63-90), `closing`, `exclusive`, and `idleAt`. It uses intrusive links for the per-entry `availableConns` list and the global LRU (intrusive_list.go), so list operations do not allocate.
- `poolEntry` (127-159) is per `connectMethodKey{proxy, scheme, addr, onlyH1}` (transport.go:1609). Fields: `availableConns` (MRU at the tail), `numConns`, `numIdle`, `sharedH2`, `waiters` and `dials` (intrusive lists of `poolWaiter`), and `coalesceDials`.
- `poolWaiter` (162-198) contains `donec chan struct{}` (buffered 1, line 683). Results are written into the waiter before signalling, so the waiter reads them without the lock (182-183).
- **Lock-then-reconcile pattern.** `withPoolLock(f)` (204-233) runs `f`, then reconciles every entry `f` marked dirty (`poolReconcileEntryLocked`, 239-294), then enforces global `MaxIdleConns` by closing LRU heads. Reconcile hands available conns to waiters in FIFO order and starts dials (`go t.poolDialForEntry`, 292) with `context.WithoutCancel(waiter.ctx)` (288), so a dial can outlive the request that caused it.

### 1.3 Picking a connection

- **Fast path.** If the entry has no waiters, `poolTryGetForEntryLocked` (716-769) walks `availableConns` **backwards (MRU first)**. It skips conns idle longer than `IdleConnTimeout` (wall clock, `Round(0)`, 731), calls `pconn.cc.Reserve()` (742), then `poolConnStateChangedLocked` (746).
- H2 `Reserve` (h2_transport.go:2812-2820) takes `cc.mu` and calls `canReserveLocked` (569-577), which requires `currentRequestCountLocked() < maxConcurrentStreams` and `!closed && !draining`. The count is `len(streams) + streamsReserved + pendingRequests + pendingResets` (582-584). Reservations are consumed by `roundTrip` (786-791: `streamsReserved--; pendingRequests++`).
- **Availability.** A conn is on `availableConns` iff its state is Idle or Active and `Available() > 0` (transport_pool.go:567). H2 `availableLocked` is `maxConcurrentStreams - currentRequestCount` (h2_transport.go:592-597). An H2 connection at its stream limit therefore drops off the list automatically, and new requests either wait or dial.
- **Dial dedup / coalescing.** `poolGetEntryLocked` sets `coalesceDials` for https whenever HTTP/2 or HTTP/3 is enabled, and for http only for prior-knowledge h2c (386-396). While coalescing, `poolCanDialLocked` (298-324) allows **at most one non-exclusive dial in flight** per entry. Other waiters queue and receive the result, or the dial error (795-801). After an H1 conn is dialled, coalescing is switched off for that entry (825-829).
- **MaxConnsPerHost.** `numConns + dials.size >= MaxConnsPerHost` means `atLimit` (299-302). When the entry is at the limit and has no idle conn, reconcile stops (268-271).
- **StrictMaxConcurrentRequests** (HTTP2Config, http.go:235-244). With it, if `sharedH2 > 0` no new dial is allowed (318-321), so there is exactly one shared H2 conn per host and requests queue in the pool. Inside the H2 conn, `strictMaxConcurrentStreams` makes `idleStateLocked` always report "can take" (525-530) and `awaitOpenSlotForStreamLocked` blocks on `cc.cond.Wait()` (1344-1376). Without strict mode, a full conn returns `errClientConnUnusable` or `errClientConnZeroStreams` (1365-1372). Those are `internal.RetryableError` values (349-352) and lead to a retry/redial in `roundTrip`.
- **Initial MAX_CONCURRENT_STREAMS** is assumed to be 100 until the server's SETTINGS arrive (51, 384). If the SETTINGS frame omits the value it becomes 1000 (55, 2563-2571).
- **Exclusive requests** (`Connection: close` / DisableKeepAlives) require an Idle conn, or dial their own conn, ignoring coalescing (657, 303-307, 737-745).

### 1.4 GOAWAY

`processGoAway` (h2_transport.go:2447-2487):
- stores the frame and calls `setDrainingLocked()` (475-485), which also zeroes `streamsReserved` and `pendingResets` and broadcasts `cond`;
- keeps the first non-NO error code;
- if no streams are open, returns `errStopReadLoop`, which closes the conn;
- otherwise aborts every stream with ID > `LastStreamID` using `goAwayErrorLocked` (1329-1340). That error is the retryable `errClientConnGotGoAway`, except on stream 1 with a non-NO code, where retrying is pointless.

The pool then sees `Err()!=nil` (2800-2810) and moves the conn to `Draining` (transport_pool.go:521-522). Draining conns no longer count against MaxConnsPerHost (81-86, 534) and are closed when idle (`checkIdleLocked`, 1317-1327). On EOF after a GOAWAY the readLoop reports `GoAwayError{LastStreamID, ErrCode, DebugData}` (1766-1771). Stream ID exhaustion (`nextStreamID >= MaxInt32`) also sets draining (1690-1692).

### 1.5 Health / ping

- `readIdleTimeout` (`HTTP2Config.SendPingTimeout`) arms `time.AfterFunc(readIdleTimeout, cc.healthCheck)` and **resets it after every frame read** (1830-1839). `healthCheck` (459-473) calls `Ping` with `PingTimeout` (default 15 s, h2_config.go:64). On failure it calls `closeForLostPing`. Ping is disabled by default because `SendPingTimeout` has no default.
- `Ping` (2634-2676) uses a random 8-byte payload in `cc.pings` map and writes under `wmu` **in a new goroutine** so the caller can time out.
- **Unresponsive-connection detection through RST_STREAM (newer).** When cancelling a stream that has seen no frame since it was sent (`readBeforeStreamID`, 160-165, 2429), the client sends RST_STREAM **plus a PING** (1251-1281, 2719-2733). It keeps counting the stream against the concurrency limit (`pendingResets`) until any PING ack arrives (2689-2694). This stops the client from piling requests onto a dead conn. `rstStreamPingsBlocked` is a workaround for gRPC's ping-rate policing (142-150).
- Write stalls: `WriteByteTimeout` → `stickyErrWriter` → `writeWithByteTimeout` (321-334, h2_http2.go:328).

### 1.6 Retirement

A connection is retired when any of these happens:
- the read loop ends (any read error; `cleanup` aborts all streams not already peer-closed, 1756-1795);
- GOAWAY is received and the conn drains;
- a `ConnectionError` occurs (GOAWAY is sent, then close, with a 250 ms cap, 1717-1732 and 697-722);
- the ping is lost;
- `Connection: close` (`setDrainingLocked`, 1019-1022);
- RST_STREAM(PROTOCOL_ERROR) is received, which drains the conn (2618-2623);
- the pool decides to close it (`poolConnStateChangedLocked`, transport_pool.go:481-511). The reasons there are: broken, exclusive and finished, keep-alives disabled, CloseIdleConnections, `numIdle > MaxIdleConnsPerHost`, or `Available()==0` while idle (a server sent MAX_CONCURRENT_STREAMS=0 and the conn is treated like a GOAWAY), plus the idle timer on the LRU (592-630).

`closeConn` sets a 250 ms `forceCloseConn`, because `tls.Conn.Close` can hang (599-618).

---

## 2. Write path (HTTP/2)

**There is no writer goroutine or task. Every goroutine that needs to emit a frame takes `cc.wmu`, encodes into the shared `Framer`/`bufio.Writer`, and calls `bw.Flush()` itself.** The client uses no write scheduler. `writesched*` is server-only, and the server default is now the RFC 9218 scheduler: server.go:312-317.

### 2.1 Goroutines per request

- `ClientConn.roundTrip` (766-885) spawns `go cs.doRequest` (793), a "writeRequest goroutine". The caller goroutine waits in a `select` on `respHeaderRecv` / `abort` / ctx (855-884).
- `writeRequest` (975-1136) waits for the peer's END_STREAM, cancellation or the header timeout. **New in 2026: detach.** Once the request is fully sent, the goroutine detaches (`detach`, 920-964; call at 1103-1105) and exits. Cleanup then runs from whichever event ends the stream (`endStream` 2392-2395, `abortStreamLocked` 285-288, `context.AfterFunc`, `time.AfterFunc` for ResponseHeaderTimeout). The point is to stop pinning one goroutine stack per long-poll stream.
- The `clientStream` struct is **embedded in `ClientRequest`** to save an allocation (h2_api.go:54-55; assigned at h2_transport.go:768). Even so, each request allocates 4-5 channels (776-779, plus `on100`).

### 2.2 Locks (h2_transport.go:114-187)

| Lock | Type | Guards | Held across |
|---|---|---|---|
| `reqHeaderMu` | `chan struct{}` cap 1 (semaphore; 167-170, 393) | ordering of stream-ID allocation and HEADERS emission. Stream IDs must hit the wire in increasing order. | From `awaitOpenSlotForStreamLocked` + `addStreamLocked` (1004-1023) **through HPACK encoding + `writeHeaders` + `bw.Flush()`** (1045-1046). The **flush syscall is inside it**, so stream opens on one conn are fully serialized, including the TLS write. |
| `cc.mu` | `sync.Mutex` + `cond *sync.Cond` (114-115) | streams map, in/out flow control (conn and per-stream: `cs.flow`, `cs.inflow` are "guarded by cc.mu", 236-237), settings, goAway, counters, pings | Short critical sections, with two exceptions: (a) `processData` holds it while **copying the DATA payload into the stream's pipe** (2325-2353), and (b) `cond.Wait` for flow control or a stream slot. |
| `cc.wmu` | `sync.Mutex` (178-186) | `bw` (bufio.Writer), `fr` (Framer), `werr`, `hbuf`, `henc` (HPACK encoder) | Frame encode **plus `bw.Flush()`, a socket write that can block on TCP backpressure**. Order: wmu before mu (179-180). `processSettings` holds both (2493-2510). |
| `pipe.mu` | per stream (pipe.go:16-25) | response buffer | copy in / copy out |
| `t.pool.mu` | Transport-global (transport_pool.go:35) | pool | taken on getConn and **on every state hook, i.e. every H2 request completion** (cleanupWriteRequest → `maybeCallStateHook`, 1314) |

### 2.3 Who writes what, and flushes

- **HEADERS.** `encodeAndWriteHeaders` (1138-1176) runs under `wmu`. `hbuf.Reset()`, then HPACK-encode every field via `cc.henc.WriteField` into the shared `cc.hbuf` (1666-1671). The **HPACK encoder is per-conn, owned under wmu**. `writeHeaders` (1379-1402) splits the block into HEADERS + CONTINUATION at the peer's `maxFrameSize` and does **one `bw.Flush()` per header block**.
- **DATA.** `writeRequestBody` (1458-1588) loops: `body.Read(buf)`, then `awaitFlowControl` (cc.mu, `cond.Wait`, 1594-1633), then `wmu.Lock(); fr.WriteData(); bw.Flush(); wmu.Unlock()` (1528-1542). There is **one flush, and so one write syscall and TLS record, per DATA frame**. The source comment at 1534-1539 says: *"this flush is for latency, not bandwidth… Nagel-like timers? … For now, always."*
- **WINDOW_UPDATE** is written by the **user goroutine reading the response body** (`transportResponseBody.Read`, 2209-2219) and by the readLoop (padding refunds and unknown streams, 2282-2296, 2355-2365). Both take `wmu` and flush. If a request-body writer is blocked in `Flush()` on a full send buffer, **the readLoop blocks on `wmu` and stops reading**. Body `Close` has the same problem; there is a TODO at 2239-2240: *"Acquiring this mutex can block indefinitely."*
- **SETTINGS ack and PING ack** are written inline by the readLoop under `wmu` and flushed (2499-2502, 2697-2703). **RST_STREAM** (+PING) is written by the cleanup goroutine (2719-2733). **GOAWAY** is written from a helper goroutine with a 250 ms abandon timeout (697-722, 1717-1732).
- **Batching is only incidental.** Frames from different goroutines coalesce only if one goroutine writes into `bw` while another is waiting on `wmu`, and every holder flushes before releasing. Nothing gathers frames across streams before a flush.

### 2.4 Buffers and frame sizes

- `cc.bw = bufio.NewWriter(stickyErrWriter{…})` uses the default **4 KiB** buffer. The source has a TODO: "adjust this writer size to account for frame size + MTU + crypto/tls record padding" (407-413). `cc.br = bufio.NewReader(c)` is also 4 KiB (414).
- Framer write: `startWrite` appends the 9-byte header and the payload is **copied into `f.wbuf`** (frame.go:395-415, 858-889). `endWrite` patches the length and does a single `w.Write(wbuf)` (417-438). Buffers over 1 KiB go back to the global `writeBufPool` after each frame so idle conns don't pin them (383-393, 444-458). That is a newer change.
- Outbound DATA frame size is `min(peer SETTINGS_MAX_FRAME_SIZE (default 16 KiB, 382), flow-control available, bytes read)` (1620-1628).
- Our advertised `SETTINGS_MAX_FRAME_SIZE` (receive side) is `MaxReadFrameSize`, default **1 MiB** (h2_http2.go:84, h2_config.go:63, h2_transport.go:437).
- HPACK: `MaxEncoderHeaderTableSize` and `MaxDecoderHeaderTableSize` default to 4096 (h2_config.go:51-52). The encoder follows the peer's SETTINGS_HEADER_TABLE_SIZE (2539-2541).

**Request body copy chain:** user `Read` → pooled scratch buffer (copy 1) → `wbuf` (copy 2) → bufio. The bufio step bypasses the buffer when the frame is larger than the free space and the buffer is empty; otherwise it copies (copy 3). Then tls.Conn copies and encrypts.

---

## 3. Read path (HTTP/2)

- **One `readLoop` goroutine per conn** (455, 1713-1733, `run` 1827-1893). `fr.ReadFrame()`, then a type switch, then `process*`. Every frame looks the stream up via `streamByID`, which **takes `cc.mu`** (2417-2435). That call also updates `readBeforeStreamID` and clears `rstStreamPingsBlocked`.
- **Frame read buffer.** `getReadBuf` reuses one per-Framer buffer. Large buffers (>1 KiB) come from global `readBufPool` and are **returned to the pool before blocking on the next frame header**, but only if bufio has nothing buffered (frame.go:527-590; `releaseReadBuf` 574, called at 706). DATA `f.Data()` aliases this buffer, so the payload must be copied before the next read.
- **Delivering DATA** (`processData`, 2263-2377):
  1. Under `cc.mu`, `takeInflows(&cc.inflow, &cs.inflow, len)` charges the conn and stream windows together. A violation is a connection FLOW_CONTROL_ERROR (2324-2329).
  2. Still under `cc.mu`, `cs.bufPipe.Write(data)` copies into the stream's `dataBuffer` (2340). `dataBuffer` is a list of chunks from 5 `sync.Pool` size classes (1/2/4/8/16 KiB), sized using `expected = Content-Length` (databuffer.go:23-61, 117-146; set at h2_transport.go:2115).
  3. Padding is refunded immediately, and any WINDOW_UPDATE is written under wmu (2330-2365).
  4. The `pipe` (pipe.go) is a mutex + `sync.Cond`. `Write` signals the reader (87-104).
- **Consumer.** `transportResponseBody.Read` (2171-2221) does `pipe.Read`, which copies chunk → user `p` (copy 3 after bufio→readBuf→chunk). Then it takes `cc.mu`, runs `inflow.add(n)` for both conn and stream, and if either returns non-zero it takes `wmu`, writes WINDOW_UPDATE(s) and **flushes**.
- **Window update policy** (flow.go:36-56). Credit accumulates in `unsent` and is sent when `unsent >= 4 KiB` (`inflowMinRefresh`, 11) **or** `unsent >= avail`, i.e. the update would at least double the peer's remaining window. With a 4 MiB stream window this means roughly every 4 KiB consumed, which is many small frames and many flushes. They are written by the reader's goroutine.
- **Initial windows (client).** The stream window is advertised in SETTINGS_INITIAL_WINDOW_SIZE as **4 MiB** (`transportDefaultStreamFlow`, 44; h2_config.go:61). The connection window gets a **WINDOW_UPDATE of 1 GiB** right after the preface (`transportDefaultConnFlow = 1<<30`, 39; 447-448). The server-side defaults are 1 MiB / 1 MiB (h2_config.go:54-59).
- **No BDP estimation.** There is no dynamic window sizing anywhere in the client: static large windows are the whole strategy. Per-stream memory is bounded by the 4 MiB window. Because of the 1 GiB conn window, a slow consumer on one stream can't starve the others; the conn window effectively never binds.
- **Outbound flow control** (flow.go:80-175). `connOutflow{initial, n}` is shared. Each stream's `outflow.delta` is relative to `initial`, so a SETTINGS_INITIAL_WINDOW_SIZE change is O(1), with no walk over streams (2534-2538). `processWindowUpdate` (2579-2608) runs under `cc.mu` and calls `cond.Broadcast()`, which **wakes every goroutine waiting on any stream's flow control and every goroutine waiting for a stream slot** (thundering herd; `forgetStreamIDLocked` 1695-1704 does the same).
- **Headers.** `MetaHeadersFrame` merges HEADERS+CONTINUATION and HPACK-decodes with a per-conn decoder inside `ReadFrame` (h2_transport.go:421; frame.go:1842+). `handleResponse` builds the response and `close(cs.respHeaderRecv)` wakes `roundTrip` (1944-1945).

---

## 4. Request body sending (both protocols)

**HTTP/2**
- The scratch buffer size is `max(1, min(peer maxFrameSize, ContentLength+1, 512 KiB))` (`frameScratchBufferLen`, 1420-1434). The +1 detects bodies longer than declared early.
- Buffers come from **7 size-class `sync.Pool`s**: ≤16K, 32K, 64K, 128K, 256K, 512K, and ∞ (`bufPools`, `bufPoolIndex`, 1436-1456, 1471-1481). This stops a long-running small-frame stream from pinning a big buffer.
- **No zero-copy.** The body is always `Read` into the scratch buffer and then copied into the frame buffer.
- **Content-Length.** `actualContentLength` (752-760) treats 0 with a non-nil body as unknown (-1). With a known length, after reading exactly CL bytes the code does a 1-byte probe read so it can set END_STREAM on the last DATA frame rather than sending an empty trailing DATA frame (1486-1505). Over-long bodies return `errReqBodyTooLong` (1501-1503). Unknown length ends with an empty DATA+END_STREAM or trailers (1577-1586).
- **100-continue.** If `ExpectContinueTimeout != 0` and the request has `Expect: 100-continue`, writeRequest waits on `cs.on100` / timer / abort after sending HEADERS (1032-1039, 1055-1075). The readLoop signals `on100` from `handleResponse` (1xx handling ~2010-2050).
- **Early response.** On a response status >299, an in-progress body upload is aborted (`abortRequestBodyWrite`, 806-819). 1xx/2xx keep streaming (full duplex).
- **Body close** runs in a separate goroutine so a slow `Close` can't hold `cc.mu` (309-319, 832-853).

**HTTP/1**
- `transferWriter.writeBody` (transfer.go:346-418). `doBodyCopy` uses a pooled **32 KiB** buffer (`copyBufPool`, server.go:958-971) with `io.CopyBuffer` (transfer.go:421-430).
- Since `bufio.Writer` implements `ReaderFrom` and `persistConnWriter` implements `ReadFrom` → `io.Copy(conn, r)` (transport.go:1538-1542), a `*os.File` body over plain TCP can use **sendfile/splice**. `unwrapBody` strips the NopCloser / readTrackingBody wrappers to make that possible (transfer.go:437-448).
- Chunked bodies use `FlushAfterChunkWriter` (flush per chunk, 365-367).
- `probeRequestBody` (216-256) reads 1 byte with a 200 ms timer to decide whether a body-less-method request with unknown length is really empty.
- **100-continue.** `waitForContinue` (transport.go:2126-2143) blocks the writeLoop on `continueCh` (fed by readLoop), the timer, or close.

---

## 5. HTTP/1 Transport

- **Current master pool** is the same `transport_pool.go` as in §1. An H1 `persistConn` has concurrency 1. `availch chan struct{}` (cap 1) is the semaphore. `reserved` and `inFlight` are under `pc.mu` (transport.go:1636, 1649-1656, 1509-1510). Idle connections live on `poolEntry.availableConns` (MRU used first) and on the global LRU (`MaxIdleConns`, default 100 in DefaultTransport, 47-58).
- `MaxIdleConnsPerHost` defaults to **2** (62, 1050-1055) and is enforced when a conn becomes idle (transport_pool.go:496-498). `IdleConnTimeout` is enforced by one pool-wide `time.Timer` keyed to the LRU head (592-630) plus a check on checkout (731-736). `MaxConnsPerHost` waiters are the `ent.waiters` FIFO (154, 248-293).
- **Legacy design (pre-2026, for contrast).** `Transport.idleConn map[connectMethodKey][]*persistConn` + `idleLRU` + `idleConnWait map[key]wantConnQueue` + `connsPerHostWait`. A `wantConn` raced a dial against an idle conn being returned (`tryDeliver`), and a dial result that lost the race was parked as idle. The new pool keeps those semantics (a dial outliving its waiter is pooled for others, transport_pool.go:844-849) but uses intrusive lists, an explicit state machine and the dirty-reconcile loop.
- **Per-conn goroutines.** `dialConn` starts `go pconn.readLoop()` and `go pconn.writeLoop()` (transport.go:1514-1515). `br` and `bw` are 4 KiB by default (`ReadBufferSize` / `WriteBufferSize`, 310-322; 1511-1512).
- **Handing a request to a conn** (`persistConn.roundTrip`, 2359-2537):
  1. Acquire `availch` (2362-2373).
  2. Send `writeRequest{req, writeErrCh, continueCh}` on `pc.writech` (cap 1) to the writeLoop (2431).
  3. Send `requestAndChan{treq, resc (unbuffered), addedGzip, continueCh, callerGone}` on `pc.reqch` (cap 1) to the readLoop (2433-2440).
  4. `select` on writeErr, resc, closech, the response-header timer, and ctx (2462-2536).
  
  So **three goroutines** cooperate per in-flight H1 request: caller, readLoop and writeLoop.
- **writeLoop** (2194-2230) runs `req.write(pc.bw)`, then **one Flush per request**. If nothing was written it wraps the error in `nothingWrittenError` (2216-2218), which allows a retry. The error goes to both `writeErrCh` (for the readLoop's reuse decision) and the caller.
- **readLoop** (1808-2015):
  - `br.Peek(1)`. With no request expected, it handles a server-initiated close or 408 (`readLoopPeekFailLocked`).
  - `<-pc.reqch`, then `readResponse`.
  - The body is wrapped in `bodyEOFSignal`, and the loop waits on `waitForBodyRead` until the body hits EOF or is closed.
  - `tryPutIdleConn` → `roundTripDone` (1059-1098) releases `availch` and calls the pool state hook, which also takes the global pool lock.
  - `eofc` makes the body reader's EOF block until the conn is back in the pool, so an immediate follow-up request can reuse it (1825-1832, 1896-1935).
  - Early close drains up to 256 KiB / 50 ms to keep the conn (1770-1799, 1992-2001).
  - `wroteRequest` waits up to 50 ms for the write result before reuse (2238-2268).
- **Retry.** `roundTrip` retries up to 6 times (`shouldRetryRequest`, 811+). The body is rewound via `readTrackingBody`/`GetBody` (754-806). H2 retries back off exponentially with 10% jitter after the first retry (723-734).

---

## 6. Notable recent changes (2024-2026) observed in source

1. **HTTP/2 moved into std** as `net/http/internal/http2` (h2_bundle.go deleted). x/net/http2 is a wrapper under go1.27 (`xnet/transport_wrap.go:5`; legacy build tag `http2legacy`).
2. **Unified pool** `transport_pool.go` (2026). Individual H2 conns are now visible to `MaxConnsPerHost` (the old alt-conn bug is described at 135-141). It adds dial coalescing, `Draining` state accounting, and a single global pool mutex with dirty-entry reconciliation.
3. **Public `net/http.ClientConn`** (`Transport.NewClientConn(ctx, scheme, addr)`, clientconn.go:114-135) with `Reserve/Release/Available/InFlight` and state hooks. A ClientConn uses strict MAX_CONCURRENT_STREAMS blocking (h2_transport.go:366-370).
4. **`Transport.Protocols`** (HTTP1 / HTTP2 / UnencryptedHTTP2 / internal http3 bit; http.go:27-87; transport.go:436-469) and **`HTTP2Config`** (http.go:227+). This enables prior-knowledge **unencrypted HTTP/2** (transport.go:1438-1445).
5. **HTTP/3 plumbing** in std: `http3.go` (2026), `h3Transport.DialClientConn` (transport.go:1229-1246). Protocols must be HTTP3-only for now.
6. **RFC 9218 priorities.** The server default write scheduler is now `newPriorityWriteSchedulerRFC9218()`, with round-robin when `Server.DisableClientPriority` is set (server.go:312-317). The RFC 7540 tree scheduler is legacy (x/net only). The client sends no priorities and has no scheduler.
7. **Writer-goroutine detach** for long-lived H2 streams (h2_transport.go:905-964, 1089-1105).
8. **RST_STREAM+PING dead-conn detection** and the gRPC ping workaround (142-165, 1251-1281, 2689-2694).
9. **Framer buffer pooling**: read and write buffers are released while idle (frame.go:296-322, 383-458, 527-590).
10. **CONTINUATION flood (CVE-2023-45288)**: the frame reader stops decoding and closes the conn when a fragment exceeds 2× the remaining header budget, or when any CONTINUATION follows an invalid header (frame.go:1920-1947). **Rapid reset (CVE-2023-44487)** on the server: handler goroutines are capped at `advMaxStreams`, and more than `4*advMaxStreams` queued unstarted handlers triggers ENHANCE_YOUR_CALM (server.go:2539-2585). Queued control frames are capped at 10000 (server.go:55-58, 1037).
11. Retryable errors now carry `Backoff` hints (`internal.RetryableError`, h2_transport.go:349-352).

---

## 7. Lessons for a Rust design (opinionated)

Go's design is "every goroutine writes to the socket under a mutex and flushes". That is acceptable in Go because a goroutine blocked on `wmu` or `cond.Wait` parks cheaply and the scheduler moves on. In tokio a blocking `std::sync::Mutex` or `Condvar` stalls the whole worker thread and every task on it. A `tokio::sync::Mutex` held across `write().await` turns into the same HOL convoy that h2#531 describes. **Do not port Go's lock structure. Port its policies.**

**Copy from Go:**
1. **Single-reader-task-per-connection** read loop with an owned frame read buffer that is released while idle (frame.go:527-590). In Rust, keep a `BytesMut` read buffer and hand DATA payloads to streams as `Bytes::split_to().freeze()` slices. That is **zero-copy**, where Go copies readBuf→dataBuffer→user. Return a large buffer to a pool only when the connection goes idle.
2. **Big static windows instead of BDP.** Use a large conn window (Go: 1 GiB), a stream window of a few MiB (Go: 4 MiB, h2_transport.go:39-44), and a 1 MiB max read frame. This is simple and almost never the bottleneck. Make it configurable, and add BDP auto-tuning only if benchmarks show a need. Keep Go's `inflow` hysteresis (send on ≥4 KiB unsent or doubling, flow.go:36-56), but **raise the threshold** (e.g. window/4 or ≥64 KiB) to cut WINDOW_UPDATE frame and flush counts.
3. **Delta-encoded stream send windows** relative to a shared `initial` (flow.go:111-153), so SETTINGS_INITIAL_WINDOW_SIZE changes are O(1).
4. **Pool semantics from transport_pool.go**: a conn-level `Reserve/Release/Available/InFlight` contract; MRU selection; `available ⇔ Available()>0`; dial coalescing (one in-flight dial per H2-capable origin, errors fan out to coalesced waiters); dials detached from the requesting future's cancellation (`WithoutCancel`, 288) with surplus dials pooled; Draining state not counted toward per-host limits; an LRU idle timer keyed to the head; intrusive lists so there are no allocations on checkout.
5. **Dead-conn heuristics**: RST_STREAM+PING with `pendingResets` counted against concurrency (h2_transport.go:1251-1281); a read-idle PING health check; a write-byte timeout; a forced TCP close if a TLS close hangs (599-618). Also copy GOAWAY replay semantics (abort only IDs > LastStreamID with a retryable error; never retry stream 1 on a non-NO code; 1329-1340).
6. **Request-body polish**: the CL+1 probe so END_STREAM rides on the last DATA frame (1486-1505); abort upload on a >299 response (806-819); a size-classed body buffer pool (1436-1456). In Rust, accept `Bytes`-yielding bodies so no scratch buffer is needed at all.
7. **The CONTINUATION budget check** (frame.go:1920-1947) and a header-list size limit, enforced before HPACK decoding.

**Avoid (Go's known costs):**
1. **Flush per frame.** Go performs one `bw.Flush()` (a syscall plus a TLS record) per DATA frame, per HEADERS block, per WINDOW_UPDATE and per PING ack (1400, 1540, 2218, 2363, 2703). Go's own TODO admits this (1534-1539). Instead, run **one writer task per connection** that owns the `TcpStream`/TLS stream, the HPACK encoder and the output buffer. Producers enqueue frames or "stream has data" notifications. The writer **drains everything ready, encodes into one buffer (or `writev` of `Bytes` chunks), and flushes once per wake-up**. This gives natural batching under load and immediate sends when idle, like Nagle without the timer.
2. **The `wmu` convoy and readLoop-blocks-on-write.** In Go the readLoop, body readers and body writers all contend on `wmu`, and a slow socket stalls the read side (2239 TODO). In the actor design, **the read task never touches the socket write half**. It posts WINDOW_UPDATE, SETTINGS-ack and PING-ack intents to the writer through a lock-free queue or the shared state below.
3. **`reqHeaderMu` held across a syscall** (1004-1046). Stream-ID ordering is guaranteed by having the **writer task assign stream IDs when it encodes the HEADERS frame**. Callers never serialize on the network.
4. **One fat `cc.mu` covering streams, flow control and settings, with `Cond.Broadcast` herds** (2606, 1703). The hyperium/h2 problem is exactly this: one `Mutex<Inner>` touched by every stream on every poll. Instead:
   - put per-stream state in its own small struct (`Arc<StreamShared>` with atomics for the send window, or a per-stream `Mutex` that is never held across `.await`), with a slab or sharded map indexed by stream ID;
   - keep the conn-level send window as an `AtomicI64` / CAS "take up to n";
   - wake **only the affected stream's waker** on WINDOW_UPDATE, plus a FIFO of streams blocked on the conn window, instead of broadcasting;
   - make connection-wide mutation (settings, goaway, stream table insert/remove) single-writer, owned by the connection task.
5. **Copying DATA under the connection lock** (processData 2325-2353). Use `Bytes` handoff through a per-stream channel or queue. The only shared step is an atomic `fetch_sub` on the recv windows. Release credit when the user **consumes** a chunk, as Go does, so backpressure stays correct.
6. **Goroutine/task per request for writing** (793) and 4-5 channel allocations per request (776-779). In Rust the request future itself pushes headers and body chunks to the conn writer. No spawned task per request is needed (Go needed the detach hack, 905-964, to undo this). Use one `oneshot` for response headers and a per-stream body queue. Consider a slab-allocated `StreamShared` to make per-request allocation O(1).
7. **A Transport-global pool mutex on every request completion** (`poolConnStateHook`, transport_pool.go:445, called from `maybeCallStateHook` on each H2 stream end, h2_transport.go:1314). At high RPS to one origin this serializes all cores. In Rust:
   - shard the pool per origin (`DashMap`/sharded `RwLock` → per-origin `Mutex` that is never held across `.await`);
   - keep H2 "available slots" as an atomic on the conn, so checkout is a CAS on the MRU conn without touching the pool lock;
   - only touch the pool on real state transitions (Idle↔Active, full↔not full, broken), not on every stream end.
8. **4 KiB bufio buffers** (h2_transport.go:407-414; transport.go:310-322). Use a 16-64 KiB write coalescing buffer sized to roughly one TLS record (16 KiB) plus frame headers, and read with a larger `BytesMut` (≥32 KiB, grown to max frame size on demand).
9. **std `Mutex`/`Condvar` in async paths.** Never block a tokio worker. Any lock in the request path must be (a) uncontended in the common case, (b) never held across `.await`, and (c) short and O(1) (`parking_lot` is fine under those rules). Waiting goes through wakers, not condvars. For multi-core throughput, prefer **N connections per origin spread across workers** over one H2 conn hammered by all cores: Go's non-strict mode already opens extra conns when MAX_CONCURRENT_STREAMS is hit. Consider a configurable "target streams per conn" below the server max so load spreads across several connections, each with its own writer task.

**Mapping summary:** Go goroutine-per-conn `readLoop` → tokio read task (or a combined conn task using `select!` over read half, command queue and timers). Go `wmu`+flush → single writer task owning the write half, with batched flush. Go `cc.mu`+`cond` → per-stream atomics/wakers plus single-writer conn state. Go `transport_pool.go` state machine → port nearly verbatim, sharded per origin, with atomic slot counts.
