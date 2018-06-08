# Blockchain Reactor

The Blockchain Reactor's high level responsibility is to enable peers who are
far behind the current state of the consensus to quickly catch up by downloading
many blocks in parallel, verifying their commits, and executing them against the
ABCI application.

Tendermint full nodes run the Blockchain Reactor as a service to provide blocks
to new nodes. New nodes run the Blockchain Reactor in "fast_sync" mode,
where they actively make requests for more blocks until they sync up.
Once caught up, "fast_sync" mode is disabled and the node switches to
using (and turns on) the Consensus Reactor.

## Message Types

```go
const (
    msgTypeBlockRequest    = byte(0x10)
    msgTypeBlockResponse   = byte(0x11)
    msgTypeNoBlockResponse = byte(0x12)
    msgTypeStatusResponse  = byte(0x20)
    msgTypeStatusRequest   = byte(0x21)
)
```

```go
type bcBlockRequestMessage struct {
    Height int64
}

type bcNoBlockResponseMessage struct {
    Height int64
}

type bcBlockResponseMessage struct {
    Block Block
}

type bcStatusRequestMessage struct {
    Height int64

type bcStatusResponseMessage struct {
    Height int64
}
```
NOTE: why we use prefix bc for those messages?

## Protocol

Blockchain reactor consists of several parallel tasks (routines):...


### Receive routine of Blockchain Reactor

It is executed upon message reception on the BlockchainChannel channel inside p2p receive routine. 

```go
upon receiving bcBlockRequestMessage m from peer p:
	block = load block for height m.Height from data store 
	// NOTE: loading block is blocking call and not cheap; p2p receive routine is blocked while this code is executed
	if block != nil then
		try to send BlockResponseMessage(block) on BlockchainChannel to p  
		// NOTE: we don't track what blocks we have already sent to a peer so faulty peer can aks us old the time for the same block
		return
	try to send bcNoBlockResponseMessage(m.Height)

upon receiving bcBlockResponseMessage m from peer p:
	// add block to a pool
	pool.mtx.Lock()
	// Note that p2p receive routine could be blocked if pool.mtx.Lock is taken at this point!
	requester = pool.requesters[m.Height]
	if requester == nil then
		error("peer sent us a block we didn't expect")
		return // Note that we ignore block in this case! Does it make sense as block might be valid!

	if requester.block == nil and requester.peerID == p then
		requester.block = m
		send msg gotBlock to a requestor task over channel
		pool.numPending -= 1  // atomic decrement
		peer = pool.peers[p]
		if peer != nil then
			peer.numPending--
			if peer.numPending == 0 then
				peer.timeout.Stop()
			else
				update recvMonitor for peer with m.size
				trigger peer timeout to expire after peerTimeout
    
	pool.mtx.Unlock()
		
		
upon receiving bcStatusRequestMessage m from peer p:
	try to send bcStatusResponseMessage(store.Height)

upon receiving bcStatusResponseMessage m from peer p:
	pool.mtx.Lock()
	// Note that p2p receive routine could be blocked if pool.mtx.Lock is taken at this point!	
	peer := pool.peers[peerID]
	if peer != nil then
		peer.height = height    
		// NOTE: there are no check if new height is bigger than the previous one. If messages arrived out of order we might actually reset height
	else
		peer = create new peer tracking data structure with peerID = p and height = m.Height
		pool.peers[p] = peer

	if m.Height > pool.maxPeerHeight then
		pool.maxPeerHeight = m.Height
    
	pool.mtx.Unlock()
		
		
onTimeout(peer):
	send error message to pool error channel
	peer.didTimeout = true

```


## Channels

Defines `maxMsgSize` for the maximum size of incoming messages,
`SendQueueCapacity` and `RecvBufferCapacity` for maximum sending and
receiving buffers respectively. These are supposed to prevent amplification
attacks by setting up the upper limit on how much data we can receive & send to
a peer.

Sending incorrectly encoded data will result in stopping the peer.
