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

### Pool data structure and peer data structure
TODO

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

### Requestor taks

Requestor is a task that is responsible for fetching a single block.   

```go
fetchBlock(height, pool):
    peerID = nil
    block = nil
    peer = pickAvailablePeer(height)
	peerId = peer.id

	enqueue BlockRequest(height, peerID) to pool.requestsCh
	while true do
	  upon receiving Quit message on pool channel or requestor channel do
	  return

	  upon receiving message on redoChannel do
	    mtx.Lock()
	    increase number of pending requests in pool
	    peerID = nil
	    block = nil
	    mtx.UnLock()

pickAvailablePeer(height):
	selectedPeer = nil

	while selectedPeer = nil do
	  pool.mtx.Lock()
		for each peer in pool.peers do
		  if !peer.didTimeout and peer.numPending < maxPendingRequestsPerPeer and peer.height >= height then
		    increase number of pending requests for peer
		     selectedPeer = peer
		     break for

		pool.mtx.Unlock()
		if selectedPeer = nil then
			sleep requestIntervalMS

	return selectedPeer
```

### Task for creating Requestors

This task is responsible for continuously creating Requestors.
```go
creteRequestors(pool, p):
    if pool.numPending >= maxPendingRequests or size(pool.requesters) >= maxTotalRequesters then
        sleep for requestIntervalMS
        pool.mtx.Lock()
                for each peer in pool.peers do
                    if !peer.didTimeout && peer.numPending > 0 && peer.curRate < minRecvRate then
                        send error on pool error channel
                        peer.didTimeout = true
    
                    if peer.didTimeout then
                        for each requester in pool.requesters do
                        if requester.getPeerID() == p then
                            requester.redo()
    
                        delete(pool.peers, peerID)
    
                pool.mtx.Unlock()
            else
                pool.mtx.Lock()
                nextHeight = pool.height + size(pool.requesters)
                requestor = create new requestor for height nextHeight
    
                pool.requesters[nextHeight] = requestor
                pool.numPending += 1 // atomic increment
                start requestor task
                pool.mtx.Unlock()
```
  

### Main blockchain controllor task 
```go
main:
	upon receiving request(Height, Peer) on requestsChannel:
		try to send bcBlockRequestMessage(request.Height) to request.Peer

	upon receiving error(peer) on errorsChannel:
		stop peer for error

	upon receiving message on statusUpdateTickerChannel:
		broadcast bcStatusRequestMessage(bcR.store.Height) // message sent in a separate routine

	upon receiving message on switchToConsensusTickerChannel:
		pool.mtx.Lock()
		// some conditions to determine if we're caught up
		receivedBlockOrTimedOut = pool.height > 0 || (time.Now() - pool.startTime) > 5 Second
		ourChainIsLongestAmongPeers = pool.maxPeerHeight == 0 || pool.height >= pool.maxPeerHeight
		haveSomePeers = size of pool.peers > 0
		pool.mtx.Unlock()

		if haveSomePeers && receivedBlockOrTimedOut && ourChainIsLongestAmongPeers then
			switch to consensus mode

	upon receiving message on trySyncTickerChannel:
		for i = 0; i < 10; i++ do
				firstBlock = get block for pool.height
				secondBlock = get block for pool.height + 1
				verify firstBlock using LastCommit from secondBlock
				if verification failed
					pool.mtx.Lock()
					peerID = pool.requesters[pool.height].peerID
					for each requester in pool.requesters do
						if requester.getPeerID() == peerID
							enqueue msg on redoChannel for requester

					delete(pool.peers, peerID)
					stop peer peerID for error
					pool.mtx.Unlock()
				else
					delete(pool.requesters, pool.height)
					pool.height++
					save firstBlock to store
					execute firstBlock
					blocksSynced++
```

 
                
## Channels

Defines `maxMsgSize` for the maximum size of incoming messages,
`SendQueueCapacity` and `RecvBufferCapacity` for maximum sending and
receiving buffers respectively. These are supposed to prevent amplification
attacks by setting up the upper limit on how much data we can receive & send to
a peer.

Sending incorrectly encoded data will result in stopping the peer.
