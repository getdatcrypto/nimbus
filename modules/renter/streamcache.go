package renter

import (
	"container/heap"
	"reflect"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/errors"
)

// streamHeap is a priority queue and implements heap.Interface and holds chunkData
type streamHeap []*chunkData

// chunkData contatins the data and the timestamp for the unfinished
// download chunks
type chunkData struct {
	id         string
	data       []byte
	lastAccess time.Time
	index      int
	file       string
}

// chunkMap keeps the chunkData for a file in a map for quick look up and
// deletion
type chunkMap struct {
	chunks map[string]*chunkData
}

// streamCache contains a streamMap for quick look up and a streamHeap for
// quick removal of old chunks
type streamCache struct {
	streamMap  map[string]chunkMap // streamMap maps the file to its chunks
	streamHeap streamHeap          // streamHeap is a heap that contains all the chunks currently streaming
	cacheSize  uint64
	mu         sync.Mutex
}

// Heap implementation for streamCache
func (sh streamHeap) Len() int           { return len(sh) }
func (sh streamHeap) Less(i, j int) bool { return sh[i].lastAccess.Before(sh[j].lastAccess) }
func (sh streamHeap) Swap(i, j int) {
	sh[i], sh[j] = sh[j], sh[i]
	sh[i].index = i
	sh[j].index = j
}
func (sh *streamHeap) Push(x interface{}) {
	n := len(*sh)
	chunkData := x.(*chunkData)
	chunkData.index = n
	*sh = append(*sh, chunkData)
}
func (sh *streamHeap) Pop() interface{} {
	old := *sh
	n := len(old)
	chunkData := old[n-1]
	chunkData.index = -1 // for safety
	*sh = old[0 : n-1]
	return chunkData
}
func (sh *streamHeap) Remove(i int) interface{} {
	n := sh.Len() - 1
	if n != i {
		sh.Swap(i, n)
		if !sh.down(i, n) {
			sh.up(i)
		}
	}
	return sh.Pop()
}
func (sh *streamHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && sh.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !sh.Less(j, i) {
			break
		}
		sh.Swap(i, j)
		i = j
	}
	return i > i0
}
func (sh *streamHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !sh.Less(j, i) {
			break
		}
		sh.Swap(i, j)
		j = i
	}
}
func (sh *streamHeap) update(cd *chunkData, id string, data []byte, lastAccess time.Time) {
	cd.id = id
	cd.data = data
	cd.lastAccess = lastAccess
	heap.Fix(sh, cd.index)
}

// removeOldest removes the oldest chunk from the chunkMap and returns it
func (cm *chunkMap) removeOldest() *chunkData {
	oldest := &chunkData{
		lastAccess: time.Now(),
	}
	for _, chunk := range cm.chunks {
		if chunk.lastAccess.Before(oldest.lastAccess) {
			oldest = chunk
		}
	}
	delete(cm.chunks, oldest.id)
	return oldest
}

// Add adds the chunk to the cache if the download is a streaming
// endpoint download.
// TODO this won't be necessary anymore once we have partial downloads.
func (sc *streamCache) Add(file, cacheID string, data []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Check if file has been added to map and make sure chuck has not already
	// been added
	if _, ok := sc.streamMap[file]; !ok {
		sc.streamMap[file] = chunkMap{
			chunks: make(map[string]*chunkData),
		}
	}
	if _, ok := sc.streamMap[file].chunks[cacheID]; ok {
		return
	}

	// pruning cache to cacheSize - 1 to make room to add the new chunk
	sc.pruneCache(file, sc.cacheSize-1)

	// Add chunk to Map and Heap
	cd := &chunkData{
		id:         cacheID,
		data:       data,
		lastAccess: time.Now(),
		file:       file,
	}
	sc.streamMap[file].chunks[cd.id] = cd
	heap.Push(&sc.streamHeap, cd)
	sc.streamHeap.update(cd, cd.id, cd.data, cd.lastAccess)
}

// numChunks counts the number of chunks in streamMap
func (sc *streamCache) numChunks() int {
	chunks := 0
	for _, cm := range sc.streamMap {
		chunks += len(cm.chunks)
	}
	return chunks
}

// pruneCache prunes the cache until it is the length of size
func (sc *streamCache) pruneCache(file string, size uint64) {
	for len(sc.streamHeap) > int(size) {
		// Check for chunks associated with file
		if cm, ok := sc.streamMap[file]; ok && len(cm.chunks) > 0 {
			// Remove oldest chunk from Heap
			cd1 := cm.removeOldest()
			cd2 := heap.Remove(&sc.streamHeap, cd1.index)
			if !reflect.DeepEqual(cd1, cd2) {
				build.Critical("chunks removed not equal")
			}
			continue
		}

		// Remove oldest chunk from Heap
		cd1 := heap.Pop(&sc.streamHeap).(*chunkData)
		// Remove from correct map
		cm, ok := sc.streamMap[cd1.file]
		if !ok {
			build.Critical("Cache Data chunk not found in streamMap.")
		}
		// TODO: implement own delete method for removing item from array
		cd2 := cm.removeOldest()
		if !reflect.DeepEqual(cd1, cd2) {
			build.Critical("chunks removed not equal")
		}
	}

	// Sanity check to confirm the Map and Heap where both pruned
	if len(sc.streamHeap) != sc.numChunks() {
		build.Critical("streamHeap and streamMap are not the same length,", len(sc.streamHeap), "and", sc.numChunks())
	}
}

// Retrieve tries to retrieve the chunk from the renter's cache. If
// successful it will write the data to the destination and stop the download
// if it was the last missing chunk. The function returns true if the chunk was
// in the cache.
// Using the entire unfinishedDownloadChunk as the argument as there are seven different fields
// used from unfinishedDownloadChunk and it allows using udc.fail()
//
// TODO: in the future we might need cache invalidation. At the
// moment this doesn't worry us since our files are static.
func (sc *streamCache) Retrieve(udc *unfinishedDownloadChunk) bool {
	udc.mu.Lock()
	defer udc.mu.Unlock()
	sc.mu.Lock()
	defer sc.mu.Unlock()

	file := udc.download.staticSiaPath
	cd, cached := sc.streamMap[file].chunks[udc.staticCacheID]
	if !cached {
		return false
	}

	// chunk exists, updating lastAccess and reinserting into map, updating heap
	cd.lastAccess = time.Now()
	sc.streamMap[file].chunks[udc.staticCacheID] = cd
	sc.streamHeap.update(cd, cd.id, cd.data, cd.lastAccess)

	start := udc.staticFetchOffset
	end := start + udc.staticFetchLength
	_, err := udc.destination.WriteAt(cd.data[start:end], udc.staticWriteOffset)
	if err != nil {
		udc.fail(errors.AddContext(err, "failed to write cached chunk to destination"))
		return true
	}

	// Check if the download is complete now.
	udc.download.mu.Lock()
	defer udc.download.mu.Unlock()

	udc.download.chunksRemaining--
	if udc.download.chunksRemaining == 0 {
		udc.download.endTime = time.Now()
		close(udc.download.completeChan)
		udc.download.destination.Close()
		udc.download.destination = nil
	}
	return true
}

// SetStreamingCacheSize sets the cache size.  When calling, add check
// to make sure cacheSize is greater than zero.  Otherwise it will remain
// the default value set during the initialization of the streamCache.
// It will also prune the cache to ensure the cache is always
// less than or equal to whatever the cacheSize is set to
func (sc *streamCache) SetStreamingCacheSize(cacheSize uint64) error {
	if cacheSize == 0 {
		return errors.New("cache size cannot be zero")
	}

	sc.mu.Lock()
	sc.cacheSize = cacheSize
	sc.pruneCache("", sc.cacheSize)
	sc.mu.Unlock()
	return nil
}

// initStreamCache initializes the streaming cache of the renter.
func newStreamCache(cacheSize uint64) *streamCache {
	streamHeap := make(streamHeap, 0, cacheSize)
	heap.Init(&streamHeap)

	return &streamCache{
		streamMap:  make(map[string]chunkMap),
		streamHeap: streamHeap,
		cacheSize:  cacheSize,
	}
}
