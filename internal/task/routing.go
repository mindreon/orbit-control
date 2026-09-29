package task

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring routes a task to one control replica. Virtual nodes keep ownership
// stable when replicas join or leave. The ring is independent from HTTP and
// can be used by both the public handler and the internal forwarder.
type Ring struct {
	mu       sync.RWMutex
	points   []uint32
	owners   map[uint32]string
	replicas int
}

func NewRing(replicas int) *Ring {
	if replicas < 1 {
		replicas = 64
	}
	return &Ring{owners: map[uint32]string{}, replicas: replicas}
}

func (r *Ring) SetMembers(members []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.points = r.points[:0]
	r.owners = map[uint32]string{}
	for _, member := range members {
		if member == "" {
			continue
		}
		for replica := 0; replica < r.replicas; replica++ {
			point := crc32.ChecksumIEEE([]byte(member + "#" + strconv.Itoa(replica)))
			r.points = append(r.points, point)
			r.owners[point] = member
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
}

func (r *Ring) Owner(taskID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", false
	}
	hash := crc32.ChecksumIEEE([]byte(taskID))
	index := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= hash })
	if index == len(r.points) {
		index = 0
	}
	return r.owners[r.points[index]], true
}
