package dynamo

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Hash func(data []byte) uint32

type ConsistentHashMap struct {
	hashFn   Hash
	replicas int
	keys     []int
	hashMap  map[int]string
	mu       sync.RWMutex
}

func NewConsistentHashMap(replicas int, fn Hash) *ConsistentHashMap {
	m := &ConsistentHashMap{
		replicas: replicas,
		hashFn:   fn,
		hashMap:  make(map[int]string),
	}

	if m.hashFn == nil {
		m.hashFn = crc32.ChecksumIEEE
	}

	return m
}

func (m *ConsistentHashMap) Add(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFn([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}

	sort.Ints(m.keys)
}

func (m *ConsistentHashMap) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hashFn([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}

func (m *ConsistentHashMap) GetN(key string, n int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return nil
	}

	hash := int(m.hashFn([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if idx == len(m.keys) {
		idx = 0
	}

	uniqueNodes := make(map[string]struct{})
	res := make([]string, 0, n)

	for len(uniqueNodes) < n {
		node := m.hashMap[m.keys[idx]]
		if _, ok := uniqueNodes[node]; !ok {
			uniqueNodes[node] = struct{}{}
			res = append(res, node)
		}
		idx = (idx + 1) % len(m.keys)
	}

	return res
}

func (m *ConsistentHashMap) Remove(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFn([]byte(strconv.Itoa(i) + key)))
			idx := sort.SearchInts(m.keys, hash)
			m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
			delete(m.hashMap, hash)
		}
	}
}
