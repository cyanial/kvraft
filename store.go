package kvraft

import "sync"

type KVStore struct {
	mu    sync.Mutex
	store map[string]string
}

func MakeStore() *KVStore {
	st := &KVStore{}
	st.store = map[string]string{}
	return st
}

func (st *KVStore) Put(key, value string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.store[key] = value
}

func (st *KVStore) Append(key, value string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.store[key] += value
}

func (st *KVStore) Get(key string) (value string, has bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	value, has = st.store[key]
	return
}
