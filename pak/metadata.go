package pak

// KV represents single key / value meta data entry
type KV struct {
	Key   string
	Value string
}

// Metadata is a list of key / value pairs
type Metadata struct {
	Meta []KV
}

// Reset resest Metadata. Useful for re-using (performance)
func (m *Metadata) Reset() {
	m.Meta = m.Meta[:0]
}

// Size returns number of metadata e ntries
func (m *Metadata) Size() int {
	return len(m.Meta)
}

// Get returns value for a given key
func (m *Metadata) Get(key string) (string, bool) {
	for _, kv := range m.Meta {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return "", false
}

// Set sets a value for a given key. Returns true if new value was added,
// false value was updated
func (m *Metadata) Set(k, v string) bool {
	for i, kv := range m.Meta {
		if kv.Key == k {
			m.Meta[i].Value = v
			return false
		}
	}
	kv := KV{
		Key:   k,
		Value: v,
	}
	m.Meta = append(m.Meta, kv)
	return true
}
