package hashring

type Slot[T any] struct {
	value T
	hash  uint32
	index int
}

func NewSlot[T any](key string, value T) Slot[T] {
	return Slot[T]{
		value: value,
		hash:  Hash(key),
	}
}

func (s *Slot[T]) GetValue() T {
	return s.value
}

func (s *Slot[T]) Hash() uint32 {
	return s.hash
}
