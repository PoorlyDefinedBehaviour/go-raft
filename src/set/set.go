package set

type T[Type comparable] struct {
	members map[Type]struct{}
}

func New[Type comparable]() *T[Type] {
	return &T[Type]{
		members: make(map[Type]struct{}),
	}
}

func (set *T[Type]) Insert(value Type) {
	set.members[value] = struct{}{}
}

func (set *T[Type]) Remove(value Type) bool {
	removed := set.Contains(value)
	delete(set.members, value)
	return removed
}

func (set *T[Type]) Contains(value Type) bool {
	_, ok := set.members[value]
	return ok
}

func (set *T[Type]) Size() int {
	return len(set.members)
}
