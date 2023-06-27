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

func (set *T[Type]) RemoveIf(predicate func(*Type) bool) bool {
	value, found := set.Find(predicate)
	if found {
		set.Remove(value)
	}

	return found
}

func (set *T[Type]) Contains(value Type) bool {
	_, ok := set.members[value]
	return ok
}

func (set *T[Type]) Find(predicate func(*Type) bool) (Type, bool) {
	for member := range set.members {
		if predicate(&member) {
			return member, true
		}
	}

	var zeroValue Type
	return zeroValue, false
}

func (set *T[Type]) Retain(predicate func(*Type) bool) {
	for member := range set.members {
		if !predicate(&member) {
			delete(set.members, member)
		}
	}
}

func (set *T[Type]) Size() int {
	return len(set.members)
}
