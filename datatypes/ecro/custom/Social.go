package custom

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type SocialOpValue struct {
	From int //id of the user who is adding/removing
	To   int //id of the user who is being added/removed
}

type SocialState struct {
	Friends    [5]mapset.Set[any] //friends[id] = set of friends of id
	Requesters [5]mapset.Set[any] //requesters[id] = set of requests made to id
}

type Social struct {
	id string
}

func (s Social) accept(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	from, to := elem.(SocialOpValue).From, elem.(SocialOpValue).To

	if !state.Requesters[from].Contains(to) {
		return state
	}

	state.Friends[to].Add(from)
	state.Friends[from].Add(to)
	state.Requesters[to].Remove(from)
	state.Requesters[from].Remove(to)

	return state
}

func (s Social) breakup(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	from, to := elem.(SocialOpValue).From, elem.(SocialOpValue).To

	if !state.Friends[to].Contains(from) {
		return state
	}

	state.Friends[to].Remove(from)
	state.Friends[from].Remove(to)
	return state
}

func (s Social) request(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	from, to := elem.(SocialOpValue).From, elem.(SocialOpValue).To

	if state.Friends[to].Contains(from) || state.Requesters[to].Contains(from) {
		return state
	}

	state.Requesters[to].Add(from)
	return state
}

func (s Social) reject(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	from, to := elem.(SocialOpValue).From, elem.(SocialOpValue).To

	if !state.Requesters[to].Contains(from) {
		return state
	}

	state.Requesters[to].Remove(from)
	state.Requesters[from].Remove(to)
	return state
}

func (s Social) Apply(state any, operations []communication.Operation) any {
	st := state.(SocialState).copy()
	for _, op := range operations {
		switch op.Type {
		case "accept":
			state = s.accept(st, op)
		case "breakup":
			state = s.breakup(st, op)
		case "request":
			state = s.request(st, op)
		case "reject":
			state = s.reject(st, op)
		}
	}
	return state
}

func (a Social) Order(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == "breakup" && op2.Type == "accept" ||
		op1.Type == "reject" && op2.Type == "request" ||
		op1.Type == "request" && op2.Type == "accept" ||
		op1.Type == "reject" && op2.Type == "accept"
}

func (a Social) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type ||
		op1.Value.(SocialOpValue).From != op2.Value.(SocialOpValue).From &&
			op1.Value.(SocialOpValue).To == op2.Value.(SocialOpValue).To ||
		op1.Value.(SocialOpValue).From == op2.Value.(SocialOpValue).From &&
			op1.Value.(SocialOpValue).To != op2.Value.(SocialOpValue).To
}

// initialize counter replica
func NewSocialReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.NewEcroCRDT(id, SocialState{
		Friends:    [5]mapset.Set[any]{mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any]()},
		Requesters: [5]mapset.Set[any]{mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any]()},
	}, Social{id}, replica.Replica{})

	r := replica.NewReplica(id, c, channels, delay)

	c.SetReplica(r)

	return r
}

// copy of socialstate
func (s SocialState) copy() SocialState {
	var friends [5]mapset.Set[any]
	var requesters [5]mapset.Set[any]

	for i := 0; i < 5; i++ {
		friends[i] = s.Friends[i].Clone()
		requesters[i] = s.Requesters[i].Clone()
	}

	return SocialState{Friends: friends, Requesters: requesters}
}

// compares if two SocialState are equal for test reasons
func CompareSocialStates(s1 SocialState, s2 SocialState) bool {
	if len(s1.Friends) != len(s2.Friends) || len(s1.Requesters) != len(s2.Requesters) {
		return false
	}

	for i := 0; i < len(s1.Friends); i++ {
		if !s1.Friends[i].Equal(s2.Friends[i]) {
			return false
		}
	}

	for i := 0; i < len(s1.Requesters); i++ {
		if !s1.Requesters[i].Equal(s2.Requesters[i]) {
			return false
		}
	}
	return true
}
