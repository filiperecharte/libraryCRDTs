package custom

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type OperationValue struct {
	id1 int //id of the user who is adding/removing
	id2 int //id of the user who is being added/removed
}

type SocialState struct {
	friends  []mapset.Set[any] //friends[id] = set of friends of id
	requests []mapset.Set[any] //requests[id] = set of requests of id
}

type Social struct {
	id string
}

func (s Social) AddFriend(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).id1, elem.(OperationValue).id2
	state.friends[id1].Add(id2)
	state.requests[id1].Remove(id2)
	return state
}

func (s Social) AddRequest(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).id1, elem.(OperationValue).id2
	state.friends[id1].Add(id2)
	return state
}

func (s Social) RemFriend(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).id1, elem.(OperationValue).id2
	state.requests[id1].Remove(id2)
	return state
}

func (s Social) RemRequest(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).id1, elem.(OperationValue).id2
	state.requests[id1].Remove(id2)
	return state
}

func (s Social) Apply(state any, operations []communication.Operation) any {
	for _, op := range operations {
		switch op.Type {
		case "AddFriend":
			state = s.AddFriend(state.(SocialState), op)
		case "AddRequest":
			state = s.RemFriend(state.(SocialState), op)
		case "RemFriend":
			state = s.AddRequest(state.(SocialState), op)
		case "RemRequest":
			state = s.RemRequest(state.(SocialState), op)
		}
	}
	return state
}

func (a Social) Order(op1 communication.Operation, op2 communication.Operation) bool {
	//order map of operations by type of operation,
	//remFriend < addFriend
	//remRequest < addRequest
	//addFriend < addRequest
	// rmFriend and rmRequest are commutative

	return op1.Type == "RemFriend" && op2.Type == "AddFriend" ||
		op1.Type == "RemRequest" && op2.Type == "AddRequest" ||
		op1.Type == "AddFriend" && op2.Type == "AddRequest"
}

func (a Social) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type
}

// initialize counter replica
func NewAddWinsReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.EcroCRDT{Id: id,
		Data: Social{id},
		Stable_st: SocialState{
			friends:  []mapset.Set[any]{},
			requests: []mapset.Set[any]{},
		},
		Unstable_operations: []communication.Operation{},
		Unstable_st: SocialState{
			friends:  []mapset.Set[any]{},
			requests: []mapset.Set[any]{},
		},
	}

	return replica.NewReplica(id, &c, channels, delay)
}
