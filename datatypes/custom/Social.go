package custom

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type OperationValue struct {
	Id1 int //id of the user who is adding/removing
	Id2 int //id of the user who is being added/removed
}

type SocialState struct {
	Friends  [5]mapset.Set[any] //friends[id] = set of friends of id
	Requests [5]mapset.Set[any] //requests[id] = set of requests of id
}

type Social struct {
	id string
}

func (s Social) AddFriend(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).Id1, elem.(OperationValue).Id2
	state.Friends[id1].Add(id2)
	state.Friends[id2].Add(id1)
	state.Requests[id1].Remove(id2)
	return state
}

func (s Social) AddRequest(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).Id1, elem.(OperationValue).Id2
	state.Requests[id1].Add(id2)
	return state
}

func (s Social) RemFriend(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).Id1, elem.(OperationValue).Id2
	state.Friends[id1].Remove(id2)
	state.Friends[id2].Remove(id1)
	return state
}

func (s Social) RemRequest(state SocialState, elem any) SocialState {
	elem = elem.(communication.Operation).Value
	id1, id2 := elem.(OperationValue).Id1, elem.(OperationValue).Id2
	state.Requests[id1].Remove(id2)
	return state
}

func (s Social) Apply(state any, operations []communication.Operation) any {
	for _, op := range operations {
		switch op.Type {
		case "AddFriend":
			state = s.AddFriend(state.(SocialState), op)
		case "RemFriend":
			state = s.RemFriend(state.(SocialState), op)
		case "AddRequest":
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
	//addRequest < addFriend
	// rmFriend and rmRequest are commutative

	return op1.Type == "RemFriend" && op2.Type == "AddFriend" ||
		op1.Type == "RemRequest" && op2.Type == "AddRequest" ||
		op1.Type == "AddRequest" && op2.Type == "AddFriend"
}

func (a Social) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type || 
	op1.Value.(OperationValue).Id1 != op2.Value.(OperationValue).Id1 && op1.Value.(OperationValue).Id2 == op2.Value.(OperationValue).Id2 || 
	op1.Value.(OperationValue).Id1 == op2.Value.(OperationValue).Id1 && op1.Value.(OperationValue).Id2 != op2.Value.(OperationValue).Id2 || 
	op1.Value.(OperationValue).Id1 != op2.Value.(OperationValue).Id1 && op1.Value.(OperationValue).Id2 != op2.Value.(OperationValue).Id2
}

// initialize counter replica
func NewSocialReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.EcroCRDT{Id: id,
		Data: Social{id},
		Stable_st: SocialState{
			Friends:  [5]mapset.Set[any]{mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any]()},
			Requests: [5]mapset.Set[any]{mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any]()},
		},
		Unstable_operations: []communication.Operation{},
		Unstable_st: SocialState{
			Friends:  [5]mapset.Set[any]{mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any]()},
			Requests: [5]mapset.Set[any]{mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any](), mapset.NewSet[any]()},
		},
	}

	return replica.NewReplica(id, &c, channels, delay)
}

// compares if two SocialState are equal for test reasons
func CompareSocialStates(s1 SocialState, s2 SocialState) bool {
	for i := 0; i < 5; i++ {
		if !s1.Friends[i].Equal(s2.Friends[i]) {
			return false
		}
		if !s1.Requests[i].Equal(s2.Requests[i]) {
			return false
		}
	}
	return true
}
