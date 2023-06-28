package datatypes

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

func (s Social) Order(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == "breakup" && op2.Type == "accept" ||
		op1.Type == "reject" && op2.Type == "request" ||
		op1.Type == "request" && op2.Type == "accept" ||
		op1.Type == "reject" && op2.Type == "accept"
}

func (s Social) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type ||
		op1.Value.(SocialOpValue).From != op2.Value.(SocialOpValue).From &&
			op1.Value.(SocialOpValue).To == op2.Value.(SocialOpValue).To ||
		op1.Value.(SocialOpValue).From == op2.Value.(SocialOpValue).From &&
			op1.Value.(SocialOpValue).To != op2.Value.(SocialOpValue).To
}

func (s Social) ArbitrationOrderMain(op1 communication.Operation, op2 communication.Operation) (bool, bool) {
	return false, op1.Type == "Pop" && op2.Type == "setPopped"
}

func (s Social) RepairRight(op1 communication.Operation, op2 communication.Operation, state any) communication.Operation {
	if op1.Type == "setPopped" && op2.Type == "Pop" {
		return communication.Operation{"Pop", false, op1.Version, op1.OriginID}
	}

	return op2
}

func (s Social) RepairLeft(op1 communication.Operation, op2 communication.Operation) communication.Operation {
	return communication.Operation{"setPopped", op1.Value, op2.Version, op2.OriginID}
}

func (s Social) SemidirectOps() []string {
	return []string{"reject", "breakup"}
}

func (s Social) ECROOps() []string {
	return []string{"request", "accept"}
}

// initialize RGA
func NewSocialCRDTECROReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	r := crdt.NewSemidirectECRO(id, SocialState{}, &Social{})

	return replica.NewReplica(id, r, channels, delay)
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
