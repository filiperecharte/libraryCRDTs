package custom

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type Enroll struct {
	Player     int
	Tournament int
}

type EgameState struct {
	Tournaments mapset.Set[any]
	Players     mapset.Set[any]
	Enrolled    mapset.Set[Enroll]
}

type Egame struct {
	id string
}

func (e Egame) AddTournament(state EgameState, elem any) EgameState {
	elem = elem.(communication.Operation).Value
	state.Tournaments.Add(elem)
	return state
}

func (e Egame) RemTournament(state EgameState, elem any) EgameState {
	elem = elem.(communication.Operation).Value
	state.Tournaments.Remove(elem)

	enrolled := state.Enrolled.ToSlice()
	//remove all enrolled tournaments
	for i := 0; i < len(enrolled); i++ {
		if enrolled[i].Tournament == elem {
			enrolled = append(enrolled[:i], enrolled[i+1:]...)
			i--
		}
	}

	state.Enrolled = mapset.NewSet(enrolled...)

	return state
}

func (e Egame) AddPlayer(state EgameState, elem any) EgameState {
	elem = elem.(communication.Operation).Value
	state.Players.Add(elem)
	return state
}

func (e Egame) RemPlayer(state EgameState, elem any) EgameState {
	elem = elem.(communication.Operation).Value
	state.Players.Remove(elem)

	enrolled := state.Enrolled.ToSlice()

	//remove all enrolled player
	for i := 0; i < len(enrolled); i++ {
		if enrolled[i].Player == elem {
			enrolled = append(enrolled[:i], enrolled[i+1:]...)
			i--
		}
	}

	state.Enrolled = mapset.NewSet(enrolled...)

	return state
}

func (e Egame) Enroll(state EgameState, elem any) EgameState {
	elem = elem.(communication.Operation).Value
	state.Enrolled.Add(elem.(Enroll))
	return state
}

func (e Egame) Apply(state any, operations []communication.Operation) any {
	for _, op := range operations {
		switch op.Type {
		case "AddTournament":
			state = e.AddTournament(state.(EgameState), op)
		case "RemTournament":
			state = e.RemTournament(state.(EgameState), op)
		case "AddPlayer":
			state = e.AddPlayer(state.(EgameState), op)
		case "RemPlayer":
			state = e.RemPlayer(state.(EgameState), op)
		case "Enroll":
			state = e.Enroll(state.(EgameState), op)
		}
	}
	return state
}

func (e Egame) Order(op1 communication.Operation, op2 communication.Operation) bool {
	//order map of operations by type of operation

	return op1.Type == "RemTournament" && op2.Type == "AddTournament" ||
		op1.Type == "RemPlayer" && op2.Type == "AddPlayer" ||
		op1.Type == "RemPlayer" && op2.Type == "Enroll" ||
		op1.Type == "RemTournament" && op2.Type == "Enroll"
}

func (e Egame) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	if (op1.Type == op2.Type ||
		(op1.Type == "AddPlayer" && op2.Type != "RemPlayer") ||
		(op2.Type == "AddPlayer" && op1.Type != "RemPlayer") ||
		(op1.Type == "AddTournament" && op2.Type != "RemTournament") ||
		(op2.Type == "AddTournament" && op1.Type != "RemTournament")) && op1.Value != op2.Value {
		return true
	} else if enrollment, ok := op1.Value.(Enroll); ok {
		if op2.Value != enrollment.Player && op2.Value != enrollment.Tournament {
			return true
		}
	} else if enrollment, ok := op2.Value.(Enroll); ok {
		if op1.Value != enrollment.Player && op1.Value != enrollment.Tournament {
			return true
		}
	}

	return false
}

// initialize counter replica
func NewEgameReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.NewEcroCRDT(id, EgameState{
		Tournaments: mapset.NewSet[any](),
		Players:     mapset.NewSet[any](),
		Enrolled:    mapset.NewSet[Enroll](),
	}, Egame{id}, replica.Replica{})

	r := replica.NewReplica(id, c, channels, delay)

	c.SetReplica(r)

	return r
}

// compares if two SocialState are equal for test reasons
func CompareEgameStates(s1 EgameState, s2 EgameState) bool {
	if !s1.Tournaments.Equal(s2.Tournaments) || !s1.Players.Equal(s2.Players) {
		return false
	}

	//check if all enrolled are equal
	if !s1.Enrolled.Equal(s2.Enrolled) {
		return false
	}

	return true
}
