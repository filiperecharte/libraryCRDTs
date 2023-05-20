package custom

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type Bid struct {
	User    int
	Ammount int
}

type AuctionState struct {
	Users  mapset.Set[any]
	Bids   mapset.Set[Bid]
	MaxBid int
}

type Auction struct {
	id string
}

// Add a new user to the auction
func (a Auction) AddUser(state AuctionState, elem any) AuctionState {
	elem = elem.(communication.Operation).Value
	state.Users.Add(elem)
	return state
}

// Remove a user from the auction
func (a Auction) RemUser(state AuctionState, elem any) AuctionState {
	elem = elem.(communication.Operation).Value
	state.Users.Remove(elem)

	//remove all bids from user
	bids := state.Bids.ToSlice()
	for i := 0; i < len(bids); i++ {
		if bids[i].User == elem {
			bids = append(bids[:i], bids[i+1:]...)
			i--
		}
	}
	return state
}

func (a Auction) PlaceBid(state AuctionState, elem any) AuctionState {
	elem = elem.(communication.Operation).Value
	state.Bids.Add(elem.(Bid))
	return state
}

func (a Auction) Close(state AuctionState) AuctionState {
	//choose max bid
	maxBid := 0
	for _, bid := range state.Bids.ToSlice() {
		if bid.Ammount > maxBid {
			maxBid = bid.Ammount
		}
	}

	state.MaxBid = maxBid

	return state
}

func (a Auction) Apply(state any, operations []communication.Operation) any {
	for _, op := range operations {
		switch op.Type {
		case "AddUser":
			state = a.AddUser(state.(AuctionState), op)
		case "RemUser":
			state = a.RemUser(state.(AuctionState), op)
		case "PlaceBid":
			state = a.PlaceBid(state.(AuctionState), op)
		case "Close":
			state = a.Close(state.(AuctionState))
		}
	}
	return state
}

func (a Auction) Order(op1 communication.Operation, op2 communication.Operation) bool {
	//order map of operations by type of operation

	return op1.Type == "RemUser" && op2.Type == "AddUser" ||
		op1.Type == "PlaceBid" && op2.Type == "RemUser" ||
		op1.Type == "PlaceBid" && op2.Type == "Close"
}

func (a Auction) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	if op1.Type == op2.Type ||
		op1.Type == "AddUser" && op2.Type == "Close" ||
		op1.Type == "RemUser" && op2.Type == "Close" {
		return true
	} else if bid, ok := op1.Value.(Bid); ok {
		if op2.Value != bid.User {
			return true
		}
	} else if bid, ok := op2.Value.(Bid); ok {
		if op1.Value != bid.User {
			return true
		}
	}

	return false
}

// initialize counter replica
func NewAuctionReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.EcroCRDT{Id: id,
		Data: Auction{id},
		Stable_st: AuctionState{
			Users:  mapset.NewSet[any](),
			Bids:   mapset.NewSet[Bid](),
		},
		Unstable_operations: []communication.Operation{},
		Unstable_st: AuctionState{
			Users:  mapset.NewSet[any](),
			Bids:   mapset.NewSet[Bid](),
		},
	}

	return replica.NewReplica(id, &c, channels, delay)
}

// compares if two SocialState are equal for test reasons
func CompareAuctionStates(a1 AuctionState, a2 AuctionState) bool {
	if !a1.Users.Equal(a2.Users) {
		return false
	}

	//check if bids are equal
	if !a1.Bids.Equal(a2.Bids) {
		return false
	}

	return true
}
