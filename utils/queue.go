package utils

import (
	"log"
	"sync"
)

type Queue struct {
	q  []interface{}
	mu sync.Mutex
}

// New returns a new empty queue
func NewQueue() Queue {
	return Queue{}
}

func (queue *Queue) Enqueue(element interface{}) interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.q = append(queue.q, element) // Simply append to enqueue.
	return element
}

func (queue *Queue) Dequeue() interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.q) == 0 {
		log.Fatal("Underflow")
	} else if len(queue.q) == 1 {
		element := queue.q[0]
		*queue = NewQueue()
		return element
	}
	element := queue.q[0]
	queue.q = (queue.q)[1:]
	return element // Slice off the element once it is dequeued.
}

func (queue *Queue) Get() []interface{} {
	return queue.q // Slice off the element once it is dequeued.
}

func (queue *Queue) GetValue(index int) interface{} {
	return queue.q[index] // Slice off the element once it is dequeued.
}

func (queue *Queue) Set(index int, value interface{}) []interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.q[index] = value
	return queue.q // Slice off the element once it is dequeued.
}

func (queue *Queue) Slice(i ...int) []interface{} {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(i) == 2 {
		queue.q = queue.q[i[0]:i[1]]
	} else if len(i) == 1 {
		queue.q = queue.q[i[0]:]
	}
	return queue.q
}
