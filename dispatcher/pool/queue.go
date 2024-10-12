package pool

import (
	"errors"
)

type CircularQueue[T any] struct {
	items  []T
	maxLen int
	rear   int
	front  int
	count  int // Count of elements in the queue
}

func NewCircularQueue[T any](capacity int) *CircularQueue[T] {
	return &CircularQueue[T]{
		items:  make([]T, capacity),
		maxLen: capacity,
		rear:   -1,
		front:  -1,
		count:  0,
	}
}

func (q *CircularQueue[T]) IsEmpty() bool {
	return q.count == 0
}

func (q *CircularQueue[T]) IsFull() bool {
	return q.count == q.maxLen
}

func (q *CircularQueue[T]) Enqueue(item T) error {
	if q.IsFull() {
		return errors.New("queue is full")
	}
	if q.front == -1 { // Queue was empty
		q.front = 0
	}
	q.rear = (q.rear + 1) % q.maxLen
	q.items[q.rear] = item
	q.count++
	return nil
}

func (q *CircularQueue[T]) Dequeue() (T, error) {
	if q.IsEmpty() {
		var zero T
		return zero, errors.New("queue is empty")
	}
	val := q.items[q.front]
	if q.front == q.rear { // Queue is now empty
		q.front = -1
		q.rear = -1
	} else {
		q.front = (q.front + 1) % q.maxLen
	}
	q.count--
	return val, nil
}

func (q *CircularQueue[T]) Peek() (T, error) {
	if q.IsEmpty() {
		var zero T
		return zero, errors.New("queue is empty")
	}
	return q.items[q.front], nil
}

func (q *CircularQueue[T]) Size() int {
	return q.count
}

// Optional: Method to clear the queue
func (q *CircularQueue[T]) Clear() {
	q.front = -1
	q.rear = -1
	q.count = 0
}
