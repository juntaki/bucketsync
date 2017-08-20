package bucketsync

type Queue struct {
	q []bucketObject
}

func NewQueue() *Queue {
	return &Queue{q: make([]bucketObject, 0)}
}

func (q *Queue) Enqueue(b bucketObject) {
	q.q = append(q.q, b)
}

func (q *Queue) Dequeue() bucketObject {
	ret := q.q[0]
	q.q = q.q[1:]
	return ret
}

func (q *Queue) Size() int {
	return len(q.q)
}
