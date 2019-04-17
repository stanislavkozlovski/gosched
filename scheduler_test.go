package gosched

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestScheduleDetectsCycles(t *testing.T) {
	// C->B->A->C - a cyclic graph
	jobA := Job{
		Name: "A",
	}
	jobB := Job{
		Name: "B",
	}
	jobC := Job{
		Name: "C",
	}
	err := jobC.scheduleAfterJobStart(30000, []*Job{&jobB})
	assert.Nil(t, err)
	err = jobB.scheduleAfterJobStart(30000, []*Job{&jobA})
	assert.Nil(t, err)
	err = jobA.scheduleAfterJobStart(30000, []*Job{&jobC})
	assert.Nil(t, err)

	scheduler := NewScheduler(0)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)

	err = scheduler.Schedule()

	assert.NotNil(t, err)
}

func TestSchedule(t *testing.T) {
	// 4 Jobs - A,B,C and D.
	// Given start time 0:00:02
	// B should start at 0:02:02 (ends at 0:03:02)
	// A should start at 0:02:32 (ends at 0:03:32)
	// C should start at 0:02:32 (ends at 0:04:02)
	// D should start at 0:06:02 (ends at 0:07:02)

	jobA := Job{
		Name:       "A",
		DurationMs: 60000,
	}
	jobB := Job{
		Name:       "B",
		DurationMs: 60000,
	}
	jobC := Job{
		Name:       "C",
		DurationMs: 90000,
	}
	jobD := Job{
		Name:       "D",
		DurationMs: 60000,
	}
	err := jobA.scheduleAfterJobStart(30000, []*Job{&jobB})
	assert.Nil(t, err)
	err = jobB.scheduleAfterJobStart(120000, nil)
	assert.Nil(t, err)
	err = jobC.scheduleAfterJobStart(0, []*Job{&jobA, &jobB})
	assert.Nil(t, err)
	err = jobD.scheduleAfterJobEnd(120000, []*Job{&jobC, &jobA})
	assert.Nil(t, err)

	baseStartMs := uint64(2000)
	scheduler := NewScheduler(baseStartMs)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)
	scheduler.AddJob(&jobD)

	err = scheduler.Schedule()

	assert.Nil(t, err)
	assert.Equal(t, jobB.startTimeMs, uint64(120000)+baseStartMs)
	assert.Equal(t, jobB.endTimeMs, jobB.startTimeMs+60000)

	assert.Equal(t, jobA.startTimeMs, jobB.startTimeMs+30000)
	assert.Equal(t, jobA.endTimeMs, jobA.startTimeMs+jobA.DurationMs)

	assert.Equal(t, jobC.startTimeMs, jobA.startTimeMs)
	assert.Equal(t, jobC.endTimeMs, jobC.startTimeMs+jobC.DurationMs)

	assert.Equal(t, jobD.endTimeMs, jobD.startTimeMs+jobD.DurationMs)
	assert.Equal(t, jobD.startTimeMs, jobC.endTimeMs+120000)
}

func TestJobCannotScheduleBothAfterStartAndEnd(t *testing.T) {
	jobA := Job{Name: "A"}
	jobB := Job{Name: "B"}
	jobC := Job{Name: "C"}

	err := jobA.scheduleAfterJobStart(0, []*Job{&jobB})
	assert.Nil(t, err)
	assert.NotNil(t, jobA.scheduleAfterStart)
	err = jobA.scheduleAfterJobEnd(0, []*Job{&jobC})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterEnd)
}

func TestJobCannotScheduleBothAfterItself(t *testing.T) {
	jobA := Job{Name: "A"}

	err := jobA.scheduleAfterJobStart(0, []*Job{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterStart)
	err = jobA.scheduleAfterJobEnd(0, []*Job{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterEnd)
}
