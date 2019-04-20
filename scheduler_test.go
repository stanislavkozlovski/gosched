package gosched

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestScheduleDetectsCycles(t *testing.T) {
	// C->B->A->C - a cyclic graph
	jobA := SchedulableJob{
		Name:       "A",
		DurationMs: 30000,
	}
	jobB := SchedulableJob{
		Name:       "B",
		DurationMs: 30000,
	}
	jobC := SchedulableJob{
		Name:       "C",
		DurationMs: 30000,
	}
	err := jobC.ScheduleAfterJobStart([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobB.ScheduleAfterJobStart([]*SchedulableJob{&jobA})
	assert.Nil(t, err)
	err = jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobC})
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

	jobA := SchedulableJob{
		Name:       "A",
		DurationMs: 60000,
		DelayMs:    30000,
	}
	jobB := SchedulableJob{
		Name:       "B",
		DurationMs: 60000,
		DelayMs:    120000,
	}
	jobC := SchedulableJob{
		Name:       "C",
		DurationMs: 90000,
	}
	jobD := SchedulableJob{
		Name:       "D",
		DurationMs: 60000,
		DelayMs:    120000,
	}
	err := jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobC.ScheduleAfterJobStart([]*SchedulableJob{&jobA, &jobB})
	assert.Nil(t, err)
	err = jobD.ScheduleAfterJobEnd([]*SchedulableJob{&jobC, &jobA})
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
	start, dur, end, err := scheduler.JobTimes(jobB.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobB.startTimeMs, start)
	assert.Equal(t, jobB.DurationMs, dur)
	assert.Equal(t, jobB.endTimeMs, end)

	assert.Equal(t, jobA.startTimeMs, jobB.startTimeMs+30000)
	assert.Equal(t, jobA.endTimeMs, jobA.startTimeMs+jobA.DurationMs)
	start, dur, end, err = scheduler.JobTimes(jobA.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobA.startTimeMs, start)
	assert.Equal(t, jobA.DurationMs, dur)
	assert.Equal(t, jobA.endTimeMs, end)

	assert.Equal(t, jobC.startTimeMs, jobA.startTimeMs)
	assert.Equal(t, jobC.endTimeMs, jobC.startTimeMs+jobC.DurationMs)
	start, dur, end, err = scheduler.JobTimes(jobC.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobC.startTimeMs, start)
	assert.Equal(t, jobC.DurationMs, dur)
	assert.Equal(t, jobC.endTimeMs, end)

	assert.Equal(t, jobD.endTimeMs, jobD.startTimeMs+jobD.DurationMs)
	assert.Equal(t, jobD.startTimeMs, jobC.endTimeMs+120000)
	start, dur, end, err = scheduler.JobTimes(jobD.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobD.startTimeMs, start)
	assert.Equal(t, jobD.DurationMs, dur)
	assert.Equal(t, jobD.endTimeMs, end)
}

func TestJobTimesThrowsErrors(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}
	scheduler := NewScheduler(0)
	_, _, _, err := scheduler.JobTimes("A") // not added
	assert.NotNil(t, err)
	scheduler.AddJob(&jobA)
	_, _, _, err = scheduler.JobTimes("A") // not scheduled
	assert.NotNil(t, err)
	jobA.scheduled = true
	_, _, _, err = scheduler.JobTimes("A")
	assert.Nil(t, err)
}

func TestJobCannotScheduleBothAfterStartAndEnd(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}
	jobB := SchedulableJob{Name: "B"}
	jobC := SchedulableJob{Name: "C"}

	err := jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	assert.NotNil(t, jobA.scheduleAfterStart)
	err = jobA.ScheduleAfterJobEnd([]*SchedulableJob{&jobC})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterEnd)
}

func TestJobCannotScheduleBothAfterItself(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}

	err := jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterStart)
	err = jobA.ScheduleAfterJobEnd([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterEnd)
}
