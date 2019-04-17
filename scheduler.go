package gosched

import "errors"

type Scheduler struct {
	jobs          []*Job
	baseStartMs   uint64
	traversedJobs map[string]bool
}

func NewScheduler(baseStartMs uint64) *Scheduler {
	return &Scheduler{
		jobs:          []*Job{},
		baseStartMs:   baseStartMs,
		traversedJobs: make(map[string]bool),
	}
}

func (s *Scheduler) AddJob(job *Job) {
	s.jobs = append(s.jobs, job)
}

// Schedule() parses all of the jobs, validates/traverses the dependency graph (topology sort)
// 	and populates each job's expected startTime and endTime
func (s *Scheduler) Schedule() error {
	for _, job := range s.jobs {
		err := s.traverseJobs(job, make(map[string]bool))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) traverseJobs(job *Job, traversedJobsThisIteration map[string]bool) error {
	if s.traversedJobs[job.Name] {
		return nil
	}

	s.traversedJobs[job.Name] = true
	traversedJobsThisIteration[job.Name] = true
	for _, depJob := range job.dependentJobs() {
		if traversedJobsThisIteration[depJob.Name] && !depJob.scheduled {
			// we have a cycle, we're trying to go back in the graph
			return errors.New("dependency cycle in jobs detected")
		}
		err := s.traverseJobs(depJob, traversedJobsThisIteration)
		if err != nil {
			return err
		}
	}

	job.schedule(s.baseStartMs)
	return nil
}

type ScheduleDelay struct {
	jobs    []*Job
	delayMs uint64
}

type Job struct {
	Name       string
	DurationMs uint64

	startTimeMs        uint64
	scheduled          bool
	endTimeMs          uint64
	scheduleAfterStart *ScheduleDelay
	scheduleAfterEnd   *ScheduleDelay
}

func (j *Job) schedule(baseStartTimeMs uint64) {
	if j.scheduleAfterEnd == nil && j.scheduleAfterStart == nil {
		j.startTimeMs = baseStartTimeMs
	} else if j.scheduleAfterEnd != nil {
		latestEndTime := baseStartTimeMs
		for _, job := range j.scheduleAfterEnd.jobs {
			if job.endTimeMs > latestEndTime {
				latestEndTime = job.endTimeMs
			}
		}
		j.startTimeMs = latestEndTime + j.scheduleAfterEnd.delayMs
	} else {
		latestStartTime := baseStartTimeMs
		for _, job := range j.scheduleAfterStart.jobs {
			if job.startTimeMs > latestStartTime {
				latestStartTime = job.startTimeMs
			}
		}
		j.startTimeMs = latestStartTime + j.scheduleAfterStart.delayMs
	}
	j.endTimeMs = j.startTimeMs + j.DurationMs
	j.scheduled = true
}

func (j *Job) scheduleAfterJobEnd(delayMs uint64, jobs []*Job) error {
	if j.scheduleAfterStart != nil {
		return errors.New("cannot schedule this job after other jobs because it is already scheduled before some others")
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return errors.New("cannot schedule job after itself")
		}
	}
	j.scheduleAfterEnd = &ScheduleDelay{jobs, delayMs}
	return nil
}

func (j *Job) scheduleAfterJobStart(delayMs uint64, jobs []*Job) error {
	if j.scheduleAfterEnd != nil {
		return errors.New("cannot schedule this job before other jobs because it is already scheduled after some others")
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return errors.New("cannot schedule job after itself")
		}
	}
	j.scheduleAfterStart = &ScheduleDelay{jobs, delayMs}
	return nil
}

func (j *Job) dependentJobs() []*Job {
	if j.scheduleAfterEnd == nil && j.scheduleAfterStart == nil {
		return []*Job{}
	} else if j.scheduleAfterEnd == nil {
		return j.scheduleAfterStart.jobs
	}
	return j.scheduleAfterEnd.jobs
}
