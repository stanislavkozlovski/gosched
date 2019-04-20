package gosched

import (
	"errors"
	"fmt"
)

type Scheduler struct {
	jobs          []*SchedulableJob
	jobsByName    map[string]*SchedulableJob
	baseStartMs   uint64
	traversedJobs map[string]bool
}

func NewScheduler(baseStartMs uint64) *Scheduler {
	return &Scheduler{
		jobs:          []*SchedulableJob{},
		baseStartMs:   baseStartMs,
		traversedJobs: make(map[string]bool),
		jobsByName:    make(map[string]*SchedulableJob),
	}
}

func (s *Scheduler) AddJob(job *SchedulableJob) {
	s.jobs = append(s.jobs, job)
	s.jobsByName[job.Name] = job
}

func (s *Scheduler) JobTimes(jobName string) (startTimeMs uint64, durationMs uint64, endTimeMs uint64, err error) {
	job := s.jobsByName[jobName]
	if job == nil {
		return 0, 0, 0, errors.New(fmt.Sprintf("job %s does not exist in the scheduler", jobName))
	}
	if !job.scheduled {
		return 0, 0, 0, errors.New(fmt.Sprintf("job %s has not been scheduled yet", jobName))
	}

	return job.startTimeMs, job.DurationMs, job.endTimeMs, nil
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

func (s *Scheduler) traverseJobs(job *SchedulableJob, traversedJobsThisIteration map[string]bool) error {
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

type SchedulableJob struct {
	Name       string
	DurationMs uint64
	DelayMs    uint64

	startTimeMs        uint64
	scheduled          bool
	endTimeMs          uint64
	scheduleAfterStart []*SchedulableJob
	scheduleAfterEnd   []*SchedulableJob
}

func (j *SchedulableJob) schedule(baseStartTimeMs uint64) {
	if len(j.scheduleAfterEnd) == 0 && len(j.scheduleAfterStart) == 0 {
		j.startTimeMs = baseStartTimeMs + j.DelayMs
	} else if len(j.scheduleAfterEnd) != 0 {
		latestEndTime := baseStartTimeMs
		for _, job := range j.scheduleAfterEnd {
			if job.endTimeMs > latestEndTime {
				latestEndTime = job.endTimeMs
			}
		}
		j.startTimeMs = latestEndTime + j.DelayMs
	} else {
		latestStartTime := baseStartTimeMs
		for _, job := range j.scheduleAfterStart {
			if job.startTimeMs > latestStartTime {
				latestStartTime = job.startTimeMs
			}
		}
		j.startTimeMs = latestStartTime + j.DelayMs
	}
	j.endTimeMs = j.startTimeMs + j.DurationMs
	j.scheduled = true
}

func (j *SchedulableJob) ScheduleAfterJobEnd(jobs []*SchedulableJob) error {
	if len(j.scheduleAfterStart) != 0 {
		return errors.New("cannot schedule this job after other jobs because it is already scheduled before some others")
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return errors.New("cannot schedule job after itself")
		}
	}
	j.scheduleAfterEnd = jobs
	return nil
}

func (j *SchedulableJob) ScheduleAfterJobStart(jobs []*SchedulableJob) error {
	if len(j.scheduleAfterEnd) != 0 {
		return errors.New("cannot schedule this job before other jobs because it is already scheduled after some others")
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return errors.New("cannot schedule job after itself")
		}
	}
	j.scheduleAfterStart = jobs
	return nil
}

func (j *SchedulableJob) dependentJobs() []*SchedulableJob {
	if len(j.scheduleAfterEnd) == 0 && len(j.scheduleAfterStart) == 0 {
		return []*SchedulableJob{}
	} else if len(j.scheduleAfterEnd) == 0 {
		return j.scheduleAfterStart
	}
	return j.scheduleAfterEnd
}
