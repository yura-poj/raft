package raft

import "time"

const disabledElectionTimer = 24 * time.Hour

func (n *Node) run() {
	defer n.wg.Done()

	generation, delay := n.timerSchedule()
	timer := time.NewTimer(delay)
	heartbeatTicker := time.NewTicker(n.heartbeatInterval)
	defer func() {
		stopAndDrainTimer(timer)
		heartbeatTicker.Stop()
	}()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.timerWake:
			generation, delay = n.timerSchedule()
			resetTimer(timer, delay)
		case <-timer.C:
			n.beginElection(generation)
			generation, delay = n.timerSchedule()
			timer.Reset(delay)
		case <-heartbeatTicker.C:
			n.launchHeartbeat()
		}
	}
}

func (n *Node) timerSchedule() (uint64, time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == RoleLeader {
		return n.electionGeneration, disabledElectionTimer
	}
	delay := time.Until(n.electionDeadline)
	if delay < 0 {
		delay = 0
	}
	return n.electionGeneration, delay
}

func (n *Node) launchTask(task func()) bool {
	select {
	case <-n.rootCtx.Done():
		return false
	default:
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		task()
	}()
	return true
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	stopAndDrainTimer(timer)
	timer.Reset(delay)
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
