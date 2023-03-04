package main

import (
	"container/heap"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime        int64
		totalWait          float64
		totalTurnaround    float64
		lastCompletionTime float64
		waitingTime        int64
		schedule           = make([][]string, len(processes))
		gantt              = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletionTime = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletionTime

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	// Sort processes by arrival time
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	var (
		currentIndex       int
		currentTime        int64
		lastCompletionTime float64
		schedule           = make([][]string, len(processes))
		gantt              = make([]TimeSlice, 0)
		remainingTime      = make([]int64, len(processes))
		waitingTimes       = make([]int64, len(processes))
	)

	for i, p := range processes {
		remainingTime[i] = p.BurstDuration
	}

	var (
		numOfCompletedProcesses int
		totalProcessCount       = len(processes)
	)

	for numOfCompletedProcesses < totalProcessCount {

		minRemainTime := int64(math.MaxInt64)
		for i, p := range processes {
			if p.ArrivalTime <= currentTime && remainingTime[i] < minRemainTime && remainingTime[i] > 0 {
				minRemainTime = remainingTime[i]
				currentIndex = i
			}
		}

		if minRemainTime == int64(math.MaxInt64) {
			currentTime = processes[numOfCompletedProcesses].ArrivalTime
			continue
		}

		if len(gantt) == 0 || gantt[len(gantt)-1].PID != processes[currentIndex].ProcessID {
			timeSlice := TimeSlice{
				PID:   processes[currentIndex].ProcessID,
				Start: currentTime,
			}
			gantt = append(gantt, timeSlice)
		}

		remainingTime[currentIndex]--
		currentTime++

		if remainingTime[currentIndex] == 0 {
			numOfCompletedProcesses++
			stop := currentTime
			gantt[len(gantt)-1].Stop = stop

			waitingTimes[currentIndex] = stop - processes[currentIndex].BurstDuration - processes[currentIndex].ArrivalTime
			if waitingTimes[currentIndex] < 0 {
				waitingTimes[currentIndex] = 0
			}

			completion := float64(processes[currentIndex].BurstDuration + processes[currentIndex].ArrivalTime + waitingTimes[currentIndex])
			if lastCompletionTime < completion {
				lastCompletionTime = completion
			}

			schedule[currentIndex] = []string{
				fmt.Sprint(processes[currentIndex].ProcessID),
				fmt.Sprint(processes[currentIndex].Priority),
				fmt.Sprint(processes[currentIndex].BurstDuration),
				fmt.Sprint(processes[currentIndex].ArrivalTime),
				fmt.Sprint(waitingTimes[currentIndex]),
				fmt.Sprint(waitingTimes[currentIndex] + processes[currentIndex].BurstDuration),
				fmt.Sprint(completion),
			}
		}
	}

	aveWait := float64(totalWaitingTime(waitingTimes)) / float64(totalProcessCount)
	aveTurnaround := float64(totalTurnAroundTime(processes, waitingTimes)) / float64(totalProcessCount)
	aveThroughput := float64(totalProcessCount) / float64(lastCompletionTime)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func totalTurnAroundTime(processes []Process, waitingTimes []int64) int64 {
	turnAroundTimes := make([]int64, len(processes))
	result := int64(0)

	for i := range processes {
		turnAroundTimes[i] = processes[i].BurstDuration + waitingTimes[i]
		result += turnAroundTimes[i]
	}
	return result
}

func totalWaitingTime(waitingTimes []int64) int64 {
	result := int64(0)

	for i := range waitingTimes {
		result += waitingTimes[i]
	}
	return result
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	var (
		totalProcessCount  = len(processes)
		turnAroundTimes    = make([]int64, totalProcessCount)
		waitingTimes       = make([]int64, totalProcessCount)
		gantt              = make([]TimeSlice, 0)
		lastCompletionTime = int64(0)
		schedule           = make([][]string, totalProcessCount)
		insertedProcessIdx = 0
		totalBurstDuration = int64(0)
		minHeap            = &IntHeap{}
	)

	heap.Init(minHeap)

	currentTime := processes[0].ArrivalTime
	processesMapIndex := make(map[int64]int)
	for i, p := range processes {
		processesMapIndex[p.ProcessID] = i
		totalBurstDuration += p.BurstDuration
	}

	for {
		for insertedProcessIdx < totalProcessCount && processes[insertedProcessIdx].ArrivalTime == currentTime {
			heap.Push(minHeap, processes[insertedProcessIdx])
			insertedProcessIdx++
		}

		if minHeap.Len() == 0 {
			if insertedProcessIdx < totalProcessCount {
				currentTime = processes[insertedProcessIdx].ArrivalTime
			} else {
				break
			}
		}

		minPriorityProcess := heap.Pop(minHeap).(Process)
		minPriorityProcess.BurstDuration--

		if len(gantt) == 0 || gantt[len(gantt)-1].PID != minPriorityProcess.ProcessID {
			timeSlice := TimeSlice{
				PID:   minPriorityProcess.ProcessID,
				Start: currentTime,
				Stop:  currentTime + minPriorityProcess.BurstDuration + 1,
			}
			gantt = append(gantt, timeSlice)
		}

		currentTime++

		if minPriorityProcess.BurstDuration == 0 {
			idx := processesMapIndex[minPriorityProcess.ProcessID]
			completionTime := currentTime
			turnAroundTimes[idx] = completionTime - minPriorityProcess.ArrivalTime
			waitingTimes[idx] = turnAroundTimes[idx] - minPriorityProcess.BurstDuration - minPriorityProcess.ArrivalTime
			if lastCompletionTime < completionTime {
				lastCompletionTime = completionTime
			}
		} else {
			heap.Push(minHeap, minPriorityProcess)
			if lastCompletionTime < currentTime {
				lastCompletionTime = currentTime
			}
		}

	}

	for i, p := range processes {
		completion := p.ArrivalTime + p.BurstDuration + waitingTimes[i]
		if lastCompletionTime < completion {
			lastCompletionTime = completion
		}
		schedule[i] = []string{
			fmt.Sprint(p.ProcessID),
			fmt.Sprint(p.Priority),
			fmt.Sprint(p.BurstDuration),
			fmt.Sprint(p.ArrivalTime),
			fmt.Sprint(waitingTimes[i]),
			fmt.Sprint(turnAroundTimes[i]),
			fmt.Sprint(completion),
		}
	}

	aveWait := float64(total(waitingTimes)) / float64(totalProcessCount)
	aveTurnaround := float64(total(turnAroundTimes)) / float64(totalProcessCount)
	aveThroughput := float64(totalProcessCount) / float64(lastCompletionTime)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func total(arr []int64) int64 {
	result := int64(0)
	for _, value := range arr {
		result += value
	}
	return result
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	outputTitle(w, title)
}

type IntHeap []Process

func (h IntHeap) Len() int { return len(h) }

func (h IntHeap) Less(i, j int) bool {
	return h[i].Priority < h[j].Priority
}

func (h IntHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *IntHeap) Push(x any) {
	*h = append(*h, x.(Process))
}

func (h *IntHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}
	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return i
}
