// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package scheduler

// The structs and helpers in this file are split out of reconciler.go for code
// manageability and should not be shared to the system schedulers! If you need
// something here for system/sysbatch jobs, double-check it's safe to use for
// all scheduler types before moving it into util.go

import (
	"errors"
	"fmt"
	log "github.com/hashicorp/go-hclog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/nomad/nomad/structs"
)

// placementResult is an allocation that must be placed. It potentially has a
// previous allocation attached to it that should be stopped only if the
// paired placement is complete. This gives an atomic place/stop behavior to
// prevent an impossible resource ask as part of a rolling update to wipe the
// job out.
type placementResult interface {
	// TaskGroup returns the task group the placement is for
	TaskGroup() *structs.TaskGroup

	// Name returns the name of the desired allocation
	Name() string

	// Canary returns whether the placement should be a canary
	Canary() bool

	// PreviousAllocation returns the previous allocation
	PreviousAllocation() *structs.Allocation

	// SetPreviousAllocation updates the reference to the previous allocation
	SetPreviousAllocation(*structs.Allocation)

	// IsRescheduling returns whether the placement was rescheduling a failed allocation
	IsRescheduling() bool

	// StopPreviousAlloc returns whether the previous allocation should be
	// stopped and if so the status description.
	StopPreviousAlloc() (bool, string)

	// PreviousLost is true if the previous allocation was lost.
	PreviousLost() bool

	// DowngradeNonCanary indicates that placement should use the latest stable job
	// with the MinJobVersion, rather than the current deployment version
	DowngradeNonCanary() bool

	MinJobVersion() uint64
}

// allocStopResult contains the information required to stop a single allocation
type allocStopResult struct {
	alloc             *structs.Allocation
	clientStatus      string
	statusDescription string
	followupEvalID    string
}

// allocPlaceResult contains the information required to place a single
// allocation
type allocPlaceResult struct {
	name          string
	canary        bool
	taskGroup     *structs.TaskGroup
	previousAlloc *structs.Allocation
	reschedule    bool
	lost          bool

	downgradeNonCanary bool
	minJobVersion      uint64
}

func (a allocPlaceResult) TaskGroup() *structs.TaskGroup           { return a.taskGroup }
func (a allocPlaceResult) Name() string                            { return a.name }
func (a allocPlaceResult) Canary() bool                            { return a.canary }
func (a allocPlaceResult) PreviousAllocation() *structs.Allocation { return a.previousAlloc }
func (a allocPlaceResult) SetPreviousAllocation(alloc *structs.Allocation) {
	a.previousAlloc = alloc
}
func (a allocPlaceResult) IsRescheduling() bool              { return a.reschedule }
func (a allocPlaceResult) StopPreviousAlloc() (bool, string) { return false, "" }
func (a allocPlaceResult) DowngradeNonCanary() bool          { return a.downgradeNonCanary }
func (a allocPlaceResult) MinJobVersion() uint64             { return a.minJobVersion }
func (a allocPlaceResult) PreviousLost() bool                { return a.lost }

// allocDestructiveResult contains the information required to do a destructive
// update. Destructive changes should be applied atomically, as in the old alloc
// is only stopped if the new one can be placed.
type allocDestructiveResult struct {
	placeName             string
	placeTaskGroup        *structs.TaskGroup
	stopAlloc             *structs.Allocation
	stopStatusDescription string
}

func (a allocDestructiveResult) TaskGroup() *structs.TaskGroup                   { return a.placeTaskGroup }
func (a allocDestructiveResult) Name() string                                    { return a.placeName }
func (a allocDestructiveResult) Canary() bool                                    { return false }
func (a allocDestructiveResult) PreviousAllocation() *structs.Allocation         { return a.stopAlloc }
func (a allocDestructiveResult) SetPreviousAllocation(alloc *structs.Allocation) {} // NOOP
func (a allocDestructiveResult) IsRescheduling() bool                            { return false }
func (a allocDestructiveResult) StopPreviousAlloc() (bool, string) {
	return true, a.stopStatusDescription
}
func (a allocDestructiveResult) DowngradeNonCanary() bool { return false }
func (a allocDestructiveResult) MinJobVersion() uint64    { return 0 }
func (a allocDestructiveResult) PreviousLost() bool       { return false }

// allocMatrix is a mapping of task groups to their allocation set.
type allocMatrix map[string]allocSet

// newAllocMatrix takes a job and the existing allocations for the job and
// creates an allocMatrix
func newAllocMatrix(job *structs.Job, allocs []*structs.Allocation) allocMatrix {
	m := allocMatrix(make(map[string]allocSet))
	for _, a := range allocs {
		s, ok := m[a.TaskGroup]
		if !ok {
			s = make(map[string]*structs.Allocation)
			m[a.TaskGroup] = s
		}
		s[a.ID] = a
	}

	if job != nil {
		for _, tg := range job.TaskGroups {
			if _, ok := m[tg.Name]; !ok {
				m[tg.Name] = make(map[string]*structs.Allocation)
			}
		}
	}
	return m
}

// allocSet is a set of allocations with a series of helper functions defined
// that help reconcile state.
type allocSet map[string]*structs.Allocation

// GoString provides a human readable view of the set
func (a allocSet) GoString() string {
	if len(a) == 0 {
		return "[]"
	}

	start := fmt.Sprintf("len(%d) [\n", len(a))
	var s []string
	for k, v := range a {
		s = append(s, fmt.Sprintf("%q: %v", k, v.Name))
	}
	return start + strings.Join(s, "\n") + "]"
}

// nameSet returns the set of allocation names
func (a allocSet) nameSet() map[string]struct{} {
	names := make(map[string]struct{}, len(a))
	for _, alloc := range a {
		names[alloc.Name] = struct{}{}
	}
	return names
}

// nameOrder returns the set of allocation names in sorted order
func (a allocSet) nameOrder() []*structs.Allocation {
	allocs := make([]*structs.Allocation, 0, len(a))
	for _, alloc := range a {
		allocs = append(allocs, alloc)
	}
	sort.Slice(allocs, func(i, j int) bool {
		return allocs[i].Index() < allocs[j].Index()
	})
	return allocs
}

// difference returns a new allocSet that has all the existing item except those
// contained within the other allocation sets
func (a allocSet) difference(others ...allocSet) allocSet {
	diff := make(map[string]*structs.Allocation)
OUTER:
	for k, v := range a {
		for _, other := range others {
			if _, ok := other[k]; ok {
				continue OUTER
			}
		}
		diff[k] = v
	}
	return diff
}

// union returns a new allocSet that has the union of the two allocSets.
// Conflicts prefer the last passed allocSet containing the value
func (a allocSet) union(others ...allocSet) allocSet {
	union := make(map[string]*structs.Allocation, len(a))
	order := []allocSet{a}
	order = append(order, others...)

	for _, set := range order {
		for k, v := range set {
			union[k] = v
		}
	}

	return union
}

// fromKeys returns an alloc set matching the passed keys
func (a allocSet) fromKeys(keys ...[]string) allocSet {
	from := make(map[string]*structs.Allocation)
	for _, set := range keys {
		for _, k := range set {
			if alloc, ok := a[k]; ok {
				from[k] = alloc
			}
		}
	}
	return from
}

// filterByTainted takes a set of tainted nodes and filters the allocation set
// into the following groups:
// 1. Those that exist on untainted nodes
// 2. Those exist on nodes that are draining
// 3. Those that exist on lost nodes or have expired
// 4. Those that are on nodes that are disconnected, but have not had their ClientState set to unknown
// 5. Those that are on a node that has reconnected.
// 6. Those that are in a state that results in a noop.
func (a allocSet) filterByTainted(taintedNodes map[string]*structs.Node, serverSupportsDisconnectedClients bool, now time.Time) (untainted, migrate, lost, disconnecting, reconnecting, ignore, expiring allocSet) {
	untainted = make(map[string]*structs.Allocation)
	migrate = make(map[string]*structs.Allocation)
	lost = make(map[string]*structs.Allocation)
	disconnecting = make(map[string]*structs.Allocation)
	reconnecting = make(map[string]*structs.Allocation)
	ignore = make(map[string]*structs.Allocation)
	expiring = make(map[string]*structs.Allocation)

	for _, alloc := range a {
		// make sure we don't apply any reconnect logic to task groups
		// without max_client_disconnect
		supportsDisconnectedClients := alloc.SupportsDisconnectedClients(serverSupportsDisconnectedClients)

		reconnect := false

		// Only compute reconnect for unknown, running, and failed since they
		// need to go through the reconnect logic.
		if supportsDisconnectedClients &&
			(alloc.ClientStatus == structs.AllocClientStatusUnknown ||
				alloc.ClientStatus == structs.AllocClientStatusRunning ||
				alloc.ClientStatus == structs.AllocClientStatusFailed) {
			reconnect = alloc.NeedsToReconnect()
		}

		// Failed allocs that need to be reconnected must be added to
		// reconnecting so that they can be handled as a failed reconnect.
		if supportsDisconnectedClients &&
			reconnect &&
			alloc.DesiredStatus == structs.AllocDesiredStatusRun &&
			alloc.ClientStatus == structs.AllocClientStatusFailed {
			reconnecting[alloc.ID] = alloc
			continue
		}

		taintedNode, nodeIsTainted := taintedNodes[alloc.NodeID]
		if taintedNode != nil && taintedNode.Status == structs.NodeStatusDisconnected {
			// Group disconnecting
			if supportsDisconnectedClients {
				// Filter running allocs on a node that is disconnected to be marked as unknown.
				if alloc.ClientStatus == structs.AllocClientStatusRunning {
					disconnecting[alloc.ID] = alloc
					continue
				}
				// Filter pending allocs on a node that is disconnected to be marked as lost.
				if alloc.ClientStatus == structs.AllocClientStatusPending {
					lost[alloc.ID] = alloc
					continue
				}

			} else {
				if alloc.PreventReplaceOnDisconnect() {
					if alloc.ClientStatus == structs.AllocClientStatusRunning {
						disconnecting[alloc.ID] = alloc
						continue
					}

					untainted[alloc.ID] = alloc
					continue
				}

				lost[alloc.ID] = alloc
				continue
			}
		}

		if alloc.TerminalStatus() && !reconnect {
			// Server-terminal allocs, if supportsDisconnectedClient and not reconnect,
			// are probably stopped replacements and should be ignored
			if supportsDisconnectedClients && alloc.ServerTerminalStatus() {
				ignore[alloc.ID] = alloc
				continue
			}

			// Terminal canaries that have been marked for migration need to be
			// migrated, otherwise we block deployments from progressing by
			// counting them as running canaries.
			if alloc.DeploymentStatus.IsCanary() && alloc.DesiredTransition.ShouldMigrate() {
				migrate[alloc.ID] = alloc
				continue
			}

			// Terminal allocs, if not reconnect, are always untainted as they
			// should never be migrated.
			untainted[alloc.ID] = alloc
			continue
		}

		// Non-terminal allocs that should migrate should always migrate
		if alloc.DesiredTransition.ShouldMigrate() {
			migrate[alloc.ID] = alloc
			continue
		}

		if supportsDisconnectedClients && alloc.Expired(now) {
			expiring[alloc.ID] = alloc
			continue
		}

		// Acknowledge unknown allocs that we want to reconnect eventually.
		if supportsDisconnectedClients &&
			alloc.ClientStatus == structs.AllocClientStatusUnknown &&
			alloc.DesiredStatus == structs.AllocDesiredStatusRun {
			untainted[alloc.ID] = alloc
			continue
		}

		// Ignore failed allocs that need to be reconnected and that have been
		// marked to stop by the server.
		if supportsDisconnectedClients &&
			reconnect &&
			alloc.ClientStatus == structs.AllocClientStatusFailed &&
			alloc.DesiredStatus == structs.AllocDesiredStatusStop {
			ignore[alloc.ID] = alloc
			continue
		}

		if !nodeIsTainted || (taintedNode != nil && taintedNode.Status == structs.NodeStatusReady) {
			// Filter allocs on a node that is now re-connected to be resumed.
			if reconnect {
				// Expired unknown allocs should be processed depending on the max client disconnect
				// and/or avoid reschedule on lost configurations, they are both treated as
				// expiring.
				if alloc.Expired(now) {
					expiring[alloc.ID] = alloc
					continue
				}

				reconnecting[alloc.ID] = alloc
				continue
			}

			// Otherwise, Node is untainted so alloc is untainted
			untainted[alloc.ID] = alloc
			continue
		}

		// Allocs on GC'd (nil) or lost nodes are Lost
		if taintedNode == nil {
			lost[alloc.ID] = alloc
			continue
		}

		// Allocs on terminal nodes that can't be rescheduled need to be treated
		// differently than those that can.
		if taintedNode.TerminalStatus() {
			if alloc.PreventReplaceOnDisconnect() {
				if alloc.ClientStatus == structs.AllocClientStatusUnknown {
					untainted[alloc.ID] = alloc
					continue
				} else if alloc.ClientStatus == structs.AllocClientStatusRunning {
					disconnecting[alloc.ID] = alloc
					continue
				}
			}

			lost[alloc.ID] = alloc
			continue
		}

		// All other allocs are untainted
		untainted[alloc.ID] = alloc
	}

	return
}

// filterByRescheduleable 过滤分配集合，返回以下三类分配：
// 1. untainted: 不需要重新调度的分配（已成功完成或不符合重调度条件）
// 2. rescheduleNow: 需要立即重新调度的分配
// 3. rescheduleLater: 需要延迟重新调度的分配（会创建后续的 Evaluation）
//
// 重新调度的核心判断流程：
// 1. 检查分配是否已经在断开连接状态下为 unknown 状态 -> 跳过
// 2. 检查分配是否已经有后续分配（NextAllocation 不为空）-> 跳过，避免重复调度
// 3. 调用 shouldFilter 判断是否应该过滤（batch 和 service 有不同逻辑）
// 4. 调用 updateByReschedulable 判断是否可以立即/延迟重新调度
func (a allocSet) filterByRescheduleable(logger log.Logger, isBatch, isDisconnecting bool, now time.Time, evalID string, deployment *structs.Deployment) (allocSet, allocSet, []*delayedRescheduleInfo) {
	untainted := make(map[string]*structs.Allocation)
	rescheduleNow := make(map[string]*structs.Allocation)
	rescheduleLater := []*delayedRescheduleInfo{}

	for _, alloc := range a {
		// 【跳过条件1】断开连接状态下已经是 unknown 的分配，避免重复处理
		// 这种情况可能发生在 canary 被断开连接打断时
		if isDisconnecting && alloc.ClientStatus == structs.AllocClientStatusUnknown {
			logger.Warn("重调度: 跳过分配 - 正在断开连接且已经是 unknown 状态",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"client_status", alloc.ClientStatus,
			)
			continue
		}

		var eligibleNow, eligibleLater bool
		var rescheduleTime time.Time

		// 【跳过条件2】已经有后续分配的终端状态分配
		// 只有 failed 或 disconnecting 状态的分配才应该被重新调度
		// 这个检查防止对正在运行的分配进行重复调度（历史 bug 的防护）
		if alloc.NextAllocation != "" && alloc.TerminalStatus() {
			logger.Warn("重调度: 跳过分配 - 已经有后续分配",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"next_alloc_id", alloc.NextAllocation,
				"client_status", alloc.ClientStatus,
				"desired_status", alloc.DesiredStatus,
			)
			continue
		}

		// 【过滤判断】调用 shouldFilter 判断分配是否应该被过滤
		// isUntainted=true 表示分配已完成，计入期望总数，不需要重新调度
		// ignore=true 表示分配应该被完全忽略（如已停止的分配）
		isUntainted, ignore := shouldFilter(alloc, isBatch)
		if isUntainted && !isDisconnecting {
			logger.Warn("重调度: 分配标记为 untainted - 不会重新调度",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"client_status", alloc.ClientStatus,
				"desired_status", alloc.DesiredStatus,
				"is_batch", isBatch,
			)
			untainted[alloc.ID] = alloc
			continue // 这些分配永远不会被重新调度，跳过后续检查
		}

		if ignore {
			logger.Warn("重调度: 分配被 shouldFilter 忽略",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"client_status", alloc.ClientStatus,
				"desired_status", alloc.DesiredStatus,
				"is_batch", isBatch,
			)
			continue
		}

		// 【核心判断】调用 updateByReschedulable 判断重新调度的时机
		// 返回值：eligibleNow=可以立即调度, eligibleLater=可以延迟调度, rescheduleTime=调度时间
		eligibleNow, eligibleLater, rescheduleTime = updateByReschedulable(logger, alloc, now, evalID, deployment, isDisconnecting)
		if eligibleNow {
			// 【立即重新调度】分配满足条件，可以立即重新调度
			logger.Warn("重调度: 分配符合立即重新调度条件",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"client_status", alloc.ClientStatus,
				"reschedule_time", rescheduleTime,
			)
			rescheduleNow[alloc.ID] = alloc
			continue
		}

		// 如果分配不满足立即重新调度的条件，将其放入 untainted 集合
		// 这样可以防止调度器认为需要创建新的分配
		untainted[alloc.ID] = alloc

		if eligibleLater {
			// 【延迟重新调度】分配满足条件，但需要延迟到指定时间
			// 会创建一个 follow-up Evaluation 在指定时间触发
			logger.Warn("重调度: 分配符合延迟重新调度条件",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"client_status", alloc.ClientStatus,
				"reschedule_time", rescheduleTime,
				"delay", rescheduleTime.Sub(now),
			)
			rescheduleLater = append(rescheduleLater, &delayedRescheduleInfo{alloc.ID, alloc, rescheduleTime})
		} else {
			// 【不满足重新调度条件】分配不满足重新调度的条件
			// 可能原因：达到最大重试次数、超出时间间隔等
			logger.Warn("重调度: 分配不符合重新调度条件",
				"alloc_id", alloc.ID,
				"job_id", alloc.JobID,
				"task_group", alloc.TaskGroup,
				"client_status", alloc.ClientStatus,
				"desired_status", alloc.DesiredStatus,
			)
		}

	}
	return untainted, rescheduleNow, rescheduleLater
}

// shouldFilter 判断分配是否应该被过滤（忽略或标记为 untainted）
//
// 返回值：
//   - untainted: true 表示分配已完成，计入期望总数，不需要重新调度
//   - ignore: true 表示分配应该被完全忽略，不计入期望总数
//
// Batch 作业的过滤逻辑：
//   - 如果已完成且运行成功 -> untainted（不需要替换）
//   - 如果期望状态是 stop -> 检查是否运行成功或最后一次重调度失败
//   - 如果客户端状态是 failed -> 需要重新调度（返回 false, false）
//
// Service 作业的过滤逻辑：
//   - 如果期望状态是 stop/evict -> 检查最后一次重调度是否失败
//   - 如果客户端状态是 complete/lost -> 忽略
func shouldFilter(alloc *structs.Allocation, isBatch bool) (untainted, ignore bool) {
	// Batch 作业的处理逻辑
	// Batch 作业完成后如果成功则不需要重新调度
	if isBatch {
		switch alloc.DesiredStatus {
		case structs.AllocDesiredStatusStop:
			// 如果分配成功运行完成，标记为 untainted
			if alloc.RanSuccessfully() {
				return true, false
			}
			// 如果最后一次重调度失败，不标记为 untainted，允许再次尝试
			if alloc.LastRescheduleFailed() {
				return false, false
			}
			// 其他情况忽略
			return false, true
		case structs.AllocDesiredStatusEvict:
			// 被驱逐的分配忽略
			return false, true
		}

		switch alloc.ClientStatus {
		case structs.AllocClientStatusFailed:
			// 失败的 batch 作业需要重新调度
			return false, false
		}

		// 其他情况标记为 untainted
		return true, false
	}

	// Service 作业的处理逻辑
	switch alloc.DesiredStatus {
	case structs.AllocDesiredStatusStop, structs.AllocDesiredStatusEvict:
		// 如果最后一次重调度失败，不忽略，允许再次尝试
		if alloc.LastRescheduleFailed() {
			return false, false
		}
		// 停止或驱逐的分配忽略
		return false, true
	}

	switch alloc.ClientStatus {
	case structs.AllocClientStatusComplete, structs.AllocClientStatusLost:
		// 完成或丢失的分配忽略
		return false, true
	}

	// 其他情况不过滤，需要进一步判断是否重新调度
	return false, false
}

// updateByReschedulable 判断一个失败的分配是否应该立即/延迟重新调度
//
// 返回值：
//   - rescheduleNow: 是否可以立即重新调度
//   - rescheduleLater: 是否需要延迟重新调度
//   - rescheduleTime: 重新调度的时间点
//
// 判断逻辑：
//  1. 如果分配属于正在进行的部署且没有被标记为可重新调度 -> 不允许重新调度
//  2. 如果分配被标记为强制重新调度(ForceReschedule) -> 立即重新调度
//  3. 根据不同场景计算重新调度时间：
//     - 断开连接场景：使用 RescheduleTimeOnDisconnect
//     - Unknown 状态且匹配 followup eval：使用 NextRescheduleTimeByTime
//     - 默认场景：使用 NextRescheduleTime
//  4. 判断是否在调度窗口内（rescheduleWindowSize = 5秒）
func updateByReschedulable(logger log.Logger, alloc *structs.Allocation, now time.Time, evalID string, d *structs.Deployment, isDisconnecting bool) (rescheduleNow, rescheduleLater bool, rescheduleTime time.Time) {
	// 【部署检查】如果分配属于正在进行的活跃部署
	// 只有被显式标记为可重新调度的分配才允许重新调度
	// 这是为了防止在部署过程中频繁创建失败的分配
	if d != nil && alloc.DeploymentID == d.ID && d.Active() && !alloc.DesiredTransition.ShouldReschedule() {
		logger.Warn("重调度: 分配被活跃部署阻止",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
			"deployment_id", alloc.DeploymentID,
			"deployment_status", d.Status,
			"should_reschedule", alloc.DesiredTransition.ShouldReschedule(),
		)
		return
	}

	// 【强制重新调度检查】如果分配被标记为强制重新调度
	// 操作者可以强制要求重新调度，即使分配不满足常规条件
	if alloc.DesiredTransition.ShouldForceReschedule() {
		logger.Warn("重调度: 分配被强制要求重新调度",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
		)
		rescheduleNow = true
	}

	// 【计算重新调度时间和资格】根据不同场景计算
	var eligible bool
	switch {
	case isDisconnecting:
		// 场景1: 节点断开连接
		// 使用 RescheduleTimeOnDisconnect 计算调度时间
		rescheduleTime, eligible = alloc.RescheduleTimeOnDisconnect(now)
		logger.Warn("重调度: 检查断开连接场景",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
			"reschedule_time", rescheduleTime,
			"eligible", eligible,
		)

	case alloc.ClientStatus == structs.AllocClientStatusUnknown && alloc.FollowupEvalID == evalID:
		// 场景2: 分配状态为 Unknown 且当前 Evaluation 是其后续 Evaluation
		// 使用上次断开连接时间计算重新调度时间
		lastDisconnectTime := alloc.LastUnknown()
		rescheduleTime, eligible = alloc.NextRescheduleTimeByTime(lastDisconnectTime)
		logger.Warn("重调度: 检查 unknown 客户端状态场景",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
			"last_disconnect_time", lastDisconnectTime,
			"reschedule_time", rescheduleTime,
			"eligible", eligible,
		)

	default:
		// 场景3: 默认情况（失败的分配）
		// 使用 NextRescheduleTime 计算重新调度时间
		// 这会考虑 ReschedulePolicy 的 Attempts、Interval、Delay 等参数
		rescheduleTime, eligible = alloc.NextRescheduleTime()
		// 记录详细的重新调度资格信息
		policy := alloc.ReschedulePolicy()
		failTime := alloc.LastEventTime()
		logger.Warn("重调度: 检查 NextRescheduleTime",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
			"task_group", alloc.TaskGroup,
			"client_status", alloc.ClientStatus,
			"desired_status", alloc.DesiredStatus,
			"reschedule_time", rescheduleTime,
			"eligible", eligible,
			"fail_time", failTime,
			"now", now,
			"policy_attempts", func() int {
				if policy != nil {
					return policy.Attempts
				}
				return -1
			}(),
			"policy_unlimited", func() bool {
				if policy != nil {
					return policy.Unlimited
				}
				return false
			}(),
			"policy_delay", func() time.Duration {
				if policy != nil {
					return policy.Delay
				}
				return 0
			}(),
			"policy_interval", func() time.Duration {
				if policy != nil {
					return policy.Interval
				}
				return 0
			}(),
			"followup_eval_id", alloc.FollowupEvalID,
			"eval_id", evalID,
			"next_allocation", alloc.NextAllocation,
			"reschedule_tracker_events", func() int {
				if alloc.RescheduleTracker != nil {
					return len(alloc.RescheduleTracker.Events)
				}
				return 0
			}(),
		)
	}

	// 【立即重新调度判断】满足以下条件之一可以立即重新调度：
	// 1. 当前 Evaluation 是分配的后续 Evaluation（FollowupEvalID == evalID）
	// 2. 重新调度时间在调度窗口内（rescheduleWindowSize = 5秒）
	//    这允许稍微提前到达的 Evaluation 也能触发重新调度
	if eligible && (alloc.FollowupEvalID == evalID || rescheduleTime.Sub(now) <= rescheduleWindowSize) {
		rescheduleNow = true
		logger.Warn("重调度: 符合立即调度条件 - 在窗口内",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
			"reschedule_time", rescheduleTime,
			"time_until_reschedule", rescheduleTime.Sub(now),
			"window_size", rescheduleWindowSize,
		)
		return
	}

	// 【延迟重新调度判断】满足以下条件需要延迟重新调度：
	// 1. 分配有资格重新调度(eligible=true)
	// 2. 分配没有后续 Evaluation（FollowupEvalID == ""）或者正在断开连接
	// 这会创建一个新的后续 Evaluation，在指定时间触发
	if eligible && (alloc.FollowupEvalID == "" || isDisconnecting) {
		rescheduleLater = true
		logger.Warn("重调度: 符合延迟调度条件",
			"alloc_id", alloc.ID,
			"job_id", alloc.JobID,
			"reschedule_time", rescheduleTime,
			"time_until_reschedule", rescheduleTime.Sub(now),
		)
	}

	// 记录最终决定
	logger.Warn("重调度: 最终决定",
		"alloc_id", alloc.ID,
		"job_id", alloc.JobID,
		"reschedule_now", rescheduleNow,
		"reschedule_later", rescheduleLater,
		"eligible", eligible,
	)

	return
}

// filterByTerminal filters out terminal allocs
func filterByTerminal(untainted allocSet) (nonTerminal allocSet) {
	nonTerminal = make(map[string]*structs.Allocation)
	for id, alloc := range untainted {
		if !alloc.TerminalStatus() {
			nonTerminal[id] = alloc
		}
	}
	return
}

// filterByDeployment filters allocations into two sets, those that match the
// given deployment ID and those that don't
func (a allocSet) filterByDeployment(id string) (match, nonmatch allocSet) {
	match = make(map[string]*structs.Allocation)
	nonmatch = make(map[string]*structs.Allocation)
	for _, alloc := range a {
		if alloc.DeploymentID == id {
			match[alloc.ID] = alloc
		} else {
			nonmatch[alloc.ID] = alloc
		}
	}
	return
}

// delayByStopAfter returns a delay for any lost allocation that's got a
// disconnect.stop_on_client_after configured
func (a allocSet) delayByStopAfter() (later []*delayedRescheduleInfo) {
	now := time.Now().UTC()
	for _, a := range a {
		if !a.ShouldClientStop() {
			continue
		}

		t := a.WaitClientStop()

		if t.After(now) {
			later = append(later, &delayedRescheduleInfo{
				allocID:        a.ID,
				alloc:          a,
				rescheduleTime: t,
			})
		}
	}
	return later
}

// delayByLostAfter returns a delay for any unknown allocation
// that has disconnect.lost_after configured
func (a allocSet) delayByLostAfter(now time.Time) ([]*delayedRescheduleInfo, error) {
	var later []*delayedRescheduleInfo

	for _, alloc := range a {
		timeout := alloc.DisconnectTimeout(now)
		if !timeout.After(now) {
			return nil, errors.New("unable to computing disconnecting timeouts")
		}

		later = append(later, &delayedRescheduleInfo{
			allocID:        alloc.ID,
			alloc:          alloc,
			rescheduleTime: timeout,
		})
	}

	return later, nil
}

// filterOutByClientStatus returns all allocs from the set without the specified client status.
func (a allocSet) filterOutByClientStatus(clientStatuses ...string) allocSet {
	allocs := make(allocSet)
	for _, alloc := range a {
		if !slices.Contains(clientStatuses, alloc.ClientStatus) {
			allocs[alloc.ID] = alloc
		}
	}

	return allocs
}

// filterByClientStatus returns allocs from the set with the specified client status.
func (a allocSet) filterByClientStatus(clientStatus string) allocSet {
	allocs := make(allocSet)
	for _, alloc := range a {
		if alloc.ClientStatus == clientStatus {
			allocs[alloc.ID] = alloc
		}
	}

	return allocs
}

// allocNameIndex is used to select allocation names for placement or removal
// given an existing set of placed allocations.
type allocNameIndex struct {
	job, taskGroup string
	count          int
	b              structs.Bitmap

	// duplicates is used to store duplicate allocation indexes which are
	// currently present within the index tracking. The map key is the index,
	// and the current count of duplicates. The map is only accessed within a
	// single routine and multiple times per job scheduler invocation,
	// therefore no lock is used.
	duplicates map[uint]int
}

// newAllocNameIndex returns an allocNameIndex for use in selecting names of
// allocations to create or stop. It takes the job and task group name, desired
// count and any existing allocations as input.
func newAllocNameIndex(job, taskGroup string, count int, in allocSet) *allocNameIndex {

	bitMap, duplicates := bitmapFrom(in, uint(count))

	return &allocNameIndex{
		count:      count,
		b:          bitMap,
		job:        job,
		taskGroup:  taskGroup,
		duplicates: duplicates,
	}
}

// bitmapFrom creates a bitmap from the given allocation set and a minimum size
// maybe given. The size of the bitmap is as the larger of the passed minimum
// and the maximum alloc index of the passed input (byte aligned).
func bitmapFrom(input allocSet, minSize uint) (structs.Bitmap, map[uint]int) {
	var max uint
	for _, a := range input {
		if num := a.Index(); num > max {
			max = num
		}
	}

	if l := uint(len(input)); minSize < l {
		minSize = l
	}

	if max < minSize {
		max = minSize
	} else if max%8 == 0 {
		// This may be possible if the job was scaled down. We want to make sure
		// that the max index is not byte-aligned otherwise we will overflow
		// the bitmap.
		max++
	}

	if max == 0 {
		max = 8
	}

	// byteAlign the count
	if remainder := max % 8; remainder != 0 {
		max = max + 8 - remainder
	}

	bitmap, err := structs.NewBitmap(max)
	if err != nil {
		panic(err)
	}

	// Initialize our duplicates mapping, allowing us to store a non-nil map
	// at the cost of 48 bytes.
	duplicates := make(map[uint]int)

	// Iterate through the allocSet input and hydrate the bitmap. We check that
	// the bitmap does not contain the index first, so we can track duplicate
	// entries.
	for _, a := range input {

		allocIndex := a.Index()

		if bitmap.Check(allocIndex) {
			duplicates[allocIndex]++
		} else {
			bitmap.Set(allocIndex)
		}
	}

	return bitmap, duplicates
}

// Highest removes and returns the highest n used names. The returned set
// can be less than n if there aren't n names set in the index
func (a *allocNameIndex) Highest(n uint) map[string]struct{} {
	h := make(map[string]struct{}, n)
	for i := a.b.Size(); i > uint(0) && uint(len(h)) < n; i-- {
		// Use this to avoid wrapping around b/c of the unsigned int
		idx := i - 1
		if a.b.Check(idx) {
			a.b.Unset(idx)
			h[structs.AllocName(a.job, a.taskGroup, idx)] = struct{}{}
		}
	}

	return h
}

// IsDuplicate checks whether the passed allocation index is duplicated within
// the tracking.
func (a *allocNameIndex) IsDuplicate(idx uint) bool {
	val, ok := a.duplicates[idx]
	return ok && val > 0
}

// UnsetIndex unsets the index as having its name used
func (a *allocNameIndex) UnsetIndex(idx uint) {

	// If this index is a duplicate, remove the duplicate entry. Otherwise, we
	// can remove it from the bitmap tracking.
	if num, ok := a.duplicates[idx]; ok {
		if num--; num == 0 {
			delete(a.duplicates, idx)
		}
	} else {
		a.b.Unset(idx)
	}
}

// NextCanaries returns the next n names for use as canaries and sets them as
// used. The existing canaries and destructive updates are also passed in.
func (a *allocNameIndex) NextCanaries(n uint, existing, destructive allocSet) []string {
	next := make([]string, 0, n)

	// Create a name index
	existingNames := existing.nameSet()

	// First select indexes from the allocations that are undergoing
	// destructive updates. This way we avoid duplicate names as they will get
	// replaced. As this process already takes into account duplicate checking,
	// we can discard the duplicate mapping when generating the bitmap.
	dmap, _ := bitmapFrom(destructive, uint(a.count))
	remainder := n
	for _, idx := range dmap.IndexesInRange(true, uint(0), uint(a.count)-1) {
		name := structs.AllocName(a.job, a.taskGroup, uint(idx))
		if _, used := existingNames[name]; !used {
			next = append(next, name)
			a.b.Set(uint(idx))

			// If we have enough, return
			remainder = n - uint(len(next))
			if remainder == 0 {
				return next
			}
		}
	}

	// Get the set of unset names that can be used
	for _, idx := range a.b.IndexesInRange(false, uint(0), uint(a.count)-1) {
		name := structs.AllocName(a.job, a.taskGroup, uint(idx))
		if _, used := existingNames[name]; !used {
			next = append(next, name)
			a.b.Set(uint(idx))

			// If we have enough, return
			remainder = n - uint(len(next))
			if remainder == 0 {
				return next
			}
		}
	}

	// We have exhausted the preferred and free set. Pick starting from n to
	// n+remainder, to avoid overlapping where possible. An example is the
	// desired count is 3 and we want 5 canaries. The first 3 canaries can use
	// index [0, 1, 2] but after that we prefer picking indexes [4, 5] so that
	// we do not overlap. Once the canaries are promoted, these would be the
	// allocations that would be shut down as well.
	for i := uint(a.count); i < uint(a.count)+remainder; i++ {
		name := structs.AllocName(a.job, a.taskGroup, i)
		next = append(next, name)
	}

	return next
}

// Next returns the next n names for use as new placements and sets them as
// used.
func (a *allocNameIndex) Next(n uint) []string {
	next := make([]string, 0, n)

	// Get the set of unset names that can be used
	remainder := n
	for _, idx := range a.b.IndexesInRange(false, uint(0), uint(a.count)-1) {
		next = append(next, structs.AllocName(a.job, a.taskGroup, uint(idx)))
		a.b.Set(uint(idx))

		// If we have enough, return
		remainder = n - uint(len(next))
		if remainder == 0 {
			return next
		}
	}

	// We have exhausted the free set, now just pick overlapping indexes
	var i uint
	for i = 0; i < remainder; i++ {
		next = append(next, structs.AllocName(a.job, a.taskGroup, i))
		a.b.Set(i)
	}

	return next
}
