"""多源多汇最小费用割求解。

自行实现 64 位整数容量的 Dinic 最大流算法（本题最大总费用为
2000 * 10**9 = 2*10**12，远小于 2**63，Python 整数按 64 位语义使用）。

建模方式：超级源 -> 每个污染源区域（容量 INF），每个保护区域 -> 超级汇
（容量 INF），每条管段为一条有向边（容量 = 关闭费用）。INF 取
全部管段费用之和 + 1，严格大于任何真实割的费用，因此最小割绝不会
切断 INF 边，污染源恒在源侧、保护区恒在汇侧。

在所有最低费用割中，源侧区域集合按包含关系最小的方案唯一：最大流
求得后，残量网络中从超级源可达的节点集合，恰好是所有最小割源侧的
交集（最小割源侧关于交、并封闭）。对该集合取"源侧 -> 汇侧"的管段
即得唯一的最小切断清单，与管段提交顺序无关。
"""

from collections import deque

# 初始推送流量上界，2**62 在 64 位整数范围内且大于任何可行流量
INF_PUSH = 1 << 62


class _Edge:
    __slots__ = ("to", "rev", "cap")

    def __init__(self, to, rev, cap):
        self.to = to
        self.rev = rev
        self.cap = cap


class ResidualCutInfeasible(Exception):
    """保持开启约束使污染源与保护区之间存在无法切断的路径。"""


class Dinic:
    """64 位整数容量的 Dinic 最大流。"""

    def __init__(self, num_nodes):
        self._graph = [[] for _ in range(num_nodes)]

    def add_edge(self, frm, to, cap):
        fwd = _Edge(to, len(self._graph[to]), cap)
        rev = _Edge(frm, len(self._graph[frm]), 0)
        self._graph[frm].append(fwd)
        self._graph[to].append(rev)

    def _bfs_levels(self, source):
        level = [-1] * len(self._graph)
        level[source] = 0
        queue = deque([source])
        while queue:
            node = queue.popleft()
            for edge in self._graph[node]:
                if edge.cap > 0 and level[edge.to] < 0:
                    level[edge.to] = level[node] + 1
                    queue.append(edge.to)
        return level

    def _dfs(self, node, sink, pushed, level, it):
        if node == sink or pushed == 0:
            return pushed
        edges = self._graph[node]
        while it[node] < len(edges):
            edge = edges[it[node]]
            if edge.cap > 0 and level[edge.to] == level[node] + 1:
                flow = self._dfs(edge.to, sink, min(pushed, edge.cap), level, it)
                if flow > 0:
                    edge.cap -= flow
                    self._graph[edge.to][edge.rev].cap += flow
                    return flow
            it[node] += 1
        return 0

    def max_flow(self, source, sink):
        flow = 0
        while True:
            level = self._bfs_levels(source)
            if level[sink] < 0:
                return flow
            it = [0] * len(self._graph)
            while True:
                pushed = self._dfs(source, sink, INF_PUSH, level, it)
                if pushed == 0:
                    break
                flow += pushed

    def reachable_from(self, source):
        """残量网络（cap > 0）中从 source 可达的节点集合。"""
        seen = [False] * len(self._graph)
        seen[source] = True
        stack = [source]
        while stack:
            node = stack.pop()
            for edge in self._graph[node]:
                if edge.cap > 0 and not seen[edge.to]:
                    seen[edge.to] = True
                    stack.append(edge.to)
        return seen


def solve_min_cut(plan):
    """求解方案的最小费用隔断。

    plan: {"zones": [...], "segments": [{"id", "from", "to", "cost"}],
           "sources": [...], "protections": [...]}
    返回: {"source_zones": 升序源侧区域, "cut_segments": 升序切断管段,
           "total_cost": 总费用}
    """
    zones = plan["zones"]
    segments = plan["segments"]
    sources = plan["sources"]
    protections = plan["protections"]

    ordered = sorted(zones)
    index = {zone: i + 2 for i, zone in enumerate(ordered)}
    super_source, super_sink = 0, 1

    dinic = Dinic(len(ordered) + 2)
    total_capacity = sum(seg["cost"] for seg in segments)
    infinity = total_capacity + 1  # 严格大于任何真实割的费用

    for zone in sources:
        dinic.add_edge(super_source, index[zone], infinity)
    for zone in protections:
        dinic.add_edge(index[zone], super_sink, infinity)
    for seg in segments:
        dinic.add_edge(index[seg["from"]], index[seg["to"]], seg["cost"])

    dinic.max_flow(super_source, super_sink)
    reachable = dinic.reachable_from(super_source)

    on_source_side = {zone: reachable[index[zone]] for zone in ordered}
    source_zones = [zone for zone in ordered if on_source_side[zone]]
    crossing = [
        seg for seg in segments
        if on_source_side[seg["from"]] and not on_source_side[seg["to"]]
    ]
    return {
        "source_zones": source_zones,
        "cut_segments": sorted(seg["id"] for seg in crossing),
        "total_cost": sum(seg["cost"] for seg in crossing),
    }


def solve_residual_min_cut(plan, closed_segment_ids, keep_open_segment_ids=None):
    """在现场约束后的有向网络上，求追加关闭费用最小的隔断。

    现场已关闭的管段视为已移除；必须保持开启的管段视为不可切断（容量为
    INF）。算法仍沿用 solve_min_cut 的建模与裁决：所有最低费用割中取源侧
    区域集合按包含关系最小的唯一方案，结果按字典序升序。plan 必须是调用方
    （持久层/接口/校验）共同冻结的同一版本，不得混入后来修订的同名管段费用。

    若仅由保持开启管段构成的路径仍连通任一污染源与任一保护区，则关闭其余
    任何可切断管段都无法隔断，抛出 ResidualCutInfeasible，调用方不得落库。
    保持开启管段上容量为 INF；它严格大于所有可切断管段费用之和，因此可行
    网络的最低费用割绝不会切断保持开启边或源/汇约束边。

    返回::

        {
          "closed_segments": 升序的现场已关闭管段,
          "keep_open_segments": 升序的必须保持开启管段（提供该约束时存在）,
          "additional_segments": 升序的新增建议关闭管段（已隔断时为空）,
          "additional_cost": 新增关闭费用之和,
          "witness": {
            "source_zones": 约束网络最小割的源侧区域（升序）,
            "cut_segments": 已关闭∪新增的合并隔断清单（升序）,
            "total_cost": 合并清单按冻结方案费用求和,
          },
        }
    """
    closed = set(closed_segment_ids)
    keep_open = set(keep_open_segment_ids or ())
    remaining = [seg for seg in plan["segments"] if seg["id"] not in closed]
    optional = [seg for seg in remaining if seg["id"] not in keep_open]

    def reachable_with_required_edges_only():
        adjacency = {}
        for seg in remaining:
            if seg["id"] in keep_open:
                adjacency.setdefault(seg["from"], []).append(seg["to"])
        seen = set(plan["sources"])
        stack = list(plan["sources"])
        while stack:
            node = stack.pop()
            for nxt in adjacency.get(node, []):
                if nxt not in seen:
                    if nxt in plan["protections"]:
                        return True
                    seen.add(nxt)
                    stack.append(nxt)
        return False

    if reachable_with_required_edges_only():
        raise ResidualCutInfeasible(
            "a required-open path still connects a pollution source to a protected zone"
        )

    zones = plan["zones"]
    ordered = sorted(zones)
    index = {zone: i + 2 for i, zone in enumerate(ordered)}
    super_source, super_sink = 0, 1
    dinic = Dinic(len(ordered) + 2)
    total_optional_capacity = sum(seg["cost"] for seg in optional)
    infinity = total_optional_capacity + 1

    for zone in plan["sources"]:
        dinic.add_edge(super_source, index[zone], infinity)
    for zone in plan["protections"]:
        dinic.add_edge(index[zone], super_sink, infinity)
    for seg in optional:
        dinic.add_edge(index[seg["from"]], index[seg["to"]], seg["cost"])
    for seg in remaining:
        if seg["id"] in keep_open:
            dinic.add_edge(index[seg["from"]], index[seg["to"]], infinity)

    dinic.max_flow(super_source, super_sink)
    reachable = dinic.reachable_from(super_source)
    on_source_side = {zone: reachable[index[zone]] for zone in ordered}
    source_zones = [zone for zone in ordered if on_source_side[zone]]
    additional = sorted(
        seg["id"]
        # 保持开启边不可能穿越最低费用割；按方案原始顺序收集后统一排序。
        for seg in optional
        if on_source_side[seg["from"]] and not on_source_side[seg["to"]]
    )
    cost_by_id = {seg["id"]: seg["cost"] for seg in plan["segments"]}
    additional_cost = sum(cost_by_id[seg_id] for seg_id in additional)

    merged = sorted(closed | set(additional))
    outcome = {
        "closed_segments": sorted(closed),
        "additional_segments": additional,
        "additional_cost": additional_cost,
        "witness": {
            "source_zones": source_zones,
            "cut_segments": merged,
            "total_cost": sum(cost_by_id[seg_id] for seg_id in merged),
        },
    }
    if keep_open_segment_ids is not None:
        outcome["keep_open_segments"] = sorted(keep_open)
    return outcome
