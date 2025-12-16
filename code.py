from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    aggregatable: bool = False


@dataclass(frozen=True)
class Table:
    name: str
    alias: str
    role: str
    columns: Dict[str, Column]


@dataclass(frozen=True)
class Join:
    left: Table
    right: Table
    join_type: str
    condition: str


@dataclass
class QueryPlan:
    base_table: Table
    joins: List[Join]
    select: List[str]
    aggregates: Dict[str, str]
    filters: List[str]
    group_by: List[str]


import yaml
from pathlib import Path


class MetadataLoader:
    def __init__(self, path: str):
        self.base = Path(path)

    def load_tables(self) -> Dict[str, Table]:
        raw = yaml.safe_load(open(self.base / "tables.yaml"))
        tables = {}

        for name, t in raw["tables"].items():
            cols = {
                c: Column(
                    name=c,
                    data_type=v["type"],
                    aggregatable=v.get("aggregatable", False)
                )
                for c, v in t["columns"].items()
            }
            tables[name] = Table(name, t["alias"], t["role"], cols)

        return tables

    def load_join_graph(self) -> list[dict]:
        return yaml.safe_load(open(self.base / "joins.yaml"))["joins"]



class JoinResolver:
    def __init__(self, join_graph: list[dict], tables: Dict[str, Table]):
        self.graph = join_graph
        self.tables = tables

    def resolve(self, base: str, targets: List[str]) -> List[Join]:
        joins = []

        for target in targets:
            for j in self.graph:
                if j["from"] == base and j["to"] == target:
                    l = self.tables[j["from"]]
                    r = self.tables[j["to"]]

                    condition = " AND ".join(
                        f"{l.alias}.{k['left']} = {r.alias}.{k['right']}"
                        for k in j["keys"]
                    )

                    joins.append(Join(l, r, j["type"], condition))

        return joins



class QueryPlanner:
    def __init__(self, tables: Dict[str, Table], join_resolver: JoinResolver):
        self.tables = tables
        self.join_resolver = join_resolver

    def build(self, semantics: dict) -> QueryPlan:
        base = semantics["base_entity"]
        dimensions = semantics.get("dimensions", [])
        measures = semantics.get("measures", [])
        filters = semantics.get("filters", [])

        base_table = self.tables[base]

        joins = self.join_resolver.resolve(base, dimensions)

        select = []
        group_by = []
        aggregates = {}

        for dim in dimensions:
            table = self.tables[dim]
            for col in table.columns.values():
                if col.data_type == "string":
                    select.append(f"{table.alias}.{col.name}")
                    group_by.append(f"{table.alias}.{col.name}")
                    break

        for m in measures:
            aggregates[m["column"]] = m["agg"]

        return QueryPlan(
            base_table=base_table,
            joins=joins,
            select=select,
            aggregates=aggregates,
            filters=filters,
            group_by=group_by
        )



class SQLBuilder:
    def build(self, plan: QueryPlan) -> str:
        parts = []

        parts.extend(plan.select)

        for col, fn in plan.aggregates.items():
            parts.append(f"{fn.upper()}({col}) AS {fn}_{col.split('.')[-1]}")

        sql = f"SELECT {', '.join(parts)} FROM {plan.base_table.name} {plan.base_table.alias}"

        for j in plan.joins:
            sql += (
                f" {j.join_type} JOIN {j.right.name} {j.right.alias}"
                f" ON {j.condition}"
            )

        if plan.filters:
            sql += " WHERE " + " AND ".join(plan.filters)

        if plan.group_by:
            sql += " GROUP BY " + ", ".join(plan.group_by)

        return sql



loader = MetadataLoader("/Workspace/metadata")

tables = loader.load_tables()
join_graph = loader.load_join_graph()

resolver = JoinResolver(join_graph, tables)
planner = QueryPlanner(tables, resolver)

semantics = {
    "base_entity": "employee_fact",
    "dimensions": ["department_dim"],
    "measures": [
        {"column": "e.employee_id", "agg": "count"}
    ],
    "filters": ["e.termination_date IS NOT NULL"]
}

plan = planner.build(semantics)
sql = SQLBuilder().build(plan)

print(sql)



