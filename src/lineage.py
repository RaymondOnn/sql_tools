import hashlib
import logging
import re
from collections import namedtuple
from typing import Any, NoReturn, Optional, Self

import polars as pl
from sqlglot import exp, parse_one
from sqlglot import optimizer as opt
from sqlglot.errors import OptimizeError
from sqlglot.expressions import Expression

LOG = logging.getLogger(__name__)

MUTATION_CLAUSES = {
    "where": "where",
    "group": "groupby",
    "having": "having",
}
NON_SELECT_CLAUSES = {"join", "where", "groupby", "having"}
LINEAGE_TABLE_NAME = 'SOME_DB.SOME_SCHEMA.SOME_TABLE'



Col = namedtuple("Col", ["column", "table", "clause"])
Column = namedtuple("Column", ["alias", "from_"])
ColumnAttr = namedtuple("ColumnAttr", ["table_alias", "name", "func", "sql"])
ColumnSource = namedtuple("ColumnSource", ["attr", "ref"])
Object = namedtuple("Object", ["name", "type"])


# TODO: Create stmt vs Select stmt
# TODO: Subqueries
class ColumnLineage:
    ast: Expression | None
    default_dialect: str
    contexts: dict[str, Any]
    root_context: dict[str, Any]
    lineage: list[dict[str, Any]]
    object: Object

    def __init__(self, default_dialect: str = "snowflake") -> None:
        self.ast = None
        self.sql_client = None
        self.dialect = default_dialect
        self.contexts = {}
        self.root_context = {}

    def build(
        self,
        sql: str,
        dialect: str = "snowflake",
        object_name: str | None = None,
        object_type: str | None = None,
    ) -> Self:
        self.ast = parse_one(self._remove_comments(sql), read=dialect)
        ast = self._qualify_query_or_raise(self.ast)
        self._record_all_contexts(ast)
        # pprint(self.contexts)
        self.lineage = self._resolve_column_lineage(
            dest_object_name=object_name,
            dest_object_type=object_type,
        )
        return self

    # TODO
    def load(self):
        raise NotImplementedError
    
    # TODO
    def find_dependencies(
        self, column_name: str, object_name: str
    ) -> pl.DataFrame:
        raise NotImplementedError

    # TODO
    def find_dependents(
        self, column_name: str, object_name: str
    ) -> pl.DataFrame:
        raise NotImplementedError

    def _qualify_query_or_raise(
        self, 
        ast: Expression, 
        schema: dict | None = None
    ) -> Expression | NoReturn:
        try:
            ast = opt.normalize_identifiers.normalize_identifiers(ast)
            ast = opt.qualify_tables.qualify_tables(ast)
            ast = opt.isolate_table_selects.isolate_table_selects(ast)
            ast = opt.qualify_columns.qualify_columns(ast, schema)
            return ast
        except OptimizeError as e:
            raise e

    # TODO
    def _extract_object_attr_or_raise(self, ast) -> bool | NoReturn:
        if isinstance(ast, exp.DDL):
            self.object = Object(ast.this.name, ast.kind)
        elif isinstance(ast, exp.DML):
            pass
        elif isinstance(ast, exp.Query):
            ...
        else:
            raise Exception(f"Unsupported statement type: {type(ast)}")
        return True

    def _hash_scope(self, scope) -> str:
        sql = scope.expression.sql()
        hashkey = "hash_" + hashlib.md5(sql.encode("utf-8")).hexdigest()
        return hashkey

    def _merge_contexts(self, ctx1: dict, ctx2: dict) -> dict[str, Any]:
        merged = {}

        # Assuming the table aliases cannot be the same within the same query
        ctx1.get("tables", {}).update(ctx2.get("tables", {}))
        merged["tables"] = ctx1.get("tables", {})
        merged["type"] = ctx1["type"]  # TODO
        merged["index"] = min(ctx1["index"], ctx2["index"])

        merged["select"] = []
        for col1 in ctx1.get("select", []):
            for col2 in ctx2.get("select", []):
                if col1.alias == col2.alias:
                    merged["select"].append(
                        Column(col1.alias, col1.from_.union(col2.from_))
                    )
                    break
            else:
                merged["select"].append(col2)

        for key in list(MUTATION_CLAUSES.values()) + ["joins"]:
            cols = ctx1.get(key, []).extend(ctx2.get(key, []))
            if cols:
                merged[key] = cols
        return merged

    def _record_all_contexts(self, ast: Expression) -> None:
        ast = opt.qualify.qualify(ast)
        if not (root := opt.scope.build_scope(ast)):
            raise Exception(
                f"Could not build scope. Query likely not valid: {ast.sql()}"
            )

        list_of_contexts: list[str] = []
        idx = 0

        for i, context in enumerate(root.traverse()):
            match context.expression.key:
                case "union" | "intersect" | "except":
                    if len(list_of_contexts) < 2:
                        raise Exception("Not enough contexts to merge")

                    ctx_1 = self.contexts.pop(list_of_contexts[-2])
                    ctx_2 = self.contexts.pop(list_of_contexts[-1])
                    ctx_dict = self._merge_contexts(ctx_1, ctx_2)
                case _:
                    ctx_dict = self._record_context(context)
                    ctx_dict["index"] = idx
                    ctx_dict["type"] = context.scope_type.name

            ctx_dict["sql"] = context.expression.sql()

            # Update variables
            hashkey = self._hash_scope(context)
            self.contexts[hashkey] = ctx_dict
            list_of_contexts.append(hashkey)

            idx = ctx_dict["index"] + 1

    def _record_context(self, context: opt.scope.Scope) -> dict[str, Any]:
        ctx_dict: dict[str, Any] = {}

        # OUTPUT_COLUMNS
        selects = context.expression.select()
        output_cols = []
        # TODO: Resolve select star
        try:
            for col in selects:
                match col.this.key:
                    case "column":
                        output_cols.append(
                            Column(
                                col.alias,
                                {
                                    ColumnSource(
                                        ColumnAttr(
                                            col.this.table,
                                            col.this.name,
                                            None,
                                            col.sql(),
                                        ),
                                        None,
                                    )
                                },
                            )
                        )
                    case "star":
                        output_cols.append(
                            Column(
                                "*",
                                {
                                    ColumnSource(
                                        ColumnAttr(
                                            col.table or None,
                                            col.name,
                                            None,
                                            col.sql(),
                                        ),
                                        None,
                                    )
                                },
                            )
                        )
                    case "literal" | "null":
                        output_cols.append(
                            Column(
                                col.alias,
                                {
                                    ColumnSource(
                                        ColumnAttr(
                                            None,
                                            col.this.name,
                                            None,
                                            col.sql(),
                                        ),
                                        None,
                                    )
                                },
                            )
                        )
                    case "window":
                        partition_cols = {
                            ColumnAttr(
                                part_.table, part_.name, None, part_.sql
                            )
                            for part_ in col.this.args.get("partition_by")
                        }
                        order_cols = {
                            ColumnAttr(
                                order_.this.table, order_.this.name, None, None
                            )
                            for order_ in col.this.args.get(
                                "order"
                            ).expressions
                        }
                        if col.this.this.args.get("expressions"):
                            # window function wuth input col i.e func(col)
                            output_cols.append(
                                Column(
                                    col.alias,
                                    {
                                        ColumnSource(
                                            ColumnAttr(
                                                col.this.this.expressions[
                                                    0
                                                ].table,
                                                col.this.this.expressions[
                                                    0
                                                ].name,
                                                col.this.this.this,
                                                col.sql(),
                                            ),
                                            partition_cols.union(order_cols),
                                        )
                                    },
                                )
                            )
                        else:
                            # window function only i.e func()

                            output_cols.append(
                                Column(
                                    col.alias,
                                    {
                                        ColumnSource(
                                            ColumnAttr(
                                                None, None, None, col.sql()
                                            ),
                                            partition_cols.union(order_cols),
                                        )
                                    },
                                )
                            )
                    case _:  # if it is a function
                        output_cols.append(
                            Column(
                                col.alias,
                                {
                                    ColumnSource(
                                        ColumnAttr(
                                            col.this.this.table,
                                            col.this.this.name,
                                            col.this.key,
                                            col.sql(),
                                        ),
                                        None,
                                    )
                                },
                            )
                        )
            ctx_dict["select"] = output_cols
        except Exception as e:
            LOG.error(e)

        # TABLE REFERENCES
        table_refs: dict[str, Any] = {}
        if context.sources:
            for k, v in context.sources.items():
                match v:
                    case str():
                        table_refs[k] = v
                    case exp.Table():
                        table_refs[k] = v.name
                    case opt.scope.Scope():
                        table_refs[k] = self._hash_scope(v)
                    case _:
                        raise Exception("Unhandled type for scope source")
        else:
            for from_ in context.expression.find_all(exp.From):
                table_refs.update({from_.alias: from_.name})
        ctx_dict["tables"] = table_refs

        # JOIN KEYS
        if expr := context.expression.args.get("joins"):
            joins = set()
            for join in expr:  # scope.find_all(exp.Join):
                on_ = join.args.get("on")
                joins.add(
                    ColumnAttr(
                        on_.left.table, on_.left.name, None, on_.left.sql()
                    )
                )
                joins.add(
                    ColumnAttr(
                        on_.right.table, on_.right.name, None, on_.right.sql()
                    )
                )
            ctx_dict["join"] = joins

        for clause in MUTATION_CLAUSES:
            if expr := context.expression.args.get(clause):
                cols = set()
                match clause:
                    case "where":
                        if expr.find(exp.And):
                            for exists_ in expr.find_all(exp.Exists):
                                exists_.find(exp.Select).expressions.pop()

                            for and_ in expr.find_all(exp.And):
                                for col in and_.expression.find_all(
                                    exp.Column
                                ):
                                    cols.add(
                                        ColumnAttr(
                                            col.table,
                                            col.name,
                                            None,
                                            col.sql(),
                                        )
                                    )

                            for col in and_.this.find_all(exp.Column):
                                cols.add(
                                    ColumnAttr(
                                        col.table, col.name, None, col.sql()
                                    )
                                )
                        else:
                            for col in expr.find_all(exp.Column):
                                cols.add(
                                    ColumnAttr(
                                        col.table, col.name, None, col.sql()
                                    )
                                )

                    case _:
                        for col in expr.find_all(exp.Column):
                            cols.add(
                                ColumnAttr(
                                    col.table, col.name, None, col.sql()
                                )
                            )
                ctx_dict[MUTATION_CLAUSES[clause]] = cols
        return ctx_dict

    # TODO
    def _resolve_src_column(
        self, 
        table: str, 
        column: str, 
        clause: str
    ) -> set[Col] | NoReturn:
        """Resolve the source column(s) for a given column."""
        if table is None or not table.startswith("hash_"):
            item = {Col(column, table, clause)}
            # print('stop', column, table, item)
            assert item is not None
            return item
        else:
            src_ctx = self.contexts.get(table, {})
            src_ctx_tables = src_ctx.get("tables", {})
            for col in src_ctx.get("select", []):
                resolved_cols: set[Col] = set()
                if column == col.alias:
                    col_refs = col.from_
                    if len(col_refs) == 1:
                        col_ref = next(iter(col_refs))
                        resolved_cols.union(self._resolve_src_column(
                            table=src_ctx_tables.get(col_ref.attr.table_alias),
                            column=col_ref.attr.name,
                            clause=clause,
                        ))
                    elif len(col_refs) > 1:
                        for col_ref in col_refs:
                            col_name = col_ref.get("name")
                            col_table = src_ctx_tables.get(
                                col_ref.get("table_alias")
                            )
                            if col_name and col_table:
                                resolved_cols.union(
                                    self._resolve_src_column(
                                        table=col_table,
                                        column=col_name,
                                        clause=clause,
                                    )
                                )
                    else:
                        raise ValueError(
                            f"No source columns found for {column} in {table}."
                        )
            assert resolved_cols is not None
            return resolved_cols

    def _resolve_column_lineage(
        self,
        dest_object_name: str | None = None,
        dest_object_type: str | None = None,
    ) -> list[dict[str, Any]] | NoReturn:
        try:
            self._extract_object_attr_or_raise(self.ast)
            dest_object_name = dest_object_name or self.object.name or None
            dest_object_type = dest_object_type or self.object.type or None
        except Exception:
            raise Exception(
                "Unable to deduce object info. Please provide object info."
            )

        if any([dest_object_name is None, dest_object_type is None]):
            raise Exception(
                "Insufficient object info. Please provide object info."
            )

        self.object = Object(dest_object_name, dest_object_type)

        lineage = []
        non_select_cols: set[Col] = set()
        last_idx = len(self.contexts) - 1
        for context in list(self.contexts.values()):
            tables = context.get("tables", {})
            if context["index"] == last_idx:
                # Process ROOT Context
                for col in context["select"]:
                    col_refs: set[Col] = set()
                    for ref_col in col.from_:
                        col_refs = col_refs.union(
                            self._resolve_src_column(
                                table=tables.get(
                                    ref_col.attr.table_alias, None
                                ),
                                column=ref_col.attr.name,
                                clause="select",
                            )
                        )

                    lineage.append(
                        {
                            "_col": Col(col.alias, dest_object_name, None),
                            "select": col_refs,
                        }
                    )

                for clause in NON_SELECT_CLAUSES:
                    for col in context.get(clause, []):
                        col_name = col.name
                        col_table = tables[col.table_alias]
                        non_select_cols = non_select_cols.union(
                            self._resolve_src_column(
                                table=col_table, column=col_name, clause=clause
                            )
                        )
            else:
                # Process Non-ROOT Contexts
                for clause in NON_SELECT_CLAUSES:
                    for col in context.get(clause, []):
                        col_name = col.name
                        col_table = tables[col.table_alias]
                        non_select_cols = non_select_cols.union(
                            self._resolve_src_column(
                                table=col_table, column=col_name, clause=clause
                            )
                        )
        for col in lineage:
            col["non_select"] = non_select_cols

        return lineage

    # TODO
    def _resolve_star(self):
        raise NotImplementedError

    def _remove_comments(self, str1: Optional[str] = "") -> str:
        """
        Remove comments/excessive spaces/"create table as"/"create view as" from the sql file
        :param str1: the original sql
        :return: the parsed sql
        """
        # remove the /* */ comments
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", str1)
        # remove whole line -- and # comments
        lines = [
            line
            for line in q.splitlines()
            if not re.match(r"^\s*(--|#)", line)
        ]
        # remove trailing -- and # comments
        # pattern = r"(?:--|#)(?!.*(['""])[^'""]*\1)[^'\n\r]*"
        # q = " ".join([re.sub(pattern, "", line) for line in lines])
        q = ""
        comment_symbol = ["--", "#"]
        for line in lines:
            new_line = line
            for c in comment_symbol:
                quoted = False
                # if there is a comment symbol
                if line.find(c) != -1:
                    c_idx = line.find(c)
                    # if there is a ' on the left
                    if line.rfind("'", c_idx) != -1:
                        q_idx = line.rfind("'", c_idx)
                        # find the corresponding ' on the right
                        if line.find("'", q_idx) != -1:
                            quoted = True
                    if not quoted:
                        new_line = re.split("--|#", line)[0]
            q += " " + new_line

        # q = " ".join(
        #     [
        #         re.split("--|#", line)[0]
        #         if line.find("'#") == -1
        #         and line.find('"#') == -1
        #         and line.find("'--") == -1
        #         and line.find('"--') == -1
        #         else line
        #         for line in lines
        #     ]
        # )
        # replace all spaces around commas
        q = re.sub(r"\s*,\s*", ",", q)
        # replace all multiple spaces to one space
        str1 = re.sub(r"\s\s+", " ", q)
        str1 = str1.replace("\n", " ").strip()
        return str1

    def to_df(self):
        rows = []
        for col in self.lineage:
            dest_col = {
                "TARGET_TABLE": col["_col"].table.upper(),
                "TARGET_COLUMN": col["_col"].column.upper(),
                "TARGET_OBJECT_TYPE": self.object.type.upper(),
            }
            col.pop("_col")

            for k, v in col.items():
                for item in v:
                    row = {
                        "SOURCE_TABLE": item.table.upper(),
                        "SOURCE_COLUMN": item.column.upper(),
                        "CLAUSE": item.clause.upper(),
                        "COLUMN_TYPE": k.upper(),
                    }
                    row.update(dest_col)
                    rows.append(row)
        return pl.DataFrame(rows)


if __name__ == "__main__":
    cte_query = """
    CREATE VIEW IF NOT EXISTS agg_sales AS 
    WITH Products AS (
        SELECT p.Product_ID, p.Product_Name, p.Product_Description
        FROM Product_Info p
    ),
    SalesSummary AS (
        SELECT p.Product_Name, SUM(s.Quantity) AS Total_Sales
        FROM Products p
        JOIN Sales s ON p.Product_ID = s.Product_ID
        WHERE s.Sale_Date BETWEEN TO_DATE('2021-01-01', 'YYYY-MM-DD') AND TO_DATE('2021-12-31', 'YYYY-MM-DD')
        GROUP BY FIRST_VALUE(p.Product_Name)
    )
    SELECT ss.Product_Name, ss.Total_Sales, c.Category_Name
    FROM SalesSummary ss
    JOIN Categories c ON ss.Product_Name = c.Product_Name
    ORDER BY ss.Total_Sales DESC;
    """

    cll = ColumnLineage()
    print(cll.build(cte_query).to_df())
    # pprint(cll.lineage)
