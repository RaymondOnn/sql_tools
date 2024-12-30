from sqlglot import parse_one, exp
from sql_metadata import Parser
from pprint import pprint
from simple_ddl_parser import DDLParser

cte_1 = """
    select
        timeline.pk,
        scd2_table1.dim1,
        scd2_table2.dim2,
        scd2_table3.dim3,
        coalesce(timeline.valid_from, '1900-01-01'::timestamp) as valid_from,            
        coalesce(timeline.valid_to, '9999-12-31'::timestamp) as valid_to
    from unified_timeline_recalculate_valid_to as timeline
        left join scd2_table1
            on timeline.pk = scd2_table1.pk 
            and scd2_table1.valid_from <= timeline.valid_from 
            and scd2_table1.valid_to >= timeline.valid_to
        left join scd2_table2
            on timeline.pk = scd2_table2.pk 
            and scd2_table2.valid_from <= timeline.valid_from 
            and scd2_table2.valid_to >= timeline.valid_to
        left join scd2_table3
            on timeline.pk = scd2_table1.pk 
            and scd2_table3.valid_from <= timeline.valid_from 
            and scd2_table3.valid_to >= timeline.valid_to
"""

scd_table1 = """
    CREATE TABLE scd2_table1 (
        pk int,
        dim1 int, 
        valid_from timestamp,
        valid_to timestamp
    )
"""

scd_table2 = """
    CREATE TABLE scd2_table1 (
        pk int,
        dim2 int, 
        valid_from timestamp,
        valid_to timestamp
    )
"""



sql_exp = parse_one(cte_1, dialect="snowflake")
for col in sql_exp.find_all(exp.Table):
    print(f"Table: {col.name}")
    
for col in sql_exp.find_all(exp.Column):
    print(col.name, col.alias_or_name)

for col in sql_exp.find_all(exp.Select):
    print(f"Select: {col}")
    print(col.name, col.alias_or_name)

print("-"*100)
parsed = Parser(cte_1)
print(parsed.tables, parsed.columns)

# Mapping relationship between columns

for join_stmt in sql_exp.find_all(exp.Join):
    print(join_stmt)
    # pprint(join_stmt.parent_select.sql())
    for from_stmt in join_stmt.parent_select.find_all(exp.From):
        pprint(from_stmt)
        for table in from_stmt.find_all(exp.Table):
            print(f"TABLE: {table.name}, {table.alias_or_name}")

ddl_dict = DDLParser(scd_table1).run(output_mode="snowflake")
col_info = { d['name']: d for d in ddl_dict[0]['columns']}
pprint(col_info)