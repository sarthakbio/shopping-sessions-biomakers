#!/usr/bin/env python3
"""Shopping Sessions pipeline (team Biomakers), DuckDB.

Lines are read raw (no CSV quoting by DuckDB) and parsed with the exact
dialect rules in SQL.  Linked IDs are joined with a union-find in Python.
"""
import json
import os
import sys
import time

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:6.2f}s]", *a, file=sys.stderr, flush=True)


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
os.makedirs(os.path.join(SCRATCH, "duckdb_tmp"), exist_ok=True)
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

DAY = 86_400_000
GAP = 1_800_000
WEEK = 7 * DAY

# ---------------------------------------------------------------- macros
con.execute(r"""
CREATE OR REPLACE MACRO days_from_civil(y, m, d) AS (
    (CASE WHEN m <= 2 THEN y - 1 ELSE y END) // 400 * 146097
    + ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) % 400) * 365
    + ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) % 400) // 4
    - ((CASE WHEN m <= 2 THEN y - 1 ELSE y END) % 400) // 100
    + (153 * (CASE WHEN m > 2 THEN m - 3 ELSE m + 9 END) + 2) // 5 + d - 1
    - 719468
);
""")
con.execute(r"""
CREATE OR REPLACE MACRO is_leap(y) AS ((y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0);
""")
con.execute(r"""
CREATE OR REPLACE MACRO dim(y, m) AS (
    CASE WHEN m = 2 THEN CASE WHEN is_leap(y) THEN 29 ELSE 28 END
         WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END
);
""")

con.execute(r"""
CREATE OR REPLACE MACRO parse_ts(s) AS (CASE
  WHEN s IS NULL THEN NULL
  WHEN regexp_full_match(s, '[0-9]+') THEN
    CASE WHEN length(ltrim(s, '0')) = 0 THEN 0::HUGEINT
         WHEN length(ltrim(s, '0')) > 30 THEN CAST('1000000000000000000000000000000' AS HUGEINT)
         ELSE CAST(ltrim(s, '0') AS HUGEINT) END
  WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])') THEN
    (epoch_ms(try_strptime(s[1:19], '%Y-%m-%dT%H:%M:%S'))
     - CASE WHEN length(s) = 20 THEN 0
            ELSE (CAST(s[21:22] AS BIGINT) * 60 + CAST(s[24:25] AS BIGINT)) * 60000
                 * (CASE WHEN s[20:20] = '+' THEN 1 ELSE -1 END) END)::HUGEINT
  ELSE NULL END)
""")
# ingest_ts is always valid F1
con.execute(r"""
CREATE OR REPLACE MACRO parse_ing(s) AS (epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%SZ')));
""")

# raw line -> struct of fields; a field is either "..." (no quotes inside) or has no quote
FIELD = '("[^",]*"|[^",]*)'


def line_regex(n):
    return "^" + ",".join([FIELD] * n) + "$"


def fields_sql(n, names):
    """SQL expression list: per-field value with quotes stripped, '' -> NULL."""
    rx = line_regex(n)
    m = f"regexp_extract(line, '{rx}', [{', '.join(repr(x) for x in names)}])"
    return m


def parsed(src, names):
    """Subquery with one column per field name: dialect-checked, quotes stripped."""
    n = len(names)
    rx = line_regex(n)
    cols = ", ".join(f"{strip('g.' + x)} AS {x}" for x in names)
    lst = ", ".join("'" + x + "'" for x in names)
    return (f"(SELECT {cols} FROM (SELECT regexp_extract(line, '{rx}', [{lst}]) AS g FROM {src} "
            f"WHERE regexp_matches(line, '{rx}')))")


def strip(col):
    return f"nullif(CASE WHEN starts_with({col}, '\"') THEN {col}[2:length({col}) - 1] ELSE {col} END, '')"


SMALL = 32 * 1024 * 1024


def _sources(paths):
    """SQL for all lines (header removed) of the given files, as column `line`.
    Small files are read whole and split on \\n only, which keeps \\r as data;
    big files go through DuckDB's parallel reader."""
    total = sum(os.path.getsize(p) for p in paths)
    # The generator never writes \\r; DuckDB's reader drops a \\r before \\n, so
    # tiny (hand-written) inputs are read whole and split on \\n exactly.
    small = paths if total < SMALL else []
    big = [] if total < SMALL else paths
    parts = []
    if small:
        lst = ", ".join("'" + p.replace("'", "''") + "'" for p in small)
        parts.append(f"SELECT unnest(string_split(content, chr(10))[2:]) AS line FROM read_text([{lst}])")
    if big:
        lst = ", ".join("'" + p.replace("'", "''") + "'" for p in big)
        parts.append(
            f"SELECT line FROM read_csv([{lst}], columns={{'line': 'VARCHAR'}}, delim='\\x1e', quote='', escape='', "
            f"header=true, auto_detect=false, new_line='\\n', strict_mode=false, ignore_errors=true, "
            f"max_line_size=10000000, parallel=true)"
        )
    if not parts:
        return "(SELECT NULL::VARCHAR AS line WHERE false)"
    return "(" + " UNION ALL ".join(parts) + ")"


def raw(sub):
    d = os.path.join(DATA, sub)
    paths = sorted(os.path.join(d, f) for f in os.listdir(d) if f.endswith(".csv")) if os.path.isdir(d) else []
    return _sources(paths)


def raw_file(name):
    return _sources([os.path.join(DATA, name)])


def has_files(sub):
    d = os.path.join(DATA, sub)
    return os.path.isdir(d) and any(f.endswith(".csv") for f in os.listdir(d))


manifest = json.load(open(os.path.join(DATA, "manifest.json")))
WS = con.execute("SELECT parse_ts(?)", [manifest["window_start"]]).fetchone()[0]
WE = con.execute("SELECT parse_ts(?)", [manifest["window_end"]]).fetchone()[0]
log("window", WS, WE)

con.execute(f"""
CREATE TABLE products AS
SELECT product_id, category FROM {parsed(raw_file('products.csv'), ['product_id', 'category'])}
WHERE product_id IS NOT NULL
""")
con.execute(f"""
CREATE TABLE campaigns AS
SELECT campaign_id, channel FROM {parsed(raw_file('campaigns.csv'), ['campaign_id', 'channel'])}
WHERE campaign_id IS NOT NULL
""")

# ---------------------------------------------------------------- links
pairs = []
if has_files("links"):
    pairs = con.execute(f"""
        SELECT a, b FROM {parsed(raw('links'), ['a', 'b'])}
        WHERE a IS NOT NULL AND b IS NOT NULL AND a <> b
    """).fetchall()
log("links", len(pairs))
parent = {}


def find(x):
    root = x
    while True:
        p = parent.get(root)
        if p is None or p == root:
            break
        root = p
    while x != root:
        p = parent[x]
        parent[x] = root
        x = p
    return root


for a, b in pairs:
    ra, rb = find(a), find(b)
    if ra != rb:
        if rb.encode() < ra.encode():
            ra, rb = rb, ra
        parent[rb] = ra
        parent.setdefault(ra, ra)
people = [(k, find(k)) for k in list(parent)]
people = [(k, v) for k, v in people if k != v]
con.execute("CREATE TABLE idmap (uid VARCHAR, person VARCHAR)")
if people:
    # IDs never hold a comma, quote or newline, but may hold \\r or tabs: write as
    # tab-free JSON lines so nothing is reinterpreted.
    mp = os.path.join(SCRATCH, "idmap.jsonl")
    with open(mp, "w", encoding="utf-8", errors="surrogatepass") as f:
        for k, v in people:
            f.write(json.dumps({"uid": k, "person": v}) + "\n")
    con.execute(f"INSERT INTO idmap SELECT uid, person FROM read_json('{mp}', columns={{'uid': 'VARCHAR', 'person': 'VARCHAR'}}, format='newline_delimited')")
log("idmap", len(people))

# ---------------------------------------------------------------- events
con.execute(f"""
CREATE TABLE ev AS
WITH r AS (
    SELECT event_id, user_id, ts_s, COALESCE(parse_ing(ing_s), parse_ts(ing_s)) AS ing, et, pid, price
    FROM {parsed(raw('events'), ['event_id', 'user_id', 'ts_s', 'ing_s', 'et', 'pid', 'price'])}
    WHERE event_id IS NOT NULL
), d AS (
    SELECT event_id, arg_min(struct_pack(user_id, ts_s, ing, et, pid, price), ing) AS k
    FROM r GROUP BY event_id
), v AS (
    SELECT k.user_id AS user_id, parse_ts(k.ts_s) AS ts, k.ing AS ing, k.et AS et, k.pid AS pid, k.price AS price
    FROM d
    WHERE k.user_id IS NOT NULL AND k.pid IS NOT NULL AND k.et IN ('view', 'cart', 'purchase')
)
SELECT COALESCE(m.person, v.user_id) AS person, CAST(v.ts AS BIGINT) AS ts, v.et AS et, v.pid AS pid,
       CASE WHEN v.et = 'purchase' THEN CAST(ltrim(v.price, '0') AS BIGINT) ELSE 0 END AS price
FROM v LEFT JOIN idmap m ON m.uid = v.user_id
WHERE v.ts IS NOT NULL AND v.ts >= {WS} AND v.ts < {WE}
  AND v.ing - v.ts <= {DAY}
  AND (v.et <> 'purchase' OR (v.price IS NOT NULL AND regexp_full_match(v.price, '[0-9]+')
        AND length(ltrim(v.price, '0')) BETWEEN 1 AND 8
        AND CAST(ltrim(v.price, '0') AS BIGINT) BETWEEN 1 AND 10000000))
""")
log("events", con.execute("SELECT count(*) FROM ev").fetchone()[0])

con.execute(f"""
CREATE TABLE evs AS
WITH a AS (
    SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) <= {GAP} THEN 0 ELSE 1 END AS ns
    FROM ev
)
SELECT person, ts, et, pid, price,
       sum(ns) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sid
FROM a
""")
con.execute("DROP TABLE ev")
con.execute("""
CREATE TABLE sess AS
SELECT person, sid, min(ts) AS start, count(*) AS n,
       sum(price) AS rev, count(*) FILTER (WHERE et = 'purchase') AS npur
FROM evs GROUP BY person, sid
""")
con.execute("""
CREATE TABLE sesscat AS
SELECT e.person, e.sid, COALESCE(p.category, 'unknown') AS category, sum(e.price) AS amt
FROM evs e LEFT JOIN products p ON p.product_id = e.pid
WHERE e.et = 'purchase'
GROUP BY ALL
""")
log("sessions", con.execute("SELECT count(*) FROM sess").fetchone()[0])

# ---------------------------------------------------------------- clicks
if has_files("clicks"):
    con.execute(f"""
    CREATE TABLE ck AS
    WITH r AS (SELECT click_id, user_id, campaign_id, ts_s
          FROM {parsed(raw('clicks'), ['click_id', 'user_id', 'campaign_id', 'ts_s'])}
          WHERE click_id IS NOT NULL),
    d AS (SELECT click_id, any_value(struct_pack(user_id, campaign_id, ts_s)) AS k FROM r GROUP BY click_id),
    v AS (SELECT click_id, k.user_id AS user_id, k.campaign_id AS campaign_id, parse_ts(k.ts_s) AS ts FROM d
          WHERE k.user_id IS NOT NULL AND k.campaign_id IN (SELECT campaign_id FROM campaigns))
    SELECT COALESCE(m.person, v.user_id) AS person, CAST(v.ts AS BIGINT) AS ts, v.click_id, v.campaign_id
    FROM v LEFT JOIN idmap m ON m.uid = v.user_id
    WHERE v.ts IS NOT NULL AND v.ts >= {WS - WEEK} AND v.ts < {WE}
    """)
else:
    con.execute("CREATE TABLE ck (person VARCHAR, ts BIGINT, click_id VARCHAR, campaign_id VARCHAR)")
log("clicks", con.execute("SELECT count(*) FROM ck").fetchone()[0])

# credit: latest click at or before start within 7 days; tie -> smallest click_id
con.execute(f"""
CREATE TABLE ckt AS
SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM ck GROUP BY person, ts
""")
con.execute(f"""
CREATE TABLE credit AS
SELECT s.person, s.sid, s.start, s.n, s.rev, s.npur,
       CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK} THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sess s ASOF LEFT JOIN ckt c ON s.person = c.person AND s.start >= c.ts
""")
con.execute(f"""
CREATE TABLE assists AS
SELECT DISTINCT s.person, s.sid, s.start, c.campaign_id
FROM sess s JOIN ck c ON s.person = c.person AND c.ts <= s.start AND c.ts >= s.start - {WEEK}
""")
log("credited")

# ---------------------------------------------------------------- outputs
ud = os.path.join(OUT, "user_daily")
cd = os.path.join(OUT, "campaign_daily")
os.makedirs(ud, exist_ok=True)
os.makedirs(cd, exist_ok=True)
con.execute(f"""
COPY (
    WITH s AS (SELECT person, sid, CAST(start // {DAY} AS INTEGER) AS dnum, n, rev, npur FROM sess),
    u AS (SELECT person, dnum, count(*) AS sessions, sum(n) AS events, sum(rev) AS revenue_cents,
                 sum(npur) AS npur FROM s GROUP BY person, dnum),
    c AS (SELECT s.person, s.dnum, sc.category, sum(sc.amt) AS amt
          FROM sesscat sc JOIN s ON s.person = sc.person AND s.sid = sc.sid GROUP BY ALL),
    t AS (SELECT person, dnum, arg_min(category, struct_pack(neg := -amt, cat := category)) AS top
          FROM c GROUP BY person, dnum)
    SELECT u.person AS user_id, (DATE '1970-01-01' + u.dnum)::DATE AS day,
           u.sessions::BIGINT AS sessions, u.events::BIGINT AS events, u.revenue_cents::BIGINT AS revenue_cents,
           CASE WHEN u.npur > 0 THEN t.top ELSE NULL END AS top_category
    FROM u LEFT JOIN t ON t.person = u.person AND t.dnum = u.dnum
) TO '{os.path.join(ud, "part-0.parquet")}' (FORMAT parquet)
""")
con.execute(f"""
COPY (
    WITH cr AS (SELECT campaign_id, CAST(start // {DAY} AS INTEGER) AS dnum, count(*) AS sessions,
                       count(*) FILTER (WHERE npur > 0) AS ps, sum(rev) AS rev
                FROM credit GROUP BY ALL),
    a AS (SELECT campaign_id, CAST(start // {DAY} AS INTEGER) AS dnum, count(*) AS ast FROM assists GROUP BY ALL),
    k AS (SELECT campaign_id, dnum FROM cr UNION SELECT campaign_id, dnum FROM a)
    SELECT k.campaign_id, CASE WHEN k.campaign_id = 'organic' THEN 'organic' ELSE cp.channel END AS channel,
           (DATE '1970-01-01' + k.dnum)::DATE AS day,
           COALESCE(cr.sessions, 0)::BIGINT AS sessions, COALESCE(cr.ps, 0)::BIGINT AS purchasing_sessions,
           COALESCE(cr.rev, 0)::BIGINT AS revenue_cents, COALESCE(a.ast, 0)::BIGINT AS assisted_sessions
    FROM k LEFT JOIN cr ON cr.campaign_id = k.campaign_id AND cr.dnum = k.dnum
           LEFT JOIN a ON a.campaign_id = k.campaign_id AND a.dnum = k.dnum
           LEFT JOIN campaigns cp ON cp.campaign_id = k.campaign_id
) TO '{os.path.join(cd, "part-0.parquet")}' (FORMAT parquet)
""")
log("done")
