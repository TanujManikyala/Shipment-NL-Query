"""
Natural-language -> MongoDB query builder (rule-based).

Returns a dict:
{
  "filter": {...},           # MongoDB filter (dict)
  "limit": int,              # limit for find
  "is_count": bool,          # True if user asked a count
  "agg": dict or None        # aggregation hint: {"type": "sum"/"group_cost"/"top", "pipeline": [...]}
}
"""
import re
from datetime import datetime, timedelta
from dateutil import parser as dateparser

# timezone handling (use zoneinfo if available)
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Kolkata")
except Exception:
    TZ = None

MONTH_KEYWORDS = ("this month", "current month")
YEAR_KEYWORDS  = ("this year", "current year")
WEEK_KEYWORDS  = ("this week", "current week")
LAST_N_DAYS_RE = re.compile(r"last\s+(\d+)\s+days?", re.IGNORECASE)
LAST_7_DAYS_RE = re.compile(r"last\s+7\s+days?", re.IGNORECASE)


COST_KEYWORDS = [
    "discount", "discounted cost", "published cost", "published",
    "marked up", "marked", "cost", "amount", "charge", "price", "freight"
]
DATE_FIELD_HINTS = ["ship date", "ship", "created", "date", "delivered", "etd"]
STATUS_FIELD_HINTS = ["status", "delivery status", "shipment type"]


def find_field(columns, keywords):
    """Return the first column name containing any of the keywords (case-insensitive)."""
    cols = list(columns)
    low = [c.lower() for c in cols]
    for kw in keywords:
        for i, c in enumerate(low):
            if kw.lower() in c:
                return cols[i]
    return None


def detect_cost_field(columns):
    # priority search using known keywords
    for kw in COST_KEYWORDS:
        f = find_field(columns, [kw])
        if f:
            return f
    # fallback: any column name containing key substrings
    for c in columns:
        lc = c.lower()
        if "cost" in lc or "amount" in lc or "charge" in lc or "price" in lc:
            return c
    return None


def detect_date_field(columns):
    return find_field(columns, DATE_FIELD_HINTS)


def detect_status_field(columns):
    return find_field(columns, STATUS_FIELD_HINTS)


def now_with_tz():
    if TZ:
        return datetime.now(TZ)
    return datetime.now()


def month_range(now=None):
    now = now or now_with_tz()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def last_n_days_range(n, now=None):
    now = now or now_with_tz()
    end = now
    start = now - timedelta(days=n)
    return start, end


def parse_date_specifics(nl, columns, date_override=None):
    """
    Return a dict with date filters if NL contains relative phrases
    or explicit yyyy-mm-dd ranges.

    Supports:
     - explicit: between YYYY-MM-DD and YYYY-MM-DD
     - this month / current month
     - this year / current year
     - this week / current week
     - last N days (e.g. "last 30 days")
     - last 7 days
    """
    nl_low = (nl or "").lower()
    date_field = date_override or detect_date_field(columns)
    if not date_field:
        return {}

    # explicit range: between YYYY-MM-DD and YYYY-MM-DD
    m = re.search(r"between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})", nl, flags=re.IGNORECASE)
    if m:
        a, b = m.groups()
        try:
            return {date_field: {"$gte": dateparser.parse(a), "$lte": dateparser.parse(b)}}
        except:
            pass

    now = now_with_tz()

    # this month / current month
    if any(k in nl_low for k in MONTH_KEYWORDS):
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return {date_field: {"$gte": start, "$lt": end}}

    # this year / current year
    if any(k in nl_low for k in YEAR_KEYWORDS):
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year + 1)
        return {date_field: {"$gte": start, "$lt": end}}

    # this week / current week (week starts Monday)
    if any(k in nl_low for k in WEEK_KEYWORDS):
        # Monday is weekday() == 0
        monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        next_monday = monday + timedelta(days=7)
        return {date_field: {"$gte": monday, "$lt": next_monday}}

    # last N days
    m = LAST_N_DAYS_RE.search(nl)
    if m:
        n = int(m.group(1))
        end = now
        start = (now - timedelta(days=n)).replace(hour=0, minute=0, second=0, microsecond=0)
        return {date_field: {"$gte": start, "$lt": end}}

    # last 7 days (special-case)
    if LAST_7_DAYS_RE.search(nl) or re.search(r"last\s+7\s+day", nl, flags=re.IGNORECASE):
        end = now
        start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        return {date_field: {"$gte": start, "$lt": end}}

    return {}


def detect_count(nl):
    return bool(re.search(r"\bhow many\b|\bcount\b|\bnumber of\b", nl.lower()))


def detect_top_n(nl):
    m = re.search(r"top\s+(\d+)", nl, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def detect_sum_request(nl):
    return bool(re.search(r"\b(total|sum|total cost|total amount)\b", nl.lower()))


def detect_group_by(nl):
    m = re.search(r"group(?:ed)? by\s+([a-z0-9 _-]+)", nl, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # special-case common phrase
    if "by status" in nl.lower() or "group by status" in nl.lower() or "grouped by status" in nl.lower():
        return "status"
    return None


def build_query(nl_text, columns, date_override=None, cost_override=None):
    nl = (nl_text or "").strip()
    filt = {}

    # date filters (use override if provided)
    date_filter = parse_date_specifics(nl, columns, date_override=date_override)
    if date_filter:
        filt.update(date_filter)

    # simple origin/destination extraction
    m_from = re.search(r"\bfrom\s+([A-Za-z0-9\-\s,]+?)(?:\s+to\b|$)", nl, flags=re.IGNORECASE)
    if m_from:
        fld = find_field(columns, ["origin", "from", "location"])
        if fld:
            filt[fld] = {"$regex": m_from.group(1).strip(), "$options": "i"}

    m_to = re.search(r"\bto\s+([A-Za-z0-9\-\s,]+?)(?:\s+with\b|$)", nl, flags=re.IGNORECASE)
    if m_to:
        fld = find_field(columns, ["destination", "to", "to company", "to id"])
        if fld:
            filt[fld] = {"$regex": m_to.group(1).strip(), "$options": "i"}

    # status mentions
    statuses = []
    for s in ["delivered", "pending", "in transit", "cancelled", "returned", "booked", "shipped"]:
        if re.search(r"\b" + re.escape(s) + r"\b", nl, flags=re.IGNORECASE):
            statuses.append(s)
    if statuses:
        status_field = detect_status_field(columns)
        if status_field:
            filt[status_field] = {"$in": statuses}

    # numeric comparisons like "cost > 1000"
    for m in re.finditer(r"([A-Za-z _#\-]{2,40})\s*(>=|<=|>|<|=)\s*([0-9,\.]+)", nl):
        ft, op, num = m.groups()
        # use cost_override if ft appears to refer to cost; otherwise detect
        col = find_field(columns, [ft.strip()]) or (cost_override or detect_cost_field(columns))
        if col:
            val = float(num.replace(",", ""))
            opmap = {"=": "$eq", ">": "$gt", "<": "$lt", ">=": "$gte", "<=": "$lte"}
            filt.setdefault(col, {})
            filt[col][opmap[op]] = val

    # prepare return structure
    q = {"filter": filt, "limit": 100, "is_count": False, "agg": None}

    # count detection
    if detect_count(nl):
        q["is_count"] = True
        return q

    # sum / total cost
    if detect_sum_request(nl):
        cost_field = cost_override or detect_cost_field(columns)
        if cost_field:
            pipeline = []
            pipeline.append({"$match": filt if filt else {}})
            pipeline.append({
                "$group": {
                    "_id": None,
                    "total": {
                        "$sum": {
                            "$convert": {
                                "input": f"${cost_field}",
                                "to": "double",
                                "onError": 0,
                                "onNull": 0
                            }
                        }
                    }
                }
            })
            q["agg"] = {"type": "sum", "pipeline": pipeline}
            return q

    # cost analysis grouped by status or requested group
    group_by = detect_group_by(nl)
    if group_by:
        if group_by.lower() in ("status", "delivery status", "shipment status"):
            group_field = detect_status_field(columns) or find_field(columns, [group_by])
        else:
            group_field = find_field(columns, [group_by])
        cost_field = cost_override or detect_cost_field(columns)
        if group_field and cost_field:
            pipeline = []
            pipeline.append({"$match": filt if filt else {}})
            pipeline.append({
                "$group": {
                    "_id": f"${group_field}",
                    "count": {"$sum": 1},
                    "total_cost": {
                        "$sum": {
                            "$convert": {"input": f"${cost_field}", "to": "double", "onError": 0, "onNull": 0}
                        }
                    },
                    "avg_cost": {
                        "$avg": {
                            "$convert": {"input": f"${cost_field}", "to": "double", "onError": 0, "onNull": 0}
                        }
                    }
                }
            })
            pipeline.append({"$sort": {"total_cost": -1}})
            q["agg"] = {"type": "group_cost", "pipeline": pipeline}
            return q

    # top N most expensive shipments (robust numeric sort)
    topn = detect_top_n(nl)
    if topn:
        cost_field = cost_override or detect_cost_field(columns)
        if cost_field:
            pipeline = []
            if filt:
                pipeline.append({"$match": filt})
            # add numeric helper for reliable sorting
            pipeline.append({
                "$addFields": {
                    "__cost_num": {
                        "$convert": {"input": f"${cost_field}", "to": "double", "onError": 0, "onNull": 0}
                    }
                }
            })
            pipeline.append({"$sort": {"__cost_num": -1}})
            pipeline.append({"$limit": topn})
            pipeline.append({"$project": {"__cost_num": 0}})
            q["agg"] = {"type": "top", "pipeline": pipeline}
            return q

    # default: no special aggregation
    return q
