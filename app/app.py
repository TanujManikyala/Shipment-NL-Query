import streamlit as st
import pandas as pd
from pymongo import MongoClient
from ingest_excel import normalize_row
from nl_to_mongo import build_query
import json

st.set_page_config(page_title="Shipment NL Query", layout="wide")
st.title(" Shipment Database — Natural Language Query")
st.caption("Query shipment data using plain English")
st.divider()

# ----------------- Sidebar: Connection & Ingest -----------------
st.sidebar.header("🔌 Database Connection")
mongo_uri = st.sidebar.text_input("MongoDB URI", value="mongodb://localhost:27017")
db_name = st.sidebar.text_input("Database name", value="shipments_db")
collection_name = st.sidebar.text_input("Collection name", value="shipments")

client = None
connected = False
if mongo_uri and db_name and collection_name:
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        client.server_info()
        connected = True
        st.sidebar.success("Connected to MongoDB")
    except Exception as e:
        st.sidebar.error(f"MongoDB connection failed: {e}")

st.sidebar.divider()
st.sidebar.header(" Ingest Excel")
uploaded = st.sidebar.file_uploader("Upload Excel file", type=["xlsx", "xls"])
if uploaded and connected and st.sidebar.button("⬆ Ingest Excel"):
    df = pd.read_excel(uploaded, dtype=object)
    coll = client[db_name][collection_name]
    docs = [normalize_row(row) for _, row in df.iterrows()]
    if docs:
        coll.insert_many(docs)
        st.sidebar.success(f"Inserted {len(docs)} rows")
    else:
        st.sidebar.warning("No rows found in Excel")

# ----------------- Main: Query UI -----------------
st.subheader(" Ask a Question")
examples = [
    "How many shipments were created this month?",
    "Show total shipment cost for the current month",
    "Provide a cost analysis of shipments grouped by status",
    "List the top 5 most expensive shipments",
    "Show shipments created in the last 7 days"
]
cols_buttons = st.columns(len(examples))
for i, ex in enumerate(examples):
    if cols_buttons[i].button(ex):
        st.session_state["query"] = ex

query = st.text_area("Natural-language query", height=120, value=st.session_state.get("query", ""))
run = st.button(" Run Query")

# ----------------- Run the query and show results -----------------
if run:
    if not connected:
        st.error("Please connect to MongoDB first.")
        st.stop()

    coll = client[db_name][collection_name]
    sample = coll.find_one()
    if not sample:
        st.warning("Collection is empty. Please ingest data first.")
        st.stop()

    # Detect columns dynamically
    columns = [c for c in sample.keys() if c != "_id"]

    # Field overrides in sidebar (optional)
    st.sidebar.divider()
    st.sidebar.header("⚙ Field Overrides (optional)")
    date_field_override = st.sidebar.selectbox("Date field to use", options=["(auto)"] + columns)
    cost_field_override = st.sidebar.selectbox("Cost field to use", options=["(auto)"] + columns)
    if date_field_override == "(auto)": date_field_override = None
    if cost_field_override == "(auto)": cost_field_override = None

    # Build query (pass overrides)
    with st.spinner("Building query..."):
        qobj = build_query(query, columns, date_override=date_field_override, cost_override=cost_field_override)
        filt = qobj.get("filter", {}) or {}

    # Debug filter (collapsible)
    with st.expander("🛠 Debug — MongoDB Filter"):
        st.code(json.dumps(filt, default=str, indent=2))

    # Compute counts:
    try:
        total_rows = coll.count_documents(filt)
    except Exception as e:
        st.error("Error counting documents: " + str(e))
        total_rows = None

    # detect a Ref-like field (common names)
    ref_field = None
    for c in columns:
        if "ref" in c.lower() or "reference" in c.lower() or "awb" in c.lower():
            ref_field = c
            break

    unique_refs = None
    if ref_field:
        try:
            # use Mongo distinct with the same filter
            unique_refs_list = coll.distinct(ref_field, filt)
            unique_refs = len([x for x in unique_refs_list if x is not None and str(x).strip() != ""])
        except Exception as e:
            st.warning("Could not run distinct() for Ref #: " + str(e))

    # Show count metrics and allow toggle to display unique vs rows
    col1, col2, col3 = st.columns([1,1,2])
    with col1:
        st.metric("Rows matched", total_rows if total_rows is not None else "—")
    with col2:
        st.metric("Unique Ref #s matched", unique_refs if unique_refs is not None else "N/A")
    with col3:
        st.info(f"Detected date field: **{date_field_override or ( 'auto-detected: ' + (next((c for c in columns if 'ship' in c.lower() and 'date' in c.lower()), 'unknown')) )}**")

    # If count requested specifically (how many...), still show counts but clarify
    if qobj.get("is_count"):
        # by default show rows; allow user to choose unique
        if unique_refs is not None:
            choice = st.radio("Return count as:", ("Rows matched (including duplicates)", "Unique shipments (by Ref #)"), index=1)
            if choice.startswith("Unique"):
                st.success(f"Unique shipments: {unique_refs}")
            else:
                st.success(f"Rows matched: {total_rows}")
        else:
            st.success(f"Rows matched: {total_rows}")
        st.stop()

    # If aggregation hint present, run aggregation
    agg = qobj.get("agg")
    if agg:
        try:
            res = list(coll.aggregate(agg["pipeline"]))
        except Exception as e:
            st.error("Aggregation failed: " + str(e))
            res = []
        if not res:
            st.info("No results returned by aggregation.")
        else:
            df_res = pd.DataFrame(res)
            if agg["type"] == "sum":
                st.metric(" Total (sum)", f"{df_res.iloc[0]['total']:.2f}")
            else:
                st.dataframe(df_res, use_container_width=True)
        st.stop()

    # Default: show documents (limit)
    docs = list(coll.find(filt).limit(qobj.get("limit", 100)))
    if docs:
        df_docs = pd.DataFrame(docs).drop(columns=["_id"], errors="ignore")
        st.subheader("Sample matching rows")
        st.dataframe(df_docs, use_container_width=True)
    else:
        st.info("No rows matched this query.")

    # Additionally show status breakdown for the matched rows (server-side aggregate)
    # detect status field name
    status_field = next((c for c in columns if "status" in c.lower()), None)
    if status_field:
        # build aggregation pipeline: match -> group by status
        pipeline = [
            {"$match": filt if filt else {}},
            {"$group": {"_id": f"${status_field}", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        try:
            status_counts = list(coll.aggregate(pipeline))
            if status_counts:
                st.subheader("Counts by Status (matched rows)")
                st.dataframe(pd.DataFrame(status_counts), use_container_width=True)
        except Exception as e:
            st.warning("Failed to compute status breakdown: " + str(e))

    # Show top duplicate refs (if duplicates exist) to help debugging
    if ref_field:
        dupe_pipeline = [
            {"$match": filt if filt else {}},
            {"$group": {"_id": f"${ref_field}", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        try:
            dupes = list(coll.aggregate(dupe_pipeline))
            if dupes:
                st.subheader("Top duplicate Ref# groups (sample)")
                st.dataframe(pd.DataFrame(dupes), use_container_width=True)
        except Exception as e:
            st.warning("Failed to compute duplicate refs: " + str(e))
