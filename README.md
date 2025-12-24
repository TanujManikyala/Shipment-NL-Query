```markdown
# 📦 Shipment NL Query

A Streamlit-based application to query shipment data stored in MongoDB using **natural language**. Users can upload Excel files of shipment data, ingest them into MongoDB, and then ask plain-English questions like:

- How many shipments were created this month?
- Show total shipment cost for the current month.
- Provide a cost analysis of shipments grouped by status.
- List the top 5 most expensive shipments.
- Show shipments created in the last 7 days.
---
---

## 📁 Project Structure

```

shipment_nl_query/
│
├── app/
│   ├── app.py               # Streamlit UI (main)
│   ├── ingest_excel.py      # Excel ingestion -> MongoDB
│   └── nl_to_mongo.py       # Natural language -> MongoDB query builder
│
├── data/
│   └── shipment_data.xlsx   # Optional sample Excel file
│
├── requirements.txt         # Python dependencies
└── README.md                # Project instructions

````

---

## ⚙️ Requirements

- Python 3.10+
- MongoDB (local or remote)
- Install dependencies:

```bash
pip install -r requirements.txt
````

**Example `requirements.txt`:**

```
streamlit
pandas
pymongo
python-dateutil
```

---

## 🚀 Setup Instructions

1. **Start MongoDB**

   Ensure you have MongoDB running locally or provide a remote URI.

2. **Run the Streamlit App**

```bash
streamlit run app/app.py
```

3. **Connect to MongoDB via the sidebar**

* MongoDB URI: `mongodb://localhost:27017` (or your custom URI)
* Database: `shipments_db` (default, can change)
* Collection: `shipments` (default, can change)

4. **Ingest Excel File**

* Upload your shipment Excel file using the sidebar uploader.
* Click `⬆ Ingest Excel` to insert data into MongoDB.
* Columns are dynamically detected; numeric and date fields are normalized.

5. **Ask Questions in Natural Language**

Examples:

* `"How many shipments were created this month?"`
* `"Show total shipment cost for the current month."`
* `"Provide a cost analysis of shipments grouped by status."`
* `"List the top 5 most expensive shipments."`
* `"Show shipments created in the last 7 days."`

6. **View Results**

* Count metrics: Rows matched / Unique shipments
* Aggregations: Total costs, grouped summaries, top N shipments
* Sample matching rows displayed in a table
* Optional field overrides for Date or Cost fields

---

## 🧰 Features

* **Dynamic Column Detection** – No hardcoding required
* **Natural Language Queries** – Rule-based parsing of counts, sums, top N, and grouped aggregations
* **Aggregation Support** – Sum, average, count, grouped by status or other fields
* **Duplicate Detection** – Top duplicate shipment references
* **Excel Ingestion** – Automatic type detection for numeric, date, and ID fields
* **Streamlit UI** – Simple web interface for querying

---

## 📌 Notes

* Date parsing uses `dateutil` and supports phrases like:

  * `this month`, `current month`
  * `this year`, `current year`
  * `last 7 days` or `last N days`
  * `between YYYY-MM-DD and YYYY-MM-DD`
* Cost detection supports columns containing: `cost`, `amount`, `charge`, `price`
* Ref-like fields (`ref`, `tracking`, `awb`) are used for unique shipment counts

---

## 📝 License

MIT License

---
