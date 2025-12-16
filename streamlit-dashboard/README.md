# 🚦 Traffic Analytics Dashboard

Real-time traffic monitoring dashboard built with Streamlit, visualizing data from TimescaleDB.

## Features

### 📊 Dashboard Pages

1. **🏠 Overview**
   - Real-time metrics (total vehicles, active cameras)
   - Top 10 busiest cameras
   - Vehicle type distribution (pie chart)
   - Hourly traffic trends

2. **📊 Detailed Analysis**
   - Per-camera deep dive
   - Vehicle type breakdown over time
   - Statistical analysis (avg, max, min, std dev)
   - Distribution histograms

3. **🗺️ Map View**
   - Interactive map with traffic density
   - Bubble size based on vehicle count
   - Color-coded by traffic intensity
   - Clickable camera markers

4. **📈 Trends**
   - Peak hours analysis (7-day average)
   - Weekday vs Weekend patterns
   - Stacked area chart by vehicle type
   - Time series forecasting (coming soon)

5. **⚠️ Alerts**
   - High traffic threshold alerts
   - Sudden spike detection
   - Anomaly detection (coming soon)

## 🚀 Quick Start

### Local Development

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up database connection:**
   ```bash
   cp .env.example .env
   # Edit .env with your TimescaleDB credentials
   ```

3. **Run the dashboard:**
   ```bash
   streamlit run app.py
   ```

4. **Open browser:**
   Navigate to `http://localhost:8501`

### Docker

1. **Build image:**
   ```bash
   docker build -t traffic-dashboard:dev .
   ```

2. **Run container:**
   ```bash
   docker run -p 8501:8501 \
     -e DB_HOST=host.docker.internal \
     -e DB_PORT=5432 \
     -e DB_NAME=postgres \
     -e DB_USER=postgres \
     -e DB_PASSWORD=postgres \
     traffic-dashboard:dev
   ```

### Kubernetes Deployment

1. **Apply deployment:**
   ```bash
   kubectl apply -f ../k8s/streamlit-dashboard.yaml -n hugedata
   ```

2. **Port forward:**
   ```bash
   kubectl port-forward svc/streamlit-dashboard 8501:8501 -n hugedata
   ```

3. **Access:**
   Open `http://localhost:8501`

## 📦 Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | TimescaleDB hostname | `localhost` |
| `DB_PORT` | TimescaleDB port | `5432` |
| `DB_NAME` | Database name | `postgres` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | `postgres` |

## 🎨 Visualizations

- **Plotly** - Interactive charts (line, bar, pie, scatter)
- **Plotly Mapbox** - Geographic visualization
- **Custom metrics** - Real-time KPIs
- **Auto-refresh** - Optional 30-second refresh

## 🔧 Configuration

### Time Ranges
- Last 1 Hour
- Last 6 Hours
- Last 24 Hours (default)
- Last 7 Days

### Alert Thresholds
Adjustable slider (50-500 vehicles)

## 📊 Data Requirements

Dashboard expects TimescaleDB with:
- `traffic_metrics` table (raw data)
- `traffic_metrics_hourly` view (aggregated)
- `latest_traffic_metrics` view (current state)

See `../timescaledb/init.sql` for schema.

## 🚧 Roadmap

- [ ] Anomaly detection with ML
- [ ] Traffic forecasting (Prophet/LSTM)
- [ ] Export reports (PDF/Excel)
- [ ] User authentication
- [ ] Custom alert rules
- [ ] Email/SMS notifications
- [ ] Multi-tenant support

## 📝 Notes

- Dashboard caches queries for 30 seconds (`@st.cache_data(ttl=30)`)
- Auto-refresh reloads entire page
- Map requires valid latitude/longitude in data

## 🐛 Troubleshooting

**Connection error:**
- Check DB credentials in `.env`
- Verify TimescaleDB is running
- For K8s, use service DNS: `timescaledb.hugedata.svc.cluster.local`

**No data showing:**
- Run `timescaledb/init.sql` to create schema
- Check if producer is sending data to Kafka
- Verify Spark job is writing to TimescaleDB

**Map not displaying:**
- Ensure cameras have latitude/longitude
- Check browser console for errors

## 📚 Tech Stack

- **Streamlit** - Web framework
- **Plotly** - Interactive charts
- **Pandas** - Data manipulation
- **psycopg2** - PostgreSQL driver
- **TimescaleDB** - Time-series database
