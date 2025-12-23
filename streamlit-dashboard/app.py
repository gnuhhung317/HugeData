import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import time

# Page config
st.set_page_config(
    page_title="Traffic Analytics Dashboard",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_db_connection():
    """Create database engine using SQLAlchemy"""
    db_name = os.getenv('DB_NAME', 'traffic')  # Changed default from 'postgres' to 'traffic'
    connection_string = (
        f"postgresql://{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASSWORD', 'postgres')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{db_name}"
    )
    engine = create_engine(connection_string)
    return engine

@st.cache_data(ttl=30)
def fetch_data(query, params=None):
    """Fetch data from database with caching"""
    try:
        engine = get_db_connection()
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        st.error(f"⚠️ Database Error: {str(e)}")
        st.info("""
        **Troubleshooting:**
        1. Check TimescaleDB is running: `kubectl get pods -n hugedata`
        2. Initialize database schema:
           ```
           kubectl get pods -n hugedata -l app=timescaledb
           kubectl cp timescaledb/init.sql <pod-name>:/tmp/init.sql -n hugedata
           kubectl exec -it <pod-name> -n hugedata -- psql -U postgres -f /tmp/init.sql
           ```
        3. Verify connection settings in deployment yaml
        """)
        return pd.DataFrame()

# Queries
def get_latest_metrics():
    """Get latest metrics for all cameras"""
    query = """
    SELECT 
        time,
        camera_id,
        camera_name,
        latitude,
        longitude,
        car_count,
        motorcycle_count,
        bus_count,
        truck_count,
        total_count
    FROM latest_traffic_metrics
    ORDER BY total_count DESC
    """
    return fetch_data(query)

def get_latest_metrics_1h():
    """Get latest metrics per camera within the last 1 hour"""
    query = """
    SELECT
        time,
        camera_id,
        camera_name,
        latitude,
        longitude,
        car_count,
        motorcycle_count,
        bus_count,
        truck_count,
        total_count
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY camera_id
                   ORDER BY time DESC
               ) AS rn
        FROM traffic_metrics
        WHERE time >= NOW() - INTERVAL '1 hour'
    ) t
    WHERE rn = 1
    ORDER BY total_count DESC
    """
    return fetch_data(query)

def get_hourly_trend(hours=24):
    """Get hourly traffic trend - fallback to raw table if view is empty"""
    # Try continuous aggregate first
    query = f"""
    SELECT 
        bucket as time,
        SUM(avg_total_count) as total_vehicles,
        SUM(avg_car_count) as cars,
        SUM(avg_motorcycle_count) as motorcycles,
        SUM(avg_bus_count) as buses,
        SUM(avg_truck_count) as trucks
    FROM traffic_metrics_hourly
    WHERE bucket >= NOW() - INTERVAL '{hours} hours'
    GROUP BY bucket
    ORDER BY bucket
    """
    df = fetch_data(query)
    
    # Fallback to raw table if view is empty
    if df is None or df.empty:
        query = f"""
        SELECT 
            time_bucket('1 hour', time) as time,
            SUM(total_count) as total_vehicles,
            SUM(car_count) as cars,
            SUM(motorcycle_count) as motorcycles,
            SUM(bus_count) as buses,
            SUM(truck_count) as trucks
        FROM traffic_metrics
        WHERE time >= NOW() - INTERVAL '{hours} hours'
        GROUP BY time_bucket('1 hour', time)
        ORDER BY time
        """
        df = fetch_data(query)
    
    return df if df is not None else pd.DataFrame()

def get_camera_stats(camera_id, hours=24):
    """Get detailed stats for a specific camera"""
    query = """
    SELECT 
        time,
        car_count,
        motorcycle_count,
        bus_count,
        truck_count,
        total_count
    FROM traffic_metrics
    WHERE camera_id = %s 
      AND time >= NOW() - INTERVAL '%s hours'
    ORDER BY time
    """
    return fetch_data(query, (camera_id, hours))

def get_peak_hours():
    """Get peak hours analysis"""
    query = """
    SELECT 
        EXTRACT(hour FROM time) as hour,
        AVG(total_count) as avg_traffic,
        MAX(total_count) as max_traffic,
        MIN(total_count) as min_traffic
    FROM traffic_metrics
    WHERE time >= NOW() - INTERVAL '7 days'
    GROUP BY hour
    ORDER BY hour
    """
    return fetch_data(query)

def get_weekday_pattern():
    """Get weekday vs weekend pattern"""
    query = """
    SELECT 
        CASE 
            WHEN EXTRACT(dow FROM time) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        EXTRACT(hour FROM time) as hour,
        AVG(total_count) as avg_traffic
    FROM traffic_metrics
    WHERE time >= NOW() - INTERVAL '30 days'
    GROUP BY day_type, hour
    ORDER BY day_type, hour
    """
    return fetch_data(query)

def get_top_cameras(limit=10, hours=24):
    """Get top busiest cameras"""
    query = """
    SELECT 
        camera_name,
        camera_id,
        AVG(total_count) as avg_count,
        MAX(total_count) as peak_count
    FROM traffic_metrics
    WHERE time > NOW() - INTERVAL '%s hours'
    GROUP BY camera_name, camera_id
    ORDER BY avg_count DESC
    LIMIT %s
    """
    return fetch_data(query, (hours, limit))

def get_vehicle_distribution(hours=24):
    """Get vehicle type distribution"""
    query = """
    SELECT 
        SUM(car_count) as cars,
        SUM(motorcycle_count) as motorcycles,
        SUM(bus_count) as buses,
        SUM(truck_count) as trucks
    FROM traffic_metrics
    WHERE time > NOW() - INTERVAL '%s hours'
    """
    return fetch_data(query, (hours,))

# Sidebar
st.sidebar.title("🚦 Traffic Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["🏠 Overview", "📊 Detailed Analysis", "🗺️ Map View", "📈 Trends", "⚠️ Alerts"]
)

time_range = st.sidebar.selectbox(
    "Time Range",
    ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"],
    index=2
)

hours_map = {
    "Last 1 Hour": 1,
    "Last 6 Hours": 6,
    "Last 24 Hours": 24,
    "Last 7 Days": 168
}
selected_hours = hours_map.get(time_range, 24)  # Default to 24 hours if key not found

auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=False)
if auto_refresh:
    st.sidebar.info("Dashboard will refresh every 30 seconds")
    time.sleep(30)
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Main content
if page == "🏠 Overview":
    st.title("🚦 Traffic Monitoring Dashboard")
    st.markdown("Real-time traffic monitoring and analytics")
    
    # Metrics
    latest = get_latest_metrics()
    vehicle_dist = get_vehicle_distribution(selected_hours)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total = latest['total_count'].sum()
        st.metric("🚗 Total Vehicles", f"{total:,}")
    
    with col2:
        cameras = len(latest)
        st.metric("📹 Active Cameras", cameras)
    
    with col3:
        if not vehicle_dist.empty:
            cars = vehicle_dist['cars'].iloc[0]
            cars_value = int(cars) if cars is not None and pd.notna(cars) else 0
            st.metric("🚙 Cars", f"{cars_value:,}")
        else:
            st.metric("🚙 Cars", "0")
    
    with col4:
        if not vehicle_dist.empty:
            motorcycles = vehicle_dist['motorcycles'].iloc[0]
            motorcycles_value = int(motorcycles) if motorcycles is not None and pd.notna(motorcycles) else 0
            st.metric("🏍️ Motorcycles", f"{motorcycles_value:,}")
        else:
            st.metric("🏍️ Motorcycles", "0")
    
    st.markdown("---")
    
    # Top cameras
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔥 Top 10 Busiest Cameras")
        top_cameras = get_top_cameras(10, selected_hours)
        if not top_cameras.empty:
            fig = px.bar(
                top_cameras,
                x='camera_name',
                y='avg_count',
                color='peak_count',
                title=f"Average Traffic Count ({time_range})",
                labels={'avg_count': 'Average Count', 'camera_name': 'Camera'},
                color_continuous_scale='Reds'
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🍩 Vehicle Type Distribution")
        if not vehicle_dist.empty:
            # Safely convert values with null checking
            cars_val = vehicle_dist['cars'].iloc[0]
            motorcycles_val = vehicle_dist['motorcycles'].iloc[0]
            buses_val = vehicle_dist['buses'].iloc[0]
            trucks_val = vehicle_dist['trucks'].iloc[0]
            
            values = [
                int(cars_val) if cars_val is not None and pd.notna(cars_val) else 0,
                int(motorcycles_val) if motorcycles_val is not None and pd.notna(motorcycles_val) else 0,
                int(buses_val) if buses_val is not None and pd.notna(buses_val) else 0,
                int(trucks_val) if trucks_val is not None and pd.notna(trucks_val) else 0
            ]
            labels = ['Cars', 'Motorcycles', 'Buses', 'Trucks']
            
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=.3,
                marker_colors=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
            )])
            fig.update_layout(title=f"Distribution ({time_range})")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No vehicle distribution data available")
    
    # Hourly trend
    st.subheader("📈 Traffic Trend Over Time")
    hourly = get_hourly_trend(selected_hours)
    if not hourly.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=hourly['time'],
            y=hourly['total_vehicles'],
            mode='lines+markers',
            name='Total',
            line=dict(color='#FF6B6B', width=3),
            fill='tonexty'
        ))
        fig.update_layout(
            title=f"Hourly Traffic Volume ({time_range})",
            xaxis_title="Time",
            yaxis_title="Number of Vehicles",
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "📊 Detailed Analysis":
    st.title("📊 Detailed Traffic Analysis")
    
    # Camera selector
    latest = get_latest_metrics()
    camera_options = latest['camera_name'].tolist()
    selected_camera = st.selectbox("Select Camera", camera_options)
    
    if selected_camera:
        camera_id = latest[latest['camera_name'] == selected_camera]['camera_id'].iloc[0]
        camera_data = get_camera_stats(camera_id, selected_hours)
        
        if not camera_data.empty:
            # Current stats
            latest_data = camera_data.iloc[-1]
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("🚗 Cars", int(latest_data['car_count']))
            with col2:
                st.metric("🏍️ Motorcycles", int(latest_data['motorcycle_count']))
            with col3:
                st.metric("🚌 Buses", int(latest_data['bus_count']))
            with col4:
                st.metric("🚚 Trucks", int(latest_data['truck_count']))
            
            st.markdown("---")
            
            # Time series for each vehicle type
            st.subheader("Vehicle Type Trends")
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(x=camera_data['time'], y=camera_data['car_count'],
                                    mode='lines', name='Cars', line=dict(color='#FF6B6B')))
            fig.add_trace(go.Scatter(x=camera_data['time'], y=camera_data['motorcycle_count'],
                                    mode='lines', name='Motorcycles', line=dict(color='#4ECDC4')))
            fig.add_trace(go.Scatter(x=camera_data['time'], y=camera_data['bus_count'],
                                    mode='lines', name='Buses', line=dict(color='#45B7D1')))
            fig.add_trace(go.Scatter(x=camera_data['time'], y=camera_data['truck_count'],
                                    mode='lines', name='Trucks', line=dict(color='#FFA07A')))
            
            fig.update_layout(
                title=f"{selected_camera} - Vehicle Breakdown",
                xaxis_title="Time",
                yaxis_title="Count",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Statistics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📊 Statistics")
                stats_df = pd.DataFrame({
                    'Metric': ['Average', 'Maximum', 'Minimum', 'Std Dev'],
                    'Value': [
                        f"{camera_data['total_count'].mean():.1f}",
                        f"{camera_data['total_count'].max()}",
                        f"{camera_data['total_count'].min()}",
                        f"{camera_data['total_count'].std():.1f}"
                    ]
                })
                st.dataframe(stats_df, use_container_width=True, hide_index=True)
            
            with col2:
                st.subheader("📈 Distribution")
                fig = px.histogram(
                    camera_data,
                    x='total_count',
                    nbins=20,
                    title="Traffic Count Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)

elif page == "🗺️ Map View":
    st.title("🗺️ Traffic Map Visualization")
    
    latest = get_latest_metrics()
    
    if not latest.empty and 'latitude' in latest.columns:
        # Remove null coordinates
        map_data = latest.dropna(subset=['latitude', 'longitude'])
        
        if not map_data.empty:
            # Prepare data for map
            map_data['size'] = map_data['total_count'] / map_data['total_count'].max() * 100
            
            # Plotly map
            fig = px.scatter_mapbox(
                map_data,
                lat='latitude',
                lon='longitude',
                size='size',
                color='total_count',
                hover_name='camera_name',
                hover_data={
                    'total_count': True,
                    'car_count': True,
                    'motorcycle_count': True,
                    'latitude': False,
                    'longitude': False,
                    'size': False
                },
                color_continuous_scale='Reds',
                size_max=30,
                zoom=10,
                title="Real-time Traffic Density Map"
            )
            
            fig.update_layout(
                mapbox_style="open-street-map",
                height=600
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Camera list
            st.subheader("📹 Camera Details")
            display_df = map_data[['camera_name', 'total_count', 'car_count', 'motorcycle_count', 'bus_count', 'truck_count']].copy()
            display_df.columns = ['Camera', 'Total', 'Cars', 'Motorcycles', 'Buses', 'Trucks']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No location data available for cameras")
    else:
        st.warning("No data available")

elif page == "📈 Trends":
    st.title("📈 Traffic Patterns & Trends")
    
    # Peak hours analysis
    st.subheader("⏰ Peak Hours Analysis (Last 7 Days)")
    peak_hours = get_peak_hours()
    
    if not peak_hours.empty:
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=peak_hours['hour'],
            y=peak_hours['avg_traffic'],
            name='Average',
            marker_color='lightblue'
        ))
        
        fig.add_trace(go.Scatter(
            x=peak_hours['hour'],
            y=peak_hours['max_traffic'],
            name='Peak',
            line=dict(color='red', width=2)
        ))
        
        fig.update_layout(
            title="Average Traffic by Hour of Day",
            xaxis_title="Hour",
            yaxis_title="Vehicle Count",
            xaxis=dict(tickmode='linear', tick0=0, dtick=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Weekday pattern
    st.subheader("📅 Weekday vs Weekend Pattern")
    weekday = get_weekday_pattern()
    
    if not weekday.empty:
        fig = px.line(
            weekday,
            x='hour',
            y='avg_traffic',
            color='day_type',
            title="Traffic Pattern: Weekday vs Weekend",
            labels={'hour': 'Hour of Day', 'avg_traffic': 'Average Traffic'},
            markers=True
        )
        fig.update_xaxes(tickmode='linear', tick0=0, dtick=1)
        st.plotly_chart(fig, use_container_width=True)
    
    # Hourly trend with vehicle breakdown
    st.subheader("🚗 Vehicle Type Trends")
    hourly = get_hourly_trend(selected_hours)
    
    if not hourly.empty:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(x=hourly['time'], y=hourly['cars'],
                                mode='lines', name='Cars', stackgroup='one'))
        fig.add_trace(go.Scatter(x=hourly['time'], y=hourly['motorcycles'],
                                mode='lines', name='Motorcycles', stackgroup='one'))
        fig.add_trace(go.Scatter(x=hourly['time'], y=hourly['buses'],
                                mode='lines', name='Buses', stackgroup='one'))
        fig.add_trace(go.Scatter(x=hourly['time'], y=hourly['trucks'],
                                mode='lines', name='Trucks', stackgroup='one'))
        
        fig.update_layout(
            title="Vehicle Type Distribution Over Time (Stacked)",
            xaxis_title="Time",
            yaxis_title="Count",
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "⚠️ Alerts":
    st.title("⚠️ Traffic Alerts & Anomalies")
    
    
    # Threshold-based alerts
    st.subheader("🔴 High Traffic Alerts")
    
    threshold = st.slider("Alert Threshold", 50, 500, 200)
    detection_mode = st.radio("Detection Mode", ["Instant (latest snapshot)", "Average (last N minutes)"], index=0)

    latest = get_latest_metrics()

    def _normalize_counts(df, col_name):
        if df is None or df.empty:
            return df
        df = df.copy()
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
        return df

    alerts = pd.DataFrame()

    # Instant mode: use latest snapshot
    if detection_mode.startswith("Instant"):
        if latest is None or latest.empty:
            st.info("No latest snapshot available; you can try the average-based detection.")
        else:
            latest = _normalize_counts(latest, 'total_count')
            alerts = latest[latest['total_count'] > threshold].copy()
            if not alerts.empty:
                alerts = alerts.rename(columns={'total_count': 'vehicle_count'})

    # Average mode or fallback: compute average over recent minutes
    if detection_mode.startswith("Average") or (alerts is None or alerts.empty):
        minutes = st.slider("Average window (minutes)", 5, 120, 30)
        query = """
        SELECT 
            camera_name,
            camera_id,
            AVG(total_count) as avg_count,
            MAX(time) as time
        FROM traffic_metrics
        WHERE time >= NOW() - INTERVAL '%s minutes'
        GROUP BY camera_name, camera_id
        HAVING AVG(total_count) > %s
        ORDER BY avg_count DESC
        """
        avg_df = fetch_data(query, (minutes, threshold))
        avg_df = _normalize_counts(avg_df, 'avg_count')
        if avg_df is not None and not avg_df.empty:
            avg_df = avg_df.rename(columns={'avg_count': 'vehicle_count'})
            alerts = avg_df.copy()

    # Display results
    if not alerts.empty:
        alerts['alert_level'] = alerts['vehicle_count'].apply(
            lambda x: '🔴 Critical' if x > threshold * 1.5 else '🟡 Warning'
        )
        # Ensure time column exists
        if 'time' not in alerts.columns:
            alerts['time'] = pd.NaT
        display_alerts = alerts[['camera_name', 'vehicle_count', 'alert_level', 'time']].copy()
        display_alerts.columns = ['Camera', 'Vehicle Count', 'Alert Level', 'Time']
        st.warning(f"⚠️ {len(display_alerts)} cameras above threshold!")
        st.dataframe(display_alerts, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No high traffic alerts")

    # Sudden spike detection
    st.subheader("📈 Recent Spike Detection")
    st.info("Detecting cameras with sudden traffic increase compared to the average over the last 1 hour")

    # Allow user to set multiplier (1.0 = greater than average)
    multiplier = st.slider("Spike multiplier (x average in last 1 hour)", 1.0, 3.0, 1.2, 0.1)

    latest = get_latest_metrics_1h()
    if latest is None or latest.empty:
        st.info("No latest snapshot available for spike detection.")
    else:
        # Fetch 1-hour averages per camera
        query = """
        SELECT camera_id, camera_name, AVG(total_count) as avg_last_hour
        FROM traffic_metrics
        WHERE time >= NOW() - INTERVAL '1 hour'
        GROUP BY camera_id, camera_name
        """
        avg_df = fetch_data(query)

        if avg_df is None or avg_df.empty:
            st.info("Not enough recent data to compute 1-hour averages.")
        else:
            # Normalize and merge
            latest_norm = _normalize_counts(latest, 'total_count')[['camera_id','camera_name','total_count','time']].copy()
            avg_df = avg_df.copy()
            avg_df['avg_last_hour'] = pd.to_numeric(avg_df['avg_last_hour'], errors='coerce').fillna(0).astype(float)

            merged = pd.merge(latest_norm, avg_df[['camera_id','avg_last_hour']], on='camera_id', how='inner')
            # Keep only cameras with a positive average in the last hour to avoid division by zero and invalid comparisons
            merged = merged[merged['avg_last_hour'] > 0].copy()

            # Detect spikes where latest > multiplier * avg_last_hour
            merged['spike'] = merged['total_count'] > (merged['avg_last_hour'] * float(multiplier))
            spike_df = merged[merged['spike']].copy()

            if spike_df.empty:
                st.success("✅ No recent spikes detected")
            else:
                # Compute percent increase where possible
                def _pct_inc(row):
                    if row['avg_last_hour'] == 0:
                        return None
                    return (row['total_count'] / row['avg_last_hour'] - 1) * 100

                spike_df['percent_increase'] = spike_df.apply(_pct_inc, axis=1)
                spike_df['percent_increase'] = spike_df['percent_increase'].map(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")

                display = spike_df[['camera_name','total_count','avg_last_hour','percent_increase','time']].copy()
                display.columns = ['Camera','Latest','Avg(1h)','Increase','Time']

                # Add alert level using the same threshold logic used by high-traffic alerts
                display['Alert Level'] = display['Latest'].apply(
                    lambda x: '🔴 Critical' if x > threshold * 1.5 else '🟡 Warning'
                )
                # Reorder columns for readability
                display = display[['Camera','Latest','Avg(1h)','Increase','Alert Level','Time']]

                # ===== PAGING =====
                page_size = st.selectbox("Rows per page", [5, 10, 20, 50], index=1)
                max_page = max(1, (len(display) - 1) // page_size + 1)
                page_num = st.number_input("Page", min_value=1, max_value=max_page, value=1)

                start = (page_num - 1) * page_size
                end = start + page_size

                num_critical = int((display['Alert Level'] == '🔴 Critical').sum())
                st.warning(f"⚠️ {len(display)} cameras exceeded {multiplier}× the 1-hour average ({num_critical} critical)")
                st.dataframe(display.iloc[start:end], use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Traffic Analytics Dashboard | Data updated every 10 seconds | 
        <a href='https://github.com/gnuhhung317/HugeData' target='_blank'>GitHub</a></p>
    </div>
    """,
    unsafe_allow_html=True
)
