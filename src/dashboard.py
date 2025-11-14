"""
simple dashboard to monitor the scraper pipeline
"""

import streamlit as st
from pymongo import MongoClient
from minio import Minio
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# connect to stuff
@st.cache_resource
def get_mongo():
    client = MongoClient('mongodb://localhost:27017/')
    return client['workplace_relations']

@st.cache_resource
def get_minio_landing():
    return Minio(
        'localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )

@st.cache_resource
def get_minio_curated():
    return Minio(
        'localhost:9002',
        access_key='minioadmin',
        secret_key='minioadmin123',
        secure=False
    )

def count_minio_files(client, bucket):
    """count files in a minio bucket"""
    try:
        objects = client.list_objects(bucket, recursive=True)
        return sum(1 for _ in objects)
    except:
        return 0

def get_pipeline_stats():
    """grab stats from mongodb and minio"""
    db = get_mongo()
    
    # mongo counts
    total_scraped = db['decisions'].count_documents({})
    with_files = db['decisions'].count_documents({'file_path': {'$exists': True}})
    curated_count = db['decisions_curated'].count_documents({})
    
    # minio counts
    landing_files = count_minio_files(get_minio_landing(), 'landing-zone')
    curated_files = count_minio_files(get_minio_curated(), 'curated-zone')
    
    return {
        'scraped': total_scraped,
        'downloaded': with_files,
        'landing_files': landing_files,
        'transformed': curated_count,
        'curated_files': curated_files
    }

def get_recent_activity():
    """show what happened recently"""
    db = get_mongo()
    
    # get last 100 scraped docs
    recent = list(db['decisions'].find(
        {'scraped_at': {'$exists': True}},
        {'identifier': 1, 'scraped_at': 1, 'published_date': 1, 'description': 1}
    ).sort('scraped_at', -1).limit(100))
    
    if not recent:
        return None
    
    # convert to dataframe
    df = pd.DataFrame(recent)
    df['scraped_at'] = pd.to_datetime(df['scraped_at'])
    df['date'] = df['scraped_at'].dt.date
    
    return df

def get_docs_by_month():
    """count docs by month"""
    db = get_mongo()
    
    docs = list(db['decisions'].find(
        {'partition_date': {'$exists': True}},
        {'partition_date': 1}
    ))
    
    if not docs:
        return None
    
    df = pd.DataFrame(docs)
    counts = df['partition_date'].value_counts().sort_index()
    
    return pd.DataFrame({
        'month': counts.index,
        'count': counts.values
    })

def get_publish_timeline():
    """docs by published date"""
    db = get_mongo()
    
    docs = list(db['decisions'].find(
        {'published_date': {'$exists': True, '$ne': None}},
        {'published_date': 1}
    ))
    
    if not docs:
        return None
    
    df = pd.DataFrame(docs)
    # extract just the date part if it's in dd/mm/yyyy format
    df['published_date'] = pd.to_datetime(df['published_date'], format='%d/%m/%Y', errors='coerce')
    df = df.dropna()
    df['month'] = df['published_date'].dt.to_period('M')
    
    counts = df['month'].value_counts().sort_index()
    
    return pd.DataFrame({
        'month': counts.index.astype(str),
        'count': counts.values
    })

def get_case_type_distribution():
    """count different case types from identifiers"""
    db = get_mongo()
    
    docs = list(db['decisions'].find(
        {'identifier': {'$exists': True}},
        {'identifier': 1}
    ))
    
    if not docs:
        return None
    
    df = pd.DataFrame(docs)
    # extract prefix from identifier (e.g., ADJ from ADJ-00012345)
    df['type'] = df['identifier'].str.extract(r'^([A-Z-]+)')
    
    counts = df['type'].value_counts()
    
    return pd.DataFrame({
        'type': counts.index,
        'count': counts.values
    })

# page setup
st.set_page_config(page_title="Scraper Dashboard", layout="wide")
st.title("Workplace Scraper Dashboard")

# auto refresh every 30 seconds
st.write("Auto-refreshes every 30 seconds")

# main stats
st.header("Pipeline Status")

stats = get_pipeline_stats()

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Scraped", f"{stats['scraped']:,}")
    
with col2:
    st.metric("Downloaded", f"{stats['downloaded']:,}")
    st.caption(f"{stats['landing_files']:,} files in MinIO")
    
with col3:
    download_pct = (stats['downloaded'] / stats['scraped'] * 100) if stats['scraped'] > 0 else 0
    st.metric("Download %", f"{download_pct:.1f}%")
    
with col4:
    st.metric("Transformed", f"{stats['transformed']:,}")
    st.caption(f"{stats['curated_files']:,} files in MinIO")
    
with col5:
    transform_pct = (stats['transformed'] / stats['downloaded'] * 100) if stats['downloaded'] > 0 else 0
    st.metric("Transform %", f"{transform_pct:.1f}%")

# health check
st.header("Health")

health_col1, health_col2, health_col3 = st.columns(3)

with health_col1:
    # check mongo sync
    mongo_sync = stats['downloaded'] == stats['landing_files']
    if mongo_sync:
        st.success("✓ MongoDB ↔ Landing MinIO synced")
    else:
        diff = abs(stats['downloaded'] - stats['landing_files'])
        st.warning(f"⚠ Sync issue: {diff} file difference")

with health_col2:
    # check curated sync
    curated_sync = stats['transformed'] == stats['curated_files']
    if curated_sync:
        st.success("✓ Curated MongoDB ↔ MinIO synced")
    else:
        diff = abs(stats['transformed'] - stats['curated_files'])
        st.warning(f"⚠ Sync issue: {diff} file difference")

with health_col3:
    # overall pipeline health
    pending_download = stats['scraped'] - stats['downloaded']
    pending_transform = stats['downloaded'] - stats['transformed']
    
    if pending_download == 0 and pending_transform == 0:
        st.success("✓ Pipeline caught up")
    else:
        st.info(f"📝 Pending: {pending_download} downloads, {pending_transform} transforms")

# charts
st.header("Pipeline Charts")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    # pipeline funnel
    fig_funnel = go.Figure(go.Funnel(
        y = ['Scraped', 'Downloaded', 'Transformed'],
        x = [stats['scraped'], stats['downloaded'], stats['transformed']],
        textinfo = "value+percent initial"
    ))
    fig_funnel.update_layout(title="Pipeline Funnel", height=400)
    st.plotly_chart(fig_funnel, width='stretch')

with chart_col2:
    # documents by storage month
    monthly = get_docs_by_month()
    if monthly is not None:
        fig_monthly = px.bar(monthly, x='month', y='count', title='Documents by Storage Month (partition_date)')
        fig_monthly.update_layout(height=400)
        st.plotly_chart(fig_monthly, width='stretch')
    else:
        st.info("No monthly data yet")

# distribution insights
st.header("Document Insights")


    # case types
case_types = get_case_type_distribution()
if case_types is not None:
    fig_types = px.pie(case_types, values='count', names='type', title='Case Types')
    fig_types.update_layout(height=400)
    st.plotly_chart(fig_types, width='stretch')
else:
    st.info("No case type data")

# published timeline
st.subheader("Publishing Timeline")

publish_timeline = get_publish_timeline()
if publish_timeline is not None and len(publish_timeline) > 0:
    fig_publish = px.line(publish_timeline, x='month', y='count', 
                         title='Documents by Published Date (actual case dates)',
                         markers=True)
    fig_publish.update_layout(height=400)
    st.plotly_chart(fig_publish, width='stretch')
else:
    st.info("No published date data")

# recent activity
st.header("Recent Activity")

recent_df = get_recent_activity()
if recent_df is not None:
    # show daily counts
    daily_counts = recent_df.groupby('date').size().reset_index(name='count')
    daily_counts = daily_counts.sort_values('date', ascending=False).head(14)
    
    fig_daily = px.bar(daily_counts, x='date', y='count', title='Scraped per Day (Last 2 weeks)')
    st.plotly_chart(fig_daily, width='stretch')
    
    # show recent docs table
    st.subheader("Latest Documents")
    display_df = recent_df[['identifier', 'description', 'published_date', 'scraped_at']].head(20)
    st.dataframe(display_df, width='stretch', hide_index=True)
else:
    st.info("No recent activity")

# auto refresh
import time
time.sleep(30)
st.rerun()
