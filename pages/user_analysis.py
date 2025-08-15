import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta


@st.cache_data
def load_data():
    """
    Load streaming data from JSON file and prepare it for analysis.
    
    Returns:
        pl.DataFrame: Processed dataframe with parsed dates and calculated completion rates
    """
    with open('streaming_data.json', 'r') as f:
        data = json.load(f)
    
    # Convert to Polars DataFrame and process columns
    df = pl.DataFrame(data)
    df = df.with_columns([
        # Parse date strings to proper date types
        pl.col("created_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S"),
        # Calculate completion rate as percentage
        (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).round(2).alias("completion_rate")
    ])
    return df


def user_analysis():
    """
    Display comprehensive user behavior and engagement analysis.
    """
    st.title("👥 User Analysis")
    st.markdown("Deep dive into user behavior, engagement patterns, and segmentation")
    st.markdown("---")
    
    # Load the data
    df = load_data()
    
    # Create filter controls
    col1, col2, col3, col4 = st.columns(4)
    
    # Date range filter
    with col1:
        date_range = st.date_input(
            "Date Range",
            value=(df["created_date"].min(), df["created_date"].max()),
            min_value=df["created_date"].min(),
            max_value=df["created_date"].max()
        )
    
    # State filter
    with col2:
        states = sorted(df["state"].unique().to_list())
        selected_states = st.multiselect(
            "States",
            options=states,
            default=states[:5] if len(states) > 5 else states
        )
    
    # Genre filter
    with col3:
        genres = sorted(df["show_genre"].unique().to_list())
        selected_genres = st.multiselect(
            "Genres",
            options=genres,
            default=genres
        )
    
    # Show type filter
    with col4:
        show_types = sorted(df["show_type"].unique().to_list())
        selected_show_types = st.multiselect(
            "Show Types",
            options=show_types,
            default=show_types
        )
    
    # Handle date range input
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range
    
    # Apply filters
    filtered_df = df.filter(
        (pl.col("created_date") >= start_date) &
        (pl.col("created_date") <= end_date) &
        (pl.col("state").is_in(selected_states)) &
        (pl.col("show_genre").is_in(selected_genres)) &
        (pl.col("show_type").is_in(selected_show_types))
    )
    
    # Additional user-specific filters
    st.markdown("### 🎯 Advanced User Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Minimum views filter
        min_views = st.slider(
            "Minimum Views per User",
            min_value=1,
            max_value=50,
            value=1,
            help="Filter users with at least this many views"
        )
    
    with col2:
        # Completion rate filter
        completion_range = st.slider(
            "Completion Rate Range (%)",
            min_value=0.0,
            max_value=100.0,
            value=(0.0, 100.0),
            step=5.0,
            help="Filter users by their average completion rate"
        )
    
    with col3:
        # Watch time filter
        min_watch_hours = st.slider(
            "Minimum Total Watch Hours",
            min_value=0.0,
            max_value=100.0,
            value=0.0,
            step=1.0,
            help="Filter users by total watch time"
        )
    
    st.markdown("---")
    
    # Calculate user metrics
    user_metrics = (
        filtered_df
        .group_by("user_id")
        .agg([
            pl.len().alias("total_views"),
            pl.col("user_watch_duration_seconds").sum().alias("total_watch_time"),
            pl.col("completion_rate").mean().alias("avg_completion_rate"),
            pl.col("show_name").n_unique().alias("unique_shows"),
            pl.col("show_genre").n_unique().alias("unique_genres"),
            pl.col("created_date").min().alias("first_view_date"),
            pl.col("created_date").max().alias("last_view_date"),
            pl.col("state").first().alias("user_state")
        ])
        .with_columns([
            (pl.col("total_watch_time") / 3600).round(2).alias("total_watch_hours"),
            (pl.col("last_view_date") - pl.col("first_view_date")).dt.total_days().alias("days_active")
        ])
    )
    
    # Apply advanced filters
    try:
        filtered_users = user_metrics.filter(
            (pl.col("total_views") >= min_views) &
            (pl.col("avg_completion_rate") >= completion_range[0]) &
            (pl.col("avg_completion_rate") <= completion_range[1]) &
            (pl.col("total_watch_hours") >= min_watch_hours)
        )
    except Exception as e:
        st.error(f"Error applying filters: try adjusting the total views or average completion rate filters. Error: {e}")
        filtered_users = user_metrics
    
    # Create tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs(["📊 User Overview", "🎯 Engagement Analysis", "👥 User Segmentation", "📈 Cohort Analysis"])
    
    # Tab 1: User Overview
    with tab1:
        st.subheader("User Base Overview")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_users = len(filtered_users)
        
        # Check if we have users after filtering
        if total_users > 0:
            avg_views = filtered_users["total_views"].mean()
            avg_completion = filtered_users["avg_completion_rate"].mean()
            avg_watch_hours = filtered_users["total_watch_hours"].mean()
        else:
            avg_views = 0
            avg_completion = 0
            avg_watch_hours = 0
        
        with col1:
            st.metric("Total Users", f"{total_users:,}")
        
        with col2:
            st.metric("Avg Views per User", f"{avg_views:.1f}")
        
        with col3:
            st.metric("Avg Completion Rate", f"{avg_completion:.1f}%")
        
        with col4:
            st.metric("Avg Watch Hours", f"{avg_watch_hours:.1f}")
        
        st.markdown("---")
        
        # Show message if no users match filters
        if total_users == 0:
            st.warning("⚠️ No users match the current filter criteria. Try adjusting the filters to see data.")
            return
        
        # User distribution charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Views distribution
            fig1 = px.histogram(
                filtered_users.to_pandas(),
                x="total_views",
                range_x=[0, filtered_users["total_views"].max() + 1],
                nbins=2,
                title="Distribution of Views per User",
                labels={"total_views": "Total Views", "count": "Number of Users"}
            )
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Completion rate distribution
            fig2 = px.histogram(
                filtered_users.to_pandas(),
                x="avg_completion_rate",
                nbins=20,
                title="Distribution of User Completion Rates",
                labels={"avg_completion_rate": "Average Completion Rate (%)", "count": "Number of Users"}
            )
            st.plotly_chart(fig2, use_container_width=True)
        
        # Geographic distribution
        user_by_state = (
            filtered_users
            .group_by("user_state")
            .agg([
                pl.len().alias("user_count"),
                pl.col("total_views").mean().alias("avg_views_per_user"),
                pl.col("avg_completion_rate").mean().alias("avg_completion")
            ])
            .sort("user_count", descending=True)
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig3 = px.bar(
                user_by_state.to_pandas(),
                x="user_state",
                y="user_count",
                title="Users by State",
                labels={"user_state": "State", "user_count": "Number of Users"}
            )
            st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            fig4 = px.scatter(
                user_by_state.to_pandas(),
                x="avg_views_per_user",
                y="avg_completion",
                size="user_count",
                color="user_state",
                title="State Performance: Avg Views vs Completion",
                labels={
                    "avg_views_per_user": "Avg Views per User",
                    "avg_completion": "Avg Completion Rate (%)"
                }
            )
            st.plotly_chart(fig4, use_container_width=True)
    
    # Tab 2: Engagement Analysis
    with tab2:
        st.subheader("User Engagement Analysis")
        
        # Engagement metrics
        engagement_stats = (
            filtered_users
            .with_columns([
                pl.when(pl.col("total_views") >= 10).then(pl.lit("High"))
                .when(pl.col("total_views") >= 5).then(pl.lit("Medium"))
                .otherwise(pl.lit("Low")).alias("engagement_level")
            ])
        )
        
        # Engagement level distribution
        engagement_dist = (
            engagement_stats
            .group_by("engagement_level")
            .agg([
                pl.len().alias("user_count"),
                pl.col("avg_completion_rate").mean().alias("avg_completion"),
                pl.col("total_watch_hours").mean().alias("avg_watch_hours")
            ])
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig5 = px.pie(
                engagement_dist.to_pandas(),
                values="user_count",
                names="engagement_level",
                title="User Engagement Distribution",
                color_discrete_map={
                    "High": "#2E8B57",
                    "Medium": "#FFD700", 
                    "Low": "#FF6347"
                }
            )
            st.plotly_chart(fig5, use_container_width=True)
        
        with col2:
            fig6 = px.bar(
                engagement_dist.to_pandas(),
                x="engagement_level",
                y="avg_completion",
                title="Avg Completion Rate by Engagement Level",
                labels={"engagement_level": "Engagement Level", "avg_completion": "Avg Completion Rate (%)"},
                color="engagement_level",
                color_discrete_map={
                    "High": "#2E8B57",
                    "Medium": "#FFD700", 
                    "Low": "#FF6347"
                }
            )
            st.plotly_chart(fig6, use_container_width=True)
        
        # User activity patterns
        user_activity = (
            filtered_df
            .with_columns([
                pl.col("created_at").dt.hour().alias("hour"),
                pl.col("created_at").dt.weekday().alias("weekday")
            ])
            .group_by(["user_id", "hour"])
            .agg([pl.len().alias("views")])
            .group_by("hour")
            .agg([
                pl.col("views").mean().alias("avg_views_per_user"),
                pl.col("user_id").n_unique().alias("active_users")
            ])
            .sort("hour")
        )
        
        fig7 = px.line(
            user_activity.to_pandas(),
            x="hour",
            y="active_users",
            title="Active Users by Hour of Day",
            labels={"hour": "Hour", "active_users": "Number of Active Users"}
        )
        st.plotly_chart(fig7, use_container_width=True)
        
        # Content diversity analysis
        content_diversity = (
            filtered_users
            .select(["user_id", "unique_shows", "unique_genres", "total_views"])
        )
        
        fig8 = px.scatter(
            content_diversity.to_pandas(),
            x="unique_shows",
            y="unique_genres",
            size="total_views",
            title="Content Diversity: Unique Shows vs Genres per User",
            labels={
                "unique_shows": "Unique Shows Watched",
                "unique_genres": "Unique Genres Watched",
                "total_views": "Total Views"
            }
        )
        st.plotly_chart(fig8, use_container_width=True)
    
    # Tab 3: User Segmentation
    with tab3:
        st.subheader("User Segmentation Analysis")
        st.warning("⚠️ In progress: More data is needed to fully segment users.")
        
        # Create user segments based on behavior
        segmented_users = (
            filtered_users
            .with_columns([
                # Viewing frequency segment
                pl.when(pl.col("total_views") >= 20).then(pl.lit("Heavy Viewer"))
                .when(pl.col("total_views") >= 10).then(pl.lit("Regular Viewer"))
                .when(pl.col("total_views") >= 5).then(pl.lit("Casual Viewer"))
                .otherwise(pl.lit("Light Viewer")).alias("viewer_segment"),
                
                # Completion behavior segment
                pl.when(pl.col("avg_completion_rate") >= 80).then(pl.lit("Completionist"))
                .when(pl.col("avg_completion_rate") >= 60).then(pl.lit("Engaged"))
                .when(pl.col("avg_completion_rate") >= 40).then(pl.lit("Selective"))
                .otherwise(pl.lit("Browser")).alias("completion_segment")
            ])
        )
        
        # Segment analysis
        segment_analysis = (
            segmented_users
            .group_by(["viewer_segment", "completion_segment"])
            .agg([
                pl.len().alias("user_count"),
                pl.col("total_watch_hours").mean().alias("avg_watch_hours"),
                pl.col("unique_shows").mean().alias("avg_unique_shows")
            ])
        )
        
        # Create segment heatmap
        segment_pivot = segment_analysis.to_pandas().pivot_table(
            values='user_count', 
            index='completion_segment', 
            columns='viewer_segment', 
            fill_value=0
        )
        
        fig9 = px.imshow(
            segment_pivot,
            title="User Segmentation Matrix",
            labels=dict(x="Viewer Segment", y="Completion Segment", color="User Count"),
            aspect="auto"
        )
        st.plotly_chart(fig9, use_container_width=True)
        
        # Segment characteristics
        viewer_segments = (
            segmented_users
            .group_by("viewer_segment")
            .agg([
                pl.len().alias("user_count"),
                pl.col("total_views").mean().alias("avg_views"),
                pl.col("avg_completion_rate").mean().alias("avg_completion"),
                pl.col("total_watch_hours").mean().alias("avg_watch_hours")
            ])
        )
        
        st.subheader("Viewer Segment Characteristics")
        col1, col2 = st.columns(2)
        
        with col1:
            fig10 = px.bar(
                viewer_segments.to_pandas(),
                x="viewer_segment",
                y="user_count",
                title="Users by Viewer Segment",
                labels={"viewer_segment": "Viewer Segment", "user_count": "Number of Users"}
            )
            st.plotly_chart(fig10, use_container_width=True)
        
        with col2:
            fig11 = px.bar(
                viewer_segments.to_pandas(),
                x="viewer_segment",
                y="avg_watch_hours",
                title="Avg Watch Hours by Segment",
                labels={"viewer_segment": "Viewer Segment", "avg_watch_hours": "Avg Watch Hours"}
            )
            st.plotly_chart(fig11, use_container_width=True)
        
        # Detailed segment table
        st.subheader("Detailed Segment Metrics")
        display_segments = viewer_segments.to_pandas()
        display_segments = display_segments.round(2)
        display_segments.columns = ["Viewer Segment", "User Count", "Avg Views", "Avg Completion (%)", "Avg Watch Hours"]
        st.dataframe(display_segments, use_container_width=True)
    
    # Tab 4: Cohort Analysis
    with tab4:
        st.subheader("User Cohort Analysis")
        
        # Define cohorts based on first view date
        cohort_data = (
            filtered_df
            .group_by("user_id")
            .agg([
                pl.col("created_date").min().alias("first_view_date"),
                pl.col("created_date").max().alias("last_view_date")
            ])
            .with_columns([
                pl.col("first_view_date").dt.strftime("%Y-%m").alias("cohort_month")
            ])
        )
        
        # Calculate retention by cohort
        cohort_summary = (
            cohort_data
            .group_by("cohort_month")
            .agg([
                pl.len().alias("total_users"),
                (pl.col("last_view_date") - pl.col("first_view_date")).dt.total_days().mean().alias("avg_lifetime_days")
            ])
            .sort("cohort_month")
        )
        
        # User lifecycle analysis
        lifecycle_stats = (
            filtered_users
            .with_columns([
                pl.when(pl.col("days_active") == 0).then(pl.lit("Single Day"))
                .when(pl.col("days_active") <= 7).then(pl.lit("1 Week"))
                .when(pl.col("days_active") <= 30).then(pl.lit("1 Month"))
                .when(pl.col("days_active") <= 90).then(pl.lit("3 Months"))
                .otherwise(pl.lit("3+ Months")).alias("lifecycle_stage")
            ])
            .group_by("lifecycle_stage")
            .agg([
                pl.len().alias("user_count"),
                pl.col("total_views").mean().alias("avg_views"),
                pl.col("avg_completion_rate").mean().alias("avg_completion")
            ])
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig12 = px.bar(
                cohort_summary.to_pandas(),
                x="cohort_month",
                y="total_users",
                title="User Acquisition by Cohort Month",
                labels={"cohort_month": "Cohort Month", "total_users": "New Users"}
            )
            fig12.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig12, use_container_width=True)
        
        with col2:
            fig13 = px.pie(
                lifecycle_stats.to_pandas(),
                values="user_count",
                names="lifecycle_stage",
                title="User Lifecycle Distribution"
            )
            st.plotly_chart(fig13, use_container_width=True)
        
        # User retention analysis
        if len(cohort_summary) > 1:
            fig14 = px.line(
                cohort_summary.to_pandas(),
                x="cohort_month",
                y="avg_lifetime_days",
                title="Average User Lifetime by Cohort",
                labels={"cohort_month": "Cohort Month", "avg_lifetime_days": "Avg Lifetime (Days)"}
            )
            fig14.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig14, use_container_width=True)
        
        # Cohort behavior comparison
        st.subheader("Cohort Behavior Comparison")
        display_cohorts = cohort_summary.to_pandas()
        display_cohorts["avg_lifetime_days"] = display_cohorts["avg_lifetime_days"].round(1)
        display_cohorts.columns = ["Cohort Month", "Total Users", "Avg Lifetime (Days)"]
        st.dataframe(display_cohorts, use_container_width=True)
    
    st.markdown("---")
    
    # User details table
    st.subheader("📋 Top Users by Engagement")
    top_users = (
        filtered_users
        .sort("total_views", descending=True)
        .head(20)
        .select([
            "user_id", "total_views", "total_watch_hours", 
            "avg_completion_rate", "unique_shows", "user_state"
        ])
    )
    
    display_users = top_users.to_pandas()
    display_users["avg_completion_rate"] = display_users["avg_completion_rate"].round(1)
    display_users.columns = [
        "User ID", "Total Views", "Watch Hours", 
        "Avg Completion (%)", "Unique Shows", "State"
    ]
    st.dataframe(display_users, use_container_width=True)