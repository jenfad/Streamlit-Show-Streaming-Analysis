import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import json


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


def detailed_analysis():
    """
    Display detailed more granular analysis.
    """
    st.title("🎬 Detailed Analysis")
    st.markdown("Deep dive into individual show performance and detailed analytics")
    st.markdown("---")
    
    # Load the data
    df = load_data()
    
    # Sidebar filters (removed for now)
    #st.sidebar.header("Filters")

    # Show selection
    shows = sorted(df["show_name"].unique().to_list())
    selected_show = st.selectbox(
        "Select a show to see its detailed analysis",
        options=["All Shows"] + shows,
        index=0
    )
    
    st.markdown("")

    # Create filter controls in 4 columns
    col1, col2, col3, col4, col5 = st.columns(5)

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
            default=states  # All states selected by default
        )
    
    # Genre filter
    with col3:
        genres = sorted(df["show_genre"].unique().to_list())
        selected_genres = st.multiselect(
            "Genres",
            options=genres,
            default=genres  # All genres selected by default
        )
    
    # Show type filter
    with col4:
        show_types = sorted(df["show_type"].unique().to_list())
        selected_show_types = st.multiselect(
            "Show Types",
            options=show_types,
            default=show_types  # All show types selected by default
        )

    
    

    
    # Handle date range input
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range
    
    # Apply base filters
    filtered_df = df.filter(
        (pl.col("created_date") >= start_date) &
        (pl.col("created_date") <= end_date) &
        (pl.col("state").is_in(selected_states)) &
        (pl.col("show_genre").is_in(selected_genres)) &
        (pl.col("show_type").is_in(selected_show_types))
    )
    
    # Apply show filter if specific show is selected
    if selected_show != "All Shows":
        filtered_df = filtered_df.filter(pl.col("show_name") == selected_show)
        
        # Show specific analysis
        st.subheader(f"📊 Analysis for: {selected_show}")
        
        # Show details
        show_info = filtered_df.select([
            "show_genre", "show_type", "show_rating", "show_description", "show_duration_seconds"
        ]).unique()
        
        if len(show_info) > 0:
            show_details = show_info.row(0)
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Genre", show_details[0])
                st.metric("Type", show_details[1])
            
            with col2:
                st.metric("Rating", show_details[2])
                duration_minutes = show_details[4] / 60
                st.metric("Duration", f"{duration_minutes:.0f} min")
            
            with col3:
                total_views = len(filtered_df)
                unique_viewers = filtered_df["user_id"].n_unique()
                st.metric("Total Views", f"{total_views:,}")
                st.metric("Unique Viewers", f"{unique_viewers:,}")
            
            st.write("**Description:**", show_details[3])
    else:
        st.subheader("📊 All Shows Analysis")
    
    st.markdown("---")
    
    # Create tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs(["🎯 Performance Metrics", "⏱️ Viewing Behavior", "📍 Geographic Analysis", "📈 Trends"])
    
    # Tab 1: Performance Metrics
    with tab1:
        if selected_show != "All Shows":
            # Single show metrics
            completion_rates = (filtered_df["user_watch_duration_seconds"] / filtered_df["show_duration_seconds"] * 100)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Completion Rate Distribution")
                fig1 = px.histogram(
                    x=completion_rates.to_list(),
                    nbins=20,
                    title=f"Completion Rate Distribution - {selected_show}",
                    labels={"x": "Completion Rate (%)", "y": "Number of Views"}
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                st.subheader("Viewing Statistics")
                avg_completion = completion_rates.mean()
                median_completion = completion_rates.median()
                max_completion = completion_rates.max()
                min_completion = completion_rates.min()
                
                st.metric("Average Completion", f"{avg_completion:.1f}%")
                st.metric("Median Completion", f"{median_completion:.1f}%")
                st.metric("Max Completion", f"{max_completion:.1f}%")
                st.metric("Min Completion", f"{min_completion:.1f}%")
        
        else:
            # All shows comparison
            show_metrics = (
                filtered_df
                .with_columns([
                    (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).alias("completion_percentage")
                ])
                .group_by("show_name")
                .agg([
                    pl.len().alias("total_views"),
                    pl.col("user_id").n_unique().alias("unique_viewers"),
                    pl.col("completion_percentage").mean().alias("avg_completion"),
                    pl.col("user_watch_duration_seconds").mean().alias("avg_watch_time")
                ])
                .sort("total_views", descending=True)
            )
            
            st.subheader("Show Performance Comparison")
            
            # Create interactive scatter plot
            fig = px.scatter(
                show_metrics.to_pandas(),
                x="total_views",
                y="avg_completion",
                size="unique_viewers",
                color="avg_completion",
                hover_name="show_name",
                title="Show Performance: Views vs Completion Rate",
                labels={
                    "total_views": "Total Views",
                    "avg_completion": "Average Completion Rate (%)",
                    "unique_viewers": "Unique Viewers"
                }
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Show metrics table
            st.subheader("Detailed Show Metrics")
            display_df = show_metrics.to_pandas()
            display_df["avg_completion"] = display_df["avg_completion"].round(1)
            display_df["avg_watch_time"] = (display_df["avg_watch_time"] / 60).round(1)  # Convert to minutes
            display_df.columns = ["Show Name", "Total Views", "Unique Viewers", "Avg Completion (%)", "Avg Watch Time (min)"]
            st.dataframe(display_df, use_container_width=True)
    
    # Tab 2: Viewing Behavior
    with tab2:
        st.subheader("Viewing Behavior Analysis")
        
        # Watch time vs completion analysis
        behavior_df = filtered_df.with_columns([
            (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).alias("completion_percentage")
        ])
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Hourly viewing pattern
            hourly_views = (
                behavior_df
                .with_columns([
                    pl.col("created_at").dt.hour().alias("hour")
                ])
                .group_by("hour")
                .agg([
                    pl.len().alias("views"),
                    pl.col("completion_percentage").mean().alias("avg_completion")
                ])
                .sort("hour")
            )
            
            fig2 = px.bar(
                hourly_views.to_pandas(),
                x="hour",
                y="views",
                title="Views by Hour of Day",
                labels={"hour": "Hour", "views": "Number of Views"}
            )
            st.plotly_chart(fig2, use_container_width=True)
        
        with col2:
            # Watch duration vs completion
            sample_df = behavior_df.sample(n=min(1000, len(behavior_df)))  # Sample for performance
            
            fig3 = px.scatter(
                sample_df.to_pandas(),
                x="user_watch_duration_seconds",
                y="completion_percentage",
                color="show_type" if selected_show == "All Shows" else None,
                title="Watch Duration vs Completion Rate",
                labels={
                    "user_watch_duration_seconds": "Watch Duration (seconds)",
                    "completion_percentage": "Completion Rate (%)"
                }
            )
            st.plotly_chart(fig3, use_container_width=True)
    
    # Tab 3: Geographic Analysis
    with tab3:
        st.subheader("Geographic Distribution")
        
        geo_stats = (
            filtered_df
            .group_by("state")
            .agg([
                pl.len().alias("views"),
                pl.col("user_id").n_unique().alias("unique_viewers"),
                (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).mean().alias("avg_completion")
            ])
            .sort("views", descending=True)
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Views by state
            fig4 = px.bar(
                geo_stats.to_pandas(),
                x="state",
                y="views",
                title="Views by State",
                labels={"state": "State", "views": "Number of Views"}
            )
            fig4.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig4, use_container_width=True)
        
        with col2:
            # Completion rate by state
            fig5 = px.bar(
                geo_stats.to_pandas(),
                x="state",
                y="avg_completion",
                title="Average Completion Rate by State",
                labels={"state": "State", "avg_completion": "Average Completion Rate (%)"}
            )
            fig5.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig5, use_container_width=True)
        
        # Geographic metrics table
        st.subheader("State Performance Metrics")
        display_geo = geo_stats.to_pandas()
        display_geo["avg_completion"] = display_geo["avg_completion"].round(1)
        display_geo.columns = ["State", "Total Views", "Unique Viewers", "Avg Completion (%)"]
        st.dataframe(display_geo, use_container_width=True)
    
    # Tab 4: Trends
    with tab4:
        st.subheader("Viewing Trends Over Time")
        
        # Daily trends
        daily_trends = (
            filtered_df
            .group_by("created_date")
            .agg([
                pl.len().alias("views"),
                pl.col("user_id").n_unique().alias("unique_viewers"),
                (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).mean().alias("avg_completion")
            ])
            .sort("created_date")
        )
        
        # Multiple line chart
        fig6 = go.Figure()
        
        # Add views trend
        fig6.add_trace(go.Scatter(
            x=daily_trends["created_date"].to_list(),
            y=daily_trends["views"].to_list(),
            mode='lines+markers',
            name='Views',
            yaxis='y'
        ))
        
        # Add completion rate trend on secondary y-axis
        fig6.add_trace(go.Scatter(
            x=daily_trends["created_date"].to_list(),
            y=daily_trends["avg_completion"].to_list(),
            mode='lines+markers',
            name='Avg Completion Rate (%)',
            yaxis='y2'
        ))
        
        # Update layout for dual y-axis
        fig6.update_layout(
            title="Daily Views and Completion Rate Trends",
            xaxis_title="Date",
            yaxis=dict(
                title="Number of Views",
                side="left"
            ),
            yaxis2=dict(
                title="Average Completion Rate (%)",
                side="right",
                overlaying="y"
            ),
            height=500
        )
        
        st.plotly_chart(fig6, use_container_width=True)
        
        # Weekly aggregation for longer time periods
        weekly_trends = (
            filtered_df
            .with_columns([
                pl.col("created_date").dt.week().alias("week"),
                pl.col("created_date").dt.year().alias("year")
            ])
            .group_by(["year", "week"])
            .agg([
                pl.len().alias("views"),
                (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).mean().alias("avg_completion")
            ])
            .sort(["year", "week"])
        )
        
        if len(weekly_trends) > 1:
            st.subheader("Weekly Trends")
            
            # Create week labels
            weekly_trends_pd = weekly_trends.to_pandas()
            weekly_trends_pd['week_label'] = weekly_trends_pd['year'].astype(str) + '-W' + weekly_trends_pd['week'].astype(str).str.zfill(2)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig7 = px.line(
                    weekly_trends_pd,
                    x="week_label",
                    y="views",
                    title="Weekly Views Trend",
                    labels={"week_label": "Week", "views": "Views"}
                )
                fig7.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig7, use_container_width=True)
            
            with col2:
                fig8 = px.line(
                    weekly_trends_pd,
                    x="week_label",
                    y="avg_completion",
                    title="Weekly Completion Rate Trend",
                    labels={"week_label": "Week", "avg_completion": "Avg Completion (%)"}
                )
                fig8.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig8, use_container_width=True)