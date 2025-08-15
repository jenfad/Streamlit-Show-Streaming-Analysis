import streamlit as st
import polars as pl
import plotly.express as px
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


def overview():
    """
    Display the main dashboard with overview analytics.
    """
    st.title("📺 Streaming Analytics Dashboard")
    st.markdown("Overview of streaming platform performance and user engagement")
    st.markdown("---")
    st.info('Navigate to other pages on this dashboard using the left sidebar', icon="ℹ️")
    
    # Load the data
    df = load_data()
    
    # Create filter controls in 4 columns
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
    
    # Handle date range input (can be single date or tuple)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range
    
    # Apply all filters to the dataframe
    filtered_df = df.filter(
        (pl.col("created_date") >= start_date) &
        (pl.col("created_date") <= end_date) &
        (pl.col("state").is_in(selected_states)) &
        (pl.col("show_genre").is_in(selected_genres)) &
        (pl.col("show_type").is_in(selected_show_types))
    )
    
    st.markdown("---")
    
    # Create tabs for different analysis views
    tab1, tab2 = st.tabs(["📊 Viewing Patterns", "🎯 Completion Analysis"])
    
    # Tab 1: Viewing Patterns Analysis
    with tab1:
        st.subheader("Daily Viewing Activity")
        
        # Aggregate daily viewing statistics
        daily_views = (
            filtered_df
            .group_by("created_date")
            .agg([
                pl.len().alias("total_views"),  # Count of views per day
                pl.col("user_watch_duration_seconds").sum().alias("total_watch_time"),
                pl.col("user_id").n_unique().alias("unique_users")  # Unique users per day
            ])
            .sort("created_date")
        )
        
        # Create line chart for daily views trend
        fig1 = px.line(
            daily_views.to_pandas(),
            x="created_date",
            y="total_views",
            title="Daily Total Views",
            labels={"total_views": "Number of Views", "created_date": "Date"}
        )
        fig1.update_layout(height=400)
        st.plotly_chart(fig1, use_container_width=True)
        
        # Create two columns for genre and state analysis
        col1, col2 = st.columns(2)
        
        # Genre analysis
        with col1:
            # Aggregate viewing statistics by genre
            genre_stats = (
                filtered_df
                .group_by("show_genre")
                .agg([
                    pl.len().alias("views"),
                    pl.col("user_watch_duration_seconds").mean().alias("avg_watch_time")
                ])
                .sort("views", descending=True)
            )
            
            # Create bar chart for views by genre
            fig2 = px.bar(
                genre_stats.to_pandas(),
                x="show_genre",
                y="views",
                title="Views by Genre",
                labels={"views": "Number of Views", "show_genre": "Genre"}
            )
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)
        
        # State analysis
        with col2:
            # Aggregate viewing statistics by state
            state_stats = (
                filtered_df
                .group_by("state")
                .agg([
                    pl.len().alias("views"),
                    pl.col("user_id").n_unique().alias("unique_users")
                ])
                .sort("views", descending=True)
            )
            
            # Create pie chart for views by state
            fig3 = px.pie(
                state_stats.to_pandas(),
                values="views",
                names="state",
                title="Views by State"
            )
            fig3.update_layout(height=350)
            st.plotly_chart(fig3, use_container_width=True)
    
    # Tab 2: Completion Analysis
    with tab2:
        st.subheader("Content Completion Analysis")
        
        # Calculate completion percentage for each view
        completion_df = filtered_df.with_columns([
            (pl.col("user_watch_duration_seconds") / pl.col("show_duration_seconds") * 100).alias("completion_percentage")
        ])
        
        # Create histogram showing distribution of completion rates
        fig4 = px.histogram(
            completion_df.to_pandas(),
            x="completion_percentage",
            nbins=20,
            title="Distribution of Completion Rates",
            labels={"completion_percentage": "Completion Percentage (%)", "count": "Number of Views"}
        )
        fig4.update_layout(height=400)
        st.plotly_chart(fig4, use_container_width=True)
        
        # Create two columns for show and content type analysis
        col1, col2 = st.columns(2)
        
        # Top shows by completion rate
        with col1:
            # Find shows with highest completion rates (minimum 5 views for statistical significance)
            show_completion = (
                completion_df
                .group_by("show_name")
                .agg([
                    pl.col("completion_percentage").mean().alias("avg_completion"),
                    pl.len().alias("views")
                ])
                .filter(pl.col("views") >= 5)  # Filter for shows with at least 5 views
                .sort("avg_completion", descending=True)
                .head(10)  # Top 10 shows
            )
            
            # Create horizontal bar chart for top shows by completion
            fig5 = px.bar(
                show_completion.to_pandas(),
                x="avg_completion",
                y="show_name",
                orientation="h",
                title="Top 10 Shows by Completion Rate",
                labels={"avg_completion": "Average Completion (%)", "show_name": "Show Name"}
            )
            fig5.update_layout(height=400)
            st.plotly_chart(fig5, use_container_width=True)
        
        # Content type completion analysis
        with col2:
            # Analyze completion rates by content type
            type_completion = (
                completion_df
                .group_by("show_type")
                .agg([
                    pl.col("completion_percentage").mean().alias("avg_completion"),
                    pl.len().alias("views")
                ])
            )
            
            # Create scatter plot showing relationship between views and completion rate
            fig6 = px.scatter(
                type_completion.to_pandas(),
                x="views",
                y="avg_completion",
                size="views",  # Bubble size represents number of views
                color="show_type",
                title="Completion Rate vs Views by Content Type",
                labels={"views": "Number of Views", "avg_completion": "Average Completion (%)"}
            )
            fig6.update_layout(height=400)
            st.plotly_chart(fig6, use_container_width=True)
    
    st.markdown("---")
    
    # Display key metrics at the bottom of the dashboard
    col1, col2, col3, col4 = st.columns(4)
    
    # Total views metric
    with col1:
        st.metric(
            "Total Views",
            f"{len(filtered_df):,}"
        )
    
    # Unique users metric
    with col2:
        st.metric(
            "Unique Users",
            f"{filtered_df['user_id'].n_unique():,}"
        )
    
    # Average completion rate metric
    with col3:
        avg_completion = (filtered_df["user_watch_duration_seconds"] / filtered_df["show_duration_seconds"] * 100).mean()
        st.metric(
            "Avg Completion Rate",
            f"{avg_completion:.1f}%"
        )
    
    # Total watch hours metric
    with col4:
        total_hours = filtered_df["user_watch_duration_seconds"].sum() / 3600  # Convert seconds to hours
        st.metric(
            "Total Watch Hours",
            f"{total_hours:,.0f}"
        )



# Dependencies installed via uv:
# uv add streamlit - Web app framework
# uv add polars - Fast data transformation library
# uv add plotly - Interactive plotting library
# Alternative installation: uv pip install -r requirements.txt