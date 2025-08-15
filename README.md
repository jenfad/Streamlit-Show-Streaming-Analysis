# Streaming Analytics Dashboard

A comprehensive multi-page Streamlit application for analyzing streaming platform data, user behavior, and content performance. Built with Python, Polars, and Plotly for fast data processing and interactive visualizations.

[See a video here](https://github.com/user-attachments/files/21800110/streamlit-show-streaming-2025-08-15-10-08-68.webm.zip)

<img width="746" height="359" alt="Screenshot of Show Streaming Dashboard" src="https://github.com/user-attachments/assets/08cec6f6-2a49-4645-bd5f-2d3158f696d6" />

## Features

- **Streaming Overview**: High-level platform analytics and key performance metrics
- **Detailed Analysis**: Show-specific performance analysis with filtering capabilities  
- **User Analysis**: Comprehensive user behavior analysis, segmentation, and cohort tracking
- **Interactive Filtering**: Dynamic filtering across date ranges, locations, genres, and content types
- **Real-time Visualizations**: Interactive charts and graphs using Plotly
- **Advanced Segmentation**: User engagement levels, viewing patterns, and behavioral cohorts

## Setup & Installation

### Prerequisites
- Python 3.8 or higher
- pip or uv package manager

### Installation

1. **Clone or download the project**
   ```bash
   git clone <repository-url>
   cd Streamlit_App_Test
   ```

2. **Install dependencies using uv (recommended)**
   ```bash
   uv pip install -r requirements.txt
   ```
   
   Or using pip:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**
   ```bash
   streamlit run main.py
   ```

4. **Open your browser**
   - Navigate to `http://localhost:8501`
   - The app will open automatically in your default browser

### Dependencies
- **streamlit**: Web application framework
- **polars**: Fast DataFrame operations and data processing
- **plotly**: Interactive visualization library
- **scikit-learn**: Machine learning utilities (for future enhancements)

## App Structure

```
Streamlit_App_Test/
├── main.py                 # Main application with navigation
├── pages/                  # Individual page modules
│   ├── __init__.py        # Package initialization
│   ├── dashboard.py       # Streaming overview page
│   ├── shows_analysis.py  # Detailed show analysis page
│   └── user_analysis.py   # User behavior analysis page
├── streaming_data.json    # Sample streaming data
├── requirements.txt       # Python dependencies
├── pyproject.toml        # Project configuration
└── README.md             # This file
```

### Page Descriptions

#### Streaming Overview (`dashboard.py`)
- Platform-wide performance metrics
- Daily viewing trends and patterns
- Content performance by genre and type
- Geographic distribution analysis
- Key summary metrics (total views, users, completion rates)

#### Detailed Analysis (`shows_analysis.py`)
- Individual show performance deep-dive
- Show-specific metrics and characteristics
- Performance comparison across all content
- Viewing behavior patterns (hourly, geographic)
- Content completion analysis and trends

#### User Analysis (`user_analysis.py`)
- User behavior and engagement analysis
- Advanced filtering by user activity levels
- User segmentation (Heavy/Regular/Casual/Light viewers)
- Cohort analysis and user lifecycle tracking
- Geographic user distribution and performance

## Data Dictionary

### Raw Data Fields

| Field | Type | Description |
|-------|------|-------------|
| `user_id` | Integer | Unique identifier for each user |
| `created_date` | Date | Date when the viewing session occurred (YYYY-MM-DD) |
| `created_at` | Datetime | Timestamp of viewing session start (YYYY-MM-DDTHH:MM:SS) |
| `timezone` | String | User's timezone (e.g., "Pacific", "Eastern") |
| `state` | String | US state code where user is located (e.g., "CA", "NY") |
| `show_duration_seconds` | Integer | Total duration of the show/episode in seconds |
| `user_watch_duration_seconds` | Integer | Actual time user spent watching in seconds |
| `show_id` | Integer | Unique identifier for each show |
| `show_name` | String | Name of the show or movie |
| `show_type` | String | Content type ("TV Show", "Movie", "Documentary", etc.) |
| `show_genre` | String | Primary genre ("Sci-Fi", "Drama", "Comedy", etc.) |
| `show_rating` | String | Content rating ("TV-14", "R", "PG", etc.) |
| `show_description` | String | Brief description of the content |

### Calculated Metrics

#### Core Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| **Completion Rate** | `(user_watch_duration_seconds / show_duration_seconds) × 100` | Percentage of content watched by user |
| **Total Watch Hours** | `sum(user_watch_duration_seconds) / 3600` | Total viewing time converted to hours |
| **Average Completion Rate** | `mean(completion_rate)` | Average percentage completion across views |
| **Unique Shows** | `count(distinct show_name)` | Number of different shows watched |
| **Unique Genres** | `count(distinct show_genre)` | Number of different genres watched |

#### User Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| **Total Views** | `count(*)` per user | Number of viewing sessions per user |
| **Days Active** | `max(created_date) - min(created_date)` | Span between first and last viewing session |
| **First View Date** | `min(created_date)` per user | Date of user's first viewing session |
| **Last View Date** | `max(created_date)` per user | Date of user's most recent session |
| **Average Watch Time** | `mean(user_watch_duration_seconds)` | Average session duration per user |

#### Engagement Segments

| Segment | Criteria | Description |
|---------|----------|-------------|
| **Heavy Viewer** | ≥20 total views | Users with very high viewing activity |
| **Regular Viewer** | 10-19 total views | Users with consistent viewing habits |
| **Casual Viewer** | 5-9 total views | Users with moderate viewing activity |
| **Light Viewer** | 1-4 total views | Users with minimal viewing activity |

#### Completion Segments

| Segment | Criteria | Description |
|---------|----------|-------------|
| **Completionist** | ≥80% avg completion | Users who typically finish content |
| **Engaged** | 60-79% avg completion | Users who watch most of content |
| **Selective** | 40-59% avg completion | Users who watch about half of content |
| **Browser** | <40% avg completion | Users who sample/browse content |

#### Lifecycle Stages

| Stage | Criteria | Description |
|-------|----------|-------------|
| **Single Day** | 0 days between first/last view | Users active for only one day |
| **1 Week** | 1-7 days active span | Short-term users |
| **1 Month** | 8-30 days active span | Medium-term users |
| **3 Months** | 31-90 days active span | Long-term users |
| **3+ Months** | >90 days active span | Highly retained users |

#### Time-based Metrics

| Metric | Description |
|--------|-------------|
| **Daily Views** | Total viewing sessions per day |
| **Unique Daily Users** | Count of distinct users per day |
| **Hourly Activity** | User activity patterns throughout the day |
| **Weekly Trends** | Viewing patterns aggregated by week |
| **Cohort Month** | Month when user first started viewing (YYYY-MM) |

#### Geographic Metrics

| Metric | Description |
|--------|-------------|
| **Views by State** | Total viewing sessions per state |
| **Users by State** | Count of unique users per state |
| **State Completion Rate** | Average completion rate per state |
| **State Engagement** | Average views per user by state |

## Usage Examples

### Filtering Data
- **Date Range**: Select specific time periods for analysis
- **Geographic**: Filter by US states to analyze regional patterns
- **Content**: Filter by genres, show types, or specific shows
- **User Behavior**: Filter users by minimum activity levels

### Key Questions the App Answers
1. **Platform Performance**: How is overall engagement trending?
2. **Content Analysis**: Which shows have the highest completion rates?
3. **User Behavior**: What are the main user segments and their characteristics?
4. **Geographic Insights**: How does viewing behavior vary by location?
5. **Retention Analysis**: How long do users typically remain active?

## Technical Details

### Performance Optimizations
- **Polars DataFrames**: Fast data processing and aggregations
- **Streamlit Caching**: Data loading cached for better performance
- **Lazy Loading**: Charts render only when tabs are accessed
- **Efficient Filtering**: Vectorized operations for fast filtering

### Data Processing Pipeline
1. **Data Loading**: JSON data loaded and cached using `@st.cache_data`
2. **Type Conversion**: Dates parsed to proper datetime types
3. **Metric Calculation**: Completion rates and derived metrics computed
4. **Filtering**: User-selected filters applied to dataset
5. **Aggregation**: Data grouped and summarized for visualizations
6. **Visualization**: Interactive charts rendered using Plotly

## Future Enhancements

- **Real-time Data**: Connection to live streaming data sources
- **Predictive Analytics**: User churn prediction and content recommendations
- **A/B Testing**: Content performance testing framework
- **Advanced Segmentation**: Machine learning-based user clustering
- **Export Capabilities**: Download reports and filtered datasets
- **Custom Dashboards**: User-configurable dashboard layouts

## Data Source

The application uses sample streaming data with the following characteristics:
- **Time Period**: 2024-2025 viewing data
- **Data Source**: Dummy data generated for demonstration
- **Geographic Coverage**: US states
- **Content Types**: TV Shows, Movies, Documentaries
- **User Base**: Simulated streaming platform users

---

*Built with Streamlit, Polars, Plotly, Claude, and Cursor*

Using dummy streaming data from [Hoyt Emerson](https://substack.com/@hoytemerson/p-162084871) and from 2024-2025
