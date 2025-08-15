import streamlit as st
from pages import dashboard, shows_analysis, user_analysis

# Configure the Streamlit page
st.set_page_config(
    page_title="Streaming Analytics Platform",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define navigation pages
pages = [
    st.Page(dashboard.overview, title="Streaming Overview", icon="📊", default=True),
    st.Page(shows_analysis.detailed_analysis, title="Detailed Analysis", icon="🎬"),
    st.Page(user_analysis.user_analysis, title="User Analysis", icon="👥")
]

# Create navigation
pg = st.navigation(pages)

# Add sidebar content
with st.sidebar:
    st.title("📺 Streaming Analytics")
    st.markdown("---")
    st.markdown("""
    **Navigate between:**
    - **Streaming Overview**: Overview analytics and key metrics
    - **Detailed Analysis**: Deep dive into individual show performance
    - **User Analysis**: Comprehensive user behavior and segmentation
    """)
    st.markdown("---")
    st.markdown("*Built with Streamlit, Polars, Plotly, Claude, and Cursor*")
    st.markdown("*Dummy data from Hoyt Emerson and from 2024-2025*")

# Run the selected page
pg.run()