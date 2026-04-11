import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# --- 1. Database Configuration ---
DB_URL = "postgresql://root:root@pgdatabase:5432/eu_gdp"
# DB_URL = "postgresql://root:root@localhost:5432/eu_gdp"

st.set_page_config(page_title="EU GDP Analytics", layout="wide")

@st.cache_data
def load_data():
    try:
        engine = create_engine(DB_URL)
        query = "SELECT * FROM eu_gdp ORDER BY year ASC"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return None

# --- 2. Header ---
st.title("🇪🇺 EU GDP Distribution Dashboard")
st.markdown("Analyze GDP growth and country-wise contribution across Europe.")

df = load_data()

if df is not None:
    country_columns = ['Belgium', 'France', 'Germany', 'Italy', 'Poland', 'Spain']

    # --- 3. Sidebar Filters ---
    st.sidebar.header("Global Filters")
    selected_countries = st.sidebar.multiselect(
        "Select Countries:", 
        options=country_columns, 
        default=country_columns
    )

    # --- 4. Main Layout (Two Columns) ---
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Historical GDP Stacked Trend")
        df_long = df.melt(id_vars=['year'], value_vars=selected_countries, 
                          var_name='Country', value_name='GDP')
        
        fig_bar = px.bar(
            df_long, x="year", y="GDP", color="Country",
            labels={"GDP": "GDP (EUR)", "year": "Year"},
            barmode="stack",
            template="plotly_white"
        )
        fig_bar.update_layout(xaxis_type='category')
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        st.subheader("GDP Share by Year")
        
        # Year selection for the Pie Chart
        available_years = df['year'].unique().tolist()
        target_year = st.selectbox("Select Year for Distribution:", options=available_years, index=len(available_years)-1)

        # Filter data for specific year and selected countries
        pie_data = df[df['year'] == target_year].melt(
            id_vars=['year'], value_vars=selected_countries, 
            var_name='Country', value_name='GDP'
        )

        fig_pie = px.pie(
            pie_data, values='GDP', names='Country',
            hole=0.4, # Makes it a Donut chart for better readability
            title=f"GDP Percentage Share in {target_year}",
            template="plotly_white"
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- 5. Data Summary Metrics ---
    st.divider()
    m1, m2, m3 = st.columns(3)
    
    total_gdp = pie_data['GDP'].sum()
    m1.metric(f"Total GDP ({target_year})", f"€{total_gdp:,.0f}")
    m2.metric("Top Contributor", pie_data.loc[pie_data['GDP'].idxmax(), 'Country'])
    m3.metric("Data Status", "Live from PostgreSQL")

    # --- 6. Table ---
    with st.expander("View Raw Data Records"):
        st.table(df)

else:
    st.warning("Please ensure your PostgreSQL Docker container is running.")