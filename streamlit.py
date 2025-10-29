import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.title("UK Crime Dashboard")

session = get_active_session()

crime_sql = """
SELECT 
    CRIME_DATE,
    REPORTED_BY,
    UPDATED_LOCATION AS LOCATION,
    CRIME_TYPE,
    LATITUDE,
    LONGITUDE,
    COUNT(*) AS CRIME_COUNT
FROM CRIME_DB.INTERMEDIATE.INT_CRIME_DATA
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY CRIME_COUNT DESC
"""

df = session.sql(crime_sql).to_pandas()

# filters
col1, col2 = st.columns(2)

with col1:
    selected_type = st.selectbox("Select Crime Type", sorted(df['CRIME_TYPE'].dropna().unique()))

with col2:
    selected_reported_by = st.selectbox("Select Reporting Authority", sorted(df['REPORTED_BY'].dropna().unique()))

filtered_df = df[
    (df['CRIME_TYPE'] == selected_type) &
    (df['REPORTED_BY'] == selected_reported_by)
]

# Summary
summary_by_location = (
    filtered_df.groupby('LOCATION')['CRIME_COUNT']
    .sum()
    .reset_index()
    .sort_values(by='CRIME_COUNT', ascending=False)
)

st.dataframe(summary_by_location.reset_index(drop=True))

# Map view
map_df = filtered_df.dropna(subset=['LATITUDE', 'LONGITUDE'])

if not map_df.empty:
    st.subheader("Crime Locations Map")
    st.map(map_df[['LATITUDE', 'LONGITUDE']])
else:
    st.info("No valid location data available for this filter.")

# Download button
st.subheader("Download Filtered Data")
csv = filtered_df.to_csv(index=False)
st.download_button("Download CSV", csv, "filtered_crime_data.csv", "text/csv")
