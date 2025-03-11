import streamlit as st
import plotly.express as px
from analysis import get_sentiment_trends

st.title("📊 GitHub Repository Analytics")

# Sentiment Trends
st.subheader("📈 Sentiment Trends Over Time")
df_sentiment = get_sentiment_trends()
fig_sentiment = px.line(df_sentiment, x="date", y="avg_sentiment", title="Sentiment Score Over Time")
st.plotly_chart(fig_sentiment)

st.success("📌 Data updated successfully! Refresh for the latest insights.")
