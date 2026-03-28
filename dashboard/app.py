import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

from queries import get_overview, get_category, get_pain_points, get_reviews

sys.path.insert(0, os.path.dirname(__file__))

st.set_page_config(
    page_title = "Gorgias Case-Study",
    page_icon = "📊",
    layout = "wide"
)
st.title("📊 Lead Insight Dashboard")

st.header("Overview")

overview = get_overview()
df = pd.DataFrame(overview)

def colour_rating(val):
    if val >= 4:
        return "background-color: #d4edda"
    elif val >= 3.0:
        return "background-color: #fff3cd"
    else:
        return "background-color: #f8d7da"

st.dataframe(
    df.style.applymap(colour_rating, subset=["avg_rating"]),
    use_container_width=True,
    hide_index=True
)

st.header("Drill-down")

domains = [row["domain"] for row in overview]
selected = st.selectbox("Select a merchant", domains)

if selected:
    col1, col2, col3, col4 = st.columns(4)
    row = next(r for r in overview if r["domain"] == selected)

    col1.metric("Avg Rating", f"{row['avg_rating']} / 5")
    col2.metric("Total Reviews", row["total_reviews"])
    col3.metric("Reply Rate", f"{row['reply_ratio']}%")
    col4.metric("% Negative",    f"{row['negative_ratio']}%")

    st.subheader("Sentiment Distribution")
    sentiment_df = pd.DataFrame([{
        "Sentiment": "Positive",
        "Percentage": row["positive_ratio"]
    }, {
        "Sentiment": "Neutral",
        "Percentage": row["neutral_ratio"]
    }, {
        "Sentiment": "Negative",
        "Percentage": row["negative_ratio"]
    }])
    fig = px.pie(
        sentiment_df,
        names="Sentiment",
        values="Percentage"
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(showlegend=True)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Review Topics")
    categories = get_category(selected)
    if categories:
        cat_df = pd.DataFrame(categories)
        fig_cat = px.pie(
            cat_df,
            names="category",
            values="count",
            hole=0.35
        )
        fig_cat.update_traces(textposition="inside", textinfo="percent+label")
        fig_cat.update_layout(showlegend=True)
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.info("No category data yet, enrichment on-going")
    
    st.subheader("Pain Point")
    pain_points = get_pain_points(selected)
    if pain_points:
        for pp in pain_points:
            with st.expander(f"{pp['star_rating']}/5 — {pp['reviewer_name']} ({pp['date_published'].strftime('%b %Y')})"):
                st.markdown(f"**Pain point:** {pp['pain_point']}")
                st.markdown(f"**Insight:** {pp['insight']}")
    else:
        st.success("No negative reviews with pain points found")
    
    st.subheader("All Reviews")
    reviews = get_reviews(selected)
    if reviews:
        fcol1, fcol2, fcol3 = st.columns(3)
        sentiment_filter = fcol1.multiselect(
            "Sentiment", ["positive", "neutral", "negative"],
            default=["positive", "neutral", "negative"]
        )
        rating_filter = fcol2.slider("Min star rating", 1, 5, 1)
        replied_filter = fcol3.checkbox("Only reviews with company reply")

        reviews_df = pd.DataFrame(reviews)
        filtered = reviews_df[
            reviews_df["sentiment"].isin(sentiment_filter) &
            (reviews_df["star_rating"] >= rating_filter)
        ]
        if replied_filter:
            filtered = filtered[filtered["company_replied"] == True]
        st.caption(f"Showing {len(filtered)} of {len(reviews)} reviews")

        for _, rev in filtered.iterrows():
            sentiment_emoji = {"positive": "🟢", "neutral": "🟡", "negative": "🔴"}.get(rev["sentiment"], "⚪")
            with st.expander(
                f"{sentiment_emoji} ⭐ {rev['star_rating']}/5 — {rev['reviewer_name']} · {rev['date_published'].strftime('%d %b %Y')}"
            ):
                if rev["title"]:
                    st.markdown(f"**{rev['title']}**")
                st.write(rev["text"] or "_No review text_")
                st.divider()
                icol1, icol2 = st.columns(2)
                icol1.markdown(f"**Category:** {rev['category'] or '—'}")
                icol1.markdown(f"**Language:** {rev['language'] or '—'}")
                icol2.markdown(f"**Company replied:** {'Yes' if rev['company_replied'] else 'No'}")
                if rev["pain_point"]:
                    st.warning(f"**Pain point:** {rev['pain_point']}")
                if rev["insight"]:
                    st.info(f"**Insight:** {rev['insight']}")
    else:
        st.info("No reviews found for this domain.")