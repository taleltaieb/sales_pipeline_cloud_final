import os
import pandas as pd
import streamlit as st
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
import altair as alt
from dotenv import load_dotenv
load_dotenv()


st.set_page_config(layout="wide", page_title="Sales Performance Dashboard")

# --- Constants ---
MONTH_ORDER = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
ITEM_TYPE_OPTIONS = ["New", "Old"]

# --- Connection ---
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse="COMPUTE_WH",
        database="SALES_PIPELINE",
        schema="MARTS",
        role="ACCOUNTADMIN"
    )

conn = get_conn()
st.title("📊 Sales Intelligence Dashboard")
st.caption("Tracking revenue, shop performance, and seasonal trends")

@st.cache_data(ttl=600)
def load_table(table_name, where_clause=""):
    fresh_conn = get_conn()
    query = f"SELECT * FROM STAGING_MARTS.{table_name} {where_clause}"
    return pd.read_sql(query, fresh_conn)


# --- Load Data ---
df_total_rev = load_table("MART_TOTAL_REVENUE")
df_daily = load_table("MART_DAILY_SALES")
df_monthly = load_table("MART_MONTHLY_SALES")
df_month_rev = load_table("MART_REVENUE_BY_MONTH")
df_top_shops = load_table("MART_TOP_SHOPS")
df_top_cats = load_table("MART_TOP_ITEM_CATEGORIES")
df_shop_profile = load_table("MART_SHOP_PROFILE")
df_item_monthly_consistency = load_table("MART_ITEM_MONTHLY_CONSISTENCY")
df_seasonality_by_category = load_table("MART_SEASONALITY_BY_CATEGORY")

# --- Sidebar Filters ---
st.sidebar.header("📊 Filters")
if st.sidebar.button("🔄 Reset All Filters"):
    st.experimental_rerun()

df_daily = df_daily.merge(df_top_shops[["SHOP_ID", "SHOP_NAME"]], on="SHOP_ID", how="left")

month_options = sorted(df_monthly["DATE_MONTH"].unique())
shop_options = sorted(df_daily["SHOP_NAME"].dropna().unique())
cat_options = sorted(df_top_cats["ITEM_CATEGORY_NAME"].unique())

all_months = st.sidebar.checkbox("Select All Months", value=True)
all_shops = st.sidebar.checkbox("Select All Shops", value=True)
all_cats = st.sidebar.checkbox("Select All Categories", value=True)
all_types = st.sidebar.checkbox("Select All Item Types", value=True)

selected_months = month_options if all_months else st.sidebar.multiselect("Select Month(s)", month_options)
selected_shops = shop_options if all_shops else st.sidebar.multiselect("Select Shop(s)", shop_options)
selected_categories = cat_options if all_cats else st.sidebar.multiselect("Select Categories", cat_options)
selected_item_types = ITEM_TYPE_OPTIONS if all_types else st.sidebar.multiselect("Select Item Type", ITEM_TYPE_OPTIONS)

# Seasonality Radar Selection
df_seasonality_by_category["MONTH_LABEL"] = pd.Categorical(df_seasonality_by_category["MONTH_LABEL"], categories=MONTH_ORDER, ordered=True)
default_top_5 = df_seasonality_by_category.groupby("ITEM_CATEGORY_NAME")["SEASONALITY_SCORE"].mean().nlargest(5).index.tolist()

select_all = st.sidebar.checkbox("Select All Categories", value=False, key="select_all_seasonal")
selected_categories = st.sidebar.multiselect(
    "Select Categories to Compare:",
    options=cat_options,
    default=cat_options if select_all else default_top_5,
    key="category_selector",
    max_selections=len(cat_options)
)





# --- Apply Filters ---
df_daily["DATE"] = pd.to_datetime(df_daily["DATE"])
df_daily["DATE_MONTH"] = df_daily["DATE"].dt.to_period("M").astype(str)
df_daily = df_daily[df_daily["DATE_MONTH"].isin(selected_months) & df_daily["SHOP_NAME"].isin(selected_shops)]
df_monthly = df_monthly[df_monthly["DATE_MONTH"].isin(selected_months)]
df_month_rev = df_month_rev[df_month_rev["DATE_MONTH"].isin(selected_months)]
df_top_shops = df_top_shops[df_top_shops["SHOP_NAME"].isin(selected_shops)]
df_top_cats = df_top_cats[df_top_cats["ITEM_CATEGORY_NAME"].isin(selected_categories)]
df_seasonality_by_category = df_seasonality_by_category[
    df_seasonality_by_category["ITEM_CATEGORY_NAME"].isin(selected_categories)
]

# If item type filtering is required elsewhere
df_lifecycle = df_monthly.copy()
first_seen = df_lifecycle.groupby("ITEM_ID")["DATE_BLOCK_NUM"].min().reset_index()
latest_block = df_lifecycle["DATE_BLOCK_NUM"].max()
cutoff_block = latest_block - 5
first_seen["ITEM_TYPE"] = first_seen["DATE_BLOCK_NUM"].apply(lambda x: "New" if x >= cutoff_block else "Old")
df_lifecycle = df_lifecycle.merge(first_seen[["ITEM_ID", "ITEM_TYPE"]], on="ITEM_ID")
df_lifecycle = df_lifecycle[df_lifecycle["ITEM_TYPE"].isin(selected_item_types)]


# KPIs
total_revenue = df_total_rev["TOTAL_REVENUE"].iloc[0]
total_items = df_daily["TOTAL_ITEM_CNT"].sum()
active_shops = df_daily["SHOP_ID"].nunique()
rev_by_month = df_month_rev.sort_values("DATE_BLOCK_NUM")
latest = rev_by_month.iloc[-1]["MONTHLY_REVENUE"] if len(rev_by_month) > 1 else 0
prev = rev_by_month.iloc[-2]["MONTHLY_REVENUE"] if len(rev_by_month) > 1 else 1
mom_change = ((latest - prev) / prev) * 100 if prev else 0

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Total Revenue", f"${total_revenue:,.0f}")
kpi2.metric("Total Items Sold", f"{total_items:,.0f}")
kpi3.metric("Active Shops", active_shops)
kpi4.metric("MoM Revenue Change", f"{mom_change:.2f}%")

st.markdown("---")

# Monthly Revenue Chart
monthly_revenue_chart = px.line(df_month_rev, x="DATE_MONTH", y="MONTHLY_REVENUE", title="Monthly Revenue")
monthly_revenue_chart.update_layout(xaxis_tickformat="%b %Y")

# Monthly Quantity Sold Chart
monthly_qty = df_monthly.groupby(["DATE_BLOCK_NUM", "DATE_MONTH"])["MONTHLY_QTY_SOLD"].sum().reset_index()
monthly_qty_chart = px.line(monthly_qty, x="DATE_MONTH", y="MONTHLY_QTY_SOLD", title="Monthly Quantity Sold")

# Top Shops Chart
top_shops_chart = px.bar(df_top_shops.sort_values("TOTAL_REVENUE", ascending=False).head(10),
                         x="SHOP_NAME", y="TOTAL_REVENUE", title="Top Shops by Revenue")

# Top Categories Chart
top_categories_chart = px.bar(df_top_cats.sort_values("TOTAL_REVENUE", ascending=False).head(10),
                              x="ITEM_CATEGORY_NAME", y="TOTAL_REVENUE", title="Top Categories by Revenue")

# Price vs Quantity Chart
avg_price_qty = df_daily.groupby("ITEM_ID").agg(
    AVG_PRICE=("TOTAL_REVENUE", lambda x: x.sum() / df_daily.loc[x.index, "TOTAL_ITEM_CNT"].sum()),
    TOTAL_SOLD=("TOTAL_ITEM_CNT", "sum")
).reset_index()
price_vs_quantity_chart = px.scatter(avg_price_qty, x="AVG_PRICE", y="TOTAL_SOLD", title="Price vs Quantity Sold")

# New vs Old Items Revenue Trend
first_seen = df_monthly.groupby("ITEM_ID")["DATE_BLOCK_NUM"].min().reset_index()
latest_block = df_monthly["DATE_BLOCK_NUM"].max()
cutoff_block = latest_block - 5
first_seen["ITEM_TYPE"] = first_seen["DATE_BLOCK_NUM"].apply(lambda x: "New" if x >= cutoff_block else "Old")
df_lifecycle = df_monthly.merge(first_seen[["ITEM_ID", "ITEM_TYPE"]], on="ITEM_ID")
trend_df = df_lifecycle.groupby(["DATE_MONTH", "ITEM_TYPE"])["MONTHLY_REVENUE"].sum().reset_index()
new_vs_old_items_chart = px.line(trend_df, x="DATE_MONTH", y="MONTHLY_REVENUE", color="ITEM_TYPE", title="Revenue Trend: New vs Old Items")

# Treemap
treemap_chart = px.treemap(df_top_cats, path=['ITEM_CATEGORY_NAME'], values='TOTAL_REVENUE',
                           title='Revenue Contribution by Item Category')

# Shop Profile Radar Chart
df_scores = df_shop_profile.copy()
metrics = ['CATEGORY_COUNT', 'AVG_ITEM_PRICE', 'REVENUE_PER_ITEM', 'MONTHLY_REVENUE_VARIANCE']
df_scores['avg_score'] = df_scores[metrics].mean(axis=1)
top_overall = df_scores.sort_values('avg_score', ascending=False).head(1)
top_revenue = df_scores.sort_values('REVENUE_PER_ITEM', ascending=False).head(1)
top_category = df_scores.sort_values('CATEGORY_COUNT', ascending=False).head(1)
most_volatile = df_scores.sort_values('MONTHLY_REVENUE_VARIANCE', ascending=False).head(1)
bottom = df_scores.sort_values('avg_score').head(1)
df_filtered = pd.concat([top_overall, top_revenue, top_category, most_volatile, bottom]).drop_duplicates()
df_filtered = df_filtered.drop(columns=["SHOP_ID", "avg_score"], errors='ignore')
df_norm = df_filtered.copy()
df_norm[metrics] = df_norm[metrics].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
df_melt = df_norm.melt(id_vars="SHOP_NAME", var_name="Metric", value_name="Score")
shop_profile_radar = px.line_polar(df_melt, r='Score', theta='Metric', color='SHOP_NAME', line_close=True,
                                   title="Selected Shops – Strategic Profile Comparison")
shop_profile_radar.update_traces(fill='toself')






# Consistency Heatmap
df_item_monthly_consistency["MONTH_DATE"] = pd.to_datetime(df_item_monthly_consistency["MONTH_DATE"])
df_item_monthly_consistency = df_item_monthly_consistency.sort_values("MONTH_DATE")
cap = df_item_monthly_consistency["MONTHLY_REVENUE"].quantile(0.99)
df_item_monthly_consistency["CAPPED_REVENUE"] = df_item_monthly_consistency["MONTHLY_REVENUE"].clip(upper=cap)
melted_df = df_item_monthly_consistency[["ITEM_NAME", "MONTH_LABEL", "CAPPED_REVENUE"]].rename(
    columns={"ITEM_NAME": "Item", "MONTH_LABEL": "Month", "CAPPED_REVENUE": "Revenue"})
item_consistency_heatmap = alt.Chart(melted_df).mark_rect().encode(
    x=alt.X("Month:N", sort=list(melted_df["Month"].unique())),
    y=alt.Y("Item:N"),
    color=alt.Color("Revenue:Q", scale=alt.Scale(scheme='blues')),
    tooltip=["Item", "Month", "Revenue"]
).properties(width=700, height=500)



# ------------------------- Layout Composition -------------------------

# Section 1: Sales Trends

col1, col2 = st.columns([3, 2])
with col1:
    st.plotly_chart(monthly_revenue_chart, use_container_width=True)
    st.plotly_chart(new_vs_old_items_chart, use_container_width=True)
with col2:
    st.plotly_chart(monthly_qty_chart, use_container_width=True)
    col21, col22 = st.columns(2)
    with col21:
        st.plotly_chart(top_shops_chart, use_container_width=True)
    with col22:
        st.plotly_chart(top_categories_chart, use_container_width=True)

st.markdown("---")

# # Section 2: Strategic Insights
# st.subheader("🔍 Pricing & Lifecycle")
# col3, col4 = st.columns(2)
# with col3:
#     st.plotly_chart(new_vs_old_items_chart, use_container_width=True)
# with col4:
#     st.plotly_chart(price_vs_quantity_chart, use_container_width=True)

# st.markdown("---")

# Section 3: Revenue Composition

col5, col6 = st.columns([2, 1.25])
with col5:
    col51, col52 = st.columns(2)
    with col51:
        st.plotly_chart(shop_profile_radar, use_container_width=True)
    with col52:
        
        if selected_categories:
            filtered = df_seasonality_by_category[
                df_seasonality_by_category['ITEM_CATEGORY_NAME'].isin(selected_categories)
            ]
            seasonality_radar_chart = px.line_polar(
                filtered,
                r='SEASONALITY_SCORE',
                theta='MONTH_LABEL',
                color='ITEM_CATEGORY_NAME',
                line_close=True,
                title="Seasonality Index (Radar Chart)",
                template="plotly"
            )
            seasonality_radar_chart.update_traces(fill='toself')
        else:
            st.info("Please select at least one item category.")
        if selected_categories:
            st.plotly_chart(seasonality_radar_chart, use_container_width=True)
with col6:
    st.altair_chart(item_consistency_heatmap, use_container_width=True)

st.markdown("---")

# Section 4: Strategic Profiles

st.plotly_chart(treemap_chart, use_container_width=True)
# col7, col8 = st.columns(2)
# with col7:
#     st.plotly_chart(shop_profile_radar, use_container_width=True)
# with col8:
#     st.plotly_chart(seasonality_radar_chart, use_container_width=True)
