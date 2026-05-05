import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
import clickhouse_connect
from streamlit_autorefresh import st_autorefresh

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "livestock")

UI_REFRESH_MS = int(os.getenv("UI_REFRESH_MS", "30000"))

st.set_page_config(
    page_title="Smart Livestock Monitoring",
    page_icon="🐄",
    layout="wide"
)

st_autorefresh(interval=UI_REFRESH_MS, key="gold_refresh")

@st.cache_resource
def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USERNAME,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )


def run_query(query: str) -> pd.DataFrame:
    client = get_client()
    try:
        df = client.query_df(query)
        if df is None:
            return pd.DataFrame()
        return df
    except Exception as e:
        st.error(f"Ошибка запроса к ClickHouse: {e}")
        return pd.DataFrame()

def load_cow_current():
    return run_query(
        f"""
        SELECT
            cow_id,
            last_event_time,
            last_temp_c,
            max_temp_1h,
            lying_now,
            lying_minutes_1h,
            lying_minutes_3h,
            milk_kg_today,
            open_alerts_count,
            risk_score,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_cow_current
        ORDER BY cow_id
        """
    )

def load_cow_day():
    return run_query(
        f"""
        SELECT
            event_date,
            cow_id,
            avg_temp_c,
            max_temp_c,
            lying_minutes,
            lying_ratio,
            milk_kg,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_cow_day
        ORDER BY event_date DESC, cow_id
        """
    )

def load_tag_current():
    return run_query(
        f"""
        SELECT
            tag_id,
            last_event_time,
            coord_x_cm,
            coord_y_cm,
            coord_z_cm,
            distance_m_1h,
            distance_m_6h,
            pressure_pa,
            elevation_m,
            head_down_ratio_1h,
            open_alerts_count,
            risk_score,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_tag_current
        ORDER BY tag_id
        """
    )

def load_tag_day():
    return run_query(
        f"""
        SELECT
            event_date,
            tag_id,
            distance_m,
            avg_pressure_pa,
            max_pressure_pa,
            avg_elevation_m,
            head_down_ratio_day,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_tag_day
        ORDER BY event_date DESC, tag_id
        """
    )

def load_environment_current():
    return run_query(
        f"""
        SELECT
            sensor_id,
            event_time,
            temperature_c,
            humidity_per,
            thi,
            thi_risk,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_environment_current
        ORDER BY sensor_id
        """
    )

def load_environment_global():
    return run_query(
        f"""
        SELECT
            source_sensor_id,
            source_mode,
            event_time,
            temperature_c,
            humidity_per,
            thi,
            thi_risk,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_environment_global_current
        """
    )

def load_alerts_open():
    return run_query(
        f"""
        SELECT
            entity_type,
            entity_id,
            alert_code,
            alert_name,
            severity,
            metric_value,
            threshold_value,
            description,
            first_seen,
            last_seen,
            is_open,
            last_update_ts
        FROM {CLICKHOUSE_DATABASE}.gold_alerts_open
        ORDER BY severity DESC, entity_type, entity_id, alert_code
        """
    )

def load_alerts_history():
    return run_query(
        f"""
        SELECT
            event_id,
            alert_event_time,
            entity_type,
            entity_id,
            alert_code,
            alert_name,
            severity,
            metric_value,
            threshold_value,
            description,
            event_status,
            first_seen,
            last_seen
        FROM {CLICKHOUSE_DATABASE}.gold_alerts_history
        ORDER BY alert_event_time DESC
        LIMIT 300
        """
    )
    st.metric(title, value, help=help_text)

def safe_float(value, digits=2):
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)

def severity_sort_value(value: str):
    if value == "critical":
        return 2
    if value == "warning":
        return 1
    return 0

def add_alert_severity_order(df: pd.DataFrame):
    if df.empty or "severity" not in df.columns:
        return df
    out = df.copy()
    out["severity_order"] = out["severity"].map(severity_sort_value)
    return out

def page_overview():
    st.title("Smart Livestock Monitoring")
    st.caption(f"Автообновление каждые {UI_REFRESH_MS // 1000} сек. Время обновления: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    cow_current = load_cow_current()
    tag_current = load_tag_current()
    env_global = load_environment_global()
    alerts_open = load_alerts_open()

    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        render_kpi("Коров под наблюдением", len(cow_current))

    with c2:
        render_kpi("Тегов под наблюдением", len(tag_current))

    with c3:
        render_kpi("Активных алертов", len(alerts_open))

    with c4:
        if not env_global.empty and pd.notna(env_global.iloc[0]["thi"]):
            render_kpi("Текущий THI", safe_float(env_global.iloc[0]["thi"]))
        else:
            render_kpi("Текущий THI", "—")

    with c5:
        if not env_global.empty:
            render_kpi("Риск THI", env_global.iloc[0]["thi_risk"])
        else:
            render_kpi("Риск THI", "unknown")

    st.divider()

    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("Коровы с наибольшим риском")
        if cow_current.empty:
            st.info("Данные по коровам пока отсутствуют.")
        else:
            risk_df = cow_current.sort_values(["risk_score", "open_alerts_count"], ascending=[False, False]).head(10)
            st.dataframe(
                risk_df[[
                    "cow_id",
                    "last_temp_c",
                    "max_temp_1h",
                    "lying_minutes_3h",
                    "milk_kg_today",
                    "open_alerts_count",
                    "risk_score"
                ]],
                use_container_width=True
            )

    with right:
        st.subheader("Алерты по типам")
        if alerts_open.empty:
            st.info("Открытых алертов нет.")
        else:
            by_type = alerts_open.groupby("alert_code", as_index=False).size()
            fig = px.bar(by_type, x="alert_code", y="size", title="Открытые алерты")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    bottom_left, bottom_right = st.columns(2)

    with bottom_left:
        st.subheader("Текущее состояние среды")
        env_current = load_environment_current()
        if env_current.empty:
            st.info("Нет данных по environment.")
        else:
            st.dataframe(env_current, use_container_width=True)

    with bottom_right:
        st.subheader("Последние открытые алерты")
        if alerts_open.empty:
            st.success("Открытых алертов нет.")
        else:
            show_df = alerts_open.copy()
            show_df = add_alert_severity_order(show_df).sort_values(
                ["severity_order", "last_seen"],
                ascending=[False, False]
            ).drop(columns=["severity_order"])
            st.dataframe(show_df.head(20), use_container_width=True)


def page_cows():
    st.title("Аналитика по коровам")

    cow_current = load_cow_current()
    cow_day = load_cow_day()

    if cow_current.empty and cow_day.empty:
        st.info("Нет данных по коровам.")
        return

    top1, top2, top3 = st.columns(3)

    if not cow_day.empty:
        latest_date = cow_day["event_date"].max()
        latest_day = cow_day[cow_day["event_date"] == latest_date].copy()
    else:
        latest_day = pd.DataFrame()

    with top1:
        if not latest_day.empty:
            avg_milk = latest_day["milk_kg"].fillna(0).mean()
            render_kpi("Средний удой за день", f"{avg_milk:.2f} кг")
        else:
            render_kpi("Средний удой за день", "—")

    with top2:
        if not latest_day.empty:
            avg_lying = latest_day["lying_minutes"].fillna(0).mean()
            render_kpi("Среднее лежание за день", f"{avg_lying:.0f} мин")
        else:
            render_kpi("Среднее лежание за день", "—")

    with top3:
        if not cow_current.empty:
            risky = (cow_current["risk_score"].fillna(0) > 0).sum()
            render_kpi("Коров с риском", int(risky))
        else:
            render_kpi("Коров с риском", "—")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Топ удоя за последний день")
        if latest_day.empty:
            st.info("Нет данных.")
        else:
            top_milk = latest_day.sort_values("milk_kg", ascending=False).head(10)
            fig = px.bar(top_milk, x="cow_id", y="milk_kg", title="Milk kg")
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Максимальная температура за последний день")
        if latest_day.empty:
            st.info("Нет данных.")
        else:
            top_temp = latest_day.sort_values("max_temp_c", ascending=False).head(10)
            fig = px.bar(top_temp, x="cow_id", y="max_temp_c", title="Max temp")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Текущее состояние коров")
        if not cow_current.empty:
            st.dataframe(cow_current, use_container_width=True)
        else:
            st.info("Нет данных.")

    with right2:
        st.subheader("Дневная витрина")
        if not cow_day.empty:
            st.dataframe(cow_day.head(200), use_container_width=True)
        else:
            st.info("Нет данных.")

def page_tags():
    st.title("Аналитика по тегам")

    tag_current = load_tag_current()
    tag_day = load_tag_day()

    if tag_current.empty and tag_day.empty:
        st.info("Нет данных по тегам.")
        return

    if not tag_day.empty:
        latest_date = tag_day["event_date"].max()
        latest_day = tag_day[tag_day["event_date"] == latest_date].copy()
    else:
        latest_day = pd.DataFrame()

    top1, top2, top3 = st.columns(3)

    with top1:
        if not latest_day.empty:
            avg_distance = latest_day["distance_m"].fillna(0).mean()
            render_kpi("Средняя дистанция за день", f"{avg_distance:.2f} м")
        else:
            render_kpi("Средняя дистанция за день", "—")

    with top2:
        if not tag_current.empty:
            low_activity = (tag_current["distance_m_6h"].fillna(999999) < 50).sum()
            render_kpi("Тегов с низкой активностью", int(low_activity))
        else:
            render_kpi("Тегов с низкой активностью", "—")

    with top3:
        if not tag_current.empty:
            risky = (tag_current["risk_score"].fillna(0) > 0).sum()
            render_kpi("Тегов с риском", int(risky))
        else:
            render_kpi("Тегов с риском", "—")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Теги с наименьшей активностью")
        if tag_current.empty:
            st.info("Нет данных.")
        else:
            low_df = tag_current.sort_values("distance_m_6h", ascending=True).head(10)
            fig = px.bar(low_df, x="tag_id", y="distance_m_6h", title="Distance 6h")
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Последние позиции тегов")
        if tag_current.empty:
            st.info("Нет данных.")
        else:
            pos_df = tag_current.dropna(subset=["coord_x_cm", "coord_y_cm"]).copy()
            if pos_df.empty:
                st.info("Нет координат.")
            else:
                fig = px.scatter(
                    pos_df,
                    x="coord_x_cm",
                    y="coord_y_cm",
                    text="tag_id",
                    title="Текущие координаты тегов"
                )
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Текущая витрина тегов")
        if not tag_current.empty:
            st.dataframe(tag_current, use_container_width=True)
        else:
            st.info("Нет данных.")

    with right2:
        st.subheader("Дневная витрина тегов")
        if not tag_day.empty:
            st.dataframe(tag_day.head(200), use_container_width=True)
        else:
            st.info("Нет данных.")

def page_environment():
    st.title("Среда")

    env_current = load_environment_current()
    env_global = load_environment_global()

    top1, top2, top3 = st.columns(3)

    with top1:
        if not env_global.empty and pd.notna(env_global.iloc[0]["temperature_c"]):
            render_kpi("Температура", f"{float(env_global.iloc[0]['temperature_c']):.2f} °C")
        else:
            render_kpi("Температура", "—")

    with top2:
        if not env_global.empty and pd.notna(env_global.iloc[0]["humidity_per"]):
            render_kpi("Влажность", f"{float(env_global.iloc[0]['humidity_per']):.2f} %")
        else:
            render_kpi("Влажность", "—")

    with top3:
        if not env_global.empty and pd.notna(env_global.iloc[0]["thi"]):
            render_kpi("THI", f"{float(env_global.iloc[0]['thi']):.2f}")
        else:
            render_kpi("THI", "—")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Глобальное состояние среды")
        if env_global.empty:
            st.info("Нет данных.")
        else:
            st.dataframe(env_global, use_container_width=True)

    with right:
        st.subheader("Сенсоры среды")
        if env_current.empty:
            st.info("Нет данных.")
        else:
            st.dataframe(env_current, use_container_width=True)

    if not env_current.empty:
        st.divider()
        st.subheader("THI по сенсорам")
        fig = px.bar(env_current, x="sensor_id", y="thi", color="thi_risk", title="THI per sensor")
        st.plotly_chart(fig, use_container_width=True)

def page_alerts():
    st.title("Алерты")

    alerts_open = load_alerts_open()
    alerts_history = load_alerts_history()

    top1, top2, top3 = st.columns(3)

    with top1:
        render_kpi("Открытых алертов", len(alerts_open))

    with top2:
        if not alerts_open.empty:
            render_kpi("Critical", int((alerts_open["severity"] == "critical").sum()))
        else:
            render_kpi("Critical", 0)

    with top3:
        if not alerts_history.empty:
            render_kpi("Исторических событий", len(alerts_history))
        else:
            render_kpi("Исторических событий", 0)

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Открытые алерты")
        if alerts_open.empty:
            st.success("Открытых алертов нет.")
        else:
            show_df = add_alert_severity_order(alerts_open).sort_values(
                ["severity_order", "last_seen"],
                ascending=[False, False]
            ).drop(columns=["severity_order"])
            st.dataframe(show_df, use_container_width=True)

    with right:
        st.subheader("История алертов")
        if alerts_history.empty:
            st.info("История пока пустая.")
        else:
            st.dataframe(alerts_history, use_container_width=True)

    if not alerts_open.empty:
        st.divider()
        st.subheader("Распределение открытых алертов")
        by_code = alerts_open.groupby("alert_code", as_index=False).size()
        fig = px.pie(by_code, names="alert_code", values="size", title="Open alerts by code")
        st.plotly_chart(fig, use_container_width=True)

with st.sidebar:
    st.header("Навигация")
    page = st.radio(
        "Раздел",
        [
            "Обзор",
            "Коровы",
            "Теги",
            "Среда",
            "Алерты"
        ]
    )

if page == "Обзор":
    page_overview()
elif page == "Коровы":
    page_cows()
elif page == "Теги":
    page_tags()
elif page == "Среда":
    page_environment()
elif page == "Алерты":
    page_alerts()