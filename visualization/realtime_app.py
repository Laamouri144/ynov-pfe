import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from kafka import KafkaConsumer

import clickhouse_connect


APP_VERSION = "2026-01-11.1"


MONTH_NAMES = {
	1: "January",
	2: "February",
	3: "March",
	4: "April",
	5: "May",
	6: "June",
	7: "July",
	8: "August",
	9: "September",
	10: "October",
	11: "November",
	12: "December",
}


def env(name: str, default: str) -> str:
	value = os.getenv(name)
	if value is None or value == "":
		return default
	return value


KAFKA_HOST = env("KAFKA_HOST", "localhost")
KAFKA_PORT = int(env("KAFKA_PORT", "9092"))
KAFKA_TOPIC = env("KAFKA_TOPIC", "airline-delays")
KAFKA_GROUP_ID = env("STREAMLIT_KAFKA_GROUP_ID", "airline-streamlit-ui")

CLICKHOUSE_HOST = env("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(env("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = env("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = env("CLICKHOUSE_DATABASE", "airline_data")


@dataclass(frozen=True)
class KafkaEvent:
	received_at: datetime
	record: Dict[str, Any]
	raw: str


def _safe_float(numerator: float, denominator: float) -> float:
	if denominator <= 0:
		return 0.0
	return float(numerator) / float(denominator)


def _parse_nifi_pretty_print_json(parsed: Any) -> Optional[Dict[str, Any]]:
	if not isinstance(parsed, list) or len(parsed) == 0:
		return None
	if all(isinstance(x, dict) and ("year" in x or "month" in x or "carrier" in x) for x in parsed):
		# Already a list of records
		return parsed[0]

	data: Dict[str, Any] = {}
	for item in parsed:
		if not isinstance(item, dict):
			continue
		for key_str, value_str in item.items():
			if str(key_str).strip() in ["{", "}"]:
				if value_str and ":" in value_str:
					left, right = value_str.split(":", 1)
					key = left.strip().strip('"').strip()
					value = right.strip().strip('"').strip()
					if key:
						data[key] = value
			else:
				# Sometimes NiFi can produce valid {"k": "v"} items
				data[str(key_str)] = value_str
	return data or None


def parse_kafka_payload(raw: str) -> List[Dict[str, Any]]:
	"""Return a list of records from Kafka payload.

	Supports:
	- JSON object dict
	- JSON array (NiFi Pretty Print JSON, or array-of-dicts)
	"""
	try:
		parsed = json.loads(raw)
	except Exception:
		return []

	if isinstance(parsed, dict):
		return [parsed]

	if isinstance(parsed, list):
		# If it's a list of dict records, keep them all
		if all(isinstance(x, dict) for x in parsed):
			# Could be array-of-records OR NiFi pretty print tokens
			if any("{" in str(k).strip() for d in parsed for k in d.keys()):
				maybe = _parse_nifi_pretty_print_json(parsed)
				return [maybe] if maybe else []
			return [d for d in parsed if isinstance(d, dict)]

		maybe = _parse_nifi_pretty_print_json(parsed)
		return [maybe] if maybe else []

	return []


@st.cache_resource(show_spinner=False)
def get_clickhouse_client():
	return clickhouse_connect.get_client(
		host=CLICKHOUSE_HOST,
		port=CLICKHOUSE_HTTP_PORT,
		username=CLICKHOUSE_USER,
		password=CLICKHOUSE_PASSWORD,
		database=CLICKHOUSE_DATABASE,
	)


def ch_query_df(sql: str, parameters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
	client = get_clickhouse_client()
	# clickhouse-connect versions differ:
	# - older: client.query(...).result_df
	# - newer: client.query_df(...) returns a pandas DataFrame
	try:
		return client.query_df(sql, parameters=parameters)
	except Exception:
		result = client.query(sql, parameters=parameters)
		columns = getattr(result, "column_names", None) or []
		rows = getattr(result, "result_rows", None) or []
		return pd.DataFrame(rows, columns=columns)


@st.cache_data(show_spinner=False, ttl=30)
def ch_years() -> List[int]:
	df = ch_query_df("SELECT DISTINCT year FROM flights ORDER BY year")
	if df.empty:
		return []
	return [int(x) for x in df["year"].tolist()]


@st.cache_data(show_spinner=False, ttl=30)
def ch_months(year: int) -> List[int]:
	df = ch_query_df(
		"SELECT DISTINCT month FROM flights WHERE year = %(year)s ORDER BY month",
		parameters={"year": year},
	)
	if df.empty:
		return []
	return [int(x) for x in df["month"].tolist()]


@st.cache_data(show_spinner=False, ttl=15)
def ch_overview_kpis(year: int, months: Optional[List[int]]) -> Dict[str, float]:
	where = "year = %(year)s"
	params: Dict[str, Any] = {"year": year}
	if months:
		where += " AND month IN %(months)s"
		params["months"] = tuple(months)

	df = ch_query_df(
		f"""
		SELECT
			sum(arr_flights) AS flights,
			sum(arr_del15) AS delayed,
			sum(arr_delay) AS delay_minutes,
			sum(arr_cancelled) AS cancelled,
			sum(arr_diverted) AS diverted
		FROM flights
		WHERE {where}
		""",
		parameters=params,
	)

	if df.empty:
		return {
			"delay_rate": 0.0,
			"avg_delay_per_flight": 0.0,
			"avg_delay_per_delayed": 0.0,
			"cancel_rate": 0.0,
			"divert_rate": 0.0,
			"flights": 0.0,
		}

	flights = float(df.at[0, "flights"] or 0)
	delayed = float(df.at[0, "delayed"] or 0)
	delay_minutes = float(df.at[0, "delay_minutes"] or 0)
	cancelled = float(df.at[0, "cancelled"] or 0)
	diverted = float(df.at[0, "diverted"] or 0)

	return {
		"delay_rate": _safe_float(delayed, flights),
		"avg_delay_per_flight": _safe_float(delay_minutes, flights),
		"avg_delay_per_delayed": _safe_float(delay_minutes, delayed),
		"cancel_rate": _safe_float(cancelled, flights),
		"divert_rate": _safe_float(diverted, flights),
		"flights": flights,
	}


@st.cache_data(show_spinner=False, ttl=15)
def ch_delay_rate_by_month(year: int) -> pd.DataFrame:
	df = ch_query_df(
		"""
		SELECT
			month,
			sum(arr_flights) AS flights,
			sum(arr_del15) AS delayed
		FROM flights
		WHERE year = %(year)s
		GROUP BY month
		ORDER BY month
		""",
		parameters={"year": year},
	)
	if df.empty:
		return df
	df["delay_rate"] = df.apply(lambda r: _safe_float(float(r["delayed"]), float(r["flights"])), axis=1)
	df["MonthName"] = df["month"].map(lambda m: MONTH_NAMES.get(int(m), str(m)))
	return df


@st.cache_data(show_spinner=False, ttl=15)
def ch_cause_breakdown(year: int, months: Optional[List[int]]) -> pd.DataFrame:
	where = "year = %(year)s"
	params: Dict[str, Any] = {"year": year}
	if months:
		where += " AND month IN %(months)s"
		params["months"] = tuple(months)

	df = ch_query_df(
		f"""
		SELECT
			sum(carrier_delay) AS carrier_delay,
			sum(weather_delay) AS weather_delay,
			sum(nas_delay) AS nas_delay,
			sum(security_delay) AS security_delay,
			sum(late_aircraft_delay) AS late_aircraft_delay
		FROM flights
		WHERE {where}
		""",
		parameters=params,
	)
	if df.empty:
		return pd.DataFrame(columns=["cause", "minutes"])

	row = df.iloc[0].to_dict()
	out = pd.DataFrame(
		[
			{"cause": "Carrier", "minutes": float(row.get("carrier_delay") or 0)},
			{"cause": "Weather", "minutes": float(row.get("weather_delay") or 0)},
			{"cause": "NAS", "minutes": float(row.get("nas_delay") or 0)},
			{"cause": "Security", "minutes": float(row.get("security_delay") or 0)},
			{"cause": "Late Aircraft", "minutes": float(row.get("late_aircraft_delay") or 0)},
		]
	)
	return out


@st.cache_data(show_spinner=False, ttl=15)
def ch_top_carriers(year: int, months: Optional[List[int]], limit: int = 10) -> pd.DataFrame:
	where = "year = %(year)s"
	params: Dict[str, Any] = {"year": year, "limit": int(limit)}
	if months:
		where += " AND month IN %(months)s"
		params["months"] = tuple(months)

	return ch_query_df(
		f"""
		SELECT
			carrier,
			any(carrier_name) AS carrier_name,
			sum(arr_flights) AS total_flights,
			sum(arr_del15) AS delayed_flights,
			round(sum(arr_del15) * 100.0 / nullIf(sum(arr_flights), 0), 2) AS delay_percentage
		FROM flights
		WHERE {where}
		GROUP BY carrier
		ORDER BY delay_percentage DESC
		LIMIT %(limit)s
		""",
		parameters=params,
	)


@st.cache_data(show_spinner=False, ttl=15)
def ch_top_airports(year: int, months: Optional[List[int]], limit: int = 10) -> pd.DataFrame:
	where = "year = %(year)s"
	params: Dict[str, Any] = {"year": year, "limit": int(limit)}
	if months:
		where += " AND month IN %(months)s"
		params["months"] = tuple(months)

	return ch_query_df(
		f"""
		SELECT
			airport,
			any(airport_name) AS airport_name,
			sum(arr_flights) AS total_flights,
			sum(arr_del15) AS delayed_flights,
			round(sum(arr_del15) * 100.0 / nullIf(sum(arr_flights), 0), 2) AS delay_percentage
		FROM flights
		WHERE {where}
		GROUP BY airport
		ORDER BY delay_percentage DESC
		LIMIT %(limit)s
		""",
		parameters=params,
	)


@st.cache_data(show_spinner=False, ttl=10)
def ch_realtime_stats_last_hour() -> pd.DataFrame:
	return ch_query_df(
		"""
		SELECT
			ingestion_minute,
			countMerge(record_count) AS record_count,
			sumMerge(total_flights) AS total_flights,
			sumMerge(total_delayed) AS total_delayed,
			avgMerge(avg_delay_rate) AS avg_delay_rate
		FROM realtime_stats
		WHERE ingestion_minute >= now() - INTERVAL 60 MINUTE
		GROUP BY ingestion_minute
		ORDER BY ingestion_minute
		"""
	)


def kafka_consumer_loop(buffer: Deque[KafkaEvent], stop_event: threading.Event) -> None:
	consumer = None
	try:
		consumer = KafkaConsumer(
			KAFKA_TOPIC,
			bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
			group_id=KAFKA_GROUP_ID,
			auto_offset_reset="latest",
			enable_auto_commit=True,
			consumer_timeout_ms=1000,
			value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
		)
		while not stop_event.is_set():
			# kafka-python: iterate triggers poll internally
			any_msg = False
			for msg in consumer:
				any_msg = True
				raw = msg.value
				for record in parse_kafka_payload(raw):
					buffer.append(KafkaEvent(received_at=datetime.utcnow(), record=record, raw=raw))
				if stop_event.is_set():
					break
			if not any_msg:
				time.sleep(0.2)
	except Exception:
		# Avoid crashing Streamlit on background thread errors
		time.sleep(1.0)
	finally:
		if consumer is not None:
			try:
				consumer.close()
			except Exception:
				pass


def ensure_kafka_thread() -> None:
	if "kafka_buffer" not in st.session_state:
		st.session_state.kafka_buffer = deque(maxlen=5000)
	if "kafka_stop" not in st.session_state:
		st.session_state.kafka_stop = threading.Event()
	if "kafka_thread" not in st.session_state:
		thread = threading.Thread(
			target=kafka_consumer_loop,
			args=(st.session_state.kafka_buffer, st.session_state.kafka_stop),
			daemon=True,
			name="kafka-consumer",
		)
		thread.start()
		st.session_state.kafka_thread = thread


def kafka_messages_per_minute(buffer: Deque[KafkaEvent], window_minutes: int = 1) -> float:
	if not buffer:
		return 0.0
	cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
	count = sum(1 for e in buffer if e.received_at >= cutoff)
	return float(count) / float(window_minutes)


st.set_page_config(page_title="Airline Delays – Real-time", layout="wide")

st.title("Airline Delays – Real-time Dashboard")
st.caption(
	f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DATABASE} | "
	f"Kafka: {KAFKA_HOST}:{KAFKA_PORT} topic={KAFKA_TOPIC}"
)

with st.expander("Diagnostics", expanded=False):
	st.write({
		"app_version": APP_VERSION,
		"python": f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}",
		"clickhouse_connect": getattr(clickhouse_connect, "__version__", "unknown"),
		"kafka_host": f"{KAFKA_HOST}:{KAFKA_PORT}",
		"kafka_topic": KAFKA_TOPIC,
		"clickhouse_host": f"{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}",
		"clickhouse_db": CLICKHOUSE_DATABASE,
	})


with st.sidebar:
	st.header("Filters")
	years = []
	clickhouse_ok = True
	try:
		years = ch_years()
	except Exception as e:
		clickhouse_ok = False
		st.error(f"ClickHouse not ready: {e}")

	if years:
		selected_year = st.selectbox("Year", years, index=len(years) - 1)
		available_months = ch_months(int(selected_year))
		month_labels = [f"{m:02d} - {MONTH_NAMES.get(m, str(m))}" for m in available_months]
		label_to_month = {label: m for label, m in zip(month_labels, available_months)}

		selected_month_labels = st.multiselect(
			"Month (slicer)",
			options=month_labels,
			default=month_labels,
		)
		selected_months = [label_to_month[x] for x in selected_month_labels]
	else:
		selected_year = datetime.utcnow().year
		selected_months = None
		st.info("No data in ClickHouse yet.")

	st.divider()
	auto_refresh = st.checkbox("Auto-refresh", value=True)
	refresh_seconds = st.slider("Refresh interval (s)", min_value=2, max_value=30, value=5)

if auto_refresh:
	st_autorefresh(interval=refresh_seconds * 1000, key="autorefresh")


st.subheader("Power BI – Overview (historique)")

if clickhouse_ok and years:
	kpis = ch_overview_kpis(int(selected_year), selected_months)
	c1, c2, c3, c4, c5 = st.columns(5)
	c1.metric("Delay rate", f"{kpis['delay_rate'] * 100:.2f}%")
	c2.metric("Avg delay / flight", f"{kpis['avg_delay_per_flight']:.2f} min")
	c3.metric("Avg delay / delayed", f"{kpis['avg_delay_per_delayed']:.2f} min")
	c4.metric("Cancel rate", f"{kpis['cancel_rate'] * 100:.2f}%")
	c5.metric("Divert rate", f"{kpis['divert_rate'] * 100:.2f}%")

	df_trend = ch_delay_rate_by_month(int(selected_year))
	if not df_trend.empty:
		fig = px.line(
			df_trend,
			x="MonthName",
			y="delay_rate",
			markers=True,
			title="Delay rate by MonthName",
		)
		fig.update_yaxes(tickformat=",.0%")
		st.plotly_chart(fig, use_container_width=True)
	else:
		st.info("No monthly trend available yet.")

	left, right = st.columns(2)
	with left:
		st.markdown("**Breakdown – delay causes (minutes)**")
		df_causes = ch_cause_breakdown(int(selected_year), selected_months)
		if not df_causes.empty and df_causes["minutes"].sum() > 0:
			fig2 = px.pie(df_causes, names="cause", values="minutes")
			st.plotly_chart(fig2, use_container_width=True)
		else:
			st.info("No delay cause breakdown available.")
	with right:
		st.markdown("**Top carriers / airports (by delay%)**")
		df_carriers = ch_top_carriers(int(selected_year), selected_months)
		df_airports = ch_top_airports(int(selected_year), selected_months)
		st.write("Carriers")
		st.dataframe(df_carriers, use_container_width=True, hide_index=True)
		st.write("Airports")
		st.dataframe(df_airports, use_container_width=True, hide_index=True)
else:
	st.warning("ClickHouse is not available yet (or flights table empty).")


st.subheader("Real-time (Kafka + ClickHouse)")
ensure_kafka_thread()

buf: Deque[KafkaEvent] = st.session_state.kafka_buffer

rt1, rt2, rt3, rt4 = st.columns(4)
rt1.metric("Kafka buffer size", f"{len(buf)}")
rt2.metric("Kafka msg/min", f"{kafka_messages_per_minute(buf, 1):.1f}")
rt3.metric("Kafka msg/5min", f"{kafka_messages_per_minute(buf, 5):.1f}")
rt4.metric("Last msg (UTC)", buf[-1].received_at.strftime("%H:%M:%S") if buf else "-")

st.markdown("**Kafka buffer (latest records)**")
if buf:
	latest = list(buf)[-25:]
	df_latest = pd.DataFrame([e.record for e in latest])
	st.dataframe(df_latest, use_container_width=True, hide_index=True)
else:
	st.info("No Kafka messages received yet.")

st.markdown("**ClickHouse realtime_stats (last hour)**")
try:
	df_rt = ch_realtime_stats_last_hour()
	if df_rt.empty:
		st.info("No realtime_stats yet (need data inserts into flights).")
	else:
		df_rt = df_rt.copy()
		df_rt["avg_delay_rate"] = df_rt["avg_delay_rate"].astype(float)
		fig_rt = px.bar(df_rt, x="ingestion_minute", y="record_count", title="Records per minute")
		st.plotly_chart(fig_rt, use_container_width=True)
		fig_rt2 = px.line(df_rt, x="ingestion_minute", y="avg_delay_rate", markers=True, title="Avg delay rate (per minute)")
		fig_rt2.update_yaxes(tickformat=",.0%")
		st.plotly_chart(fig_rt2, use_container_width=True)
except Exception as e:
	st.error(f"Could not query realtime_stats: {type(e).__name__}: {e}")
	st.exception(e)
