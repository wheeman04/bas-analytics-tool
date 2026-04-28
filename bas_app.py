
import streamlit as st
import pandas as pd
import sys
import os

st.set_page_config(page_title="BAS Analytics Tool", layout="wide")

st.title("BAS Analytics Tool")
st.caption("Upload Niagara exports to generate alarm and trend analysis reports.")

st.divider()

col1, col2 = st.columns(2)
with col1:
    run_alarms = st.checkbox("Analyze alarms", value=True)
with col2:
    run_trends = st.checkbox("Analyze trends", value=True)

alarm_file = None
trend_file = None
temp_cols = []

if run_alarms:
    st.subheader("Alarm CSV")
    alarm_file = st.file_uploader("Upload alarm history export", type="csv", key="alarm")

if run_trends:
    st.subheader("Trend CSV")
    trend_file = st.file_uploader("Upload trend log export", type="csv", key="trend")
    if trend_file is not None:
        trend_preview = pd.read_csv(trend_file, encoding="utf-8-sig", nrows=2)
        trend_file.seek(0)
        all_cols = [c for c in trend_preview.columns if c.lower() != "timestamp"]
        temp_cols = st.multiselect("Select columns to analyze", all_cols, default=all_cols)

st.divider()
run_button = st.button("Run Analysis", type="primary")

if run_button:
    if not run_alarms and not run_trends:
        st.warning("Please select at least one analysis type.")
    else:
        if run_alarms and alarm_file is None:
            st.error("Please upload an alarm CSV.")
        elif run_trends and trend_file is None:
            st.error("Please upload a trend CSV.")
        elif run_trends and len(temp_cols) == 0:
            st.error("Please select at least one column to analyze.")
        else:
            import bas_functions
            import importlib
            importlib.reload(bas_functions)
            from bas_functions import (analyze_alarms, analyze_trends,
                                       write_site_health_report,
                                       plot_alarm_chart, plot_trend_chart)
            import tempfile
            import matplotlib.pyplot as plt

            st.success("Running analysis...")

            if run_alarms and alarm_file is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(alarm_file.read())
                    alarm_tmp_path = tmp.name

                site_counts, class_counts, active_alarms, resolved_alarms, source_counts = analyze_alarms(alarm_tmp_path)

                st.subheader("Alarm summary")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Total alarms", sum(site_counts.values()))
                m2.metric("Active alarms", sum(active_alarms.values()))
                m3.metric("Resolved", sum(resolved_alarms.values()))
                m4.metric("Buildings", len(site_counts))

                st.subheader("Top 10 active alarm buildings")
                sorted_active = sorted(active_alarms.items(), key=lambda x: x[1], reverse=True)[:10]
                buildings = [i[0] for i in sorted_active]
                counts = [i[1] for i in sorted_active]

                fig, ax = plt.subplots(figsize=(10, 5))
                bars = ax.barh(buildings, counts, color="#378ADD")
                ax.invert_yaxis()
                ax.set_xlabel("Active alarm count")
                ax.set_title("Top 10 buildings by active alarm count")
                for bar, count in zip(bars, counts):
                    ax.text(bar.get_width() + 0.3,
                            bar.get_y() + bar.get_height()/2,
                            str(count), va="center", fontsize=10)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close()

                st.subheader("Top 10 alarm sources")
                sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:10]
                source_df = pd.DataFrame(sorted_sources, columns=["Source", "Count"])
                st.dataframe(source_df, use_container_width=True)

            if run_trends and trend_file is not None and len(temp_cols) > 0:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(trend_file.read())
                    trend_tmp_path = tmp.name

                df, df_resampled, spikes, gaps, stats = analyze_trends(trend_tmp_path, temp_cols)

                st.subheader("Trend summary")
                t1, t2, t3 = st.columns(3)
                t1.metric("Records", len(df))
                t2.metric("Spikes flagged", sum(len(v) for v in spikes.values()))
                t3.metric("Data gaps", len(gaps))

                st.subheader("Temperature trends")
                fig2, ax2 = plt.subplots(figsize=(10, 4))
                for col in temp_cols:
                    ax2.plot(df_resampled.index, df_resampled[col], label=col, linewidth=1.5)
                ax2.set_xlabel("Timestamp")
                ax2.set_ylabel("Temperature (°F)")
                ax2.set_title("Temperature trends over time")
                ax2.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()
                st.pyplot(fig2)
                plt.close()

                if any(spikes.values()):
                    st.subheader("Spikes detected")
                    for col, spike_list in spikes.items():
                        if spike_list:
                            for ts, val in spike_list:
                                st.warning(f"{col} — spike at {ts}, value: {val}")

                if gaps:
                    st.subheader("Data gaps detected")
                    for ts, duration in gaps:
                        st.warning(f"Gap starting {ts} — duration: {duration}")

            st.success("Analysis complete.")
