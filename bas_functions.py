
def check_points(points, threshold=3):
    results = []
    for name, value, setpoint in points:
        deviation = round(value - setpoint, 2)
        if deviation > threshold:
            status = "HIGH ALARM"
        elif deviation < -threshold:
            status = "LOW ALARM"
        else:
            status = "normal"
        results.append((name, deviation, status))
    return results


def analyze_alarms(filepath):
    import csv
    site_counts = {}
    class_counts = {}
    active_alarms = {}
    resolved_alarms = {}
    source_counts = {}

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = row["Source"]
            state = row["Source State"]
            alarm_class = row["Alarm Class"]

            if " : " in source:
                site = source.split(" : ")[0]
            elif ":" in source:
                site = source.split(":")[0]
            else:
                site = source
            site = site.strip()

            site_counts[site] = site_counts.get(site, 0) + 1
            class_counts[alarm_class] = class_counts.get(alarm_class, 0) + 1
            source_counts[source] = source_counts.get(source, 0) + 1

            if state == "Offnormal":
                active_alarms[site] = active_alarms.get(site, 0) + 1
            else:
                resolved_alarms[site] = resolved_alarms.get(site, 0) + 1

    return site_counts, class_counts, active_alarms, resolved_alarms, source_counts


def write_alarm_report(report_path, site_counts, class_counts, active_alarms, resolved_alarms, source_counts):
    import os
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    sorted_sites = sorted(site_counts.items(), key=lambda x: x[1], reverse=True)
    sorted_classes = sorted(class_counts.items(), key=lambda x: x[1], reverse=True)
    sorted_active = sorted(active_alarms.items(), key=lambda x: x[1], reverse=True)
    sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)

    with open(report_path, "w") as f:
        f.write("UNCC ALARM ANALYSIS REPORT\n")
        f.write("=" * 55 + "\n")
        f.write(f"Total alarms:    {sum(site_counts.values())}\n")
        f.write(f"Active alarms:   {sum(active_alarms.values())}\n")
        f.write(f"Resolved alarms: {sum(resolved_alarms.values())}\n")
        f.write(f"Total buildings: {len(site_counts)}\n")
        f.write("=" * 55 + "\n\n")

        f.write("TOP 10 BUILDINGS — ACTIVE ALARMS ONLY\n")
        f.write("-" * 55 + "\n")
        for site, count in sorted_active[:10]:
            f.write(f"{site:<45} {count} active\n")

        f.write("\n")
        f.write("TOP 10 ALARM SOURCES (SPECIFIC POINTS)\n")
        f.write("-" * 55 + "\n")
        for source, count in sorted_sources[:10]:
            f.write(f"{source:<45} {count} alarms\n")

        f.write("\n")
        f.write("TOP 10 BUILDINGS — TOTAL ALARM COUNT\n")
        f.write("-" * 55 + "\n")
        for site, count in sorted_sites[:10]:
            f.write(f"{site:<45} {count} alarms\n")

        f.write("\n")
        f.write("ALARMS BY CLASS\n")
        f.write("-" * 55 + "\n")
        for alarm_class, count in sorted_classes:
            f.write(f"{alarm_class:<45} {count}\n")


def analyze_trends(filepath, temp_cols, std_threshold=2, gap_minutes=30):
    import pandas as pd

    df = pd.read_csv(filepath, encoding="utf-8-sig")
    df["timestamp"] = df["timestamp"].str.replace(" EDT", "").str.replace(" EST", "")
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d-%b-%y %I:%M:%S %p")
    df = df.drop_duplicates(subset="timestamp")
    df = df.dropna(subset=temp_cols)
    df = df.sort_values("timestamp").reset_index(drop=True)

    spikes = {}
    for col in temp_cols:
        mean = df[col].mean()
        std = df[col].std()
        upper = mean + (std_threshold * std)
        lower = mean - (std_threshold * std)
        spike_rows = df[(df[col] > upper) | (df[col] < lower)]
        spikes[col] = [(str(row["timestamp"]), row[col]) for _, row in spike_rows.iterrows()]

    df["time_diff"] = df["timestamp"].diff()
    gap_threshold = pd.Timedelta(minutes=gap_minutes)
    gap_rows = df[df["time_diff"] > gap_threshold]
    gaps = [(str(row["timestamp"]), str(row["time_diff"])) for _, row in gap_rows.iterrows()]

    stats = df[temp_cols].describe().round(2)

    df_indexed = df.set_index("timestamp")
    df_resampled = df_indexed[temp_cols].resample("h").mean()

    return df, df_resampled, spikes, gaps, stats


def write_trend_report(report_path, df, spikes, gaps, stats):
    import os
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    with open(report_path, "w") as f:
        f.write("TREND ANALYSIS REPORT\n")
        f.write("=" * 55 + "\n")
        f.write(f"Total records: {len(df)}\n")
        f.write(f"Start: {df['timestamp'].min()}\n")
        f.write(f"End:   {df['timestamp'].max()}\n")
        f.write("=" * 55 + "\n\n")

        f.write("STATISTICAL SUMMARY\n")
        f.write("-" * 55 + "\n")
        f.write(stats.to_string())
        f.write("\n\n")

        f.write("SPIKE DETECTION\n")
        f.write("-" * 55 + "\n")
        for col, spike_list in spikes.items():
            f.write(f"{col}: {len(spike_list)} spikes detected\n")
            for timestamp, value in spike_list:
                f.write(f"  {timestamp}  value: {value}\n")

        f.write("\n")
        f.write("GAP DETECTION\n")
        f.write("-" * 55 + "\n")
        f.write(f"Gaps detected: {len(gaps)}\n")
        for timestamp, duration in gaps:
            f.write(f"  {timestamp}  gap: {duration}\n")


def write_site_health_report(report_path, site_name,
                              site_counts, class_counts,
                              active_alarms, resolved_alarms,
                              source_counts, df, spikes, gaps):
    import os
    from datetime import datetime
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    total_alarms = sum(site_counts.values())
    total_active = sum(active_alarms.values())
    total_spikes = sum(len(v) for v in spikes.values())
    sorted_active = sorted(active_alarms.items(), key=lambda x: x[1], reverse=True)
    sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)

    with open(report_path, "w") as f:
        f.write(f"SITE HEALTH REPORT — {site_name.upper()}\n")
        f.write("=" * 55 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write("=" * 55 + "\n\n")

        f.write("EXECUTIVE SUMMARY\n")
        f.write("-" * 55 + "\n")
        f.write(f"Total alarms (period):  {total_alarms}\n")
        f.write(f"Active alarms (now):    {total_active}\n")
        f.write(f"Resolved alarms:        {total_alarms - total_active}\n")
        f.write(f"Buildings monitored:    {len(site_counts)}\n")
        f.write(f"Trend sensors checked:  {len(spikes)}\n")
        f.write(f"Sensor spikes flagged:  {total_spikes}\n")
        f.write(f"Data gaps detected:     {len(gaps)}\n")
        f.write("\n")

        f.write("KEY FINDINGS — ACTION REQUIRED\n")
        f.write("-" * 55 + "\n")
        finding_num = 1

        if sorted_active:
            top_site, top_count = sorted_active[0]
            f.write(f"{finding_num}. {top_site} has {top_count} active alarms")
            f.write(" — highest active alarm building. Investigate immediately.\n")
            finding_num += 1

        if sorted_sources:
            top_source, top_source_count = sorted_sources[0]
            f.write(f"{finding_num}. {top_source} generated {top_source_count} alarms")
            f.write(" — top single point. Likely nuisance or fault condition.\n")
            finding_num += 1

        for col, spike_list in spikes.items():
            if spike_list:
                f.write(f"{finding_num}. {col} flagged {len(spike_list)} spike(s)")
                f.write(f" — first at {spike_list[0][0]}, value {spike_list[0][1]}.\n")
                finding_num += 1

        if gaps:
            f.write(f"{finding_num}. {len(gaps)} data gap(s) detected in trend data")
            f.write(f" — longest starting {gaps[0][0]}.\n")
            finding_num += 1

        if finding_num == 1:
            f.write("No critical findings. System operating normally.\n")

        f.write("\n")
        f.write("TOP 5 ACTIVE ALARM BUILDINGS\n")
        f.write("-" * 55 + "\n")
        for site, count in sorted_active[:5]:
            f.write(f"  {site:<43} {count} active\n")

        f.write("\n")
        f.write("TOP 5 ALARM SOURCES\n")
        f.write("-" * 55 + "\n")
        for source, count in sorted_sources[:5]:
            f.write(f"  {source:<43} {count} alarms\n")

        f.write("\n")
        f.write("TREND SENSOR STATUS\n")
        f.write("-" * 55 + "\n")
        for col, spike_list in spikes.items():
            status = "OK" if not spike_list else f"{len(spike_list)} spike(s)"
            f.write(f"  {col:<43} {status}\n")


def plot_alarm_chart(active_alarms, save_path):
    import matplotlib.pyplot as plt
    import os
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    sorted_active = sorted(active_alarms.items(), key=lambda x: x[1], reverse=True)[:10]
    buildings = [item[0] for item in sorted_active]
    counts = [item[1] for item in sorted_active]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(buildings, counts, color='#378ADD')
    ax.invert_yaxis()
    ax.set_xlabel('Active alarm count')
    ax.set_title('Top 10 buildings by active alarm count')

    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                str(count), va='center', fontsize=11)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()


def plot_trend_chart(df_resampled, temp_cols, save_path):
    import matplotlib.pyplot as plt
    import os
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    fig, ax = plt.subplots(figsize=(12, 5))

    for col in temp_cols:
        ax.plot(df_resampled.index, df_resampled[col], label=col, linewidth=1.5)

    ax.set_xlabel('Timestamp')
    ax.set_ylabel('Temperature (°F)')
    ax.set_title('Temperature trends over time — gaps shown honestly')
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150)
    plt.close()
