# Creating Hunt Dashboards in Superset

## Dashboard 1: Executive Overview

### Chart 1: Findings by Severity (Pie Chart)
- **Dataset:** vw_severity_distribution
- **Chart Type:** Pie Chart
- **Dimensions:** severity
- **Metrics:** SUM(count)
- **Colors:** 
  - critical: #DC3545 (red)
  - high: #FD7E14 (orange)
  - medium: #FFC107 (yellow)
  - low: #28A745 (green)

### Chart 2: Daily Findings Trend (Line Chart)
- **Dataset:** vw_daily_findings
- **Chart Type:** Line Chart
- **X-Axis:** finding_date
- **Y-Axis:** SUM(finding_count)
- **Group By:** hunt_name
- **Time Grain:** Day

### Chart 3: Hunt Performance (Table)
- **Dataset:** vw_hunt_performance
- **Chart Type:** Table
- **Columns:**
  - hunt_name
  - execution_count
  - total_findings
  - avg_execution_time
  - successful_runs

### Chart 4: Top Threat Sources (Bar Chart)
- **Dataset:** vw_top_sources
- **Chart Type:** Bar Chart
- **X-Axis:** source_ip
- **Y-Axis:** finding_count
- **Limit:** 10
- **Order:** Descending

## Dashboard 2: Hunt Operations

### Chart 1: Hunt Timeline Heatmap
- **Dataset:** vw_hunt_timeline
- **Chart Type:** Heatmap
- **X-Axis:** hour
- **Y-Axis:** date
- **Metric:** SUM(finding_count)

### Chart 2: Hunt Effectiveness
- **Dataset:** vw_hunt_effectiveness
- **Chart Type:** Table
- **Columns:**
  - hunt_name
  - total_findings
  - confirmed_threats
  - false_positives
  - true_positive_rate

### Chart 3: Investigation Status
- **Dataset:** vw_investigation_status
- **Chart Type:** Stacked Bar Chart
- **X-Axis:** status
- **Y-Axis:** count
- **Group By:** severity

## Dashboard 3: IOC Intelligence

### Chart 1: IOC Feed Table
- **Dataset:** dashboard_ioc_feed
- **Chart Type:** Table
- **Columns:**
  - ioc_value
  - ioc_type
  - times_seen
  - threat_level
  - first_seen
  - last_seen

### Chart 2: Finding Types Distribution
- **Dataset:** dashboard_finding_types
- **Chart Type:** Treemap
- **Dimensions:** finding_type
- **Metric:** count

### Chart 3: Severity Trends
- **Dataset:** dashboard_severity_trends
- **Chart Type:** Area Chart
- **X-Axis:** date
- **Y-Axis:** count
- **Group By:** severity