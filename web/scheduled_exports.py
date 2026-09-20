"""
Scheduled Exports - Automatically export dashboard data on a recurring schedule.

Supports exporting dashboard widgets to CSV, Parquet, or PNG on daily, weekly,
monthly intervals or custom cron expressions.
"""

import os
import json
import time
import uuid
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
EXPORTS_DIR = os.path.join(WAREHOUSE_DIR, "exports")
SCHEDULED_EXPORTS_FILE = os.path.join(METADATA_DIR, "scheduled_exports.json")

# Ensure exports directory exists
os.makedirs(EXPORTS_DIR, exist_ok=True)

# Global scheduler instance
scheduler = None

# {schedule_id: {dashboard_id, widget_ids, format, frequency, enabled, created_at, created_by, last_run, next_run}}
SCHEDULED_EXPORTS = {}


def init_scheduler():
    """Initialize the background scheduler."""
    global scheduler
    if scheduler is None:
        scheduler = BackgroundScheduler()
        scheduler.start()
        load_schedules()
        # Restore all enabled schedules
        for schedule_id, schedule in SCHEDULED_EXPORTS.items():
            if schedule.get("enabled", True):
                _add_job_to_scheduler(schedule_id, schedule)


def shutdown_scheduler():
    """Shutdown the scheduler."""
    global scheduler
    if scheduler:
        scheduler.shutdown()
        scheduler = None


def load_schedules():
    """Load scheduled exports from file."""
    global SCHEDULED_EXPORTS
    if os.path.exists(SCHEDULED_EXPORTS_FILE):
        try:
            with open(SCHEDULED_EXPORTS_FILE, "r") as f:
                SCHEDULED_EXPORTS = json.load(f)
        except Exception as e:
            print(f"Error loading scheduled exports: {e}")
            SCHEDULED_EXPORTS = {}
    return SCHEDULED_EXPORTS


def save_schedules():
    """Save scheduled exports to file."""
    try:
        with open(SCHEDULED_EXPORTS_FILE, "w") as f:
            json.dump(SCHEDULED_EXPORTS, f, indent=2)
    except Exception as e:
        print(f"Error saving scheduled exports: {e}")


def _add_job_to_scheduler(schedule_id: str, schedule: Dict[str, Any]):
    """Add a job to the APScheduler."""
    if not scheduler:
        return

    frequency = schedule.get("frequency", "daily")

    try:
        # Remove existing job if it exists
        if scheduler.get_job(schedule_id):
            scheduler.remove_job(schedule_id)

        # Parse frequency and create trigger
        if frequency == "hourly":
            trigger = IntervalTrigger(hours=1)
        elif frequency == "daily":
            trigger = CronTrigger(hour=schedule.get("hour", 0), minute=schedule.get("minute", 0))
        elif frequency == "weekly":
            trigger = CronTrigger(
                day_of_week=schedule.get("day_of_week", 0),
                hour=schedule.get("hour", 0),
                minute=schedule.get("minute", 0)
            )
        elif frequency == "monthly":
            trigger = CronTrigger(
                day=schedule.get("day_of_month", 1),
                hour=schedule.get("hour", 0),
                minute=schedule.get("minute", 0)
            )
        elif frequency == "custom" and schedule.get("cron_expression"):
            # Parse cron expression (minute hour day month day_of_week)
            parts = schedule["cron_expression"].split()
            if len(parts) == 5:
                trigger = CronTrigger(
                    minute=parts[0],
                    hour=parts[1],
                    day=parts[2],
                    month=parts[3],
                    day_of_week=parts[4]
                )
            else:
                print(f"Invalid cron expression for schedule {schedule_id}")
                return
        else:
            trigger = CronTrigger(hour=0, minute=0)  # Default to daily at midnight

        # Add job to scheduler
        scheduler.add_job(
            func=execute_scheduled_export,
            trigger=trigger,
            id=schedule_id,
            args=[schedule_id],
            replace_existing=True
        )

        # Update next_run time
        job = scheduler.get_job(schedule_id)
        if job and job.next_run_time:
            schedule["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
            save_schedules()

    except Exception as e:
        print(f"Error adding job to scheduler: {e}")


def execute_scheduled_export(schedule_id: str):
    """Execute a scheduled export."""
    load_schedules()

    if schedule_id not in SCHEDULED_EXPORTS:
        print(f"Schedule {schedule_id} not found")
        return

    schedule = SCHEDULED_EXPORTS[schedule_id]

    if not schedule.get("enabled", True):
        print(f"Schedule {schedule_id} is disabled")
        return

    try:
        # Import here to avoid circular dependency
        from web.dashboards import load_dashboards_store, execute_widget_query
        import duckrun

        dashboard_id = schedule["dashboard_id"]
        export_format = schedule.get("format", "csv")
        widget_ids = schedule.get("widget_ids", [])

        # Load dashboard
        dashboards = load_dashboards_store()
        dashboard = None
        for d in dashboards:
            if d["id"] == dashboard_id:
                dashboard = d
                break

        if not dashboard:
            print(f"Dashboard {dashboard_id} not found")
            return

        # Get database connection
        conn = duckrun.connect(WAREHOUSE_DIR, read_only=True)   # duckrun has no get_connection(); exports never ran before
        from web.governance import gateway
        gateway.ensure_masks(conn)
        # Exports run as the schedule's owner (role resolved now): a masked owner gets masked files.
        owner = gateway.principal_for_username(schedule.get("created_by"))

        # Create export directory for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_dir = os.path.join(EXPORTS_DIR, schedule_id, timestamp)
        os.makedirs(export_dir, exist_ok=True)

        exported_files = []

        # Export each widget
        widgets_to_export = [w for w in dashboard.get("widgets", []) if not widget_ids or w["id"] in widget_ids]

        for widget in widgets_to_export:
            try:
                # Execute query for widget
                query = widget.get("query", "")
                if not query:
                    continue

                result = execute_widget_query(conn, query, principal=owner)

                if not result or "rows" not in result:
                    continue

                # Generate filename
                widget_name = widget.get("title", widget["id"]).replace(" ", "_").replace("/", "_")

                if export_format == "csv":
                    filepath = os.path.join(export_dir, f"{widget_name}.csv")
                    _export_to_csv(result, filepath)
                    exported_files.append(filepath)

                elif export_format == "parquet":
                    filepath = os.path.join(export_dir, f"{widget_name}.parquet")
                    _export_to_parquet(result, filepath)
                    exported_files.append(filepath)

                elif export_format == "png":
                    # PNG export would require headless browser/chart rendering
                    # For now, export as CSV instead
                    filepath = os.path.join(export_dir, f"{widget_name}.csv")
                    _export_to_csv(result, filepath)
                    exported_files.append(filepath)

            except Exception as e:
                print(f"Error exporting widget {widget['id']}: {e}")

        # Update last run info
        schedule["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        schedule["last_run_status"] = "success"
        schedule["last_run_files"] = len(exported_files)
        schedule["last_export_dir"] = export_dir

        # Send email if enabled
        if schedule.get("email_enabled") and schedule.get("email_recipients"):
            try:
                from web.email_reports import send_dashboard_report

                email_result = send_dashboard_report(
                    dashboard_id=dashboard_id,
                    dashboard_name=dashboard.get("name", "Dashboard"),
                    recipients=schedule["email_recipients"],
                    include_attachments=True,
                    attachment_format=export_format,
                    export_dir=export_dir,
                    custom_message=schedule.get("email_message"),
                    cc=schedule.get("email_cc")
                )

                if email_result.get("success"):
                    schedule["last_email_status"] = "sent"
                    print(f"Email sent successfully for schedule {schedule_id}")
                else:
                    schedule["last_email_status"] = "failed"
                    schedule["last_email_error"] = email_result.get("error", "Unknown error")
                    print(f"Failed to send email for schedule {schedule_id}: {email_result.get('error')}")

            except Exception as e:
                schedule["last_email_status"] = "error"
                schedule["last_email_error"] = str(e)
                print(f"Error sending email for schedule {schedule_id}: {e}")

        # Update next run time
        if scheduler:
            job = scheduler.get_job(schedule_id)
            if job and job.next_run_time:
                schedule["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")

        save_schedules()

        print(f"Successfully exported {len(exported_files)} files for schedule {schedule_id}")

    except Exception as e:
        print(f"Error executing scheduled export {schedule_id}: {e}")
        schedule["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        schedule["last_run_status"] = "error"
        schedule["last_run_error"] = str(e)
        save_schedules()


def _export_to_csv(result: Dict[str, Any], filepath: str):
    """Export query result to CSV."""
    import csv

    rows = result.get("rows", [])
    if not rows:
        return

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def _export_to_parquet(result: Dict[str, Any], filepath: str):
    """Export query result to Parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = result.get("rows", [])
    if not rows:
        return

    # Convert rows to Arrow table
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, filepath)


def create_schedule(
    dashboard_id: str,
    name: str,
    frequency: str,
    format: str = "csv",
    widget_ids: Optional[List[str]] = None,
    created_by: str = "admin",
    hour: int = 0,
    minute: int = 0,
    day_of_week: int = 0,
    day_of_month: int = 1,
    cron_expression: Optional[str] = None,
    enabled: bool = True,
    email_enabled: bool = False,
    email_recipients: Optional[List[str]] = None,
    email_cc: Optional[List[str]] = None,
    email_message: Optional[str] = None
) -> str:
    """Create a new scheduled export."""
    load_schedules()

    schedule_id = f"schedule_{uuid.uuid4().hex[:12]}"

    schedule = {
        "id": schedule_id,
        "dashboard_id": dashboard_id,
        "name": name,
        "frequency": frequency,
        "format": format,
        "widget_ids": widget_ids or [],
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "created_by": created_by,
        "enabled": enabled,
        "hour": hour,
        "minute": minute,
        "day_of_week": day_of_week,
        "day_of_month": day_of_month,
        "cron_expression": cron_expression,
        "email_enabled": email_enabled,
        "email_recipients": email_recipients or [],
        "email_cc": email_cc or [],
        "email_message": email_message,
        "last_run": None,
        "last_run_status": None,
        "last_run_files": 0,
        "next_run": None
    }

    SCHEDULED_EXPORTS[schedule_id] = schedule
    save_schedules()

    if enabled and scheduler:
        _add_job_to_scheduler(schedule_id, schedule)

    return schedule_id


def list_schedules(dashboard_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """List all scheduled exports, optionally filtered by dashboard."""
    load_schedules()

    schedules = list(SCHEDULED_EXPORTS.values())

    if dashboard_id:
        schedules = [s for s in schedules if s["dashboard_id"] == dashboard_id]

    return sorted(schedules, key=lambda x: x["created_at"], reverse=True)


def get_schedule(schedule_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific scheduled export."""
    load_schedules()
    return SCHEDULED_EXPORTS.get(schedule_id)


def update_schedule(
    schedule_id: str,
    name: Optional[str] = None,
    frequency: Optional[str] = None,
    format: Optional[str] = None,
    widget_ids: Optional[List[str]] = None,
    enabled: Optional[bool] = None,
    hour: Optional[int] = None,
    minute: Optional[int] = None,
    day_of_week: Optional[int] = None,
    day_of_month: Optional[int] = None,
    cron_expression: Optional[str] = None,
    email_enabled: Optional[bool] = None,
    email_recipients: Optional[List[str]] = None,
    email_cc: Optional[List[str]] = None,
    email_message: Optional[str] = None
) -> bool:
    """Update a scheduled export."""
    load_schedules()

    if schedule_id not in SCHEDULED_EXPORTS:
        return False

    schedule = SCHEDULED_EXPORTS[schedule_id]

    if name is not None:
        schedule["name"] = name
    if frequency is not None:
        schedule["frequency"] = frequency
    if format is not None:
        schedule["format"] = format
    if widget_ids is not None:
        schedule["widget_ids"] = widget_ids
    if hour is not None:
        schedule["hour"] = hour
    if minute is not None:
        schedule["minute"] = minute
    if day_of_week is not None:
        schedule["day_of_week"] = day_of_week
    if day_of_month is not None:
        schedule["day_of_month"] = day_of_month
    if cron_expression is not None:
        schedule["cron_expression"] = cron_expression
    if email_enabled is not None:
        schedule["email_enabled"] = email_enabled
    if email_recipients is not None:
        schedule["email_recipients"] = email_recipients
    if email_cc is not None:
        schedule["email_cc"] = email_cc
    if email_message is not None:
        schedule["email_message"] = email_message

    if enabled is not None:
        schedule["enabled"] = enabled
        if enabled and scheduler:
            _add_job_to_scheduler(schedule_id, schedule)
        elif not enabled and scheduler:
            if scheduler.get_job(schedule_id):
                scheduler.remove_job(schedule_id)
            schedule["next_run"] = None
    else:
        # Re-add job if schedule parameters changed
        if schedule.get("enabled", True) and scheduler:
            _add_job_to_scheduler(schedule_id, schedule)

    save_schedules()
    return True


def delete_schedule(schedule_id: str) -> bool:
    """Delete a scheduled export."""
    load_schedules()

    if schedule_id not in SCHEDULED_EXPORTS:
        return False

    # Remove from scheduler
    if scheduler and scheduler.get_job(schedule_id):
        scheduler.remove_job(schedule_id)

    del SCHEDULED_EXPORTS[schedule_id]
    save_schedules()
    return True


def trigger_schedule_now(schedule_id: str) -> bool:
    """Manually trigger a scheduled export immediately."""
    load_schedules()

    if schedule_id not in SCHEDULED_EXPORTS:
        return False

    # Execute in background thread
    import threading
    thread = threading.Thread(target=execute_scheduled_export, args=[schedule_id])
    thread.start()

    return True


def get_export_history(schedule_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get export history for a schedule."""
    schedule = get_schedule(schedule_id)

    if not schedule:
        return []

    # List export directories
    schedule_dir = os.path.join(EXPORTS_DIR, schedule_id)

    if not os.path.exists(schedule_dir):
        return []

    history = []

    for entry in sorted(os.listdir(schedule_dir), reverse=True)[:limit]:
        export_dir = os.path.join(schedule_dir, entry)
        if os.path.isdir(export_dir):
            files = [f for f in os.listdir(export_dir) if os.path.isfile(os.path.join(export_dir, f))]
            history.append({
                "timestamp": entry,
                "path": export_dir,
                "file_count": len(files),
                "files": files
            })

    return history
