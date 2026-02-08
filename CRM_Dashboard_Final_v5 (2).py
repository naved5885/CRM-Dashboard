"""
CRM Functional Dashboard - Fixed Version
"""

import os
import time
import math
import datetime as dt
import base64
import io
import json
import traceback
from typing import Optional, Dict, List, Any
import pandas as pd
import requests
import sqlite3

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import bcrypt

import dash
from dash import Dash, dcc, html, dash_table, Input, Output, State, ctx, no_update
import dash_bootstrap_components as dbc
import plotly.express as px

# ---------------- CONFIG ----------------

DB_FILENAME = os.path.join(os.getcwd(), "crm.db")
READ_DB_URI = f"sqlite:///{DB_FILENAME}"
WRITE_DB_URI = f"sqlite:///{DB_FILENAME}"

REMINDER_HOURS = [12, 18]
REMINDER_WINDOW_MINUTES = 6
MASTER_RESET_KEY = "manus-reset-2025"

# ---------------- DATABASE ENGINES ----------------

READ_ENGINE = create_engine(
    READ_DB_URI,
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_pre_ping=True,
    future=True,
)

WRITE_ENGINE = create_engine(
    WRITE_DB_URI,
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_pre_ping=True,
    future=True,
)

# ---------------- HELPER FUNCTIONS ----------------

def with_write_retry(fn):
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(6):
            try:
                return fn(*args, **kwargs)
            except OperationalError as e:
                last_exc = e
                if "locked" in str(e).lower():
                    time.sleep(0.15 * (1 + attempt * 0.25))
                    continue
                raise
        if last_exc: raise last_exc
    return wrapper

def init_db():
    try:
        conn = sqlite3.connect(DB_FILENAME, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()
        conn.close()
    except Exception: pass

    @with_write_retry
    def _create():
        with WRITE_ENGINE.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    database_name TEXT,
                    customer_name TEXT,
                    phone TEXT,
                    location TEXT,
                    customer_type TEXT,
                    current_status TEXT,
                    assigned_user TEXT,
                    last_call_date DATE,
                    no_response_attempts INTEGER DEFAULT 0,
                    catalogue_attempts INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS call_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER,
                    user_name TEXT,
                    call_date TIMESTAMP,
                    outcome TEXT,
                    pitch_used TEXT,
                    notes TEXT,
                    FOREIGN KEY (lead_id) REFERENCES leads(id)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER,
                    reminder_date DATE,
                    reminder_type TEXT,
                    user_name TEXT,
                    is_done INTEGER DEFAULT 0,
                    FOREIGN KEY (lead_id) REFERENCES leads(id)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1,
                    role TEXT DEFAULT 'user',
                    email TEXT
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pitch_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_name TEXT NOT NULL,
                    title TEXT NOT NULL,
                    pitch_text TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """))

            # Default Admin
            row = conn.execute(text("SELECT COUNT(*) AS c FROM users WHERE username = 'naved'")).mappings().fetchone()
            if row and row["c"] == 0:
                hashed = bcrypt.hashpw("naved123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                conn.execute(text("INSERT INTO users (username, password, is_active, role) VALUES ('naved', :p, 1, 'admin')"), {"p": hashed})
    _create()

init_db()

def cu_username(current_user):
    if not current_user: return None
    return current_user.get("username") if isinstance(current_user, dict) else current_user

def is_admin(username):
    if not username: return False
    try:
        with READ_ENGINE.begin() as conn:
            row = conn.execute(text("SELECT role FROM users WHERE username = :u"), {"u": username}).mappings().fetchone()
            return bool(row and row["role"] == "admin")
    except Exception: return False

def sanitize_df_for_json(df):
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: int.from_bytes(x, byteorder="little")
            if isinstance(x, (bytes, bytearray))
            else x
        )
    return df

def insert_or_update_leads_from_df(df: pd.DataFrame) -> None:
    df = df.rename(columns={
        "Database": "database_name",
        "Customer name": "customer_name",
        "mobile": "phone",
        "NBD/CRR": "customer_type",
        "Location": "location",
    })
    cols = ["database_name", "customer_name", "phone", "customer_type", "location"]
    for col in cols:
        if col not in df.columns: df[col] = None
    df["assigned_user"] = None
    df["current_status"] = "New"
    df["last_call_date"] = None

    @with_write_retry
    def _write(df_in: pd.DataFrame):
        with WRITE_ENGINE.begin() as conn:
            df_in[cols + ["assigned_user", "current_status", "last_call_date"]].to_sql("leads", conn, if_exists="append", index=False)
    _write(df)

def get_all_pitch_templates():
    try:
        with READ_ENGINE.begin() as conn:
            df = pd.read_sql(text("SELECT id, title, pitch_text, created_at, user_name FROM pitch_templates ORDER BY created_at DESC"), conn)
            return df.to_dict("records")
    except Exception: return []

def create_reminders_for_no_response(conn, lead_id, user_name, call_date, attempts):
    if attempts <= 3:
        next_date = call_date + dt.timedelta(days=1)
        conn.execute(text("INSERT INTO reminders (lead_id, reminder_date, reminder_type, user_name) VALUES (:lid, :rdate, 'No response', :user)"),
                     {"lid": lead_id, "rdate": next_date.isoformat(), "user": user_name})
    else:
        for d in (30, 60, 90):
            rdate = call_date + dt.timedelta(days=d)
            conn.execute(text("INSERT INTO reminders (lead_id, reminder_date, reminder_type, user_name) VALUES (:lid, :rdate, 'No response long interval', :user)"),
                         {"lid": lead_id, "rdate": rdate.isoformat(), "user": user_name})

def create_reminders_for_catalogue(conn, lead_id, user_name, call_date, attempts):
    if attempts <= 5:
        next_date = call_date + dt.timedelta(days=1)
        conn.execute(text("INSERT INTO reminders (lead_id, reminder_date, reminder_type, user_name) VALUES (:lid, :rdate, 'Catalogue', :user)"),
                     {"lid": lead_id, "rdate": next_date.isoformat(), "user": user_name})
    else:
        for d in (30, 60, 90):
            rdate = call_date + dt.timedelta(days=d)
            conn.execute(text("INSERT INTO reminders (lead_id, reminder_date, reminder_type, user_name) VALUES (:lid, :rdate, 'Catalogue long interval', :user)"),
                         {"lid": lead_id, "rdate": rdate.isoformat(), "user": user_name})

# ---------------- LAYOUT COMPONENTS ----------------

REMARK_OPTIONS = ["No response", "Catalogue", "Store visit Mumbai", "Store visit Delhi", "Purchased", "Not Interested", "Invalid number", "Follow up", "Others"]

def layout_data_view(current_user):
    uname = cu_username(current_user)
    admin_status = is_admin(uname)
    try:
        with READ_ENGINE.begin() as conn:
            df = pd.read_sql("SELECT DISTINCT database_name FROM leads", conn)
            dbs = df["database_name"].dropna().unique().tolist()
            df_users = pd.read_sql("SELECT DISTINCT assigned_user FROM leads", conn)
            users = df_users["assigned_user"].dropna().unique().tolist()
    except Exception:
        dbs = []
        users = []

    return html.Div([
        dbc.Row([
            dbc.Col([html.Label("Database"), dcc.Dropdown(id="filter-database", options=[{"label": d, "value": d} for d in dbs], placeholder="All")], width=3),
            dbc.Col([html.Label("CRM User"), dcc.Dropdown(id="filter-user", options=[{"label": u, "value": u} for u in users], placeholder="All")], width=3),
            dbc.Col([html.Label("Search"), dcc.Input(id="search-input", placeholder="Search name/phone/location...", type="text", className="w-100")], width=6),
        ], className="mb-3"),
        dash_table.DataTable(
            id="leads-table",
            columns=[{"name": i, "id": i} for i in ["id", "database_name", "customer_name", "phone", "location", "customer_type", "assigned_user", "current_status", "last_call_date"]],
            page_current=0, page_size=15, page_action="custom", page_count=0, sort_action="custom", sort_mode="single",
            row_selectable="multi" if admin_status else "single",
            style_table={"overflowX": "auto"}, style_cell={"textAlign": "left", "fontSize": 12},
        ),
        dbc.Button("Download Data View (Excel)", id="download-data-view-btn", color="secondary", className="mt-2 me-2"),
        html.Div([
            dbc.Button("Delete Selected", id="delete-leads-btn", color="danger", className="mt-2"),
            html.Div(id="delete-leads-status", className="mt-2")
        ], style={"display": "block" if admin_status else "none"})
    ])

def layout_workflow(current_user):
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H5("1. Select Lead from Data View"),
                html.Div(id="selected-lead-summary", className="p-2 border rounded bg-light mb-3"),
                html.H5("2. Call Workflow"),
                dbc.Label("Call Date"), dcc.DatePickerSingle(id="call-date", date=dt.date.today(), className="mb-2 d-block"),
                dbc.Label("Remark / Outcome"), dcc.Dropdown(id="remark-dropdown", options=[{"label": o, "value": o} for o in REMARK_OPTIONS], className="mb-2"),
                dbc.Label("Pitch Used"), dcc.Dropdown(id="workflow-pitch-dropdown", className="mb-2"),
                html.Div(id="pitch-display-area", children=[dbc.Textarea(id="selected-pitch-display", readOnly=True, style={"height": 100}, className="mb-2")]),
                dbc.Label("Assign To (Optional)"), dcc.Dropdown(id="assign-to-dropdown", className="mb-2"),
                dbc.Label("Next Follow-up Date (Optional)"), dcc.DatePickerSingle(id="followup-date", className="mb-2 d-block"),
                dbc.Label("Notes"), dbc.Textarea(id="call-notes", className="mb-3"),
                dbc.Button("Save Call & Update Lead", id="save-call-btn", color="success", className="w-100"),
                html.Div(id="workflow-status", className="mt-2")
            ], width=6),
            dbc.Col([
                html.H5("Call History"),
                html.Div(id="call-history-area")
            ], width=6)
        ])
    ])

def layout_followups(current_user):
    return html.Div([
        dbc.Row([
            dbc.Col([dcc.DatePickerRange(id="followup-date-filter", start_date=dt.date.today(), end_date=dt.date.today() + dt.timedelta(days=7))], width=6),
            dbc.Col([dbc.Button("Show All Pending", id="show-all-pending-btn", color="info")], width=6),
        ], className="mb-3"),
        dash_table.DataTable(
            id="followups-table",
            columns=[{"name": i, "id": i} for i in ["id", "reminder_date", "reminder_type", "customer_name", "phone"]],
            row_selectable="single",
            style_table={"overflowX": "auto"},
        ),
        dbc.Row([
            dbc.Col([dbc.Input(id="followup-remark", placeholder="Follow-up notes...")], width=8),
            dbc.Col([dbc.Button("Mark Done", id="mark-reminder-done-btn", color="success", className="w-100")], width=4),
        ], className="mt-3"),
        html.Div(id="followup-action-status", className="mt-2")
    ])

def layout_reports():
    return html.Div([
        dbc.Row([
            dbc.Col([dcc.DatePickerRange(id="report-date-filter", start_date=dt.date.today() - dt.timedelta(days=30), end_date=dt.date.today())], width=6),
            dbc.Col([dbc.Button("Refresh Reports", id="refresh-reports-btn", color="primary")], width=6),
        ], className="mb-3"),
        html.Div(id="reports-graphs-container")
    ])

def layout_admin():
    return html.Div([
        dbc.Tabs([
            dbc.Tab(label="Import Leads", children=[
                html.Div([
                    html.H5("Upload Excel", className="mt-3"),
                    dcc.Upload(id="upload-excel", children=html.Div(["Drag and Drop or ", html.A("Select Excel File")]), style={"width": "100%", "height": "60px", "lineHeight": "60px", "borderWidth": "1px", "borderStyle": "dashed", "borderRadius": "5px", "textAlign": "center"}),
                    html.Div(id="upload-status", className="mt-2"),
                    html.Hr(),
                    html.H5("Import from Google Sheet (CSV URL)"),
                    dbc.Input(id="google-sheet-url", placeholder="https://docs.google.com/spreadsheets/d/.../export?format=csv"),
                    dbc.Button("Import GSheet", id="import-gsheet-btn", color="primary", className="mt-2"),
                    html.Div(id="gsheet-status", className="mt-2")
                ], className="p-3")
            ]),
            dbc.Tab(label="Manage Pitches", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([dbc.Label("Title"), dbc.Input(id="template-edit-title")], width=4),
                        dbc.Col([dbc.Label("Pitch Text"), dbc.Textarea(id="template-edit-text")], width=8),
                    ], className="mb-2"),
                    dbc.Button("Add/Update Template", id="update-template-btn", color="success", className="me-2"),
                    dbc.Button("Delete Selected", id="delete-template-btn", color="danger"),
                    dash_table.DataTable(id="templates-table", columns=[{"name": i, "id": i} for i in ["id", "title", "created_at"]], row_selectable="single", style_table={"marginTop": "1rem"}),
                    html.Div(id="template-action-status")
                ], className="p-3")
            ]),
            dbc.Tab(label="Manage Users", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([dbc.Label("Username"), dbc.Input(id="new-user-username")], width=4),
                        dbc.Col([dbc.Label("Password"), dbc.Input(id="new-user-password", type="password")], width=4),
                        dbc.Col([dbc.Label("Email"), dbc.Input(id="new-user-email")], width=4),
                    ], className="mb-2"),
                    dbc.Button("Add User", id="add-user-button", color="success", className="me-2"),
                    dash_table.DataTable(id="users-table", columns=[{"name": i, "id": i} for i in ["id", "username", "role", "is_active", "email"]], row_selectable="single", style_table={"marginTop": "1rem"}),
                    dbc.Button("Activate", id="activate-user-button", color="info", className="mt-2 me-2"),
                    dbc.Button("Deactivate", id="deactivate-user-button", color="warning", className="mt-2"),
                    html.Div(id="user-manage-status")
                ], className="p-3")
            ]),
            dbc.Tab(label="Database Management", children=[
                html.Div([
                    html.H5("⚠️ Danger Zone", className="text-danger mt-3"),
                    html.P("The following actions will permanently delete data. Use with caution!"),
                    html.Hr(),
                    html.H6("Delete All Leads Data", className="mt-3"),
                    html.P("This will delete all leads, call logs, and reminders. Users and pitch templates will be preserved."),
                    dbc.Input(id="delete-confirm-key", placeholder="Type 'DELETE ALL DATA' to confirm", type="text", className="mb-2"),
                    dbc.Button("Delete All Leads Data", id="delete-all-data-btn", color="danger", className="me-2"),
                    html.Div(id="delete-data-status", className="mt-2"),
                    html.Hr(),
                    html.H6("Master Reset", className="mt-3"),
                    html.P("This will delete EVERYTHING including all users except the default admin. Use only for complete reset."),
                    dbc.Input(id="master-reset-key-input", placeholder="Enter master reset key", type="password", className="mb-2"),
                    dbc.Button("Master Reset Database", id="master-reset-btn", color="danger"),
                    html.Div(id="master-reset-status", className="mt-2")
                ], className="p-3")
            ])
        ])
    ])

# ---------------- APP INITIALIZATION ----------------

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

app.layout = html.Div([
    dcc.Store(id="current-user", storage_type="session"),
    dcc.Store(id="selected-lead-id"),
    dcc.Store(id="workflow-pitch-store"),
    dbc.Navbar([
        dbc.NavbarBrand("📞 MAISON SIA CRM Dashboard", className="ms-2"),
        dbc.Nav([html.Span(id="navbar-user-label", className="me-3 text-white"), dbc.Button("Logout", id="logout-button", size="sm", color="outline-light")], className="ms-auto", navbar=True),
    ], color="primary", dark=True, className="mb-3"),

    html.Div(id="login-wrapper", children=[
        dbc.Row(dbc.Col(dbc.Card(dbc.CardBody([
            html.Div(id="login-form", children=[
                html.H3("Login", className="text-center"),
                dbc.Label("Username"), dbc.Input(id="login-username", type="text"),
                dbc.Label("Password"), dbc.Input(id="login-password", type="password"),
                dbc.Button("Login", id="login-button", color="primary", className="w-100 mt-3"),
                html.Div(id="login-status", className="mt-2 text-danger text-center"),
                html.Hr(),
                html.Div(dbc.Button("Forgot Password?", id="forgot-password-link", color="link", className="p-0"), className="text-center")
            ]),
            html.Div(id="reset-form", style={"display": "none"}, children=[
                html.H3("Reset Admin Password", className="text-center"),
                html.P(f"Enter the master reset key to reset 'naved' password to 'admin123'.", className="small text-muted"),
                dbc.Label("Master Reset Key"), dbc.Input(id="reset-key", type="password"),
                dbc.Button("Reset Password", id="reset-submit-btn", color="warning", className="w-100 mt-3"),
                html.Div(id="reset-status", className="mt-2 text-center"),
                html.Hr(),
                html.Div(dbc.Button("Back to Login", id="back-to-login-link", color="link", className="p-0"), className="text-center")
            ])
        ])), width=4), justify="center", className="mt-5")
    ]),

    html.Div(id="main-wrapper", style={"display": "none"}, children=[
        dcc.Tabs(id="tabs", value="tab-data", children=[
            dcc.Tab(label="📋 Data View", value="tab-data"),
            dcc.Tab(label="📞 Call Workflow", value="tab-workflow"),
            dcc.Tab(label="📅 Follow Ups", value="tab-followups"),
            dcc.Tab(label="📊 Reports", value="tab-reports"),
            dcc.Tab(label="⚙️ Admin", value="tab-admin"),
        ]),
        html.Div(id="tab-content", className="p-3"),
    ]),
    dcc.Download(id="download-data-view-xlsx"),
])

# ---------------- AUTH CALLBACKS ----------------

@app.callback(
    Output("login-wrapper", "style"),
    Output("main-wrapper", "style"),
    Output("navbar-user-label", "children"),
    Input("current-user", "data")
)
def toggle_view(user):
    if user:
        return {"display": "none"}, {"display": "block"}, f"Welcome, {user['username']}"
    return {"display": "block"}, {"display": "none"}, ""

@app.callback(
    Output("current-user", "data"),
    Output("login-status", "children"),
    Input("login-button", "n_clicks"),
    Input("logout-button", "n_clicks"),
    State("login-username", "value"),
    State("login-password", "value"),
    prevent_initial_call=True
)
def handle_auth(login_n, logout_n, username, password):
    triggered = ctx.triggered_id
    if triggered == "logout-button":
        return None, ""

    if triggered == "login-button":
        if not username or not password:
            return no_update, "Enter username and password."

        try:
            with READ_ENGINE.begin() as conn:
                row = conn.execute(text("SELECT * FROM users WHERE username = :u"), {"u": username}).mappings().fetchone()
                if row and bcrypt.checkpw(password.encode('utf-8'), row["password"].encode('utf-8')):
                    if not row["is_active"]:
                        return no_update, "Account is inactive."
                    return {"username": row["username"], "role": row["role"]}, ""
                else:
                    return no_update, "Invalid credentials."
        except Exception as e:
            return no_update, f"Error: {e}"
    return no_update, no_update

@app.callback(
    Output("login-form", "style"),
    Output("reset-form", "style"),
    Input("forgot-password-link", "n_clicks"),
    Input("back-to-login-link", "n_clicks"),
    prevent_initial_call=True
)
def toggle_reset_form(forgot_n, back_n):
    if ctx.triggered_id == "forgot-password-link":
        return {"display": "none"}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}

@app.callback(
    Output("reset-status", "children"),
    Input("reset-submit-btn", "n_clicks"),
    State("reset-key", "value"),
    prevent_initial_call=True
)
def handle_reset(n, key):
    if key == MASTER_RESET_KEY:
        hashed = bcrypt.hashpw("admin123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        with WRITE_ENGINE.begin() as conn:
            conn.execute(text("UPDATE users SET password = :p WHERE username = 'naved'"), {"p": hashed})
        return "Password for 'naved' reset to 'admin123'."
    return "Invalid Master Key."

# ---------------- TAB CONTENT CALLBACK ----------------

@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("current-user", "data")
)
def render_tab_content(tab, user):
    if not user: return html.Div()
    if tab == "tab-data": return layout_data_view(user)
    if tab == "tab-workflow": return layout_workflow(user)
    if tab == "tab-followups": return layout_followups(user)
    if tab == "tab-reports": return layout_reports()
    if tab == "tab-admin":
        if is_admin(user["username"]): return layout_admin()
        return html.Div("Admin access required.")
    return html.Div("Tab not found.")

# ---------------- DATA VIEW CALLBACKS ----------------

@app.callback(
    Output("leads-table", "data"),
    Output("leads-table", "page_count"),
    Output("leads-table", "page_current"),
    Input("tabs", "value"),
    Input("leads-table", "page_current"),
    Input("leads-table", "page_size"),
    Input("leads-table", "sort_by"),
    Input("search-input", "value"),
    Input("filter-database", "value"),
    Input("filter-user", "value"),
    State("current-user", "data")
)
def update_leads(active_tab, page, size, sort, search, db, user_filter, current_user):
    if not current_user or active_tab != "tab-data":
        return [], 1, 0

    if page is None: page = 0
    where = []
    params = {}

    if db:
        where.append("database_name = :db")
        params["db"] = db
    if user_filter:
        where.append("assigned_user = :u")
        params["u"] = user_filter
    if search:
        s = f"%{search}%"
        where.append("(customer_name LIKE :s OR phone LIKE :s OR location LIKE :s)")
        params["s"] = s

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    with READ_ENGINE.begin() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM leads {where_sql}"), params).scalar()
        order = "ORDER BY id DESC"
        if sort:
            order = f"ORDER BY {sort[0]['column_id']} {'ASC' if sort[0]['direction']=='asc' else 'DESC'}"

        df = pd.read_sql(text(f"SELECT * FROM leads {where_sql} {order} LIMIT :l OFFSET :o"), conn, params={**params, "l": size, "o": page * size})

    df = sanitize_df_for_json(df)
    page_count = max(1, math.ceil(total / size)) if size else 1
    return df.to_dict("records"), page_count, page

@app.callback(Output("selected-lead-id", "data"), Input("leads-table", "selected_rows"), State("leads-table", "data"))
def select_lead(rows, data):
    if not rows or not data: return None
    try: return data[rows[0]]["id"]
    except: return None

@app.callback(
    Output("delete-leads-status", "children"),
    Output("leads-table", "selected_rows"),
    Input("delete-leads-btn", "n_clicks"),
    State("leads-table", "selected_rows"),
    State("leads-table", "data"),
    State("current-user", "data"),
    prevent_initial_call=True
)
def delete_leads(n, rows, data, user):
    if not n:
        return no_update, no_update
    if not rows or not data:
        return "No rows selected.", no_update
    if not is_admin(cu_username(user)):
        return "Admin only.", no_update

    try:
        ids = [data[r]["id"] for r in rows if r < len(data)]
        if not ids:
            return "No valid rows selected.", no_update

        with WRITE_ENGINE.begin() as conn:
            # SQLite doesn't support passing a tuple directly to IN via SQLAlchemy text() easily
            # We construct the placeholders dynamically for the IN clause
            placeholders = ", ".join([f":id{i}" for i in range(len(ids))])
            query = text(f"DELETE FROM leads WHERE id IN ({placeholders})")
            params = {f"id{i}": val for i, val in enumerate(ids)}
            conn.execute(query, params)
        return f"Deleted {len(ids)} rows.", []
    except Exception as e:
        return f"Error: {str(e)}", no_update

# ---------------- WORKFLOW CALLBACKS ----------------

@app.callback(Output("selected-lead-summary", "children"), Input("selected-lead-id", "data"))
def update_summary(lid):
    if not lid: return "No lead selected."
    with READ_ENGINE.begin() as conn: df = pd.read_sql("SELECT * FROM leads WHERE id = :id", conn, params={"id": lid})
    if df.empty: return "Not found."
    r = df.iloc[0]
    return html.Div([html.B(r["customer_name"]), f" ({r['phone']}) - Status: {r['current_status']}"])

@app.callback(Output("workflow-pitch-dropdown", "options"), Input("current-user", "data"))
def load_pitches(user):
    templates = get_all_pitch_templates()
    return [{"label": f"{t['title']} ({t['user_name']})", "value": t["pitch_text"]} for t in templates]

@app.callback(Output("selected-pitch-display", "value"), Input("workflow-pitch-dropdown", "value"))
def show_pitch(p): return p or ""

@app.callback(Output("workflow-status", "children"), Input("save-call-btn", "n_clicks"), State("selected-lead-id", "data"), State("current-user", "data"), State("call-date", "date"), State("remark-dropdown", "value"), State("workflow-pitch-dropdown", "value"), State("assign-to-dropdown", "value"), State("followup-date", "date"), State("call-notes", "value"), prevent_initial_call=True)
def save_call(n, lid, user, cdate, remark, pitch, assign, fdate, notes):
    uname = cu_username(user)
    if not lid or not remark: return "Select lead and remark."
    @with_write_retry
    def _save():
        with WRITE_ENGINE.begin() as conn:
            conn.execute(text("INSERT INTO call_logs (lead_id, user_name, call_date, outcome, pitch_used, notes) VALUES (:lid, :u, :d, :o, :p, :n)"), {"lid": lid, "u": uname, "d": cdate, "o": remark, "p": pitch or "", "n": notes or ""})
            lead = pd.read_sql("SELECT * FROM leads WHERE id = :id", conn, params={"id": lid}).iloc[0]
            no_resp = (lead["no_response_attempts"] or 0) + (1 if remark == "No response" else 0)
            cat_att = (lead["catalogue_attempts"] or 0) + (1 if remark == "Catalogue" else 0)
            active = 0 if remark in ["Purchased", "Not Interested", "Invalid number"] else 1
            if remark == "No response": create_reminders_for_no_response(conn, lid, uname, dt.date.fromisoformat(cdate), no_resp)
            elif remark == "Catalogue": create_reminders_for_catalogue(conn, lid, uname, dt.date.fromisoformat(cdate), cat_att)
            elif fdate: conn.execute(text("INSERT INTO reminders (lead_id, reminder_date, reminder_type, user_name) VALUES (:lid, :rdate, :rtype, :user)"), {"lid": lid, "rdate": fdate, "rtype": remark, "user": uname})
            conn.execute(text("UPDATE leads SET current_status=:s, last_call_date=:d, no_response_attempts=:nr, catalogue_attempts=:ca, is_active=:a, assigned_user=:au WHERE id=:id"), {"s": remark, "d": cdate, "nr": no_resp, "ca": cat_att, "a": active, "au": assign or lead["assigned_user"], "id": lid})
            return "Call saved."
    return _save()

# ---------------- FOLLOWUPS CALLBACKS ----------------

@app.callback(
    Output("followups-table", "data"),
    Output("followup-action-status", "children"),
    Input("mark-reminder-done-btn", "n_clicks"),
    Input("followup-date-filter", "start_date"),
    Input("followup-date-filter", "end_date"),
    Input("show-all-pending-btn", "n_clicks"),
    State("followups-table", "selected_rows"),
    State("followups-table", "data"),
    State("followup-remark", "value"),
    State("current-user", "data"),
    prevent_initial_call=False
)
def update_followups(n_done, start_date, end_date, n_all, rows, data, remark, user):
    uname = cu_username(user)
    if not uname: return [], ""
    triggered = ctx.triggered_id
    status_msg = ""
    if triggered == "mark-reminder-done-btn" and rows:
        rid = data[rows[0]]["id"]
        lid = data[rows[0]]["lead_id"]
        with WRITE_ENGINE.begin() as conn:
            conn.execute(text("UPDATE reminders SET is_done=1 WHERE id=:id"), {"id": rid})
            conn.execute(text("INSERT INTO call_logs (lead_id, user_name, call_date, outcome, notes) VALUES (:lid, :u, :d, 'Follow up', :n)"), {"lid": lid, "u": uname, "d": dt.date.today().isoformat(), "n": remark or ""})
        status_msg = "Marked done."
    query = "SELECT r.id, r.lead_id, r.reminder_date, r.reminder_type, l.customer_name, l.phone FROM reminders r JOIN leads l ON r.lead_id = l.id WHERE r.user_name = :u AND r.is_done = 0"
    params = {"u": uname}
    if triggered != "show-all-pending-btn" and start_date and end_date:
        query += " AND r.reminder_date BETWEEN :start AND :end"
        params["start"] = start_date
        params["end"] = end_date
    with READ_ENGINE.begin() as conn:
        df = pd.read_sql(text(query), conn, params=params)
    return df.to_dict("records"), status_msg

# ---------------- REPORTS CALLBACKS ----------------

@app.callback(
    Output("reports-graphs-container", "children"),
    Input("report-date-filter", "start_date"),
    Input("report-date-filter", "end_date"),
    Input("refresh-reports-btn", "n_clicks"),
    prevent_initial_call=False
)
def update_reports(start_date, end_date, n_clicks):
    try:
        query = "SELECT cl.*, l.database_name FROM call_logs cl LEFT JOIN leads l ON cl.lead_id = l.id"
        params = {}
        if start_date and end_date:
            query += " WHERE cl.call_date BETWEEN :start AND :end"
            params["start"] = start_date + " 00:00:00"
            params["end"] = end_date + " 23:59:59"
        with READ_ENGINE.begin() as conn:
            calls_df = pd.read_sql(text(query), conn, params=params)
        if calls_df.empty: return dbc.Alert("No call logs found.", color="warning")
        calls_df["call_date"] = pd.to_datetime(calls_df["call_date"], errors="coerce")
        trend_df = calls_df.groupby(calls_df["call_date"].dt.date).size().reset_index(name="calls")
        fig_trend = px.line(trend_df, x="call_date", y="calls", title="Calls Trend")
        outcome_df = calls_df["outcome"].value_counts().reset_index()
        fig_pie = px.pie(outcome_df, names="outcome", values="count", title="Outcome Distribution")
        return dbc.Row([dbc.Col(dcc.Graph(figure=fig_trend), width=6), dbc.Col(dcc.Graph(figure=fig_pie), width=6)])
    except Exception as e: return dbc.Alert(f"Error: {e}", color="danger")

# ---------------- ADMIN CALLBACKS ----------------

@app.callback(Output("upload-status", "children"), Input("upload-excel", "contents"), State("upload-excel", "filename"), prevent_initial_call=True)
def handle_upload(contents, filename):
    if not contents: return no_update
    try:
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        df = pd.read_excel(io.BytesIO(decoded))
        insert_or_update_leads_from_df(df)
        return f"Imported {len(df)} rows."
    except Exception as e: return f"Error: {e}"

@app.callback(Output("gsheet-status", "children"), Input("import-gsheet-btn", "n_clicks"), State("google-sheet-url", "value"), prevent_initial_call=True)
def import_gsheet(n, url):
    if not url: return "Enter URL."
    try:
        resp = requests.get(url)
        df = pd.read_csv(io.StringIO(resp.text))
        insert_or_update_leads_from_df(df)
        return f"Imported {len(df)} rows."
    except Exception as e: return f"Error: {e}"

@app.callback(
    Output("templates-table", "data"),
    Output("template-action-status", "children"),
    Input("update-template-btn", "n_clicks"),
    Input("delete-template-btn", "n_clicks"),
    Input("tabs", "value"),
    State("templates-table", "selected_rows"),
    State("templates-table", "data"),
    State("template-edit-title", "value"),
    State("template-edit-text", "value"),
    State("current-user", "data")
)
def manage_templates(u_n, d_n, active_tab, rows, data, title, text_val, user):
    if not user or active_tab != "tab-admin":
        return no_update, no_update

    uname = cu_username(user)
    triggered = ctx.triggered_id
    status = ""

    try:
        with WRITE_ENGINE.begin() as conn:
            if triggered in ["update-template-btn", "delete-template-btn"]:
                tid = data[rows[0]]["id"] if rows and data and rows[0] < len(data) else None
                if triggered == "update-template-btn":
                    if tid:
                        conn.execute(text("UPDATE pitch_templates SET title=:t, pitch_text=:p WHERE id=:id"), {"t": title, "p": text_val, "id": tid})
                        status = "Template updated."
                    else:
                        conn.execute(text("INSERT INTO pitch_templates (user_name, title, pitch_text, created_at) VALUES (:u, :t, :p, :c)"), {"u": uname, "t": title, "p": text_val, "c": dt.datetime.now().isoformat()})
                        status = "Template added."
                elif triggered == "delete-template-btn" and tid:
                    conn.execute(text("DELETE FROM pitch_templates WHERE id=:id"), {"id": tid})
                    status = "Template deleted."

            df = pd.read_sql("SELECT id, title, created_at, pitch_text FROM pitch_templates WHERE user_name = :u", conn, params={"u": uname})
            return df.to_dict("records"), status
    except Exception as e:
        return no_update, f"Error: {str(e)}"

@app.callback(
    Output("users-table", "data"),
    Output("user-manage-status", "children"),
    Input("add-user-button", "n_clicks"),
    Input("activate-user-button", "n_clicks"),
    Input("deactivate-user-button", "n_clicks"),
    Input("tabs", "value"),
    State("new-user-username", "value"),
    State("new-user-password", "value"),
    State("new-user-email", "value"),
    State("users-table", "selected_rows"),
    State("users-table", "data"),
    State("current-user", "data")
)
def manage_users(a, act, de, active_tab, u, p, e, rows, data, admin):
    if not admin or active_tab != "tab-admin" or not is_admin(cu_username(admin)):
        return no_update, no_update

    triggered = ctx.triggered_id
    status = ""

    try:
        with WRITE_ENGINE.begin() as conn:
            if triggered in ["add-user-button", "activate-user-button", "deactivate-user-button"]:
                tid = data[rows[0]]["id"] if rows and data and rows[0] < len(data) else None
                if triggered == "add-user-button" and u and p:
                    hashed = bcrypt.hashpw(p.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                    conn.execute(text("INSERT INTO users (username, password, email, role) VALUES (:u, :p, :e, 'user')"), {"u": u, "p": hashed, "e": e})
                    status = "User added."
                elif tid and triggered in ["activate-user-button", "deactivate-user-button"]:
                    is_active = 1 if triggered == "activate-user-button" else 0
                    conn.execute(text("UPDATE users SET is_active = :a WHERE id = :id"), {"a": is_active, "id": tid})
                    status = "User status updated."

            df = pd.read_sql("SELECT id, username, role, is_active, email FROM users", conn)
            return df.to_dict("records"), status
    except Exception as e:
        return no_update, f"Error: {str(e)}"

@app.callback(
    Output("download-data-view-xlsx", "data"),
    Input("download-data-view-btn", "n_clicks"),
    prevent_initial_call=True
)
def download_data_view(n):
    if not n: return no_update
    with READ_ENGINE.begin() as conn: df = pd.read_sql("SELECT * FROM leads", conn)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer: df.to_excel(writer, index=False, sheet_name="Leads")
    return dcc.send_bytes(out.getvalue(), "leads_export.xlsx")

@app.callback(
    Output("delete-data-status", "children"),
    Input("delete-all-data-btn", "n_clicks"),
    State("delete-confirm-key", "value"),
    State("current-user", "data"),
    prevent_initial_call=True
)
def delete_all_leads_data(n, confirm_key, user):
    if not n or not user:
        return no_update

    uname = cu_username(user)
    if not is_admin(uname):
        return dbc.Alert("Only admins can delete data.", color="danger")

    if confirm_key != "DELETE ALL DATA":
        return dbc.Alert("Please type 'DELETE ALL DATA' exactly to confirm.", color="warning")

    try:
        @with_write_retry
        def _delete():
            with WRITE_ENGINE.begin() as conn:
                # Delete all reminders
                result_reminders = conn.execute(text("DELETE FROM reminders"))
                # Delete all call logs
                result_logs = conn.execute(text("DELETE FROM call_logs"))
                # Delete all leads
                result_leads = conn.execute(text("DELETE FROM leads"))

                return result_leads.rowcount, result_logs.rowcount, result_reminders.rowcount

        leads_deleted, logs_deleted, reminders_deleted = _delete()
        return dbc.Alert(
            f"Successfully deleted: {leads_deleted} leads, {logs_deleted} call logs, {reminders_deleted} reminders. Users and templates preserved.",
            color="success"
        )
    except Exception as e:
        return dbc.Alert(f"Error deleting data: {str(e)}", color="danger")

@app.callback(
    Output("master-reset-status", "children"),
    Input("master-reset-btn", "n_clicks"),
    State("master-reset-key-input", "value"),
    State("current-user", "data"),
    prevent_initial_call=True
)
def master_reset_database(n, reset_key, user):
    if not n or not user:
        return no_update

    uname = cu_username(user)
    if not is_admin(uname):
        return dbc.Alert("Only admins can perform master reset.", color="danger")

    if reset_key != MASTER_RESET_KEY:
        return dbc.Alert("Incorrect master reset key.", color="danger")

    try:
        @with_write_retry
        def _master_reset():
            with WRITE_ENGINE.begin() as conn:
                # Delete all data
                conn.execute(text("DELETE FROM reminders"))
                conn.execute(text("DELETE FROM call_logs"))
                conn.execute(text("DELETE FROM leads"))
                conn.execute(text("DELETE FROM pitch_templates"))
                conn.execute(text("DELETE FROM users"))

                # Recreate default admin
                hashed = bcrypt.hashpw("naved123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                conn.execute(text("INSERT INTO users (username, password, is_active, role) VALUES ('naved', :p, 1, 'admin')"), {"p": hashed})

        _master_reset()
        return dbc.Alert("Master reset complete! All data deleted. Default admin restored (username: naved, password: naved123). Please refresh the page.", color="success")
    except Exception as e:
        return dbc.Alert(f"Error during master reset: {str(e)}", color="danger")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8051)