%%writefile inventilytics.py
"""
EQPIS - Earthly Q Production Intelligence System
Version 1.6 - Formula Cleaning, Duplicate Inventory, & Restock List
"""

import streamlit as st
import pandas as pd
import sqlite3
import os
import random
import string
import time
import zipfile
import threading
from datetime import datetime, timedelta
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
from functools import wraps

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH = "eqpis_database.sqlite"
BATCH_PREFIX = "EQ"
DEFAULT_BATCH_SIZE = 1000  # grams

COLORS = {
    'primary': '#C2185B', 'primary_light': '#F8BBD0', 'primary_pale': '#FCE4EC',
    'accent': '#E91E63', 'bg_beige': '#F5F0E6', 'white': '#FFFFFF',
    'success': '#4CAF50', 'warning': '#FF9800', 'error': '#F44336',
    'gray_bg': '#F5F5F5', 'text_dark': '#333333', 'text_light': '#666666'
}

def apply_css():
    st.markdown(f"""
    <style>
        .stApp {{ background-color: {COLORS['bg_beige']}; }}
        [data-testid="stSidebar"] {{ background-color: {COLORS['white']}; }}
        h1, h2, h3, h4 {{ color: {COLORS['primary']} !important; }}
        .stButton > button {{ background-color: {COLORS['primary']}; color: white; border: none; border-radius: 8px; padding: 10px 24px; font-weight: 600; transition: all 0.3s ease; }}
        .stButton > button:hover {{ background-color: {COLORS['accent']}; transform: translateY(-1px); }}
        .capacity-card {{ background: white; border-radius: 10px; padding: 15px; margin: 8px 0; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
        .can-produce {{ border-left: 4px solid {COLORS['success']}; }}
        .cannot-produce {{ border-left: 4px solid {COLORS['error']}; }}
        .queue-card {{ background: white; border-radius: 10px; padding: 20px; margin: 10px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .active-batch {{ background: linear-gradient(135deg, #E8F5E9, #C8E6C9); border: 2px solid {COLORS['success']}; border-radius: 12px; padding: 20px; margin: 10px 0; }}
        .pending-batch {{ background-color: {COLORS['gray_bg']}; border-radius: 8px; padding: 15px; margin: 10px 0; border: 1px solid #ddd; }}
        .user-info-card {{ background: {COLORS['primary_pale']}; padding: 15px; border-radius: 10px; margin-bottom: 20px; }}
        .role-badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 0.8em; color: white; }}
        .role-owner {{ background-color: {COLORS['primary']}; }}
        .role-manager {{ background-color: {COLORS['accent']}; }}
        .role-worker {{ background-color: #F48FB1; }}
        [data-testid="stMetric"] {{ background: white; border-radius: 8px; padding: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .completion-report {{ background: white; border: 2px solid {COLORS['success']}; border-radius: 12px; padding: 25px; margin: 20px 0; }}
        .stTabs [data-baseweb="tab-list"] {{ gap: 8px; padding: 10px; border-radius: 10px; background: {COLORS['bg_beige']}; }}
        .stTabs [data-baseweb="tab"] {{ background: white; border-radius: 8px; padding: 12px 24px; font-size: 0.95em; white-space: nowrap; border: 1px solid {COLORS['primary_light']}; }}
        .stTabs [aria-selected="true"] {{ background: {COLORS['primary']} !important; color: white !important; }}
        .section-divider {{ border-top: 2px solid {COLORS['primary_light']}; margin: 25px 0; }}
        .info-box {{ background: white; border-left: 4px solid {COLORS['primary']}; padding: 15px; border-radius: 5px; margin: 10px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
        .stExpander {{ background: white; border-radius: 8px; }}
        .fifo-priority {{ background: #E3F2FD; border-left: 4px solid #1976D2; padding: 8px 12px; margin: 4px 0; border-radius: 4px; }}
        .status-ok {{ color: {COLORS['success']}; font-weight: bold; }}
        .status-low {{ color: {COLORS['warning']}; font-weight: bold; }}
        .status-critical {{ color: {COLORS['error']}; font-weight: bold; }}
        .supplier-card {{ background: white; border-radius: 10px; padding: 15px; margin: 8px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.08); border-left: 4px solid {COLORS['primary']}; }}
        .restock-item {{ background: #FFF8E1; border-radius: 8px; padding: 10px; margin: 5px 0; border-left: 4px solid {COLORS['warning']}; }}
        .restock-critical {{ background: #FFEBEE; border-radius: 8px; padding: 10px; margin: 5px 0; border-left: 4px solid {COLORS['error']}; }}
        .duplicate-batch {{ background: #F3E5F5; border-radius: 6px; padding: 8px 12px; margin: 3px 0; border-left: 3px solid #9C27B0; }}
    </style>
    """, unsafe_allow_html=True)

# ============================================================================
# DATABASE MANAGER
# ============================================================================

def retry_on_lock(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
                        continue
                    raise
            return None
        return wrapper
    return decorator

class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.init_db()
        self.migrate_if_needed()

    def connect(self):
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        return conn

    @retry_on_lock(max_retries=3, delay=1)
    def execute_query(self, query, params=None, fetch=False):
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if fetch:
                return [self.dict_from_row(row) for row in cursor.fetchall()]
            else:
                conn.commit()
                return cursor
        finally:
            if conn:
                conn.close()

    @retry_on_lock(max_retries=3, delay=1)
    def execute_one(self, query, params=None):
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            row = cursor.fetchone()
            return self.dict_from_row(row) if row else None
        finally:
            if conn:
                conn.close()

    def dict_from_row(self, row):
        if row is None: return None
        return {key: row[key] for key in row.keys()}

    @staticmethod
    def safe_float(val, default=0):
        if val is None: return default
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, str):
            val = val.strip().replace('%', '').replace(',', '').replace('R', '').replace(' ', '')
            try: return float(val)
            except: return default
        try: return float(val)
        except: return default

    def get_table_columns(self, table_name):
        try:
            result = self.execute_query(f"PRAGMA table_info({table_name})", fetch=True)
            return [col['name'] for col in result] if result else []
        except:
            return []

    def migrate_if_needed(self):
        conn = None
        try:
            conn = self.connect()
            c = conn.cursor()

            # Check formula_ingredients columns
            c.execute("PRAGMA table_info(formula_ingredients)")
            columns = [col[1] for col in c.fetchall()]

            if 'product_id' not in columns:
                try:
                    c.execute("ALTER TABLE formula_ingredients ADD COLUMN product_id INTEGER")
                    conn.commit()
                except:
                    pass

            # Check for new columns in packaging_costs
            c.execute("PRAGMA table_info(packaging_costs)")
            columns = [col[1] for col in c.fetchall()]

            if 'packaging_description' not in columns:
                c.execute("ALTER TABLE packaging_costs ADD COLUMN packaging_description TEXT DEFAULT ''")
            if 'container_cost' not in columns:
                c.execute("ALTER TABLE packaging_costs ADD COLUMN container_cost REAL DEFAULT 0")
            if 'lid_cost' not in columns:
                c.execute("ALTER TABLE packaging_costs ADD COLUMN lid_cost REAL DEFAULT 0")
            if 'label_cost' not in columns:
                c.execute("ALTER TABLE packaging_costs ADD COLUMN label_cost REAL DEFAULT 0")

            # Check for new columns in raw_materials
            c.execute("PRAGMA table_info(raw_materials)")
            columns = [col[1] for col in c.fetchall()]

            if 'batch_number' not in columns:
                c.execute("ALTER TABLE raw_materials ADD COLUMN batch_number TEXT DEFAULT ''")
            if 'expiry_date' not in columns:
                c.execute("ALTER TABLE raw_materials ADD COLUMN expiry_date TEXT DEFAULT ''")

            conn.commit()
        except Exception as e:
            pass
        finally:
            if conn:
                conn.close()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.executescript('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                is_current INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS formula_ingredients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                ingredient_name TEXT NOT NULL,
                percentage REAL NOT NULL,
                unit TEXT DEFAULT 'g'
            );

            CREATE TABLE IF NOT EXISTS raw_materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                supplier TEXT DEFAULT '',
                cost_per_unit REAL DEFAULT 0,
                unit TEXT DEFAULT 'g',
                stock_quantity REAL DEFAULT 0,
                reorder_quantity REAL DEFAULT 0,
                batch_number TEXT DEFAULT '',
                expiry_date TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_name TEXT NOT NULL,
                supplier1_name TEXT DEFAULT '',
                supplier1_price REAL DEFAULT 0,
                supplier1_size TEXT DEFAULT '',
                supplier1_price_per_unit REAL DEFAULT 0,
                link1 TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS packaging_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL,
                packaging_description TEXT DEFAULT '',
                container_cost REAL DEFAULT 0,
                lid_cost REAL DEFAULT 0,
                label_cost REAL DEFAULT 0,
                total_packaging_cost REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS product_cost_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT NOT NULL,
                raw_material_cost_batch REAL DEFAULT 0,
                units_produced INTEGER DEFAULT 0,
                selling_price_unit REAL DEFAULT 0,
                labour_cost_hour REAL DEFAULT 0,
                production_time_hours REAL DEFAULT 0,
                time_per_unit_minutes REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS production_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_number TEXT NOT NULL UNIQUE,
                product_id INTEGER NOT NULL,
                batch_size REAL NOT NULL,
                production_date TEXT,
                start_time TEXT,
                end_time TEXT,
                time_spent REAL DEFAULT 0,
                units_produced INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                status TEXT DEFAULT 'queued',
                completed_by TEXT DEFAULT '',
                total_material_cost REAL DEFAULT 0,
                total_packaging_cost REAL DEFAULT 0,
                total_batch_cost REAL DEFAULT 0,
                has_shortages INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS batch_materials_used (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                ingredient_name TEXT NOT NULL,
                quantity_used REAL NOT NULL,
                unit TEXT DEFAULT 'g',
                batch_number TEXT DEFAULT '',
                expiry_date TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS batch_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                filename TEXT,
                file_data BLOB,
                file_type TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS batch_completion_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                batch_number TEXT,
                product_name TEXT,
                completed_by TEXT,
                time_spent REAL,
                units_produced INTEGER,
                low_stock_alerts TEXT,
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS unmapped_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT,
                column_name TEXT,
                sample_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS data_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT,
                source_column TEXT,
                target_field TEXT,
                target_category TEXT,
                target_product TEXT DEFAULT '',
                manual_value TEXT DEFAULT '',
                mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS business_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT,
                field_name TEXT,
                field_value TEXT,
                field_category TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS production_instructions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL UNIQUE,
                instructions TEXT DEFAULT '',
                safety_notes TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS restock_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_name TEXT NOT NULL,
                quantity_needed REAL,
                estimated_cost REAL,
                status TEXT DEFAULT 'pending',
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ops_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                note_text TEXT,
                note_type TEXT DEFAULT 'general',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS inventory_movements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_material_id INTEGER,
                change_quantity REAL NOT NULL,
                reason TEXT,
                reference TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        conn.commit()
        conn.close()

    def reset_database(self):
        try:
            conn = self.connect()
            conn.close()
        except:
            pass
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except:
                return False
        self.init_db()
        return True

    # ============ BASIC QUERIES ============

    def has_data(self):
        result = self.execute_one("SELECT COUNT(*) as count FROM products WHERE is_current=1")
        return result and result['count'] > 0

    def get_stats(self):
        stats = {'products': 0, 'formula_ingredients': 0, 'raw_materials': 0,
                 'suppliers': 0, 'packaging': 0, 'cost_analysis': 0}
        try:
            result = self.execute_one("SELECT COUNT(*) as count FROM products WHERE is_current=1")
            stats['products'] = result['count'] if result else 0
        except: pass
        try:
            result = self.execute_one("SELECT COUNT(*) as count FROM formula_ingredients")
            stats['formula_ingredients'] = result['count'] if result else 0
        except: pass
        try:
            result = self.execute_one("SELECT COUNT(*) as count FROM raw_materials")
            stats['raw_materials'] = result['count'] if result else 0
        except: pass
        try:
            result = self.execute_one("SELECT COUNT(*) as count FROM suppliers")
            stats['suppliers'] = result['count'] if result else 0
        except: pass
        try:
            result = self.execute_one("SELECT COUNT(*) as count FROM packaging_costs")
            stats['packaging'] = result['count'] if result else 0
        except: pass
        try:
            result = self.execute_one("SELECT COUNT(*) as count FROM product_cost_analysis")
            stats['cost_analysis'] = result['count'] if result else 0
        except: pass
        return stats

    def get_db_status(self):
        stats = self.get_stats()
        return {
            'has_data': self.has_data(),
            'stats': stats,
            'total_units': self.get_total_units_produced(),
            'queued_count': len(self.get_batches(status='queued')),
            'active_count': len(self.get_batches(status='active')),
            'completed_count': len(self.get_batches(status='completed')),
        }

    # ============ PRODUCTS & FORMULAS ============

    def add_product(self, name):
        existing = self.execute_one("SELECT id FROM products WHERE name=?", (name,))
        if existing:
            return existing['id']
        cursor = self.execute_query("INSERT INTO products (name) VALUES (?)", (name,))
        return cursor.lastrowid if cursor else None

    def rename_product(self, old_name, new_name):
        self.execute_query("UPDATE products SET name=? WHERE name=?", (new_name, old_name))
        self.execute_query("UPDATE packaging_costs SET product_name=? WHERE product_name=?", (new_name, old_name))
        self.execute_query("UPDATE product_cost_analysis SET product_name=? WHERE product_name=?", (new_name, old_name))
        self.execute_query("UPDATE business_data SET product_name=? WHERE product_name=?", (new_name, old_name))

    def get_all_products(self):
        return self.execute_query("SELECT * FROM products WHERE is_current=1 ORDER BY name", fetch=True) or []

    def has_formula(self, product_name):
        try:
            result = self.execute_one('''
                SELECT COUNT(*) as count FROM formula_ingredients fi
                JOIN products p ON fi.product_id = p.id
                WHERE p.name=?
            ''', (product_name,))
            return result and result['count'] > 0
        except:
            return False

    def clean_formula_data(self, df):
        """Remove rows with 'total' or 100% from formula data"""
        # Remove rows where ingredient name contains 'total' (case insensitive)
        if 'ingredient_name' in df.columns:
            df = df[~df['ingredient_name'].str.lower().str.contains('total|sum|grand total', na=False)]

        # Remove rows where percentage is 100 or close to 100 (within 1%)
        if 'percentage' in df.columns:
            df = df[~((df['percentage'] >= 99) & (df['percentage'] <= 101))]

        return df

    def save_formula(self, product_name, ingredients_df):
        # Clean the formula data first
        ingredients_df = self.clean_formula_data(ingredients_df)

        if ingredients_df.empty:
            return 0

        pid = self.add_product(product_name)
        self.execute_query("DELETE FROM formula_ingredients WHERE product_id=?", (pid,))
        count = 0
        for _, row in ingredients_df.iterrows():
            ing_name = str(row.get('ingredient_name', row.get('Ingredient', '')))
            if not ing_name or ing_name == 'nan':
                continue
            pct = 0
            for col in ['percentage', '%', 'Percentage']:
                if col in row.index:
                    try:
                        pct = float(row[col])
                        break
                    except:
                        continue
            if ing_name and pct > 0:
                self.execute_query('''
                    INSERT INTO formula_ingredients (product_id, ingredient_name, percentage)
                    VALUES (?,?,?)
                ''', (pid, ing_name, pct))
                count += 1
        return count

    def get_formula(self, product_id):
        try:
            return self.execute_query(
                "SELECT * FROM formula_ingredients WHERE product_id=? ORDER BY id",
                (product_id,), fetch=True
            ) or []
        except:
            return []

    def delete_formula(self, product_id):
        self.execute_query("DELETE FROM formula_ingredients WHERE product_id=?", (product_id,))
        self.execute_query("UPDATE products SET is_current=0 WHERE id=?", (product_id,))

    # ============ INVENTORY (RAW MATERIALS) - SUPPORTS DUPLICATES ============

    def add_material(self, name, unit='g', stock=0, batch_number='', expiry_date='', supplier='', cost_per_unit=0):
        # Allow multiple entries with same name (different batches)
        cursor = self.execute_query('''
            INSERT INTO raw_materials (name, unit, stock_quantity, batch_number, expiry_date, supplier, cost_per_unit)
            VALUES (?,?,?,?,?,?,?)
        ''', (name, unit, float(stock), batch_number, expiry_date, supplier, cost_per_unit))
        return cursor.lastrowid if cursor else None

    def save_materials(self, df):
        for _, row in df.iterrows():
            name = str(row.get('name', row.get('Name', '')))
            if not name or name == 'nan':
                continue
            stock = self.safe_float(row.get('stock_quantity', row.get('Stock', 0)))
            unit = str(row.get('unit', row.get('Unit', 'g')))
            batch_number = str(row.get('batch_number', row.get('Batch', '')))
            expiry_date = str(row.get('expiry_date', row.get('Expiry', '')))
            supplier = str(row.get('supplier', row.get('Supplier', '')))
            cost_per_unit = self.safe_float(row.get('cost_per_unit', row.get('Cost', 0)))

            self.add_material(name, unit, stock, batch_number, expiry_date, supplier, cost_per_unit)

    def get_all_materials(self):
        return self.execute_query("SELECT * FROM raw_materials ORDER BY name, expiry_date ASC", fetch=True) or []

    def get_materials_grouped(self):
        """Get materials grouped by name with multiple batches"""
        materials = self.get_all_materials()
        grouped = {}
        for mat in materials:
            if mat['name'] not in grouped:
                grouped[mat['name']] = []
            grouped[mat['name']].append(mat)
        return grouped

    def update_material(self, mat_id, name=None, stock=None, unit=None, batch_number=None,
                        expiry_date=None, supplier=None, cost_per_unit=None):
        updates = []
        params = []
        if name is not None:
            updates.append("name=?")
            params.append(str(name))
        if stock is not None:
            updates.append("stock_quantity=?")
            params.append(float(stock))
        if unit is not None:
            updates.append("unit=?")
            params.append(str(unit))
        if batch_number is not None:
            updates.append("batch_number=?")
            params.append(str(batch_number))
        if expiry_date is not None:
            updates.append("expiry_date=?")
            params.append(str(expiry_date))
        if supplier is not None:
            updates.append("supplier=?")
            params.append(str(supplier))
        if cost_per_unit is not None:
            updates.append("cost_per_unit=?")
            params.append(float(cost_per_unit))
        if updates:
            params.append(mat_id)
            self.execute_query(f"UPDATE raw_materials SET {', '.join(updates)} WHERE id=?", tuple(params))

    def remove_material(self, mat_id):
        self.execute_query("DELETE FROM raw_materials WHERE id=?", (mat_id,))

    def calculate_reorder(self):
        """Calculate reorder quantities based on formulas"""
        mats = self.execute_query("SELECT DISTINCT name FROM raw_materials", fetch=True) or []
        prods = self.execute_query("SELECT id FROM products WHERE is_current=1", fetch=True) or []

        for mat in mats:
            total = 0
            for p in prods:
                try:
                    ings = self.get_formula(p['id'])
                    for ing in ings:
                        if ing['ingredient_name'].lower() == mat['name'].lower():
                            total += (ing['percentage'] / 100) * DEFAULT_BATCH_SIZE
                except:
                    continue
            # Update all entries with this name
            self.execute_query(
                "UPDATE raw_materials SET reorder_quantity=? WHERE LOWER(name)=LOWER(?)",
                (total, mat['name'])
            )

    def get_materials_with_shortages(self):
        """Get all materials that are low or critical"""
        materials = self.get_all_materials()
        shortages = {'critical': [], 'low': []}

        for mat in materials:
            reorder = mat['reorder_quantity'] or 0
            stock = mat['stock_quantity']

            if stock <= 0:
                shortages['critical'].append(mat)
            elif reorder > 0 and stock < reorder:
                # Check if it's critically low (less than 50% of reorder)
                if stock < reorder * 0.5:
                    shortages['critical'].append(mat)
                else:
                    shortages['low'].append(mat)

        return shortages

    # ============ SUPPLIERS ============

    def save_suppliers(self, df):
        for _, row in df.iterrows():
            ing = str(row.get('ingredient_name', row.get('Ingredient', '')))
            if not ing or ing == 'nan':
                continue
            existing = self.execute_one("SELECT id FROM suppliers WHERE ingredient_name=?", (ing,))
            data = {
                'ingredient_name': ing,
                'supplier1_name': str(row.get('supplier1_name', row.get('Supplier', ''))),
                'supplier1_price': self.safe_float(row.get('supplier1_price', row.get('Price', 0))),
                'supplier1_size': str(row.get('supplier1_size', row.get('Size', ''))),
                'supplier1_price_per_unit': self.safe_float(row.get('supplier1_price_per_unit', row.get('Price/Unit', 0))),
                'link1': str(row.get('link1', row.get('Link', '')))
            }
            if existing:
                set_clause = ', '.join([f"{k}=?" for k in data])
                self.execute_query(f"UPDATE suppliers SET {set_clause} WHERE id=?", list(data.values()) + [existing['id']])
            else:
                cols = ', '.join(data.keys())
                placeholders = ', '.join(['?'] * len(data))
                self.execute_query(f"INSERT INTO suppliers ({cols}) VALUES ({placeholders})", list(data.values()))

    def get_all_suppliers(self):
        return self.execute_query("SELECT * FROM suppliers ORDER BY ingredient_name", fetch=True) or []

    def get_supplier_for_ingredient(self, ingredient_name):
        return self.execute_one(
            "SELECT * FROM suppliers WHERE LOWER(ingredient_name)=LOWER(?)",
            (ingredient_name,)
        )

    # ============ PACKAGING ============

    def save_packaging(self, df):
        for _, row in df.iterrows():
            pn = str(row.get('product_name', ''))
            if not pn or pn == 'nan':
                continue
            existing = self.execute_one("SELECT id FROM packaging_costs WHERE product_name=?", (pn,))

            description = str(row.get('packaging_description', row.get('Description', '')))
            if description == 'nan':
                description = ''

            container_cost = self.safe_float(row.get('container_cost', row.get('Container', 0)))
            lid_cost = self.safe_float(row.get('lid_cost', row.get('Lid', 0)))
            label_cost = self.safe_float(row.get('label_cost', row.get('Label', 0)))
            total_cost = container_cost + lid_cost + label_cost

            if existing:
                self.execute_query('''
                    UPDATE packaging_costs SET
                        packaging_description=?, container_cost=?, lid_cost=?,
                        label_cost=?, total_packaging_cost=?
                    WHERE id=?
                ''', (description, container_cost, lid_cost, label_cost, total_cost, existing['id']))
            else:
                self.execute_query('''
                    INSERT INTO packaging_costs
                    (product_name, packaging_description, container_cost, lid_cost, label_cost, total_packaging_cost)
                    VALUES (?,?,?,?,?,?)
                ''', (pn, description, container_cost, lid_cost, label_cost, total_cost))

    def get_all_packaging_costs(self):
        return self.execute_query("SELECT * FROM packaging_costs ORDER BY product_name", fetch=True) or []

    def get_packaging_cost(self, product_name):
        return self.execute_one("SELECT * FROM packaging_costs WHERE product_name=?", (product_name,))

    # ============ COST ANALYSIS ============

    def save_cost_analysis(self, df):
        for _, row in df.iterrows():
            pn = str(row.get('product_name', ''))
            if not pn or pn == 'nan':
                continue
            fields = {
                'raw_material_cost_batch': self.safe_float(row.get('raw_material_cost_batch', 0)),
                'units_produced': int(self.safe_float(row.get('units_produced', 0))),
                'selling_price_unit': self.safe_float(row.get('selling_price_unit', 0)),
                'labour_cost_hour': self.safe_float(row.get('labour_cost_hour', 0)),
                'production_time_hours': self.safe_float(row.get('production_time_hours', 0)),
                'time_per_unit_minutes': self.safe_float(row.get('time_per_unit_minutes', 0))
            }
            existing = self.execute_one("SELECT id FROM product_cost_analysis WHERE product_name=?", (pn,))
            if existing:
                set_clause = ', '.join([f"{k}=?" for k in fields])
                self.execute_query(
                    f"UPDATE product_cost_analysis SET {set_clause} WHERE id=?",
                    list(fields.values()) + [existing['id']]
                )
            else:
                cols = ', '.join(fields.keys())
                placeholders = ', '.join(['?'] * len(fields))
                self.execute_query(
                    f"INSERT INTO product_cost_analysis (product_name, {cols}) VALUES (?, {placeholders})",
                    [pn] + list(fields.values())
                )

    def get_cost_analysis(self, product_name):
        return self.execute_one("SELECT * FROM product_cost_analysis WHERE product_name=?", (product_name,))

    # ============ PRODUCTION BATCHES ============

    def create_batch(self, product_id, batch_size, mat_cost=0, pkg_cost=0, total_cost=0):
        prod = self.execute_one("SELECT name FROM products WHERE id=?", (product_id,))
        if not prod:
            return None
        acronym = ''.join([w[0].upper() for w in prod['name'].split() if w])[:4] or prod['name'][:4].upper()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        random_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        batch_number = f"{BATCH_PREFIX}-{acronym}-{timestamp}-{random_suffix}"

        cursor = self.execute_query('''
            INSERT INTO production_batches
            (batch_number, product_id, batch_size, production_date, status,
             total_material_cost, total_packaging_cost, total_batch_cost)
            VALUES (?,?,?,?,?,?,?,?)
        ''', (batch_number, product_id, float(batch_size), datetime.now().strftime("%Y-%m-%d"),
              'queued', float(mat_cost), float(pkg_cost), float(total_cost)))
        batch_id = cursor.lastrowid if cursor else None

        formula = self.get_formula(product_id)
        for ing in formula:
            qty = (ing['percentage'] / 100) * float(batch_size)
            self.execute_query('''
                INSERT INTO batch_materials_used (batch_id, ingredient_name, quantity_used, unit)
                VALUES (?,?,?,?)
            ''', (batch_id, ing['ingredient_name'], qty, ing.get('unit', 'g')))

        return batch_number

    def get_batches(self, status=None):
        q = "SELECT pb.*, p.name as product_name FROM production_batches pb JOIN products p ON pb.product_id=p.id"
        if status:
            return self.execute_query(q+" WHERE pb.status=? ORDER BY pb.id DESC", (status,), fetch=True) or []
        return self.execute_query(q+" ORDER BY pb.id DESC", fetch=True) or []

    def get_batch_by_id(self, batch_id):
        return self.execute_one('''
            SELECT pb.*, p.name as product_name
            FROM production_batches pb
            JOIN products p ON pb.product_id=p.id
            WHERE pb.id=?
        ''', (batch_id,))

    def get_batch_materials(self, batch_id):
        return self.execute_query(
            "SELECT * FROM batch_materials_used WHERE batch_id=?", (batch_id,), fetch=True
        ) or []

    def update_batch_materials(self, batch_id, materials_data):
        for item in materials_data:
            self.execute_query('''
                UPDATE batch_materials_used
                SET batch_number=?, expiry_date=?
                WHERE batch_id=? AND ingredient_name=?
            ''', (item.get('batch_number', ''), item.get('expiry_date', ''), batch_id, item['ingredient_name']))

    def start_batch(self, batch_id, started_by=""):
        self.execute_query('''
            UPDATE production_batches
            SET status='active', start_time=?
            WHERE id=?
        ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), batch_id))

    def save_batch_file(self, batch_id, filename, file_data, file_type):
        self.execute_query(
            "INSERT INTO batch_files (batch_id, filename, file_data, file_type) VALUES (?,?,?,?)",
            (batch_id, filename, file_data, file_type)
        )

    def get_batch_files(self, batch_id):
        return self.execute_query(
            "SELECT id, filename, file_type, uploaded_at FROM batch_files WHERE batch_id=?",
            (batch_id,), fetch=True
        ) or []

    def complete_batch(self, batch_id, units, notes="", completed_by=""):
        batch = self.execute_one("SELECT * FROM production_batches WHERE id=?", (batch_id,))
        if not batch or not batch['start_time']:
            return False, {}

        start = datetime.strptime(batch['start_time'], "%Y-%m-%d %H:%M:%S")
        elapsed = (datetime.now() - start).total_seconds() / 3600

        low_stock_alerts = []
        materials_used = self.get_batch_materials(batch_id)

        for mat in materials_used:
            existing_mat = self.execute_one(
                "SELECT id, stock_quantity, reorder_quantity FROM raw_materials WHERE LOWER(name)=LOWER(?) AND stock_quantity > 0 ORDER BY expiry_date ASC",
                (mat['ingredient_name'],)
            )
            if existing_mat:
                new_stock = max(0, existing_mat['stock_quantity'] - mat['quantity_used'])
                self.execute_query(
                    "UPDATE raw_materials SET stock_quantity=? WHERE id=?",
                    (new_stock, existing_mat['id'])
                )
                self.execute_query('''
                    INSERT INTO inventory_movements (raw_material_id, change_quantity, reason, reference)
                    VALUES (?, ?, ?, ?)
                ''', (existing_mat['id'], -mat['quantity_used'], 'production', batch['batch_number']))

                if new_stock < existing_mat['reorder_quantity']:
                    low_stock_alerts.append(
                        f"{mat['ingredient_name']}: {new_stock:.1f}g (below reorder)"
                    )

        self.execute_query('''
            UPDATE production_batches
            SET status='completed', end_time=?, time_spent=?, units_produced=?, notes=?, completed_by=?
            WHERE id=?
        ''', (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), elapsed, int(units), notes, completed_by, batch_id))

        prod_name = self.execute_one("SELECT name FROM products WHERE id=?", (batch['product_id'],))
        report = {
            'batch_number': batch['batch_number'],
            'product_name': prod_name['name'] if prod_name else 'Unknown',
            'completed_by': completed_by,
            'time_spent': round(elapsed, 2),
            'units_produced': int(units),
            'low_stock_alerts': '; '.join(low_stock_alerts) if low_stock_alerts else 'None'
        }

        self.execute_query('''
            INSERT INTO batch_completion_reports
            (batch_id, batch_number, product_name, completed_by, time_spent, units_produced, low_stock_alerts)
            VALUES (?,?,?,?,?,?,?)
        ''', (batch_id, report['batch_number'], report['product_name'], report['completed_by'],
              report['time_spent'], report['units_produced'], report['low_stock_alerts']))

        return True, report

    def get_completion_reports(self):
        return self.execute_query(
            "SELECT * FROM batch_completion_reports ORDER BY completed_at DESC",
            fetch=True
        ) or []

    def get_completion_reports_csv(self):
        reports = self.get_completion_reports()
        if not reports:
            return None
        return pd.DataFrame(reports).to_csv(index=False)

    def get_total_units_produced(self):
        result = self.execute_one(
            "SELECT COALESCE(SUM(units_produced), 0) as total FROM production_batches WHERE status='completed'"
        )
        return result['total'] if result else 0

    def get_production_capacity(self):
        prods = self.get_all_products()
        mats = self.get_all_materials()
        capacity = {'can_produce': [], 'cannot_produce': []}
        for p in prods:
            ings = self.get_formula(p['id'])
            if not ings:
                continue
            can_make = True
            for ing in ings:
                mat = next((m for m in mats if m['name'].lower() == ing['ingredient_name'].lower() and m['stock_quantity'] > 0), None)
                if mat:
                    needed = (ing['percentage'] / 100) * 1000
                    if mat['stock_quantity'] < needed:
                        can_make = False
            if can_make:
                capacity['can_produce'].append({'name': p['name'], 'id': p['id']})
            else:
                capacity['cannot_produce'].append({'name': p['name'], 'id': p['id']})
        return capacity

    def get_insights_data(self):
        batches = self.get_batches()
        completed = [b for b in batches if b['status'] == 'completed']
        ingredient_usage = {}
        for b in completed:
            for m in self.get_batch_materials(b['id']):
                ing = m['ingredient_name']
                if ing not in ingredient_usage:
                    ingredient_usage[ing] = 0
                ingredient_usage[ing] += m['quantity_used']
        product_margins = {}
        for b in completed:
            ca = self.get_cost_analysis(b['product_name'])
            revenue = (b.get('units_produced', 0) * (ca['selling_price_unit'] if ca else 0))
            cost = b.get('total_batch_cost', 0)
            pn = b['product_name']
            if pn not in product_margins:
                product_margins[pn] = {'revenue': 0, 'cost': 0, 'profit': 0, 'units': 0}
            product_margins[pn]['revenue'] += revenue
            product_margins[pn]['cost'] += cost
            product_margins[pn]['profit'] += revenue - cost
            product_margins[pn]['units'] += b.get('units_produced', 0)
        for pn in product_margins:
            if product_margins[pn]['revenue'] > 0:
                product_margins[pn]['margin'] = product_margins[pn]['profit'] / product_margins[pn]['revenue'] * 100
        return {'ingredient_usage': ingredient_usage, 'product_margins': product_margins, 'completed_count': len(completed)}

    # ============ DATA MATCHING ============

    def get_uploaded_sheets(self):
        sheets = set()
        result1 = self.execute_query("SELECT DISTINCT sheet_name FROM unmapped_data", fetch=True) or []
        for r in result1:
            sheets.add(r['sheet_name'])
        result2 = self.execute_query("SELECT DISTINCT sheet_name FROM data_mappings", fetch=True) or []
        for r in result2:
            sheets.add(r['sheet_name'])
        return sorted(list(sheets))

    def get_sheet_columns(self, sheet_name):
        result = self.execute_query(
            "SELECT column_name, sample_data FROM unmapped_data WHERE sheet_name=?",
            (sheet_name,), fetch=True
        ) or []
        return [{'column': r.get('column_name', ''), 'sample': r.get('sample_data', '')} for r in result]

    def save_data_mapping(self, sheet_name, source_column, target_field, target_category, target_product=""):
        existing = self.execute_one(
            "SELECT id FROM data_mappings WHERE sheet_name=? AND source_column=? AND target_field=?",
            (sheet_name, source_column, target_field)
        )
        if existing:
            self.execute_query('''
                UPDATE data_mappings SET target_category=?, target_product=?, mapped_at=? WHERE id=?
            ''', (target_category, target_product, datetime.now(), existing['id']))
        else:
            self.execute_query('''
                INSERT INTO data_mappings (sheet_name, source_column, target_field, target_category, target_product)
                VALUES (?,?,?,?,?)
            ''', (sheet_name, source_column, target_field, target_category, target_product))

    def get_data_mappings(self):
        return self.execute_query("SELECT * FROM data_mappings ORDER BY target_category, target_field", fetch=True) or []

    def save_business_data(self, product_name, field_name, field_value, field_category):
        existing = self.execute_one(
            "SELECT id FROM business_data WHERE product_name=? AND field_name=?",
            (product_name, field_name)
        )
        if existing:
            self.execute_query('''
                UPDATE business_data SET field_value=?, field_category=?, updated_at=? WHERE id=?
            ''', (str(field_value), field_category, datetime.now(), existing['id']))
        else:
            self.execute_query('''
                INSERT INTO business_data (product_name, field_name, field_value, field_category)
                VALUES (?,?,?,?)
            ''', (product_name, field_name, str(field_value), field_category))

    def get_business_data(self, product_name=None):
        if product_name:
            return self.execute_query(
                "SELECT * FROM business_data WHERE product_name=? ORDER BY field_category, field_name",
                (product_name,), fetch=True
            ) or []
        return self.execute_query(
            "SELECT * FROM business_data ORDER BY product_name, field_category, field_name",
            fetch=True
        ) or []

    def apply_mappings_to_data(self, sheet_name):
        mappings = self.get_data_mappings()
        mappings = [m for m in mappings if m['sheet_name'] == sheet_name]
        count = 0
        for mp in mappings:
            data = self.execute_one(
                "SELECT sample_data FROM unmapped_data WHERE sheet_name=? AND column_name=?",
                (sheet_name, mp['source_column'])
            )
            if data:
                values = data['sample_data'].split('|')
                product = mp['target_product'] if mp['target_product'] else 'General'
                for val in values:
                    if val and val != 'nan':
                        self.save_business_data(product, mp['target_field'], val, mp['target_category'])
                        count += 1
        return count

    def save_unmapped_data(self, sheet_name, df, mapped_columns):
        all_cols = df.columns.tolist()
        unmapped = [c for c in all_cols if c not in mapped_columns]
        for col in unmapped:
            sample = '|'.join([str(v) for v in df[col].dropna().head(50).tolist()])
            existing = self.execute_one(
                "SELECT id FROM unmapped_data WHERE sheet_name=? AND column_name=?",
                (sheet_name, col)
            )
            if existing:
                self.execute_query("UPDATE unmapped_data SET sample_data=? WHERE id=?", (sample, existing['id']))
            else:
                self.execute_query(
                    "INSERT INTO unmapped_data (sheet_name, column_name, sample_data) VALUES (?,?,?)",
                    (sheet_name, col, sample)
                )

    def get_unmapped_data(self):
        return self.execute_query("SELECT * FROM unmapped_data ORDER BY created_at DESC", fetch=True) or []

    # ============ OPERATIONS ============

    def save_instructions(self, product_id, instructions, safety=""):
        existing = self.execute_one("SELECT id FROM production_instructions WHERE product_id=?", (product_id,))
        if existing:
            self.execute_query(
                "UPDATE production_instructions SET instructions=?, safety_notes=? WHERE product_id=?",
                (instructions, safety, product_id)
            )
        else:
            self.execute_query(
                "INSERT INTO production_instructions (product_id, instructions, safety_notes) VALUES (?,?,?)",
                (product_id, instructions, safety)
            )

    def get_instructions(self, product_id):
        return self.execute_one("SELECT * FROM production_instructions WHERE product_id=?", (product_id,))

    def create_restock_request(self, ingredient_name, quantity_needed, estimated_cost):
        self.execute_query('''
            INSERT INTO restock_requests (ingredient_name, quantity_needed, estimated_cost)
            VALUES (?,?,?)
        ''', (ingredient_name, quantity_needed, estimated_cost))

    def get_restock_requests(self):
        return self.execute_query("SELECT * FROM restock_requests ORDER BY requested_at DESC", fetch=True) or []

    def save_ops_note(self, note_text, note_type="general"):
        self.execute_query("INSERT INTO ops_notes (note_text, note_type) VALUES (?,?)", (note_text, note_type))

    def get_ops_notes(self):
        return self.execute_query("SELECT * FROM ops_notes ORDER BY created_at DESC LIMIT 20", fetch=True) or []

# ============================================================================
# DATA PARSER
# ============================================================================

class DataParser:
    @staticmethod
    def detect_category(df, sheet_name=""):
        cols = ' '.join([c.lower().strip() for c in df.columns])
        sheet_lower = sheet_name.lower()

        if any(k in cols for k in ['supplier1', 'supplier2', 'price/unit', 'supplier', 'vendor']):
            return 'suppliers'
        if any(k in cols for k in ['container', 'bottle', 'jar', 'packaging', 'label', 'box', 'cap', 'lid']):
            return 'packaging'
        if any(k in cols for k in ['cost of raw materials', 'gross profit', 'selling price', 'labour', 'profit']):
            return 'cost_analysis'

        formula_score = sum(1 for k in ['ingredient', 'percentage', '%'] if k in cols or k in sheet_lower)
        material_score = sum(1 for k in ['stock', 'quantity', 'inventory'] if k in cols or k in sheet_lower)

        if formula_score >= material_score and formula_score > 0:
            return 'formulas'
        elif material_score > 0:
            return 'materials'
        return 'unknown'

    @staticmethod
    def clean_pct(v):
        if pd.isna(v):
            return 0
        if isinstance(v, str):
            v = v.strip().replace('%', '').replace(',', '')
        try:
            n = float(v)
            return n * 100 if 0 < n <= 1 else n
        except:
            return 0

# ============================================================================
# RBAC
# ============================================================================

class RBAC:
    PERMS = {
        'business_owner': {'data': 1, 'ops': 1, 'production': 1, 'insights': 1, 'import': 1,
            'edit_formula': 1, 'view_pct': 1, 'edit_inv': 1, 'add_inv': 1,
            'remove_inv': 1, 'queue': 1, 'start': 1, 'complete': 1,
            'instructions': 1, 'restock': 1, 'download': 1, 'delete': 1},
        'production_manager': {'data': 1, 'ops': 1, 'production': 1, 'insights': 1, 'import': 1,
            'edit_formula': 1, 'view_pct': 1, 'edit_inv': 1, 'add_inv': 0,
            'remove_inv': 0, 'queue': 1, 'start': 1, 'complete': 0,
            'instructions': 0, 'restock': 1, 'download': 0, 'delete': 0},
        'factory_worker': {'data': 0, 'ops': 0, 'production': 1, 'insights': 0, 'import': 0,
            'edit_formula': 0, 'view_pct': 0, 'edit_inv': 1, 'add_inv': 0,
            'remove_inv': 0, 'queue': 0, 'start': 1, 'complete': 1,
            'instructions': 0, 'restock': 0, 'download': 0, 'delete': 0}
    }
    @classmethod
    def can(cls, role, perm):
        return cls.PERMS.get(role, {}).get(perm, 0)

# ============================================================================
# UI - SIDEBAR
# ============================================================================

def render_sidebar(db):
    st.sidebar.markdown(f'<div style="text-align:center;padding:20px;"><h2 style="color:{COLORS["primary"]};">🌿 EQPIS</h2></div>', unsafe_allow_html=True)
    st.sidebar.markdown("---")

    st.sidebar.subheader("🗄️ Database Management")
    if st.sidebar.button("🗑️ Clear All Data", type="secondary", use_container_width=True, key="clear_db_btn"):
        st.session_state.show_clear_confirm = True
    if st.session_state.get('show_clear_confirm', False):
        st.sidebar.warning("⚠️ **WARNING: This will DELETE ALL DATA!**")
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("✅ Yes, Clear Everything", type="primary", use_container_width=True, key="confirm_clear"):
                success = db.reset_database()
                if success:
                    st.session_state.show_clear_confirm = False
                    st.session_state.data_ok = False
                    st.session_state.pending_sheets = []
                    st.session_state.import_results = None
                    st.sidebar.success("✅ Database cleared successfully!")
                    time.sleep(1)
                    st.rerun()
        with col2:
            if st.button("❌ Cancel", use_container_width=True, key="cancel_clear"):
                st.session_state.show_clear_confirm = False
                st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.subheader("👤 Select Role")
    roles = {'business_owner': '💼 Business Owner', 'production_manager': '📋 Production Manager', 'factory_worker': '👷 Factory Worker'}
    if 'role' not in st.session_state:
        st.session_state.role = 'business_owner'
    selected_role = st.sidebar.selectbox("Role:", list(roles.keys()), format_func=lambda x: roles[x],
        index=list(roles.keys()).index(st.session_state.role), key="role_selector")
    st.session_state.role = selected_role
    rc = f"role-{selected_role.split('_')[0]}" if '_' in selected_role else "role-owner"
    st.sidebar.markdown(f'<div class="user-info-card"><p><span class="role-badge {rc}">{roles[selected_role]}</span></p></div>', unsafe_allow_html=True)

    if RBAC.can(selected_role, 'import'):
        st.sidebar.markdown("---")
        st.sidebar.subheader("📥 Data Upload")
        uf = st.sidebar.file_uploader("Excel/CSV/ZIP files", type=['xlsx', 'xls', 'csv', 'zip'],
            accept_multiple_files=True, key="sfu", label_visibility="collapsed")
        if uf and st.sidebar.button("🔍 Process Files", type="primary", use_container_width=True):
            sheets = []
            for f in uf:
                try:
                    if f.name.endswith('.zip'):
                        with zipfile.ZipFile(f) as zf:
                            for fn in zf.namelist():
                                if fn.endswith('.csv'):
                                    df = pd.read_csv(zf.open(fn))
                                    sheets.append({'name': fn.replace('.csv', ''), 'df': df, 'source': f.name})
                                elif fn.endswith(('.xlsx', '.xls')):
                                    df = pd.read_excel(zf.open(fn))
                                    sheets.append({'name': fn, 'df': df, 'source': f.name})
                    elif f.name.endswith('.csv'):
                        df = pd.read_csv(f)
                        sheets.append({'name': f.name.replace('.csv', ''), 'df': df, 'source': f.name})
                    else:
                        for sn in pd.ExcelFile(f).sheet_names:
                            df = pd.read_excel(f, sheet_name=sn)
                            sheets.append({'name': sn, 'df': df, 'source': f.name})
                except Exception as e:
                    st.sidebar.error(f"Error: {f.name}")
            for s in sheets:
                s['category'] = DataParser.detect_category(s['df'], s['name'])
            st.session_state.pending_sheets = sheets
            st.session_state.show_mapping = True
            st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Status")
    stats = db.get_stats()
    c1, c2 = st.sidebar.columns(2)
    with c1:
        st.metric("Products", stats['products'])
        st.metric("Materials", stats['raw_materials'])
    with c2:
        st.metric("Ingredients", stats['formula_ingredients'])
        st.metric("Suppliers", stats['suppliers'])
    db_status = db.get_db_status()
    st.sidebar.markdown("---")
    st.sidebar.caption(f"📦 Total Units: {db_status['total_units']}")
    st.sidebar.caption(f"⏳ Queued: {db_status['queued_count']} | 🔄 Active: {db_status['active_count']}")
    return selected_role

# ============================================================================
# UI - WELCOME & DATA MAPPING
# ============================================================================

def render_welcome_page():
    st.markdown(f"<h1 style='text-align:center;color:{COLORS['primary']};'>🌿 Welcome to EQPIS!</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;'>Select your role and upload Excel/CSV/ZIP files via sidebar to begin.</p>", unsafe_allow_html=True)

def render_mapping(db):
    st.subheader("🔧 Column Mapping")
    if not st.session_state.get('pending_sheets'):
        return
    sti = []
    for i, s in enumerate(st.session_state.pending_sheets):
        df = s['df']
        sheet_name = s['name']
        with st.expander(f"📄 {sheet_name}", expanded=True):
            st.dataframe(df.head(3), use_container_width=True)
            dc = s.get('category', 'unknown')
            co = ['formulas', 'materials', 'packaging', 'cost_analysis', 'suppliers', 'unknown']
            try: di = co.index(dc)
            except: di = 5
            cat = st.selectbox("Category", co, index=di, key=f"cat_{i}")
            ign = st.checkbox(f"☐ Ignore: **{sheet_name}**", key=f"ign_{i}")
            if not ign and cat != 'unknown':
                mp = {}
                cols = df.columns.tolist()
                mapped_cols = []
                if cat == 'formulas':
                    ic = next((c for c in cols if 'ingredient' in c.lower()), cols[0] if cols else None)
                    if ic:
                        mp['ingredient_name'] = st.selectbox("Ingredient", cols, index=cols.index(ic) if ic in cols else 0, key=f"in_{i}")
                        mapped_cols.append(ic)
                    pc = next((c for c in cols if '%' in c.lower() or 'percent' in c.lower()), cols[1] if len(cols) > 1 else (cols[0] if cols else None))
                    if pc:
                        mp['percentage'] = st.selectbox("%", cols, index=cols.index(pc) if pc in cols else 0, key=f"pct_{i}")
                        mapped_cols.append(pc)
                elif cat == 'materials':
                    nc = next((c for c in cols if 'name' in c.lower() or 'material' in c.lower()), cols[0] if cols else None)
                    if nc:
                        mp['name'] = st.selectbox("Name", cols, index=cols.index(nc) if nc in cols else 0, key=f"mn_{i}")
                        mapped_cols.append(nc)
                    sc = next((c for c in cols if 'stock' in c.lower() or 'quantity' in c.lower()), cols[1] if len(cols) > 1 else (cols[0] if cols else None))
                    if sc:
                        mp['stock_quantity'] = st.selectbox("Stock", cols, index=cols.index(sc) if sc in cols else 0, key=f"ms_{i}")
                        mapped_cols.append(sc)
                    bc = next((c for c in cols if 'batch' in c.lower()), None)
                    if bc:
                        mp['batch_number'] = st.selectbox("Batch #", ['None'] + cols, index=cols.index(bc) + 1 if bc in cols else 0, key=f"bn_{i}")
                        if mp['batch_number'] != 'None':
                            mapped_cols.append(bc)
                    ec = next((c for c in cols if 'expiry' in c.lower()), None)
                    if ec:
                        mp['expiry_date'] = st.selectbox("Expiry Date", ['None'] + cols, index=cols.index(ec) + 1 if ec in cols else 0, key=f"ex_{i}")
                        if mp['expiry_date'] != 'None':
                            mapped_cols.append(ec)
                elif cat == 'packaging':
                    pc = next((c for c in cols if 'product' in c.lower()), cols[0] if cols else None)
                    if pc:
                        mp['product_name'] = st.selectbox("Product", cols, index=cols.index(pc) if pc in cols else 0, key=f"pn_{i}")
                        mapped_cols.append(pc)
                    desc = next((c for c in cols if 'description' in c.lower()), None)
                    if desc:
                        mp['packaging_description'] = st.selectbox("Description", ['None'] + cols, index=cols.index(desc) + 1 if desc in cols else 0, key=f"pd_{i}")
                        if mp['packaging_description'] != 'None':
                            mapped_cols.append(desc)
                    for fld, label, keywords in [('container_cost', 'Container', ['container']),
                                                 ('lid_cost', 'Lid/Cap', ['lid', 'cap']),
                                                 ('label_cost', 'Label', ['label'])]:
                        mc = next((c for c in cols if any(k in c.lower() for k in keywords)), None)
                        mp[fld] = st.selectbox(label, ['None'] + cols,
                            index=cols.index(mc) + 1 if mc and mc in cols else 0, key=f"pk_{fld}_{i}")
                        if mc and mp[fld] != 'None':
                            mapped_cols.append(mc)
                elif cat == 'cost_analysis':
                    pc = next((c for c in cols if 'product' in c.lower()), cols[0] if cols else None)
                    if pc:
                        mp['product_name'] = st.selectbox("Product", cols, index=cols.index(pc) if pc in cols else 0, key=f"cn_{i}")
                        mapped_cols.append(pc)
                    for fld, trms in [('raw_material_cost_batch', ['raw material']), ('units_produced', ['units']),
                                     ('selling_price_unit', ['selling price']), ('labour_cost_hour', ['labour'])]:
                        mc = next((c for c in cols if any(t in c.lower() for t in trms)), None)
                        mp[fld] = st.selectbox(fld.replace('_', ' ').title(), ['None'] + cols,
                            index=cols.index(mc) + 1 if mc and mc in cols else 0, key=f"cf_{fld}_{i}")
                        if mc and mp[fld] != 'None':
                            mapped_cols.append(mc)
                elif cat == 'suppliers':
                    st.markdown("**Map Supplier Fields:**")
                    def find_column(cols, keywords, default_idx=0):
                        for keyword in keywords:
                            for i, col in enumerate(cols):
                                if keyword.lower() in col.lower():
                                    return i
                        return default_idx
                    ing_idx = find_column(cols, ['ingredient', 'material', 'name'])
                    mp['ingredient_name'] = st.selectbox("Ingredient Name", cols, index=ing_idx, key=f"sn_ing_{i}")
                    mapped_cols.append(mp['ingredient_name'])
                    sup_idx = find_column(cols, ['supplier', 'vendor'])
                    mp['supplier1_name'] = st.selectbox("Supplier Name", cols, index=sup_idx, key=f"sn_sup_{i}")
                    mapped_cols.append(mp['supplier1_name'])
                    optional_fields = [('supplier1_price', 'Price (R)', ['price']),
                                     ('supplier1_size', 'Package Size', ['size']),
                                     ('supplier1_price_per_unit', 'Price/Unit (R)', ['price per unit', 'unit price']),
                                     ('link1', 'Link/URL', ['link', 'url'])]
                    for fld, label, keywords in optional_fields:
                        idx = find_column(cols, keywords, 0)
                        mp[fld] = st.selectbox(label, ['None'] + cols, index=idx + 1 if idx < len(cols) else 0, key=f"sn_{fld}_{i}")
                        if mp[fld] != 'None':
                            mapped_cols.append(mp[fld])
                if mp:
                    cdf = pd.DataFrame()
                    for k, col in mp.items():
                        if col and col != 'None' and col in df.columns:
                            cdf[k] = df[col]
                    if not cdf.empty:
                        st.caption("Preview:")
                        st.dataframe(cdf.head(5), use_container_width=True)
                db.save_unmapped_data(sheet_name, df, mapped_cols)
                sti.append({'name': sheet_name, 'category': cat, 'data': df, 'mapping': mp, 'ignore': False})
            elif ign:
                sti.append({'name': sheet_name, 'ignore': True})
    if sti and st.button("💾 Save and Import All", type="primary", use_container_width=True):
        ir = []
        for sd in sti:
            if sd.get('ignore'):
                ir.append({'sheet': sd['name'], 'status': 'ignored'})
                continue
            try:
                df = sd['data'].copy()
                cat = sd['category']
                mp = sd['mapping']
                sn = cat.replace('_', ' ').title()
                if cat == 'formulas':
                    pn = sd['name']
                    if db.has_formula(pn):
                        ir.append({'sheet': sd['name'], 'status': 'skipped'})
                        continue
                    cdf = pd.DataFrame()
                    if 'ingredient_name' in mp:
                        cdf['ingredient_name'] = df[mp['ingredient_name']]
                    if 'percentage' in mp:
                        cdf['percentage'] = df[mp['percentage']].apply(DataParser.clean_pct)
                    if not cdf.empty:
                        # Clean the formula data (remove totals and 100%)
                        cdf = db.clean_formula_data(cdf)
                        if not cdf.empty:
                            db.save_formula(pn, cdf)
                            ir.append({'sheet': sd['name'], 'status': 'imported', 'section': sn})
                        else:
                            ir.append({'sheet': sd['name'], 'status': 'skipped', 'reason': 'No valid ingredients after cleaning'})
                elif cat == 'materials':
                    cdf = pd.DataFrame()
                    if 'name' in mp:
                        cdf['name'] = df[mp['name']]
                    if 'stock_quantity' in mp:
                        cdf['stock_quantity'] = pd.to_numeric(df[mp['stock_quantity']], errors='coerce').fillna(0)
                    if 'batch_number' in mp and mp['batch_number'] != 'None':
                        cdf['batch_number'] = df[mp['batch_number']]
                    if 'expiry_date' in mp and mp['expiry_date'] != 'None':
                        cdf['expiry_date'] = df[mp['expiry_date']]
                    if not cdf.empty:
                        db.save_materials(cdf)
                        ir.append({'sheet': sd['name'], 'status': 'imported', 'section': sn})
                elif cat == 'packaging':
                    cdf = pd.DataFrame()
                    if 'product_name' in mp:
                        cdf['product_name'] = df[mp['product_name']]
                    if 'packaging_description' in mp and mp['packaging_description'] != 'None':
                        cdf['packaging_description'] = df[mp['packaging_description']]
                    for f in ['container_cost', 'lid_cost', 'label_cost']:
                        if f in mp and mp[f] != 'None':
                            cdf[f] = pd.to_numeric(df[mp[f]], errors='coerce').fillna(0)
                    if not cdf.empty:
                        db.save_packaging(cdf)
                        ir.append({'sheet': sd['name'], 'status': 'imported', 'section': sn})
                elif cat == 'cost_analysis':
                    cdf = pd.DataFrame()
                    if 'product_name' in mp:
                        cdf['product_name'] = df[mp['product_name']]
                    for f in ['raw_material_cost_batch', 'units_produced', 'selling_price_unit', 'labour_cost_hour']:
                        if f in mp and mp[f] != 'None':
                            cdf[f] = pd.to_numeric(df[mp[f]], errors='coerce').fillna(0)
                    if not cdf.empty:
                        db.save_cost_analysis(cdf)
                        ir.append({'sheet': sd['name'], 'status': 'imported', 'section': sn})
                elif cat == 'suppliers':
                    cdf = pd.DataFrame()
                    for f in ['ingredient_name', 'supplier1_name', 'supplier1_price', 'supplier1_size',
                              'supplier1_price_per_unit', 'link1']:
                        if f in mp and mp[f] != 'None':
                            cdf[f] = df[mp[f]]
                    if not cdf.empty:
                        db.save_suppliers(cdf)
                        ir.append({'sheet': sd['name'], 'status': 'imported', 'section': sn})
            except Exception as e:
                ir.append({'sheet': sd['name'], 'status': 'error', 'reason': str(e)})
        db.calculate_reorder()
        st.session_state.import_results = ir
        st.session_state.show_mapping = False
        st.session_state.pending_sheets = []
        st.session_state.data_ok = True
        st.rerun()

# ============================================================================
# UI - DATA CENTRE
# ============================================================================

def render_data_centre(db):
    st.header("🖥️ Data Centre")
    if st.session_state.get('import_results'):
        with st.expander("📋 Last Import Results", expanded=True):
            for r in st.session_state.import_results:
                if r['status'] == 'imported':
                    st.success(f"✅ **{r['sheet']}** → {r.get('section', 'Imported')}")
                elif r['status'] == 'skipped':
                    st.warning(f"⚠️ **{r['sheet']}**")
                elif r['status'] == 'error':
                    st.error(f"❌ **{r['sheet']}**")
    if st.session_state.get('pending_sheets'):
        render_mapping(db)
    else:
        st.info("📤 Upload files via sidebar, then click 'Process Files'.")

# ============================================================================
# UI - INSIGHTS HUB
# ============================================================================

def render_insights(db):
    st.header("📊 Insights Hub")
    role = st.session_state.get('role', '')
    ins_tab = st.radio("Section:", ["📈 Analytics & Charts", "🔗 Data Matching Hub", "📋 Reports"], horizontal=True, key="ins_tab")
    if ins_tab == "🔗 Data Matching Hub":
        render_data_matching_hub(db)
        return
    if ins_tab == "📋 Reports":
        render_reports_section(db, role)
        return
    render_analytics_dashboard(db, role)

def render_data_matching_hub(db):
    st.subheader("🔗 Data Matching Hub")
    st.caption("Match your uploaded data columns to business metrics.")
    sheets = db.get_uploaded_sheets()
    prods = db.get_all_products()
    if not sheets:
        st.warning("No data uploaded yet.")
        return
    selected_sheet = st.selectbox("📄 Select Uploaded Sheet:", sheets, key="dm_sheet")
    if selected_sheet:
        columns = db.get_sheet_columns(selected_sheet)
        if columns:
            st.markdown("---")
            st.markdown(f"### 📊 Map Columns from '{selected_sheet}'")
            with st.expander("👁️ View Sample Data", expanded=False):
                for col in columns:
                    sample = col.get('sample', '')[:100] if col.get('sample') else 'No data'
                    st.text(f"Column: {col.get('column', 'Unknown')} | Sample: {sample}...")
            st.markdown("---")
            for category, fields in BUSINESS_DATA_FIELDS.items():
                with st.expander(f"📁 {category} ({len(fields)} fields)", expanded=False):
                    existing_mappings = [m for m in db.get_data_mappings() if m['sheet_name'] == selected_sheet and m['target_category'] == category]
                    for field_key, field_label in fields.items():
                        col1, col2, col3 = st.columns([2, 2, 1])
                        with col1:
                            st.markdown(f"**{field_label}**")
                        with col2:
                            existing = next((m for m in existing_mappings if m['target_field'] == field_key), None)
                            current_value = existing['source_column'] if existing else None
                            col_options = ['-- Select Column --'] + [c.get('column', '') for c in columns]
                            idx = col_options.index(current_value) if current_value and current_value in col_options else 0
                            selected_col = st.selectbox("Source", col_options, index=idx,
                                key=f"dm_{selected_sheet}_{category}_{field_key}", label_visibility="collapsed")
                        with col3:
                            manual_val = st.text_input("Manual", value=existing['manual_value'] if existing and existing.get('manual_value') else '',
                                key=f"mv_{selected_sheet}_{category}_{field_key}", label_visibility="collapsed", placeholder="Or type value")
                        if selected_col != '-- Select Column --' or manual_val:
                            target_product = st.selectbox("For Product:", ['General'] + [p['name'] for p in prods],
                                key=f"tp_{selected_sheet}_{category}_{field_key}", label_visibility="collapsed")
                            if st.button(f"💾 Save", key=f"sv_{selected_sheet}_{category}_{field_key}"):
                                if selected_col != '-- Select Column --':
                                    db.save_data_mapping(selected_sheet, selected_col, field_key, category, target_product)
                                if manual_val:
                                    db.save_business_data(target_product if target_product != 'General' else 'General', field_key, manual_val, category)
                                st.success(f"✅ Saved!")
                                st.rerun()
            st.markdown("---")
            if st.button("🚀 Apply All Mappings", type="primary", use_container_width=True):
                count = db.apply_mappings_to_data(selected_sheet)
                st.success(f"✅ Applied {count} data points!")
                st.rerun()
    st.markdown("---")
    st.markdown("### 📋 Current Business Data")
    all_bd = db.get_business_data()
    if all_bd:
        st.dataframe(pd.DataFrame(all_bd), use_container_width=True, hide_index=True)

BUSINESS_DATA_FIELDS = {
    'Product Information': {'product_name': 'Product Name', 'product_category': 'Product Category'},
    'Pricing & Sales': {'selling_price_unit': 'Selling Price', 'wholesale_price': 'Wholesale Price'},
    'Production Costs': {'raw_material_cost_batch': 'Raw Material Cost', 'labour_cost_hour': 'Labour Cost'},
    'Batch Production': {'units_produced': 'Units Produced', 'production_time_hours': 'Production Time'},
    'Inventory': {'stock_quantity': 'Stock Quantity', 'reorder_level': 'Reorder Level'},
    'Supplier Information': {'supplier_name': 'Supplier Name', 'supplier_price': 'Supplier Price'},
}

def render_analytics_dashboard(db, role):
    st.subheader("📈 Analytics Dashboard")
    batches = db.get_batches()
    completed = [b for b in batches if b['status'] == 'completed']
    total_units = db.get_total_units_produced()
    q, a, c = sum(1 for b in batches if b['status'] == 'queued'), sum(1 for b in batches if b['status'] == 'active'), len(completed)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📋 Queued", q); c2.metric("🔄 Active", a); c3.metric("✅ Completed", c); c4.metric("📦 Total Units", total_units)
    if completed:
        batch_df = pd.DataFrame(completed)
        product_units = batch_df.groupby('product_name')['units_produced'].sum().reset_index()
        if not product_units.empty:
            fig = px.bar(product_units, x='product_name', y='units_produced', title='Units Produced by Product',
                color='product_name', color_discrete_sequence=[COLORS['primary'], COLORS['accent']])
            fig.update_layout(plot_bgcolor=COLORS['bg_beige'], paper_bgcolor=COLORS['bg_beige'])
            st.plotly_chart(fig, use_container_width=True)

def render_reports_section(db, role):
    st.subheader("📋 Reports")
    reports = db.get_completion_reports()
    if reports:
        st.markdown(f"### Batch Completion Reports ({len(reports)})")
        st.dataframe(pd.DataFrame(reports), use_container_width=True, hide_index=True)
        if RBAC.can(role, 'download'):
            csv_data = db.get_completion_reports_csv()
            if csv_data:
                st.download_button("📥 Download CSV", csv_data, "completion_reports.csv", "text/csv")

# ============================================================================
# UI - OPERATIONS HUB
# ============================================================================

def render_operations_centre(db):
    st.header("🏭 Operations Hub")
    role = st.session_state.get('role', '')
    if not RBAC.can(role, 'ops'):
        st.error("Access denied")
        return

    op_tab = st.radio("Section:", ["📊 Capacity & Planning", "🧪 Formulas", "🏪 Suppliers & Restock", "📝 Notes"], horizontal=True, key="ops_tab")

    if op_tab == "📊 Capacity & Planning":
        render_capacity_planning(db)
    elif op_tab == "🧪 Formulas":
        render_formula_management(db, role)
    elif op_tab == "🏪 Suppliers & Restock":
        render_supplier_management(db, role)
    elif op_tab == "📝 Notes":
        render_notes_section(db)

def render_capacity_planning(db):
    st.markdown("## 📊 Production Capacity")

    cap = db.get_production_capacity()
    materials = db.get_all_materials()
    products = db.get_all_products()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"### ✅ Can Produce ({len(cap['can_produce'])})")
        if cap['can_produce']:
            for p in cap['can_produce']:
                st.markdown(f'<div class="capacity-card can-produce"><strong>✅ {p["name"]}</strong></div>', unsafe_allow_html=True)
        else:
            st.info("No products can be produced with current inventory.")

    with col2:
        st.markdown(f"### ❌ Cannot Produce ({len(cap['cannot_produce'])})")
        if cap['cannot_produce']:
            for p in cap['cannot_produce']:
                st.markdown(f'<div class="capacity-card cannot-produce"><strong>❌ {p["name"]}</strong></div>', unsafe_allow_html=True)
        else:
            st.success("✅ All products can be produced!")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # Shortages Display - Critical and Low
    st.markdown("## 📦 Restock List")
    shortages = db.get_materials_with_shortages()

    # Display Critical Items
    st.markdown("### 🔴 Critical (Out of Stock or Very Low)")
    if shortages['critical']:
        for mat in shortages['critical']:
            reorder = mat['reorder_quantity'] or 0
            needed = reorder - mat['stock_quantity']
            st.markdown(f'''
                <div class="restock-critical">
                    <strong>⚠️ {mat['name']}</strong><br>
                    Stock: {mat['stock_quantity']:.1f}g | Reorder Level: {reorder:.1f}g |
                    <span style="color:{COLORS['error']};">Need: {needed:.1f}g</span>
                    <br><small>Batch: {mat.get('batch_number', 'N/A')} | Expiry: {mat.get('expiry_date', 'N/A')}</small>
                </div>
            ''', unsafe_allow_html=True)
    else:
        st.success("✅ No critical shortages!")

    # Display Low Items
    st.markdown("### 🟡 Low (Below Reorder Level)")
    if shortages['low']:
        for mat in shortages['low']:
            reorder = mat['reorder_quantity'] or 0
            needed = reorder - mat['stock_quantity']
            st.markdown(f'''
                <div class="restock-item">
                    <strong>⚠️ {mat['name']}</strong><br>
                    Stock: {mat['stock_quantity']:.1f}g | Reorder Level: {reorder:.1f}g |
                    <span style="color:{COLORS['warning']};">Need: {needed:.1f}g</span>
                    <br><small>Batch: {mat.get('batch_number', 'N/A')} | Expiry: {mat.get('expiry_date', 'N/A')}</small>
                </div>
            ''', unsafe_allow_html=True)
    else:
        st.success("✅ All materials above reorder levels!")

    # Bulk Restock Button
    total_critical = len(shortages['critical'])
    total_low = len(shortages['low'])
    if total_critical > 0 or total_low > 0:
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📤 Create Restock Requests for All Shortages", type="primary", use_container_width=True):
                count = 0
                all_shortages = shortages['critical'] + shortages['low']
                for mat in all_shortages:
                    reorder = mat['reorder_quantity'] or 0
                    needed = max(0, reorder - mat['stock_quantity'])
                    if needed > 0:
                        # Estimate cost (use cost_per_unit if available)
                        est_cost = needed * (mat.get('cost_per_unit', 0) or 0)
                        db.create_restock_request(mat['name'], needed, est_cost)
                        count += 1
                st.success(f"✅ Created {count} restock requests!")
                st.rerun()
        with col2:
            st.caption(f"Total: {total_critical + total_low} items need restocking")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    st.markdown("## 📋 Production Planning")

    if products and materials:
        pd_data = []
        for p in products:
            ings = db.get_formula(p['id'])
            if not ings:
                continue
            mu = float('inf')
            for i in ings:
                # Sum all stock for this ingredient across batches
                total_stock = sum(m['stock_quantity'] for m in materials if m['name'].lower() == i['ingredient_name'].lower())
                if total_stock > 0:
                    nd = (i['percentage'] / 100) * 100
                    if nd > 0:
                        mu = min(mu, total_stock / nd)
            mu = int(mu) if mu != float('inf') else 0
            pd_data.append({
                'Product': p['name'],
                'Product ID': p['id'],
                'Batch Size (units)': min(mu, 50),
                'Max Possible': mu
            })

        if pd_data:
            pdf = pd.DataFrame(pd_data)
            edf = st.data_editor(
                pdf,
                column_config={
                    'Product': st.column_config.TextColumn(disabled=True),
                    'Product ID': st.column_config.NumberColumn(disabled=True),
                    'Batch Size (units)': st.column_config.NumberColumn(min_value=0, step=1),
                    'Max Possible': st.column_config.NumberColumn(disabled=True)
                },
                hide_index=True,
                use_container_width=True,
                key="ops_pp"
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔍 Check Availability", type="primary", use_container_width=True):
                    for _, row in edf.iterrows():
                        if row['Batch Size (units)'] > 0:
                            ings = db.get_formula(row['Product ID'])
                            bs = row['Batch Size (units)'] * 100
                            st.markdown(f"**{row['Product']}** - {row['Batch Size (units)']} units ({bs}g)")
                            cd = []
                            all_available = True
                            for i in ings:
                                qn = (i['percentage'] / 100) * bs
                                # Sum total stock across all batches
                                total_stock = sum(m['stock_quantity'] for m in materials if m['name'].lower() == i['ingredient_name'].lower())
                                available = total_stock >= qn
                                if not available:
                                    all_available = False
                                cd.append({
                                    'Ingredient': i['ingredient_name'],
                                    'Needed (g)': f"{qn:.1f}",
                                    'Available': f"{total_stock:.1f}",
                                    'Status': '✅' if available else '❌'
                                })
                            st.dataframe(pd.DataFrame(cd), use_container_width=True, hide_index=True)
                            if all_available:
                                st.success("✅ All ingredients available!")
                            else:
                                st.error("❌ Some ingredients are short. Please restock.")

            with col2:
                if st.button("📋 Queue for Production", type="primary", use_container_width=True):
                    count = 0
                    for _, row in edf.iterrows():
                        if row['Batch Size (units)'] > 0:
                            bn = db.create_batch(row['Product ID'], row['Batch Size (units)'] * 100)
                            if bn:
                                count += 1
                                st.success(f"✅ Queued: {row['Product']} ({bn})")
                    if count > 0:
                        st.success(f"### {count} batches sent to production!")
                        st.rerun()
    else:
        st.info("Import products and materials first.")

def render_formula_management(db, role):
    st.subheader("🧪 Formula Management")

    prods = db.get_all_products()
    if not prods:
        st.info("No products. Import formulas first.")
        return

    can_edit = RBAC.can(role, 'edit_formula')
    can_view_pct = RBAC.can(role, 'view_pct')
    is_owner = role == 'business_owner'

    for p in prods:
        ings = db.get_formula(p['id'])
        if not ings:
            continue

        with st.expander(f"📝 {p['name']} ({len(ings)} ingredients)", expanded=False):
            if is_owner:
                new_name = st.text_input("Formula Name:", value=p['name'], key=f"rn_{p['id']}")
                if new_name != p['name'] and st.button("✏️ Rename", key=f"rename_{p['id']}"):
                    db.rename_product(p['name'], new_name)
                    st.success("✅ Renamed!")
                    st.rerun()

            instructions = db.get_instructions(p['id'])
            if instructions:
                with st.expander("📋 Production Instructions", expanded=False):
                    st.text_area("Instructions", value=instructions.get('instructions', ''), height=100, key=f"inst_display_{p['id']}", disabled=True)
                    if instructions.get('safety_notes'):
                        st.warning(f"⚠️ Safety: {instructions['safety_notes']}")

            if can_view_pct:
                fd = [{'Ingredient': i['ingredient_name'], '%': i['percentage']} for i in ings]
                fdf = pd.DataFrame(fd)

                if can_edit:
                    edf = st.data_editor(
                        fdf,
                        column_config={
                            'Ingredient': st.column_config.TextColumn('Ingredient'),
                            '%': st.column_config.NumberColumn('%', min_value=0.0, max_value=100.0, step=0.1, format="%.1f%%")
                        },
                        hide_index=True,
                        use_container_width=True,
                        key=f"fe_ops_{p['id']}"
                    )
                    total = edf['%'].sum()
                    color = "green" if 95 <= total <= 105 else "red"
                    st.markdown(f"**Total:** <span style='color:{color}'>{total:.1f}%</span>", unsafe_allow_html=True)

                    c1, c2, c3 = st.columns(3)
                    if c1.button("💾 Save", key=f"sf_ops_{p['id']}"):
                        if 95 <= total <= 105:
                            # Clean the data before saving
                            cleaned_df = db.clean_formula_data(edf)
                            if not cleaned_df.empty:
                                db.save_formula(p['name'], cleaned_df)
                                st.success("✅ Saved! (Removed total rows and 100% entries)")
                                st.rerun()
                            else:
                                st.error("⚠️ No valid ingredients after cleaning. Please check your data.")
                        else:
                            st.error(f"⚠️ Percentages must sum to ~100% (currently {total:.1f}%)")

                    if c2.button("📝 Instructions", key=f"inst_{p['id']}"):
                        st.session_state[f"show_inst_{p['id']}"] = True

                    if c3.button("🗑️ Delete", key=f"df_ops_{p['id']}", type="secondary") and is_owner:
                        db.delete_formula(p['id'])
                        st.success("Deleted!")
                        st.rerun()

                    if st.session_state.get(f"show_inst_{p['id']}", False):
                        with st.expander("✏️ Edit Instructions", expanded=True):
                            with st.form(f"inst_form_{p['id']}"):
                                inst_text = st.text_area("Instructions", value=instructions.get('instructions', '') if instructions else '', height=100)
                                safety_text = st.text_area("Safety Notes", value=instructions.get('safety_notes', '') if instructions else '', height=50)
                                if st.form_submit_button("💾 Save Instructions"):
                                    db.save_instructions(p['id'], inst_text, safety_text)
                                    st.success("✅ Instructions saved!")
                                    st.session_state[f"show_inst_{p['id']}"] = False
                                    st.rerun()
                else:
                    st.dataframe(fdf, use_container_width=True, hide_index=True)
            else:
                st.dataframe(pd.DataFrame([{'Ingredient': i['ingredient_name']} for i in ings]),
                           use_container_width=True, hide_index=True)

def render_supplier_management(db, role):
    st.markdown("## 🏪 Supplier Directory")

    sups = db.get_all_suppliers()
    mats = db.get_all_materials()

    with st.expander("➕ Add New Supplier", expanded=False):
        with st.form("add_supplier_form"):
            col1, col2 = st.columns(2)
            with col1:
                ingredient = st.selectbox("Ingredient/Material", sorted(set([m['name'] for m in mats])) if mats else [])
                supplier_name = st.text_input("Supplier Name")
                price = st.number_input("Price (R)", min_value=0.0, step=0.01)
            with col2:
                package_size = st.text_input("Package Size")
                price_per_unit = st.number_input("Price per Unit (R)", min_value=0.0, step=0.01)
                link = st.text_input("Supplier Link/URL")

            if st.form_submit_button("Add Supplier", type="primary"):
                if ingredient and supplier_name:
                    df = pd.DataFrame([{
                        'ingredient_name': ingredient,
                        'supplier1_name': supplier_name,
                        'supplier1_price': price,
                        'supplier1_size': package_size,
                        'supplier1_price_per_unit': price_per_unit,
                        'link1': link
                    }])
                    db.save_suppliers(df)
                    st.success(f"✅ Added supplier for {ingredient}")
                    st.rerun()

    if sups:
        sup_data = []
        for s in sups:
            # Get all batches for this ingredient
            ingredient_mats = [m for m in mats if m['name'].lower() == s['ingredient_name'].lower()]
            total_stock = sum(m['stock_quantity'] for m in ingredient_mats)
            batch_info = ", ".join([f"{m.get('batch_number', 'N/A')} ({m['stock_quantity']:.1f}g)" for m in ingredient_mats[:3]])
            if len(ingredient_mats) > 3:
                batch_info += f" +{len(ingredient_mats)-3} more"

            sup_data.append({
                'Ingredient': s['ingredient_name'],
                'Supplier': s.get('supplier1_name', ''),
                'Price': f"R{s.get('supplier1_price', 0):.2f}",
                'Size': s.get('supplier1_size', ''),
                'Price/Unit': f"R{s.get('supplier1_price_per_unit', 0):.4f}",
                'Total Stock': f"{total_stock:.1f}g",
                'Batches': batch_info,
                'Link': s.get('link1', '')
            })
        st.dataframe(pd.DataFrame(sup_data), use_container_width=True, hide_index=True)
    else:
        st.info("No suppliers added yet.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    st.markdown("## 📦 Restock Calculator")

    if RBAC.can(role, 'restock'):
        prods = db.get_all_products()
        if prods:
            sp = st.selectbox("Product:", [p['name'] for p in prods], key="rp")
            du = st.number_input("Desired Units:", min_value=1, value=10, step=1)

            if st.button("🧮 Calculate Restock", type="primary"):
                prod = next((p for p in prods if p['name'] == sp), None)
                if prod:
                    ings = db.get_formula(prod['id'])
                    bs = du * 100
                    st.markdown(f"### Restock for {sp} ({du} units / {bs}g)")

                    rd = []
                    total_cost = 0
                    for i in ings:
                        qn = (i['percentage'] / 100) * bs
                        # Sum total stock across all batches
                        mat_matches = [m for m in mats if m['name'].lower() == i['ingredient_name'].lower()]
                        total_stock = sum(m['stock_quantity'] for m in mat_matches) if mat_matches else 0
                        sup = db.get_supplier_for_ingredient(i['ingredient_name'])

                        needed = max(0, qn - total_stock)
                        cpu = sup.get('supplier1_price_per_unit', 0) if sup else 0
                        est_cost = needed * cpu if cpu else 0
                        total_cost += est_cost

                        rd.append({
                            'Ingredient': i['ingredient_name'],
                            'Needed (g)': f"{qn:.1f}",
                            'In Stock': f"{total_stock:.1f}",
                            'To Order': f"{needed:.1f}",
                            'Est. Cost': f"R{est_cost:.2f}",
                            'Supplier': sup.get('supplier1_name', 'N/A') if sup else 'N/A'
                        })

                    st.dataframe(pd.DataFrame(rd), use_container_width=True, hide_index=True)
                    st.metric("Total Estimated Cost", f"R{total_cost:.2f}")

                    if st.button("📤 Create Restock Requests", type="primary"):
                        for r in rd:
                            if float(r['To Order']) > 0:
                                db.create_restock_request(
                                    r['Ingredient'],
                                    float(r['To Order']),
                                    float(r['Est. Cost'].replace('R', ''))
                                )
                        st.success("✅ Restock requests created!")
                        st.rerun()
        else:
            st.info("No products available.")

def render_notes_section(db):
    st.markdown("## 📝 Production Notes")

    with st.form("ops_note_form"):
        nt = st.text_area("Add Note:", height=150, placeholder="Enter your production notes here...")
        nty = st.selectbox("Note Type:", ["general", "production", "inventory", "quality", "maintenance"])
        if st.form_submit_button("💾 Save Note", type="primary") and nt:
            db.save_ops_note(nt, nty)
            st.success("✅ Note saved!")
            st.rerun()

    st.markdown("---")
    st.subheader("📜 Recent Notes")
    notes = db.get_ops_notes()
    if notes:
        for n in notes:
            nd = n.get('created_at', '')
            try:
                nd = pd.to_datetime(nd).strftime('%Y-%m-%d %H:%M') if nd else ''
            except:
                pass
            note_type = n.get('note_type', 'general').title()
            st.markdown(f'''
                <div style="background:white;border-left:4px solid {COLORS['primary']};padding:15px;margin:8px 0;border-radius:5px;box-shadow:0 2px 4px rgba(0,0,0,0.05);">
                    <small><strong>{note_type}</strong> | {nd}</small>
                    <p style="margin-top:5px;">{n.get('note_text', '')}</p>
                </div>
            ''', unsafe_allow_html=True)
    else:
        st.info("No notes yet. Add your first production note above.")

# ============================================================================
# UI - PRODUCTION HUB
# ============================================================================

def render_production_centre(db):
    st.header("🔧 Production Hub")
    role = st.session_state.get('role', '')
    user_name = {'business_owner': 'Business Owner', 'production_manager': 'Production Manager', 'factory_worker': 'Factory Worker'}.get(role, 'Worker')

    prod_tab = st.radio("Section:", ["📋 Production Line", "📦 Inventory Management"], horizontal=True, key="prod_tab")

    if prod_tab == "📦 Inventory Management":
        render_inventory_management(db, role)
        return

    render_production_line(db, role, user_name)

def render_inventory_management(db, role):
    st.subheader("📦 Inventory Management")
    st.caption("Multiple batches of the same ingredient can exist with different batch numbers and expiry dates.")

    can_add = RBAC.can(role, 'add_inv')
    can_edit = RBAC.can(role, 'edit_inv')
    can_remove = RBAC.can(role, 'remove_inv')

    if can_add:
        with st.expander("➕ Add New Material Batch", expanded=False):
            with st.form("add_material_form"):
                st.markdown("**Add a new batch of an existing or new ingredient**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    name = st.text_input("Material Name*")
                with col2:
                    unit = st.selectbox("Unit", ['g', 'ml', 'kg', 'l', 'piece', 'bottle'])
                with col3:
                    stock = st.number_input("Stock Quantity", min_value=0.0, step=0.1)

                col1, col2, col3 = st.columns(3)
                with col1:
                    batch_number = st.text_input("Batch Number", placeholder="Optional - auto-generate if empty")
                with col2:
                    expiry_date = st.date_input("Expiry Date", value=datetime.now() + timedelta(days=365))
                with col3:
                    cost_per_unit = st.number_input("Cost per Unit", min_value=0.0, step=0.01)

                if st.form_submit_button("Add Material Batch", type="primary"):
                    if name:
                        batch = batch_number or f"{name[:3].upper()}-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000,9999)}"
                        db.add_material(
                            name, unit, stock, batch,
                            expiry_date.strftime('%Y-%m-%d'), '', cost_per_unit
                        )
                        st.success(f"✅ Added {name} batch: {batch}")
                        st.rerun()

    # Display materials grouped by name
    grouped_materials = db.get_materials_grouped()

    if not grouped_materials:
        st.info("No materials in inventory. Add your first material above.")
        return

    # Search
    search = st.text_input("🔍 Search materials...", placeholder="Type material name")
    if search:
        grouped_materials = {k: v for k, v in grouped_materials.items() if search.lower() in k.lower()}

    for mat_name, batches in grouped_materials.items():
        # Calculate total stock
        total_stock = sum(b['stock_quantity'] for b in batches)
        reorder = batches[0]['reorder_quantity'] if batches else 0

        if total_stock <= 0:
            status = "🔴 CRITICAL"
            status_color = COLORS['error']
        elif reorder > 0 and total_stock < reorder * 0.5:
            status = "🔴 CRITICAL"
            status_color = COLORS['error']
        elif reorder > 0 and total_stock < reorder:
            status = "🟡 LOW"
            status_color = COLORS['warning']
        else:
            status = "✅ OK"
            status_color = COLORS['success']

        with st.expander(
            f"📦 **{mat_name}** (Total: {total_stock:.1f} {batches[0]['unit']} | {len(batches)} batches | {status})",
            expanded=False
        ):
            # Show each batch
            for idx, mat in enumerate(batches):
                batch_status = "✅" if mat['stock_quantity'] > 0 else "❌"
                st.markdown(f'''
                    <div class="duplicate-batch">
                        <strong>Batch #{idx+1}:</strong> {batch_status}
                        <strong>Stock:</strong> {mat['stock_quantity']:.1f} {mat['unit']} |
                        <strong>Batch #:</strong> {mat.get('batch_number', 'N/A')} |
                        <strong>Expiry:</strong> {mat.get('expiry_date', 'N/A')} |
                        <strong>Cost/Unit:</strong> R{mat.get('cost_per_unit', 0):.2f}
                        {f" | <strong>Supplier:</strong> {mat.get('supplier', 'N/A')}" if mat.get('supplier') else ''}
                    </div>
                ''', unsafe_allow_html=True)

                # Edit form for each batch
                if can_edit:
                    with st.form(key=f"edit_batch_{mat['id']}"):
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            new_stock = st.number_input("Stock", value=mat['stock_quantity'], min_value=0.0, step=0.1, key=f"stock_{mat['id']}")
                        with col2:
                            new_batch = st.text_input("Batch #", value=mat.get('batch_number', ''), key=f"batch_{mat['id']}")
                        with col3:
                            new_expiry = st.text_input("Expiry Date", value=mat.get('expiry_date', ''), key=f"expiry_{mat['id']}")
                        with col4:
                            new_cost = st.number_input("Cost/Unit", value=mat.get('cost_per_unit', 0), min_value=0.0, step=0.01, key=f"cost_{mat['id']}")

                        col1, col2 = st.columns(2)
                        with col1:
                            if st.form_submit_button("💾 Update Batch", type="primary"):
                                db.update_material(
                                    mat['id'],
                                    stock=new_stock,
                                    batch_number=new_batch,
                                    expiry_date=new_expiry,
                                    cost_per_unit=new_cost
                                )
                                st.success("✅ Batch updated!")
                                st.rerun()
                        with col2:
                            if can_remove and st.form_submit_button("🗑️ Delete Batch", type="secondary"):
                                db.remove_material(mat['id'])
                                st.success("Batch deleted!")
                                st.rerun()

            # Add another batch to this ingredient
            if can_add:
                with st.expander(f"➕ Add Another Batch of {mat_name}", expanded=False):
                    with st.form(key=f"add_batch_{mat_name}"):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            new_batch = st.text_input("Batch Number", placeholder="Auto-generate if empty", key=f"ab_{mat_name}")
                        with col2:
                            new_stock = st.number_input("Stock", min_value=0.0, step=0.1, key=f"as_{mat_name}")
                        with col3:
                            new_expiry = st.date_input("Expiry Date", value=datetime.now() + timedelta(days=365), key=f"ae_{mat_name}")

                        if st.form_submit_button("Add Batch", type="primary"):
                            batch = new_batch or f"{mat_name[:3].upper()}-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000,9999)}"
                            db.add_material(
                                mat_name,
                                batches[0]['unit'],
                                new_stock,
                                batch,
                                new_expiry.strftime('%Y-%m-%d'),
                                '',
                                batches[0].get('cost_per_unit', 0)
                            )
                            st.success(f"✅ Added batch: {batch}")
                            st.rerun()

def render_production_line(db, role, user_name):
    st.subheader("🏗️ Production Line")

    queued = db.get_batches(status='queued')
    active = db.get_batches(status='active')

    if not queued and not active:
        st.info("No batches in queue. Go to Operations Hub → Capacity & Planning to queue batches.")
        return

    if queued:
        st.markdown(f"### ⏳ Queued ({len(queued)})")
        for batch in queued:
            with st.container():
                st.markdown(f'''
                    <div class="pending-batch">
                        <h4>📦 {batch['product_name']}</h4>
                        <p><strong>Batch #:</strong> {batch['batch_number']}</p>
                        <p><strong>Size:</strong> {batch['batch_size']}g | <strong>Status:</strong> ⏳ Queued</p>
                    </div>
                ''', unsafe_allow_html=True)

                if RBAC.can(role, 'start'):
                    if st.button(f"▶️ Start Production", key=f"start_{batch['id']}"):
                        st.session_state[f"fifo_selection_{batch['id']}"] = True

                if st.session_state.get(f"fifo_selection_{batch['id']}", False):
                    render_fifo_confirmation(db, batch, role)

    if active:
        st.markdown(f"### 🔄 Active ({len(active)})")
        for batch in active:
            timer_text = "N/A"
            if batch['start_time']:
                try:
                    st_time = datetime.strptime(batch['start_time'], "%Y-%m-%d %H:%M:%S")
                    elapsed = datetime.now() - st_time
                    h, rem = divmod(int(elapsed.total_seconds()), 3600)
                    m, s = divmod(rem, 60)
                    timer_text = f"{h:02d}:{m:02d}:{s:02d}"
                except:
                    pass

            with st.container():
                st.markdown(f'''
                    <div class="active-batch">
                        <h4>🔄 {batch['product_name']}</h4>
                        <p><strong>Batch #:</strong> {batch['batch_number']}</p>
                        <p><strong>Started:</strong> {batch['start_time']}</p>
                        <p><strong>⏱️ Timer:</strong> {timer_text}</p>
                    </div>
                ''', unsafe_allow_html=True)

                if RBAC.can(role, 'complete'):
                    col1, col2 = st.columns([3, 1])
                    with col2:
                        if st.button("⏹️ Complete Batch", key=f"complete_{batch['id']}"):
                            st.session_state[f"complete_form_{batch['id']}"] = True

                if st.session_state.get(f"complete_form_{batch['id']}", False):
                    render_completion_form(db, batch, user_name)

def render_fifo_confirmation(db, batch, role):
    st.markdown(f"### 🔍 FIFO Ingredient Selection for {batch['product_name']}")

    formula = db.get_formula(batch['product_id'])
    if not formula:
        st.error("No formula found for this product")
        return

    st.markdown('''
        <div class="info-box">
            <strong>📋 FIFO (First Expiry, First Out) Allocation</strong><br>
            Ingredients with the earliest expiry dates will be used first across all batches.
        </div>
    ''', unsafe_allow_html=True)

    allocation_data = []
    all_valid = True

    for ingredient in formula:
        required_quantity = (ingredient['percentage'] / 100) * batch['batch_size']

        # Get all batches for this ingredient with stock > 0, ordered by expiry
        materials = db.execute_query('''
            SELECT * FROM raw_materials
            WHERE LOWER(name)=LOWER(?) AND stock_quantity > 0
            ORDER BY expiry_date ASC, stock_quantity ASC
        ''', (ingredient['ingredient_name'],), fetch=True) or []

        if not materials:
            st.error(f"❌ No stock available for {ingredient['ingredient_name']}")
            all_valid = False
            continue

        remaining_needed = required_quantity
        selected_batches = []

        for mat in materials:
            if remaining_needed <= 0:
                break
            use_quantity = min(mat['stock_quantity'], remaining_needed)
            selected_batches.append({
                'name': mat['name'],
                'batch_number': mat.get('batch_number', 'N/A'),
                'quantity_used': use_quantity,
                'expiry_date': mat.get('expiry_date', 'N/A'),
                'unit': mat.get('unit', 'g'),
                'remaining': mat['stock_quantity'] - use_quantity
            })
            remaining_needed -= use_quantity

        if remaining_needed > 0:
            st.error(f"❌ Insufficient stock for {ingredient['ingredient_name']}. Need {required_quantity:.1f}g")
            all_valid = False
            continue

        for selected in selected_batches:
            allocation_data.append({
                'Ingredient': ingredient['ingredient_name'],
                'Batch #': selected['batch_number'],
                'Quantity': selected['quantity_used'],
                'Unit': selected['unit'],
                'Expiry': selected['expiry_date'],
                'Status': '✅ FIFO Selected'
            })

    if allocation_data:
        alloc_df = pd.DataFrame(allocation_data)
        st.dataframe(alloc_df[['Ingredient', 'Batch #', 'Quantity', 'Unit', 'Expiry']],
                    use_container_width=True, hide_index=True)

        if all_valid:
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Confirm & Start Production", type="primary", key=f"confirm_fifo_{batch['id']}"):
                    for alloc in allocation_data:
                        # Find and deduct from the specific batch
                        mat = db.execute_one(
                            "SELECT id, stock_quantity FROM raw_materials WHERE LOWER(name)=LOWER(?) AND batch_number=?",
                            (alloc['Ingredient'], alloc['Batch #'])
                        )
                        if mat:
                            new_stock = max(0, mat['stock_quantity'] - alloc['Quantity'])
                            db.execute_query(
                                "UPDATE raw_materials SET stock_quantity=? WHERE id=?",
                                (new_stock, mat['id'])
                            )
                            db.execute_query('''
                                INSERT INTO inventory_movements (raw_material_id, change_quantity, reason, reference)
                                VALUES (?, ?, ?, ?)
                            ''', (mat['id'], -alloc['Quantity'], 'production', batch['batch_number']))

                    db.start_batch(batch['id'], st.session_state.get('user', 'System'))
                    st.success(f"✅ Production started for {batch['product_name']}")
                    st.session_state[f"fifo_selection_{batch['id']}"] = False
                    st.rerun()

            with col2:
                if st.button("❌ Cancel", key=f"cancel_fifo_{batch['id']}"):
                    st.session_state[f"fifo_selection_{batch['id']}"] = False
                    st.rerun()
        else:
            st.error("❌ Cannot start production due to ingredient shortages")
            if st.button("🔄 Recalculate", key=f"recalc_{batch['id']}"):
                st.rerun()

def render_completion_form(db, batch, user_name):
    st.markdown("---")
    st.markdown("### 📦 Complete Batch")

    col1, col2 = st.columns(2)
    with col1:
        units = st.number_input(
            "Units Produced *",
            min_value=0,
            value=10,
            step=1,
            key=f"u_{batch['id']}"
        )
    with col2:
        notes = st.text_area("Notes", key=f"n_{batch['id']}", height=80)

    uploaded_files = st.file_uploader(
        "📎 Attach files (photos, PDFs)",
        type=['png', 'jpg', 'jpeg', 'pdf'],
        accept_multiple_files=True,
        key=f"files_{batch['id']}"
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("💾 Save & Complete", type="primary", key=f"sc_{batch['id']}"):
            if units <= 0:
                st.error("⚠️ Please enter units produced")
            else:
                if uploaded_files:
                    for uf in uploaded_files:
                        db.save_batch_file(batch['id'], uf.name, uf.read(), uf.type)

                success, report = db.complete_batch(batch['id'], units, notes, user_name)

                if success:
                    st.session_state[f"complete_form_{batch['id']}"] = False
                    st.success("✅ Batch completed!")
                    st.balloons()

                    st.markdown(f'''
                        <div class="completion-report">
                            <h3>📋 Completion Report</h3>
                            <p><strong>Batch:</strong> {report['batch_number']}</p>
                            <p><strong>Product:</strong> {report['product_name']}</p>
                            <p><strong>Completed By:</strong> {report['completed_by']}</p>
                            <p><strong>Time Spent:</strong> {report['time_spent']} hrs</p>
                            <p><strong>Units Produced:</strong> {report['units_produced']}</p>
                            <p><strong>Low Stock Alerts:</strong> {report['low_stock_alerts']}</p>
                        </div>
                    ''', unsafe_allow_html=True)

                    if RBAC.can(st.session_state.get('role', ''), 'download'):
                        csv_data = db.get_completion_reports_csv()
                        if csv_data:
                            st.download_button(
                                "📥 Download Report",
                                csv_data,
                                f"report_{batch['batch_number']}.csv",
                                "text/csv"
                            )

                    time.sleep(3)
                    st.rerun()

    with col2:
        if st.button("Cancel", key=f"cc_{batch['id']}"):
            st.session_state[f"complete_form_{batch['id']}"] = False
            st.rerun()

# ============================================================================
# MAIN
# ============================================================================

def main():
    st.set_page_config(
        page_title="EQPIS - Production Intelligence",
        page_icon="🌿",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    apply_css()

    defaults = {
        'data_ok': False,
        'show_mapping': False,
        'pending_sheets': [],
        'import_results': None,
        'role': 'business_owner',
        'user': 'System',
        'show_clear_confirm': False
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

    db = DatabaseManager()
    role = render_sidebar(db)

    if not st.session_state.data_ok:
        st.session_state.data_ok = db.has_data()

    if st.session_state.get('show_mapping') and st.session_state.get('pending_sheets'):
        st.header("🖥️ Data Centre")
        render_data_centre(db)
        return

    if not st.session_state.data_ok and role != 'factory_worker':
        render_welcome_page()
        return

    if role == 'factory_worker':
        render_production_centre(db)
        return

    tabs = []
    if RBAC.can(role, 'data'):
        tabs.append(("🖥️ Data Centre", "data"))
    if RBAC.can(role, 'insights'):
        tabs.append(("📊 Insights Hub", "insights"))
    if RBAC.can(role, 'ops'):
        tabs.append(("🏭 Operations Hub", "ops"))
    if RBAC.can(role, 'production'):
        tabs.append(("🔧 Production Hub", "production"))

    if tabs:
        tab_objects = st.tabs([t[0] for t in tabs])
        for i, (_, tab_name) in enumerate(tabs):
            with tab_objects[i]:
                if tab_name == 'data':
                    render_data_centre(db)
                elif tab_name == 'insights':
                    render_insights(db)
                elif tab_name == 'ops':
                    render_operations_centre(db)
                elif tab_name == 'production':
                    render_production_centre(db)

if __name__ == "__main__":
    main()
