from flask import Flask, render_template, request, redirect, flash, jsonify
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this to a random secret key

# PostgreSQL connection settings
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'gestione_rischio',
    'user': 'postgres',
    'password': '57123Li15!'
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_table_schema(table_name):
    """Get column information for a table"""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (table_name,))
    schema = cursor.fetchall()
    cursor.close()
    conn.close()
    return schema

def get_primary_key(table_name):
    """Get the primary key column(s) for a table"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """, (table_name,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in result]

def get_foreign_key_relationships(table_name):
    """Get foreign key relationships for a table"""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.key_column_usage AS kcu
        JOIN information_schema.constraint_column_usage AS ccu
            ON kcu.constraint_name = ccu.constraint_name
        JOIN information_schema.table_constraints AS tc
            ON kcu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND kcu.table_name = %s;
    """, (table_name,))
    foreign_keys = cursor.fetchall()
    cursor.close()
    conn.close()
    return {fk['column_name']: {'table': fk['foreign_table_name'], 'column': fk['foreign_column_name']} 
            for fk in foreign_keys}

def get_foreign_key_options(table_name, column_name):
    """Get options for foreign key dropdown"""
    foreign_keys = get_foreign_key_relationships(table_name)
    if column_name not in foreign_keys:
        return []
    
    fk_info = foreign_keys[column_name]
    fk_table = fk_info['table']
    fk_column = fk_info['column']
    display_col = get_display_columns(fk_table)
    
    if not display_col:
        return []
    
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Get options ordered by display column
        cursor.execute(f'SELECT "{fk_column}", "{display_col}" FROM "{fk_table}" ORDER BY "{display_col}"')
        options = cursor.fetchall()
        return [{'value': opt[fk_column], 'display': opt[display_col]} for opt in options]
    except Exception:
        return []
    finally:
        cursor.close()
        conn.close()

def get_display_columns(table_name):
    """Get the best columns to display for a table (name, title, description, etc.)"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Priority order for display columns
    preferred_names = ['name', 'title', 'description', 'label', 'value', 'text']
    
    for pref in preferred_names:
        for col_name, data_type in columns:
            if pref in col_name.lower() and data_type in ['text', 'character varying', 'varchar']:
                return col_name
    
    # Fallback to first text column
    for col_name, data_type in columns:
        if data_type in ['text', 'character varying', 'varchar']:
            return col_name
    
    # Last resort: return first column
    if columns:
        return columns[0][0]
    
    return None

def get_all_tables():
    """Get all table names from the database"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

@app.route('/')
def landing_page():
    # Get all available tables from the database
    try:
        tables = get_all_tables()
    except Exception as e:
        tables = []
        flash(f'Error connecting to database: {str(e)}', 'error')
    
    return render_template('landing.html', tables=tables)

@app.route('/main-tables')
def main_tables():
    """Show only the principal tables"""
    try:
        all_tables = get_all_tables()
        
        # Define principal table patterns and their display info
        principal_tables = {
            'cultural_heritage_site': {
                'icon': 'university',
                'color': 'primary',
                'title': 'Cultural Heritage Site',
                'description': 'Gestisci e monitora i siti del patrimonio culturale'
            },
            'value_aspect_dimension': {
                'icon': 'cube',
                'color': 'success',
                'title': 'Value Aspect Dimension',
                'description': 'Analizza le dimensioni di valore e aspetti del patrimonio'
            },
            'value_class_dimension': {
                'icon': 'table',
                'color': 'success',
                'title': 'Value Class Dimension',
                'description': 'Classificazioni e dimensioni di valore NARA'
            },
            'risk_analysis': {
                'icon': 'chart-line',
                'color': 'danger',
                'title': 'Risk Analysis',
                'description': 'Analisi completa dei rischi e delle minacce'
            }
        }
        
        # Find matching tables with more flexible matching
        available_principal_tables = {}
        for table in all_tables:
            table_lower = table.lower()
            
            # Direct matches first
            if table in principal_tables:
                available_principal_tables[table] = principal_tables[table]
            # Flexible matching for variations
            elif 'cultural' in table_lower and 'heritage' in table_lower:
                available_principal_tables[table] = principal_tables['cultural_heritage_site']
            elif 'value' in table_lower and ('aspect' in table_lower or 'dimension' in table_lower):
                if 'aspect' in table_lower:
                    available_principal_tables[table] = principal_tables['value_aspect_dimension']
                elif 'class' in table_lower:
                    available_principal_tables[table] = principal_tables['value_class_dimension']
                else:
                    # Default to aspect if unclear
                    available_principal_tables[table] = principal_tables['value_aspect_dimension']
            elif 'risk' in table_lower and 'analysis' in table_lower:
                available_principal_tables[table] = principal_tables['risk_analysis']
        
        return render_template('main_tables.html', 
                             tables=available_principal_tables,
                             all_table_count=len(all_tables))
    except Exception as e:
        flash(f'Error connecting to database: {str(e)}', 'error')
        return redirect('/')

@app.route('/all-tables')
def all_tables():
    """Show all available tables"""
    try:
        tables = get_all_tables()
        return render_template('all_tables.html', tables=tables)
    except Exception as e:
        flash(f'Error connecting to database: {str(e)}', 'error')
        return redirect('/')

@app.route('/tables')
def list_all_tables():
    """API endpoint to get all tables as JSON"""
    try:
        tables = get_all_tables()
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/table/<table_name>')
def show_table(table_name):
    """Enhanced table view with CRUD operations"""
    try:
        # First, verify that the table exists
        available_tables = get_all_tables()
        if table_name not in available_tables:
            flash(f'La tabella "{table_name}" non esiste nel database. Tabelle disponibili: {", ".join(available_tables)}', 'error')
            return redirect('/')
        
        page = request.args.get('page', 1, type=int)
        per_page = 20
        search = request.args.get('search', '')
        filter_column = request.args.get('filter_column', '')
        filter_value = request.args.get('filter_value', '')
        
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get foreign key relationships
        foreign_keys = get_foreign_key_relationships(table_name)
        
        # Build the SELECT clause with JOINs for foreign keys
        select_columns = []
        join_clauses = []
        alias_counter = 1
        
        schema = get_table_schema(table_name)
        for col in schema:
            col_name = col['column_name']
            if col_name in foreign_keys:
                # This is a foreign key, join to get the display value
                fk_info = foreign_keys[col_name]
                fk_table = fk_info['table']
                fk_column = fk_info['column']
                display_col = get_display_columns(fk_table)
                
                if display_col:
                    alias = f"fk_{alias_counter}"
                    select_columns.append(f'"{table_name}"."{col_name}"')
                    select_columns.append(f'{alias}."{display_col}" AS "{col_name}_display"')
                    join_clauses.append(f'LEFT JOIN "{fk_table}" AS {alias} ON "{table_name}"."{col_name}" = {alias}."{fk_column}"')
                    alias_counter += 1
                else:
                    select_columns.append(f'"{table_name}"."{col_name}"')
            else:
                select_columns.append(f'"{table_name}"."{col_name}"')
        
        # Build the base query with JOINs
        base_select = f"SELECT {', '.join(select_columns)}"
        base_from = f'FROM "{table_name}"'
        if join_clauses:
            base_from += f" {' '.join(join_clauses)}"
        
        where_conditions = []
        params = []
        
        if search:
            # Get all text columns for search (including display columns)
            text_columns = []
            for col in schema:
                col_name = col['column_name']
                if col['data_type'] in ['text', 'character varying', 'varchar', 'char']:
                    if col_name in foreign_keys:
                        # Search in the display column if it exists
                        fk_info = foreign_keys[col_name]
                        display_col = get_display_columns(fk_info['table'])
                        if display_col:
                            text_columns.append(f'fk_{list(foreign_keys.keys()).index(col_name) + 1}."{display_col}"')
                    else:
                        text_columns.append(f'"{table_name}"."{col_name}"')
            
            if text_columns:
                search_conditions = [f'{col} ILIKE %s' for col in text_columns]
                where_conditions.append(f"({' OR '.join(search_conditions)})")
                params.extend([f'%{search}%'] * len(text_columns))
        
        if filter_column and filter_value:
            where_conditions.append(f'"{table_name}"."{filter_column}" = %s')
            params.append(filter_value)
        
        where_clause = f" WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # Get total count
        count_query = f'SELECT COUNT(*) {base_from}{where_clause}'
        cursor.execute(count_query, params)
        total = cursor.fetchone()['count']
        
        # Get paginated data
        offset = (page - 1) * per_page
        data_query = f'{base_select} {base_from}{where_clause} LIMIT %s OFFSET %s'
        cursor.execute(data_query, params + [per_page, offset])
        data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Get table schema for form generation
        primary_keys = get_primary_key(table_name)
        
        # Calculate pagination info
        total_pages = (total + per_page - 1) // per_page
        
        return render_template('enhanced_table.html', 
                             table_name=table_name, 
                             data=data, 
                             schema=schema,
                             foreign_keys=foreign_keys,
                             primary_keys=primary_keys,
                             page=page,
                             total_pages=total_pages,
                             total=total,
                             search=search,
                             filter_column=filter_column,
                             filter_value=filter_value)
    
    except Exception as e:
        flash(f'Errore nell\'accesso alla tabella "{table_name}": {str(e)}', 'error')
        return redirect('/')

@app.route('/insert/<table_name>', methods=['GET', 'POST'])
def insert_record(table_name):
    """Insert a new record"""
    try:
        # Verify table exists
        available_tables = get_all_tables()
        if table_name not in available_tables:
            flash(f'La tabella "{table_name}" non esiste nel database.', 'error')
            return redirect('/')
        
        if request.method == 'GET':
            schema = get_table_schema(table_name)
            foreign_keys = get_foreign_key_relationships(table_name)
            
            # Get foreign key options for dropdowns
            fk_options = {}
            for col_name in foreign_keys.keys():
                fk_options[col_name] = get_foreign_key_options(table_name, col_name)
            
            return render_template('insert_form.html', 
                                 table_name=table_name, 
                                 schema=schema,
                                 foreign_keys=foreign_keys,
                                 fk_options=fk_options)
        
        # Handle POST - insert the record
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            # Get form data
            form_data = request.form.to_dict()
            columns = list(form_data.keys())
            values = list(form_data.values())
            
            # Filter out empty values and handle them appropriately
            filtered_columns = []
            filtered_values = []
            placeholders = []
            
            for col, val in zip(columns, values):
                if val.strip():  # Only include non-empty values
                    filtered_columns.append(f'"{col}"')
                    filtered_values.append(val)
                    placeholders.append('%s')
            
            if filtered_columns:
                query = f'INSERT INTO "{table_name}" ({", ".join(filtered_columns)}) VALUES ({", ".join(placeholders)})'
                cursor.execute(query, filtered_values)
                conn.commit()
                flash(f'Record inserito con successo nella tabella {table_name}!', 'success')
            else:
                flash('Nessun dato fornito per l\'inserimento!', 'error')
                
        except Exception as e:
            conn.rollback()
            flash(f'Errore nell\'inserimento del record: {str(e)}', 'error')
        finally:
            cursor.close()
            conn.close()
        
        return redirect(f'/table/{table_name}')
    
    except Exception as e:
        flash(f'Errore nell\'accesso alla tabella "{table_name}": {str(e)}', 'error')
        return redirect('/')

@app.route('/update/<table_name>/<record_id>', methods=['GET', 'POST'])
def update_record(table_name, record_id):
    """Update an existing record"""
    primary_keys = get_primary_key(table_name)
    if not primary_keys:
        flash('Cannot update: No primary key found for this table!', 'error')
        return redirect(f'/table/{table_name}')
    
    primary_key = primary_keys[0]  # Use first primary key for simplicity
    
    if request.method == 'GET':
        # Get current record data
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(f'SELECT * FROM "{table_name}" WHERE "{primary_key}" = %s', (record_id,))
        record = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not record:
            flash('Record not found!', 'error')
            return redirect(f'/table/{table_name}')
        
        schema = get_table_schema(table_name)
        foreign_keys = get_foreign_key_relationships(table_name)
        
        # Get foreign key options for dropdowns
        fk_options = {}
        for col_name in foreign_keys.keys():
            fk_options[col_name] = get_foreign_key_options(table_name, col_name)
        
        return render_template('update_form.html', 
                             table_name=table_name, 
                             schema=schema, 
                             record=record,
                             primary_key=primary_key,
                             foreign_keys=foreign_keys,
                             fk_options=fk_options)
    
    # Handle POST - update the record
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        form_data = request.form.to_dict()
        set_clauses = []
        values = []
        
        for col, val in form_data.items():
            if col != primary_key:  # Don't update primary key
                set_clauses.append(f'"{col}" = %s')
                values.append(val if val.strip() else None)
        
        if set_clauses:
            values.append(record_id)
            query = f'UPDATE "{table_name}" SET {", ".join(set_clauses)} WHERE "{primary_key}" = %s'
            cursor.execute(query, values)
            conn.commit()
            flash(f'Record updated successfully in {table_name}!', 'success')
        else:
            flash('No changes made!', 'warning')
            
    except Exception as e:
        conn.rollback()
        flash(f'Error updating record: {str(e)}', 'error')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(f'/table/{table_name}')

@app.route('/delete/<table_name>/<record_id>', methods=['POST'])
def delete_record(table_name, record_id):
    """Delete a record"""
    primary_keys = get_primary_key(table_name)
    if not primary_keys:
        flash('Cannot delete: No primary key found for this table!', 'error')
        return redirect(f'/table/{table_name}')
    
    primary_key = primary_keys[0]
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f'DELETE FROM "{table_name}" WHERE "{primary_key}" = %s', (record_id,))
        if cursor.rowcount > 0:
            conn.commit()
            flash(f'Record deleted successfully from {table_name}!', 'success')
        else:
            flash('Record not found!', 'error')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting record: {str(e)}', 'error')
    finally:
        cursor.close()
        conn.close()
    
    return redirect(f'/table/{table_name}')

# Legacy routes for backward compatibility
@app.route('/cultural-heritage')
def show_cultural_heritage():
    return redirect('/table/cultural_heritage_site')

@app.route('/nara-grid')
def show_nara_grid():
    return redirect('/table/value_class_dimension')

@app.route('/test-db')
def test_database():
    """Test database connection and show available tables"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Test basic connection
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()[0]
        
        # Get all tables
        cursor.execute("""
            SELECT schemaname, tablename, tableowner 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
        tables_info = cursor.fetchall()
        
        # Get table counts
        table_counts = {}
        for schema, table, owner in tables_info:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                count = cursor.fetchone()[0]
                table_counts[table] = count
            except Exception as e:
                table_counts[table] = f"Error: {str(e)}"
        
        cursor.close()
        conn.close()
        
        return f"""
        <h2>Database Connection Test</h2>
        <p><strong>Database Version:</strong> {db_version}</p>
        <h3>Available Tables:</h3>
        <ul>
        {''.join([f'<li>{table} (Records: {table_counts[table]}) - <a href="/table/{table}">View</a></li>' for schema, table, owner in tables_info])}
        </ul>
        <p><a href="/">Back to Home</a></p>
        """
    except Exception as e:
        return f"""
        <h2>Database Connection Error</h2>
        <p style="color: red;"><strong>Error:</strong> {str(e)}</p>
        <p>Please check your database configuration in index.py</p>
        <p><a href="/">Back to Home</a></p>
        """

@app.route('/risk-analysis')
def show_risk_analysis():
    conn = get_connection()
    dataframes = {}
    for table in ['value_agents_occurrence', 'agent_risk_sentence', 'risk_analysis']:
        df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
        dataframes[table] = {
            'columns': df.columns.tolist(),
            'rows': df.to_dict(orient='records')
        }
    conn.close()
    return render_template('multi_table.html', tables=dataframes)

@app.route('/plot/<table_name>/<column_name>')
def plot_column(table_name, column_name):
    import matplotlib.pyplot as plt
    import seaborn as sns
    import io
    import base64

    conn = get_connection()
    df = pd.read_sql(f'SELECT "{column_name}" FROM "{table_name}"', conn)
    conn.close()

    fig, ax = plt.subplots()
    try:
        sns.histplot(df[column_name], kde=True, ax=ax)
        ax.set_title(f'Distribution of {column_name}')
    except:
        ax.text(0.5, 0.5, 'Cannot plot selected column', ha='center')

    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()
    plt.close()

    return render_template('plot.html', table_name=table_name, column_name=column_name, plot_url=plot_url)


if __name__ == '__main__':
    app.run(debug=True)
