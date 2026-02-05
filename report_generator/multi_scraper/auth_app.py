import os
import sqlite3
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, render_template, request, redirect, url_for, session, flash, g

DB_PATH = os.path.join(os.path.dirname(__file__), 'auth_users.db')

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    db.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        created_by TEXT,
        created_at TEXT,
        last_login TEXT
    )
    ''')
    db.commit()

def create_user(username, password, role, created_by=None):
    db = get_db()
    password_hash = generate_password_hash(password)
    now = datetime.utcnow().isoformat()
    try:
        db.execute('INSERT INTO users (username, password_hash, role, created_by, created_at) VALUES (?,?,?,?,?)',
                   (username, password_hash, role, created_by, now))
        db.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_user_by_username(username):
    db = get_db()
    cur = db.execute('SELECT * FROM users WHERE username = ?', (username,))
    return cur.fetchone()

def update_last_login(username):
    db = get_db()
    now = datetime.utcnow().isoformat()
    db.execute('UPDATE users SET last_login = ? WHERE username = ?', (now, username))
    db.commit()

def list_users():
    db = get_db()
    cur = db.execute('SELECT username, role, created_by, created_at, last_login FROM users ORDER BY role, username')
    return cur.fetchall()

def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv('FLASK_SECRET', 'dev-secret-key')

    @app.before_request
    def before_request():
        # ensure db exists
        init_db()

    @app.teardown_appcontext
    def close_connection(exception):
        db = getattr(g, '_database', None)
        if db is not None:
            db.close()

    # create default admin if none exists (perform immediately inside app context)
    with app.app_context():
        init_db()
        db = get_db()
        cur = db.execute('SELECT COUNT(*) as cnt FROM users WHERE role = ?', ('admin',))
        r = cur.fetchone()
        if not r or r['cnt'] == 0:
            # create a default admin with password from env or fallback
            admin_pass = os.getenv('DEFAULT_ADMIN_PASSWORD', 'admin123')
            create_user('admin', admin_pass, 'admin', created_by='system')
            app.logger.info('Default admin created with username `admin`. Change the password immediately.')

    def login_required(role=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if 'username' not in session:
                    return redirect(url_for('login'))
                if role and session.get('role') != role:
                    flash('Unauthorized', 'danger')
                    return redirect(url_for('login'))
                return func(*args, **kwargs)
            wrapper.__name__ = func.__name__
            return wrapper
        return decorator

    @app.route('/')
    def index():
        if 'username' in session:
            role = session.get('role')
            if role == 'admin':
                return redirect(url_for('admin_dashboard'))
            if role == 'researcher':
                return redirect(url_for('researcher_dashboard'))
            if role == 'creator':
                return redirect(url_for('creator_dashboard'))
        return redirect(url_for('login'))

    @app.route('/auth/login', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            user = get_user_by_username(username)
            if user and check_password_hash(user['password_hash'], password):
                session['username'] = user['username']
                session['role'] = user['role']
                update_last_login(user['username'])
                flash('Logged in', 'success')
                return redirect(url_for('index'))
            else:
                flash('Invalid credentials', 'danger')
        return render_template('auth/login.html')

    @app.route('/auth/logout')
    def logout():
        session.clear()
        flash('Logged out', 'info')
        return redirect(url_for('login'))

    @app.route('/auth/admin')
    @login_required(role='admin')
    def admin_dashboard():
        users = list_users()
        return render_template('auth/admin_dashboard.html', users=users)

    @app.route('/auth/admin/create_user', methods=['GET', 'POST'])
    @login_required(role='admin')
    def admin_create_user():
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            role = request.form.get('role')
            if role not in ('researcher', 'creator'):
                flash('Invalid role', 'danger')
                return redirect(url_for('admin_create_user'))
            created_by = session.get('username')
            ok = create_user(username, password, role, created_by)
            if ok:
                flash('User created', 'success')
                return redirect(url_for('admin_dashboard'))
            else:
                flash('User already exists', 'warning')
        return render_template('auth/create_user.html')

    @app.route('/auth/researcher')
    @login_required(role='researcher')
    def researcher_dashboard():
        return render_template('auth/researcher_dashboard.html', username=session.get('username'))

    @app.route('/auth/creator')
    @login_required(role='creator')
    def creator_dashboard():
        return render_template('auth/creator_dashboard.html', username=session.get('username'))

    return app

if __name__ == '__main__':
    application = create_app()
    application.run(host='127.0.0.1', port=5001, debug=True)
