import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), 'auth_users.db')
print(f"Database path: {db_path}")
print(f"Exists: {os.path.exists(db_path)}")

conn = sqlite3.connect(db_path)
c = conn.cursor()

# Get all tables
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in c.fetchall()]
print(f"\nTables: {tables}")

# Check rd_submissions schema
if 'rd_submissions' in tables:
    c.execute("PRAGMA table_info(rd_submissions)")
    cols = [(r[1], r[2]) for r in c.fetchall()]
    print(f"\nrd_submissions columns: {cols}")
    
    # Count rows
    c.execute("SELECT COUNT(*) FROM rd_submissions")
    count = c.fetchone()[0]
    print(f"\nTotal rows: {count}")
    
    if count > 0:
        c.execute("SELECT market_name, researcher_username, submitted_at FROM rd_submissions LIMIT 5")
        print(f"\nSample data:")
        for row in c.fetchall():
            print(f"  {row}")
else:
    print("\nrd_submissions table does not exist!")

conn.close()
