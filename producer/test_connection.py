import psycopg2

# Connection settings
conn_params = {
    'host': 'localhost',
    'port': 5433,  # Using port 5433 since we changed it
    'user': 'postgres',
    'password': 'postgres',
    'database': 'logtaildb'
}

def test_connection():
    """Test basic connection to Postgres"""
    try:
        # Connect
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Run a test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Connected successfully!")
        print(f"PostgreSQL version: {version[0][:50]}...")
        
        # Check WAL level
        cursor.execute("SHOW wal_level;")
        wal_level = cursor.fetchone()
        print(f"✅ WAL Level: {wal_level[0]}")
        
        if wal_level[0] != 'logical':
            print("❌ WARNING: WAL level is not 'logical'!")
        
        # Check replication settings
        cursor.execute("SHOW max_replication_slots;")
        slots = cursor.fetchone()
        print(f"✅ Max Replication Slots: {slots[0]}")
        
        # Count users
        cursor.execute("SELECT COUNT(*) FROM users;")
        count = cursor.fetchone()
        print(f"✅ Users in database: {count[0]}")
        
        # Show all users
        cursor.execute("SELECT name, email, role FROM users ORDER BY id;")
        users = cursor.fetchall()
        print("\n📋 Current users:")
        for user in users:
            print(f"   - {user[0]} ({user[1]}) - {user[2]}")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 All checks passed! Ready for CDC!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_connection()