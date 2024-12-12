import duckdb
import os
from datetime import datetime
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBLoader:
    def __init__(self, db_path: str = "moovitamix.duckdb"):
        """Initialize DuckDB connection and create schema"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._create_schema()
    
    def _create_schema(self):
        """Create the database schema if it doesn't exist"""
        try:
            # Drop existing tables if they exist
            self.conn.execute("DROP TABLE IF EXISTS dim_tracks;")
            self.conn.execute("DROP TABLE IF EXISTS dim_users;")
            self.conn.execute("DROP TABLE IF EXISTS fact_listen_history;")
            
            # Create dimensions tables
            self.conn.execute("""
                CREATE TABLE dim_tracks (
                    track_id INTEGER,
                    name VARCHAR,
                    artist VARCHAR,
                    songwriters VARCHAR,
                    duration VARCHAR,
                    genres VARCHAR,
                    album VARCHAR,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    etl_updated_at TIMESTAMP,
                    PRIMARY KEY (track_id)
                );
            """)

            self.conn.execute("""
                CREATE TABLE dim_users (
                    user_id INTEGER,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    email VARCHAR,
                    gender VARCHAR,
                    favorite_genres VARCHAR,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    etl_updated_at TIMESTAMP,
                    PRIMARY KEY (user_id)
                );
            """)

            self.conn.execute("""
                CREATE TABLE fact_listen_history (
                    user_id INTEGER,
                    track_id INTEGER,
                    listened_at TIMESTAMP,
                    load_date DATE
                );
            """)

            logger.info("Schema created successfully")

        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise

    def load_daily_data(self, data_date: Optional[str] = None):
        """Load daily data from parquet files into DuckDB"""
        if data_date is None:
            data_date = datetime.now().strftime('%Y-%m-%d')
            
        data_dir = os.path.join('data', 'raw', data_date)
        
        logger.info(f"Starting data load for {data_date}")
        
        try:
            # Start transaction
            self.conn.execute("BEGIN TRANSACTION;")
            
            # Load tracks
            tracks_path = os.path.join(data_dir, 'tracks.parquet')
            if os.path.exists(tracks_path):
                # Direct insert for tracks
                self.conn.execute("""
                    INSERT INTO dim_tracks 
                    SELECT 
                        id as track_id,
                        name,
                        artist,
                        songwriters,
                        duration,
                        genres,
                        album,
                        created_at,
                        updated_at,
                        now() as etl_updated_at
                    FROM read_parquet($1);
                """, [tracks_path])
                logger.info("Tracks loaded successfully")
            else:
                logger.warning(f"Tracks file not found: {tracks_path}")

            # Load users
            users_path = os.path.join(data_dir, 'users.parquet')
            if os.path.exists(users_path):
                # Direct insert for users
                self.conn.execute("""
                    INSERT INTO dim_users 
                    SELECT 
                        id as user_id,
                        first_name,
                        last_name,
                        email,
                        gender,
                        favorite_genres,
                        created_at,
                        updated_at,
                        now() as etl_updated_at
                    FROM read_parquet($1);
                """, [users_path])
                logger.info("Users loaded successfully")
            else:
                logger.warning(f"Users file not found: {users_path}")

            # Load listen history
            history_path = os.path.join(data_dir, 'listen_history.parquet')
            if os.path.exists(history_path):
                self.conn.execute("""
                    INSERT INTO fact_listen_history (user_id, track_id, listened_at, load_date)
                    SELECT 
                        h.user_id,
                        unnest(h.items) as track_id,
                        h.created_at as listened_at,
                        current_date as load_date
                    FROM read_parquet($1) h;
                """, [history_path])
                logger.info("Listen history loaded successfully")
            else:
                logger.warning(f"Listen history file not found: {history_path}")

            # Commit transaction
            self.conn.execute("COMMIT;")
            logger.info(f"Successfully loaded data for {data_date}")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            try:
                self.conn.execute("ROLLBACK;")
                logger.info("Transaction rolled back")
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {str(rollback_error)}")
            raise e

    def verify_data(self):
        """Verify that data was loaded correctly"""
        try:
            counts = self.conn.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM dim_tracks) as track_count,
                    (SELECT COUNT(*) FROM dim_users) as user_count,
                    (SELECT COUNT(*) FROM fact_listen_history) as history_count
            """).fetchone()
            
            logger.info(f"Data verification:\n"
                       f"Tracks: {counts[0]}\n"
                       f"Users: {counts[1]}\n"
                       f"Listen History: {counts[2]}")
            
            # Show sample data
            logger.info("\nSample tracks:")
            tracks_sample = self.conn.execute("SELECT * FROM dim_tracks LIMIT 3;").fetchall()
            for track in tracks_sample:
                logger.info(track)
            
        except Exception as e:
            logger.error(f"Error verifying data: {str(e)}")
            raise

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    loader = None
    try:
        loader = DuckDBLoader()
        loader.load_daily_data()
        loader.verify_data()
        return 0
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        return 1
    finally:
        if loader:
            loader.close()

if __name__ == "__main__":
    exit(main())