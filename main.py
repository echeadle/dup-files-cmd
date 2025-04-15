import argparse
import hashlib
import os
import sqlite3
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
CONFIG_DIR = BASE_DIR / "config"
DB_FILE = CONFIG_DIR / "file_hashes.db"


def load_config_file(file_path):
    try:
        with open(file_path) as f:
            return set(json.load(f))
    except Exception:
        return set()


def create_default_config():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    defaults = {
        "skip_types.json": [],
        "exclude_dirs.json": ["anaconda3", "venv"],
        "include_files.json": []
    }
    for filename, content in defaults.items():
        path = CONFIG_DIR / filename
        if not path.exists():
            with open(path, "w") as f:
                json.dump(content, f, indent=2)
            print(f"Created default config: {path}")


def hash_file(file_path):
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def human_readable_size(size_bytes):
    for unit in ['B', 'K', 'M', 'G', 'T']:
        if size_bytes < 1024:
            return f"{size_bytes:.0f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.0f}P"


def initialize_db():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL,
            hash TEXT NOT NULL,
            size TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON files (hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_size ON files (size)")
    conn.commit()
    return conn


def index_directory(base_dir, conn, skip_types, exclude_dirs, include_files, verbose=False):
    cur = conn.cursor()
    file_count = 0
    for root, dirs, files in os.walk(base_dir):
        if any(excl in root for excl in exclude_dirs):
            continue
        if verbose:
            print(f"Scanning directory: {root}")
        for file in files:
            file_path = os.path.join(root, file)
            if include_files and file_path not in include_files:
                continue
            if any(file.endswith(ext) for ext in skip_types):
                continue
            file_hash = hash_file(file_path)
            try:
                size_bytes = os.path.getsize(file_path)
                size_hr = human_readable_size(size_bytes)
            except Exception:
                size_hr = "0B"
            if file_hash:
                cur.execute("INSERT INTO files (path, hash, size) VALUES (?, ?, ?)", (file_path, file_hash, size_hr))
                file_count += 1
                if file_count % 200 == 0:
                    print(f"Processed {file_count} files...")
    conn.commit()
    print(f"Total files processed: {file_count}")


def size_to_bytes(size_str):
    size_str = size_str.upper()
    units = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
    if size_str[-1] in units:
        return int(float(size_str[:-1]) * units[size_str[-1]])
    return int(size_str)


def find_duplicates(conn, min_size=None):
    cur = conn.cursor()
    if min_size:
        cur.execute("""
            SELECT hash, GROUP_CONCAT(path, '\n') AS paths
            FROM files
            WHERE CAST(REPLACE(size, SUBSTR(size, -1), '') AS FLOAT) *
                  CASE SUBSTR(size, -1)
                      WHEN 'B' THEN 1
                      WHEN 'K' THEN 1024
                      WHEN 'M' THEN 1048576
                      WHEN 'G' THEN 1073741824
                      WHEN 'T' THEN 1099511627776
                      WHEN 'P' THEN 1125899906842624
                      ELSE 1
                  END >= ?
            GROUP BY hash
            HAVING COUNT(*) > 1
        """, (min_size,))
    else:
        cur.execute("""
            SELECT hash, GROUP_CONCAT(path, '\n') AS paths
            FROM files
            GROUP BY hash
            HAVING COUNT(*) > 1
        """)
    return cur.fetchall()


def main():
    parser = argparse.ArgumentParser(description="File hashing and duplicate finder tool")
    parser.add_argument("--dir", type=str, help="Directory to scan")
    parser.add_argument("--find-dupes", action="store_true", help="Print duplicate files")
    parser.add_argument("--min-size", type=str, help="Minimum file size for duplicates (e.g., 10M, 1G)")
    parser.add_argument("--skip-types", type=str, help="Path to JSON file listing extensions to skip")
    parser.add_argument("--exclude-dirs", type=str, help="Path to JSON file listing directories to exclude")
    parser.add_argument("--include-files", type=str, help="Path to JSON file listing files to include")
    parser.add_argument("--init-config", action="store_true", help="Create default config files in the config directory")
    parser.add_argument("--verbose", action="store_true", help="Print directories as they are scanned")
    args = parser.parse_args()

    if args.init_config:
        create_default_config()
        print("Default configuration files initialized.")
        return

    # Load config from command-line paths or defaults
    skip_types = load_config_file(args.skip_types) if args.skip_types else load_config_file(CONFIG_DIR / "skip_types.json")
    exclude_dirs = load_config_file(args.exclude_dirs) if args.exclude_dirs else load_config_file(CONFIG_DIR / "exclude_dirs.json")
    include_files = load_config_file(args.include_files) if args.include_files else load_config_file(CONFIG_DIR / "include_files.json")

    conn = initialize_db()

    if args.dir:
        index_directory(args.dir, conn, skip_types, exclude_dirs, include_files, verbose=args.verbose)

    if args.find_dupes:
        min_size_bytes = size_to_bytes(args.min_size) if args.min_size else None
        duplicates = find_duplicates(conn, min_size=min_size_bytes)
        if not duplicates:
            print("No duplicates found.")
        for file_hash, paths in duplicates:
            print(f"\nDuplicate Hash: {file_hash}\nPaths:\n{paths}")

    conn.close()


if __name__ == "__main__":
    main()

