#!/usr/bin/env python3

import os
import sys
import json
import logging
import argparse
import redis
import psycopg2
import psycopg2.extras
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("DuplicateViewer")

class Config:
    def __init__(self, config_path="config.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
    def db(self): return self.config["database"]
    def redis(self): return self.config["redis"]

class DuplicateViewer:
    def __init__(self, config_path="config.json"):
        cfg = Config(config_path)
        self.db_conf = cfg.db()
        self.redis_conf = cfg.redis()
        self.conn = None
        self.r = None
        self.prefix = self.redis_conf.get("prefix", "imgproc:")

    def key(self, name): return f"{self.prefix}{name}"

    def connect_db(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.db_conf["host"], port=self.db_conf["port"],
                user=self.db_conf["user"], password=self.db_conf["password"],
                dbname=self.db_conf["dbname"]
            )
        return self.conn

    def connect_redis(self):
        if not self.r:
            self.r = redis.Redis(
                host=self.redis_conf.get("host", "localhost"),
                port=self.redis_conf.get("port", 6379),
                db=self.redis_conf.get("db", 0),
                password=self.redis_conf.get("password", None),
                decode_responses=True
            )
        return self.r

    def get_duplicates(self):
        conn = self.connect_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT internal_id, duplicate_ids
                FROM image_links
                WHERE duplicate_ids IS NOT NULL
                  AND duplicate_ids != 'null'::jsonb
                  AND duplicate_ids != '[]'::jsonb
            """)
            return {str(row['internal_id']): row['duplicate_ids'] for row in cur.fetchall() if row['duplicate_ids']}

    def entry(self, internal_id):
        conn = self.connect_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT internal_id, hyper_reality, sreality_image_urls,
                       bezrealitky_image_urls, idnes, local_path, image_hashes
                FROM image_links WHERE internal_id = %s
            """, (internal_id,))
            r = cur.fetchone()
            return dict(r) if r else None

    def extract_urls(self, entry):
        urls = []
        for col in ['hyper_reality', 'sreality_image_urls', 'bezrealitky_image_urls', 'idnes']:
            val = entry.get(col)
            if val:
                try:
                    data = json.loads(val) if isinstance(val, str) else val
                    if isinstance(data, list): urls.extend([(col, u) for u in data])
                    else: urls.append((col, data))
                except: urls.append((col, val))
        return urls

    def local_paths(self, entry):
        paths = []
        lp = entry.get('local_path')
        if lp:
            try:
                data = json.loads(lp) if isinstance(lp, str) else lp
                if isinstance(data, list): paths.extend(data)
                else: paths.append(data)
            except: paths.append(lp)
        return paths

    def view(self, out=None, limit=None):
        dups = self.get_duplicates()
        if not dups:
            logger.info("No duplicates found")
            return
        if limit: dups = dict(list(dups.items())[:limit])

        output = []
        output.append(f"Duplicate Images Report - {datetime.now():%Y-%m-%d %H:%M:%S}")
        output.append(f"Found {len(dups)} entries with duplicates\n")
        for idx, (eid, dup_ids) in enumerate(dups.items(), 1):
            entry = self.entry(eid)
            if not entry: continue
            urls = self.extract_urls(entry)
            lpaths = self.local_paths(entry)
            output.append(f"\n{'='*80}")
            output.append(f"Entry {idx}: Internal ID {eid} - {len(dup_ids)} duplicates")
            output.append(f"{'-'*80}")
            if urls:
                output.append("URLs:")
                for n, (src, url) in enumerate(urls, 1): output.append(f"  {n}. [{src}] {url}")
            if lpaths:
                output.append("\nLocal Paths:")
                for n, path in enumerate(lpaths, 1): output.append(f"  {n}. {path}")
            output.append(f"\nDuplicate Entries:")
            for dn, did in enumerate(dup_ids, 1):
                dentry = self.entry(did)
                output.append(f"  {dn}. Duplicate ID {did}")
                if not dentry: continue
                du = self.extract_urls(dentry)
                dp = self.local_paths(dentry)
                if du:
                    output.append("     URLs:")
                    for n, (src, url) in enumerate(du, 1): output.append(f"       {n}. [{src}] {url}")
                if dp:
                    output.append("     Local Paths:")
                    for n, path in enumerate(dp, 1): output.append(f"       {n}. {path}")
        for line in output: print(line)
        if out:
            with open(out, 'w') as f: f.write('\n'.join(output))
            logger.info(f"Output saved to {out}")

    def close(self):
        if self.conn and not self.conn.closed: self.conn.close()
        # Redis auto-close

def main():
    parser = argparse.ArgumentParser(description="View duplicate image entries with details")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--output")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    viewer = DuplicateViewer(args.config)
    try:
        viewer.view(args.output, args.limit)
    finally:
        viewer.close()

if __name__ == "__main__":
    main()