#!/usr/bin/env python3
"""
SFTP Downloader with concurrent processing, extraction, and keyword filtering.

Usage:
    python sftp_downloader.py --start_date 20260501 --end_date 20260503
"""

import os
import sys
import re
import shutil
import tarfile
import logging
import argparse
import queue
import threading
import time
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import paramiko
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FileTask:
    remote_path: str       # full remote path
    local_archive_path: str  # local destination inside archive/
    market_name: str
    date_str: str
    file_size: int = 0


@dataclass
class TaskResult:
    task: FileTask
    success: bool
    error_msg: str = ""
    untar_files: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Logger setup
# ---------------------------------------------------------------------------

def setup_logger(log_dir: str) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"sftp_downloader_{timestamp}.log")

    logger = logging.getLogger("sftp_downloader")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"Log file: {log_filename}")
    return logger


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_keywords(keyword_file: str) -> List[str]:
    if not os.path.exists(keyword_file):
        return []
    with open(keyword_file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y%m%d")


def date_range(start: datetime, end: datetime) -> List[str]:
    result = []
    cur = start
    while cur <= end:
        result.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return result


# ---------------------------------------------------------------------------
# SFTP helper
# ---------------------------------------------------------------------------

class SFTPClient:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg = cfg["sftp"]
        self.logger = logger
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        self._lock = threading.Lock()

    def connect(self):
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname=self.cfg["host"],
            port=int(self.cfg["port"]),
            username=self.cfg["username"],
            password=self.cfg["password"],
            timeout=30,
        )
        self._sftp = self._ssh.open_sftp()
        self.logger.info(
            f"Connected to SFTP: {self.cfg['host']}:{self.cfg['port']}"
        )

    def disconnect(self):
        if self._sftp:
            self._sftp.close()
        if self._ssh:
            self._ssh.close()

    def listdir_attr(self, path: str):
        return self._sftp.listdir_attr(path)

    def stat(self, path: str):
        return self._sftp.stat(path)

    def get(self, remote_path: str, local_path: str, callback=None):
        self._sftp.get(remote_path, local_path, callback=callback)


def make_thread_sftp(cfg: dict, logger: logging.Logger) -> SFTPClient:
    """Create a new SFTP connection per thread."""
    client = SFTPClient(cfg, logger)
    client.connect()
    return client


# ---------------------------------------------------------------------------
# Discovery: collect all file tasks
# ---------------------------------------------------------------------------

def discover_tasks(
    sftp: SFTPClient,
    base_path: str,
    date_list: List[str],
    archive_root: str,
    logger: logging.Logger,
) -> List[FileTask]:
    tasks: List[FileTask] = []

    try:
        entries = sftp.listdir_attr(base_path)
    except Exception as e:
        logger.error(f"Cannot list base path {base_path}: {e}")
        return tasks

    for entry in entries:
        name = entry.filename
        # Include USM_ but exclude USMlte_
        if not name.startswith("USM_"):
            continue
        if name.startswith("USMlte_"):
            continue

        market_name = name
        market_path = f"{base_path}/{market_name}"
        logger.info(f"Scanning market: {market_name}")

        try:
            date_entries = sftp.listdir_attr(market_path)
        except Exception as e:
            logger.warning(f"Cannot list {market_path}: {e}")
            continue

        for de in date_entries:
            date_str = de.filename
            if date_str not in date_list:
                continue

            date_path = f"{market_path}/{date_str}"
            try:
                file_entries = sftp.listdir_attr(date_path)
            except Exception as e:
                logger.warning(f"Cannot list {date_path}: {e}")
                continue

            for fe in file_entries:
                fname = fe.filename
                remote_full = f"{date_path}/{fname}"
                local_rel = os.path.join(market_name, date_str, fname)
                local_full = os.path.join(archive_root, local_rel)

                task = FileTask(
                    remote_path=remote_full,
                    local_archive_path=local_full,
                    market_name=market_name,
                    date_str=date_str,
                    file_size=fe.st_size if fe.st_size else 0,
                )
                tasks.append(task)

    return tasks


# ---------------------------------------------------------------------------
# Progress callback for individual file download
# ---------------------------------------------------------------------------

def make_progress_bar(filename: str, total_size: int) -> Tuple[tqdm, callable]:
    pbar = tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"  {filename[:40]}",
        leave=False,
        dynamic_ncols=True,
    )

    def callback(transferred: int, total: int):
        pbar.n = transferred
        pbar.refresh()

    return pbar, callback


# ---------------------------------------------------------------------------
# Preprocessing: untar and filter by keywords
# ---------------------------------------------------------------------------

def preprocess_file(
    archive_path: str,
    market_name: str,
    untar_root: str,
    keywords: List[str],
    logger: logging.Logger,
) -> List[str]:
    """
    Extract tar.gz, filter by keywords, move matching files to untar_root/market_name/.
    Returns list of moved file paths.
    """
    tmp_dir = tempfile.mkdtemp(prefix="sftp_untar_")
    moved_files = []

    try:
        # Extract
        logger.debug(f"Extracting {archive_path} -> {tmp_dir}")
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(path=tmp_dir)

        # Walk extracted files
        dest_dir = os.path.join(untar_root, market_name)
        os.makedirs(dest_dir, exist_ok=True)

        for root, dirs, files in os.walk(tmp_dir):
            for fname in files:
                full_path = os.path.join(root, fname)
                matched = any(kw in fname for kw in keywords) if keywords else True

                if matched:
                    dest_path = os.path.join(dest_dir, fname)
                    # Handle duplicates
                    if os.path.exists(dest_path):
                        base, ext = os.path.splitext(fname)
                        dest_path = os.path.join(
                            dest_dir, f"{base}_{int(time.time())}{ext}"
                        )
                    shutil.move(full_path, dest_path)
                    moved_files.append(dest_path)
                    logger.debug(f"  Kept: {fname} -> {dest_path}")
                else:
                    os.remove(full_path)
                    logger.debug(f"  Removed (no keyword match): {fname}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return moved_files


# ---------------------------------------------------------------------------
# Worker: download + preprocess one file
# ---------------------------------------------------------------------------

def process_task(
    task: FileTask,
    cfg: dict,
    keywords: List[str],
    logger: logging.Logger,
    global_pbar: tqdm,
) -> TaskResult:
    sftp = None
    try:
        sftp = make_thread_sftp(cfg, logger)

        # Ensure local directory exists
        os.makedirs(os.path.dirname(task.local_archive_path), exist_ok=True)

        # Download with progress
        fname = os.path.basename(task.remote_path)
        pbar, cb = make_progress_bar(fname, task.file_size)

        dl_start = time.time()
        sftp.get(task.remote_path, task.local_archive_path, callback=cb)
        dl_elapsed = time.time() - dl_start

        pbar.close()

        dl_size = os.path.getsize(task.local_archive_path)
        speed = dl_size / dl_elapsed / 1024 / 1024 if dl_elapsed > 0 else 0
        logger.info(
            f"Downloaded: {fname} ({dl_size/1024/1024:.2f} MB, {speed:.2f} MB/s)"
        )

        # Preprocess (untar + filter)
        untar_root = cfg["paths"]["untar"]
        moved = preprocess_file(
            task.local_archive_path,
            task.market_name,
            untar_root,
            keywords,
            logger,
        )
        logger.info(
            f"Preprocessed: {fname} -> {len(moved)} file(s) kept in {untar_root}"
        )

        # Remove archive after success
        os.remove(task.local_archive_path)
        logger.debug(f"Removed archive: {task.local_archive_path}")

        return TaskResult(task=task, success=True, untar_files=moved)

    except Exception as e:
        logger.error(f"Error processing {task.remote_path}: {e}")
        return TaskResult(task=task, success=False, error_msg=str(e))

    finally:
        if sftp:
            sftp.disconnect()
        # Update global progress bar
        global_pbar.update(1)


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.2f} PB"


def format_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SFTP Downloader")
    parser.add_argument("--start_date", required=True, help="Start date (YYYYMMDD)")
    parser.add_argument("--end_date", required=True, help="End date (YYYYMMDD)")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    args = parser.parse_args()

    # Load config
    cfg = load_config(args.config)
    log_dir = cfg["paths"].get("log", "logs/")
    logger = setup_logger(log_dir)

    logger.info("=" * 60)
    logger.info("SFTP Downloader started")
    logger.info(f"  start_date : {args.start_date}")
    logger.info(f"  end_date   : {args.end_date}")
    logger.info(f"  concurrent : {cfg.get('concurrent', 1)}")
    logger.info("=" * 60)

    # Load keywords
    keywords = load_keywords(cfg.get("keyword_list_file", "family_list.txt"))
    logger.info(f"Keywords loaded: {keywords}")

    # Date range
    start_dt = parse_date(args.start_date)
    end_dt = parse_date(args.end_date)
    date_list = date_range(start_dt, end_dt)
    logger.info(f"Target dates: {date_list}")

    total_start = time.time()

    # Step 1~3: Discover files via single SFTP connection
    logger.info("Scanning SFTP for target files...")
    discovery_sftp = SFTPClient(cfg, logger)
    discovery_sftp.connect()

    base_path = cfg["sftp"]["base_path"]
    archive_root = cfg["paths"]["archive"]

    tasks = discover_tasks(discovery_sftp, base_path, date_list, archive_root, logger)
    discovery_sftp.disconnect()

    # Step 4: Summary of discovered files
    total_count = len(tasks)
    total_size = sum(t.file_size for t in tasks)
    logger.info("=" * 60)
    logger.info(f"Files to download : {total_count}")
    logger.info(f"Total size        : {human_size(total_size)}")
    logger.info("=" * 60)

    if total_count == 0:
        logger.info("No files to process. Exiting.")
        return
    
    # Step 5~10: Process concurrently
    concurrent = int(cfg.get("concurrent", 1))
    failed_results: List[TaskResult] = []
    success_results: List[TaskResult] = []

    completed = 0
    task_start_times = {}

    global_pbar = tqdm(
        total=total_count,
        desc="Overall progress",
        unit="file",
        dynamic_ncols=True,
    )

    wall_start = time.time()

    with ThreadPoolExecutor(max_workers=concurrent) as executor:
        future_map = {}
        for i, task in enumerate(tasks):
            task_start_times[i] = time.time()
            fut = executor.submit(
                process_task, task, cfg, keywords, logger, global_pbar
            )
            future_map[fut] = i

        for fut in as_completed(future_map):
            idx = future_map[fut]
            result: TaskResult = fut.result()

            completed += 1
            elapsed = time.time() - wall_start
            avg_per_file = elapsed / completed
            remaining = (total_count - completed) * avg_per_file

            if result.success:
                success_results.append(result)
                logger.info(
                    f"[{completed}/{total_count}] OK: {os.path.basename(result.task.remote_path)}"
                    f" | ETA: {format_duration(remaining)}"
                )
            else:
                failed_results.append(result)
                logger.warning(
                    f"[{completed}/{total_count}] FAIL: {os.path.basename(result.task.remote_path)}"
                    f" | ETA: {format_duration(remaining)}"
                )

    global_pbar.close()

    # Step 11: Final summary
    total_elapsed = time.time() - total_start

    untar_files_all = [f for r in success_results for f in r.untar_files]
    untar_total_size = sum(os.path.getsize(f) for f in untar_files_all if os.path.exists(f))

    # Download summary (using original task sizes for successfully downloaded)
    downloaded_size = sum(r.task.file_size for r in success_results)

    logger.info("")
    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total files scanned    : {total_count}")
    logger.info(f"  Downloaded (success)   : {len(success_results)} files / {human_size(downloaded_size)}")
    logger.info(f"  Preprocessed files     : {len(untar_files_all)} files / {human_size(untar_total_size)}")
    logger.info(f"  Failed                 : {len(failed_results)}")
    logger.info(f"  Total elapsed          : {format_duration(total_elapsed)}")
    logger.info("=" * 60)

    if failed_results:
        logger.warning("")
        logger.warning("FAILED FILES:")
        for r in failed_results:
            logger.warning(f"  {r.task.remote_path}")
            logger.warning(f"    Reason: {r.error_msg}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
