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
        timeout        = int(self.cfg.get("timeout", 60))
        banner_timeout = int(self.cfg.get("banner_timeout", 60))
        auth_timeout   = int(self.cfg.get("auth_timeout", 60))
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname=self.cfg["host"],
            port=int(self.cfg["port"]),
            username=self.cfg["username"],
            password=self.cfg["password"],
            timeout=timeout,
            banner_timeout=banner_timeout,
            auth_timeout=auth_timeout,
        )
        self._sftp = self._ssh.open_sftp()
        self.logger.debug(
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

    def ls(self, path: str) -> List[str]:
        """
        Return filenames in path using SFTP listdir() — names only, no stat.
        Falls back to listdir_attr() if listdir() is unavailable.
        This avoids exec_command which many SFTP-only servers do not permit.
        """
        try:
            return self._sftp.listdir(path)
        except AttributeError:
            return [e.filename for e in self._sftp.listdir_attr(path)]


def make_thread_sftp(
    cfg: dict,
    logger: logging.Logger,
    max_retries: int = 5,
    base_delay: float = 2.0,
) -> SFTPClient:
    """
    Create a new SFTP connection per thread.
    Retries on transient errors (e.g. SSH banner timeout) with exponential backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            client = SFTPClient(cfg, logger)
            client.connect()
            return client
        except Exception as e:
            if attempt == max_retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))  # 2, 4, 8, 16 sec
            logger.debug(
                f"SFTP connect failed (attempt {attempt}/{max_retries}): {e} "
                f"— retrying in {delay:.0f}s"
            )
            time.sleep(delay)


# ---------------------------------------------------------------------------
# SFTP Connection Pool
# ---------------------------------------------------------------------------

class SFTPConnectionPool:
    """
    Fixed-size pool of persistent SFTP connections.
    Workers borrow a connection, use it, then return it.
    Total simultaneous SSH connections = pool_size (never exceeds this).
    """

    def __init__(self, cfg: dict, pool_size: int, logger: logging.Logger):
        self._cfg = cfg
        self._logger = logger
        self._pool: queue.Queue = queue.Queue()
        self._all_clients: List[SFTPClient] = []

        self._broken_slots: int = 0
        logger.info(f"Initialising SFTP connection pool (size={pool_size}) ...")
        # Build connections one at a time with a small stagger to avoid
        # hitting the server with a simultaneous burst during setup.
        for i in range(pool_size):
            client = make_thread_sftp(cfg, logger)
            self._pool.put(client)
            self._all_clients.append(client)
            if i < pool_size - 1:
                time.sleep(0.5)   # 500 ms stagger — more conservative
        logger.info(f"Connection pool ready ({pool_size} connections)")

    def acquire(self) -> SFTPClient:
        """Block until a connection is available, reconnecting broken slots on demand."""
        client = self._pool.get()
        if client is None:
            client = self._reconnect_slot()
        return client

    def release(self, client: SFTPClient, broken: bool = False):
        """
        Return a connection to the pool.
        Pass broken=True when the caller caught an error — the slot will be
        reconnected lazily before next use instead of right now (avoids
        triggering a burst of reconnects when the server is under load).
        """
        if broken:
            self._logger.debug("Marking pool slot for lazy reconnect ...")
            try:
                client.disconnect()
            except Exception:
                pass
            # Replace with a sentinel None; acquire() will reconnect on demand
            self._broken_slots += 1
            self._pool.put(None)
        else:
            self._pool.put(client)

    def _reconnect_slot(self) -> SFTPClient:
        """Reconnect one broken slot with backoff."""
        for attempt in range(1, 6):
            try:
                client = make_thread_sftp(self._cfg, self._logger)
                self._broken_slots -= 1
                return client
            except Exception as e:
                if attempt == 5:
                    raise
                delay = 2 * attempt
                self._logger.warning(
                    f"Pool reconnect attempt {attempt}/5 failed: {e} — retry in {delay}s"
                )
                time.sleep(delay)

    def close_all(self):
        """Drain the pool and disconnect every connection."""
        while not self._pool.empty():
            try:
                c = self._pool.get_nowait()
                c.disconnect()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Discovery: collect all file tasks (parallel)
# ---------------------------------------------------------------------------

def _scan_date_dir(
    args_tuple: tuple,
) -> List[FileTask]:
    """
    Worker: list files inside one market/date directory using a shared pool.
    """
    pool, market_name, date_path, date_str, archive_root, logger = args_tuple
    tasks: List[FileTask] = []
    broken = False
    sftp = pool.acquire()
    try:
        file_entries = sftp.listdir_attr(date_path)
        for fe in file_entries:
            fname = fe.filename
            remote_full = f"{date_path}/{fname}"
            local_rel = os.path.join(market_name, date_str, fname)
            local_full = os.path.join(archive_root, local_rel)
            tasks.append(FileTask(
                remote_path=remote_full,
                local_archive_path=local_full,
                market_name=market_name,
                date_str=date_str,
                file_size=fe.st_size if fe.st_size else 0,
            ))
    except Exception as e:
        logger.warning(f"Cannot scan {date_path}: {e}")
        broken = True
    finally:
        pool.release(sftp, broken=broken)
    return tasks


def discover_tasks_parallel(
    cfg: dict,
    base_path: str,
    date_list: List[str],
    archive_root: str,
    concurrent: int,
    logger: logging.Logger,
    discovery_concurrent: Optional[int] = None,
) -> List[FileTask]:
    """
    3-phase parallel discovery:
      Phase 1: list base_path (single call, very fast)
      Phase 2: list each market dir in parallel -> collect (market, date_path) pairs
      Phase 3: list each date dir in parallel -> collect FileTask list

    discovery_concurrent caps the SFTP connections used during discovery,
    independent of the download concurrency setting.
    """
    d_workers = discovery_concurrent if discovery_concurrent else min(concurrent, 16)

    # ── Build pool first — Phase 1 borrows one slot from it ─────────────────
    date_set = set(date_list)
    pool = SFTPConnectionPool(cfg, d_workers, logger)

    # ── Phase 1: get market list (SFTP listdir, names only) ─────────────────
    logger.info(f"[Phase 1/3] Listing markets in {base_path} ...")
    all_names = []
    probe = pool.acquire()
    try:
        all_names = probe.ls(base_path)
        pool.release(probe)
    except Exception as e:
        pool.release(probe, broken=True)
        logger.error(f"[Phase 1/3] Cannot list base path: {e}")
        pool.close_all()
        return []

    markets = [
        n for n in all_names
        if n.startswith("USM_") and not n.startswith("USMlte_")
    ]
    logger.info(f"[Phase 1/3] Done — markets found: {len(markets)}")

    try:
        # Phase 2 ── find matching date dirs inside each market ──────────────
        def _scan_market_pooled(market_name: str) -> List[tuple]:
            result = []
            broken = False
            sftp = pool.acquire()
            try:
                market_path = f"{base_path}/{market_name}"
                dir_names = sftp.ls(market_path)
                for name in dir_names:
                    if name in date_set:
                        result.append((market_name, name, f"{market_path}/{name}"))
            except Exception as e:
                logger.warning(f"Cannot scan market {market_name}: {e}")
                broken = True
            finally:
                pool.release(sftp, broken=broken)
            return result

        date_dir_list: List[tuple] = []
        logger.info(f"[Phase 2/3] Scanning date dirs inside {len(markets)} markets "
                    f"(pool={d_workers}) ...")
        with ThreadPoolExecutor(max_workers=d_workers) as ex:
            futs = {ex.submit(_scan_market_pooled, m): m for m in markets}
            with tqdm(total=len(markets), desc="  Phase 2 markets ", unit="market",
                      dynamic_ncols=True) as pbar:
                for fut in as_completed(futs):
                    date_dir_list.extend(fut.result())
                    pbar.update(1)
                    pbar.set_postfix({"date_dirs": len(date_dir_list)})

        logger.info(f"[Phase 2/3] Done — target date directories: {len(date_dir_list)}")

        # Phase 3 ── list files inside each date dir ─────────────────────────
        scan_args = [
            (pool, market_name, date_path, date_str, archive_root, logger)
            for market_name, date_str, date_path in date_dir_list
        ]

        all_tasks: List[FileTask] = []
        logger.info(f"[Phase 3/3] Listing files in {len(scan_args)} date directories "
                    f"(pool={d_workers}) ...")
        with ThreadPoolExecutor(max_workers=d_workers) as ex:
            futs = [ex.submit(_scan_date_dir, args) for args in scan_args]
            with tqdm(total=len(scan_args), desc="  Phase 3 date dirs", unit="dir",
                      dynamic_ncols=True) as pbar:
                for fut in as_completed(futs):
                    result = fut.result()
                    all_tasks.extend(result)
                    pbar.update(1)
                    pbar.set_postfix({"files_found": len(all_tasks)})

        logger.info(f"[Phase 3/3] Done — total files found: {len(all_tasks)}")
        return all_tasks

    finally:
        pool.close_all()


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
    pool: "SFTPConnectionPool",
    cfg: dict,
    keywords: List[str],
    logger: logging.Logger,
    global_pbar: tqdm,
) -> TaskResult:
    broken = False
    sftp = pool.acquire()
    try:
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
        broken = True
        logger.error(f"Error processing {task.remote_path}: {e}")
        return TaskResult(task=task, success=False, error_msg=str(e))

    finally:
        pool.release(sftp, broken=broken)
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
    keywords = load_keywords(cfg.get("keyword_list_file", "keyword_list.txt"))
    logger.info(f"Keywords loaded: {keywords}")

    # Date range
    start_dt = parse_date(args.start_date)
    end_dt = parse_date(args.end_date)
    date_list = date_range(start_dt, end_dt)
    logger.info(f"Target dates: {date_list}")

    total_start = time.time()
    concurrent = int(cfg.get("concurrent", 4))

    # Step 1~3: Discover files in parallel
    logger.info("Scanning SFTP for target files (parallel discovery)...")
    base_path = cfg["sftp"]["base_path"]
    archive_root = cfg["paths"]["archive"]

    discovery_concurrent = cfg.get("discovery_concurrent", None)
    if discovery_concurrent:
        logger.info(f"Discovery workers capped at: {discovery_concurrent}")

    discovery_start = time.time()
    tasks = discover_tasks_parallel(
        cfg, base_path, date_list, archive_root, concurrent, logger,
        discovery_concurrent=discovery_concurrent,
    )
    discovery_elapsed = time.time() - discovery_start
    logger.info(f"Discovery completed in {format_duration(discovery_elapsed)}")

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

    # Step 5~10: Process concurrently using a shared download connection pool
    logger.info(f"Initialising download connection pool (size={concurrent}) ...")
    dl_pool = SFTPConnectionPool(cfg, concurrent, logger)

    failed_results: List[TaskResult] = []
    success_results: List[TaskResult] = []

    completed = 0

    global_pbar = tqdm(
        total=total_count,
        desc="Overall progress",
        unit="file",
        dynamic_ncols=True,
    )

    wall_start = time.time()

    try:
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            future_map = {}
            for task in tasks:
                fut = executor.submit(
                    process_task, task, dl_pool, cfg, keywords, logger, global_pbar
                )
                future_map[fut] = task

            for fut in as_completed(future_map):
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
    finally:
        dl_pool.close_all()

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
