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
class FolderTask:
    remote_date_path: str      # full remote path to date folder
    local_date_path: str       # local destination: archive/market/date/
    market_name: str
    date_str: str
    file_count: int = 0        # number of tar.gz files in the date folder
    total_size: int = 0        # sum of file sizes


@dataclass
class ErrorDetail:
    date_path: str
    filename: str
    reason: str


@dataclass
class FolderResult:
    task: FolderTask
    success: bool
    error_details: List["ErrorDetail"] = field(default_factory=list)
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
) -> List[FolderTask]:
    """
    Worker: stat files in one market/date directory and return a single FolderTask.
    """
    pool, market_name, date_path, date_str, archive_root, logger = args_tuple
    broken = False
    sftp = pool.acquire()
    try:
        file_entries = sftp.listdir_attr(date_path)
        tar_entries = [fe for fe in file_entries if fe.filename.endswith(".tar.gz")]
        file_count  = len(tar_entries)
        total_size  = sum(fe.st_size for fe in tar_entries if fe.st_size)
        local_date_path = os.path.join(archive_root, market_name, date_str)
        return [FolderTask(
            remote_date_path=date_path,
            local_date_path=local_date_path,
            market_name=market_name,
            date_str=date_str,
            file_count=file_count,
            total_size=total_size,
        )]
    except Exception as e:
        logger.warning(f"Cannot scan {date_path}: {e}")
        broken = True
        return []
    finally:
        pool.release(sftp, broken=broken)


def discover_tasks_parallel(
    cfg: dict,
    base_path: str,
    date_list: List[str],
    archive_root: str,
    concurrent: int,
    logger: logging.Logger,
    discovery_concurrent: Optional[int] = None,
) -> List[FolderTask]:
    """
    3-phase parallel discovery:
      Phase 1: list base_path (single call, very fast)
      Phase 2: list each market dir in parallel -> collect (market, date_path) pairs
      Phase 3: list each date dir in parallel -> collect FolderTask list

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

        all_tasks: List[FolderTask] = []
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

        logger.info(f"[Phase 3/3] Done — total folders queued: {len(all_tasks)}")
        return all_tasks

    finally:
        pool.close_all()
        logger.info("Discovery connection pool closed.")


# ---------------------------------------------------------------------------
# Download: recursively download a remote date folder
# ---------------------------------------------------------------------------

def download_folder(
    sftp: "SFTPClient",
    remote_dir: str,
    local_dir: str,
    logger: logging.Logger,
) -> List[str]:
    """
    Download all tar.gz files from remote_dir into local_dir.
    Returns list of local file paths downloaded.
    """
    os.makedirs(local_dir, exist_ok=True)
    entries = sftp.listdir_attr(remote_dir)
    tar_entries = [e for e in entries if e.filename.endswith(".tar.gz")]
    downloaded = []

    for fe in tar_entries:
        remote_path = f"{remote_dir}/{fe.filename}"
        local_path  = os.path.join(local_dir, fe.filename)
        file_size   = fe.st_size or 0
        logger.debug(f"  Downloading {fe.filename} ({file_size/1024/1024:.1f} MB)")
        sftp.get(remote_path, local_path)
        downloaded.append(local_path)

    return downloaded


# ---------------------------------------------------------------------------
# Preprocess: untar and filter by keyword_name
# ---------------------------------------------------------------------------

def preprocess_folder(
    local_date_path: str,
    market_name: str,
    untar_root: str,
    keywords: List[str],
    logger: logging.Logger,
    temp_root: str = "",
) -> tuple:
    """
    1. Extract non-_RE_ tar.gz files into tmp_dir
    2. Extract _RE_ tar.gz files into same tmp_dir (overwrite)
    3. For each extracted file:
         keyword_name = filename.split('_')[1]
         if keyword_name in keywords → move to untar_root/market_name/keyword_name/
         else                        → delete
    Returns (moved_files, error_details).
    temp_root: base directory for temporary extraction; uses OS default if empty.
    """
    moved_files: List[str]    = []
    error_details: List[dict] = []

    # Gather tar.gz files
    all_tars = sorted(
        f for f in os.listdir(local_date_path) if f.endswith(".tar.gz")
    )
    non_re = [f for f in all_tars if "_RE_" not in f]
    re_    = [f for f in all_tars if "_RE_" in f]

    os.makedirs(temp_root, exist_ok=True) if temp_root else None
    tmp_dir = tempfile.mkdtemp(prefix="sftp_untar_", dir=temp_root or None)
    try:
        # Step 1: extract non-_RE_ files
        for fname in non_re:
            fpath = os.path.join(local_date_path, fname)
            try:
                with tarfile.open(fpath, "r:gz") as tar:
                    tar.extractall(path=tmp_dir)
                logger.debug(f"  Extracted (non-RE): {fname}")
            except Exception as e:
                error_details.append({"filename": fname, "reason": f"extract failed: {e}"})
                logger.warning(f"  Extract failed [{fname}]: {e}")

        # Step 2: extract _RE_ files (overwrite)
        for fname in re_:
            fpath = os.path.join(local_date_path, fname)
            try:
                with tarfile.open(fpath, "r:gz") as tar:
                    tar.extractall(path=tmp_dir)
                logger.debug(f"  Extracted (_RE_): {fname}")
            except Exception as e:
                error_details.append({"filename": fname, "reason": f"extract failed: {e}"})
                logger.warning(f"  Extract failed [{fname}]: {e}")

        # Step 3: classify and move/delete extracted files
        for fname in os.listdir(tmp_dir):
            full_path = os.path.join(tmp_dir, fname)
            if not os.path.isfile(full_path):
                continue
            try:
                parts = fname.split("_")
                keyword_name = parts[1] if len(parts) > 1 else ""
            except Exception:
                keyword_name = ""

            if keyword_name and keyword_name in keywords:
                dest_dir = os.path.join(untar_root, market_name, keyword_name)
                os.makedirs(dest_dir, exist_ok=True)
                dest_path = os.path.join(dest_dir, fname)
                shutil.move(full_path, dest_path)
                moved_files.append(dest_path)
                logger.debug(f"  Kept [{keyword_name}]: {fname}")
            else:
                os.remove(full_path)
                logger.debug(f"  Deleted (keyword '{keyword_name}' not in list): {fname}")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return moved_files, error_details


# ---------------------------------------------------------------------------
# Worker: download + preprocess one date folder
# ---------------------------------------------------------------------------

def process_folder(
    task: FolderTask,
    pool: "SFTPConnectionPool",
    cfg: dict,
    keywords: List[str],
    logger: logging.Logger,
    global_pbar: tqdm,
) -> FolderResult:
    tag = f"{task.market_name}/{task.date_str}"
    broken = False
    sftp = pool.acquire()
    error_details: List[ErrorDetail] = []

    try:
        # ── Download entire date folder ──────────────────────────────────────
        dl_start = time.time()
        downloaded = download_folder(
            sftp, task.remote_date_path, task.local_date_path, logger
        )
        dl_elapsed = time.time() - dl_start
        dl_size = sum(os.path.getsize(p) for p in downloaded)
        speed = dl_size / dl_elapsed / 1024 / 1024 if dl_elapsed > 0 else 0
        logger.info(
            f"[DL] {tag} — {len(downloaded)} files, "
            f"{human_size(dl_size)}, {speed:.2f} MB/s"
        )

        # ── Preprocess (untar + keyword filter) ─────────────────────────────
        untar_root = cfg["paths"]["untar"]
        temp_root  = cfg["paths"].get("temp", "")
        moved, preproc_errors = preprocess_folder(
            task.local_date_path,
            task.market_name,
            untar_root,
            keywords,
            logger,
            temp_root=temp_root,
        )
        for pe in preproc_errors:
            error_details.append(ErrorDetail(
                date_path=task.remote_date_path,
                filename=pe["filename"],
                reason=pe["reason"],
            ))

        logger.info(
            f"[PP] {tag} — {len(moved)} file(s) kept, "
            f"{len(preproc_errors)} error(s)"
        )

        # ── Remove local date folder on full success ─────────────────────────
        if not preproc_errors:
            shutil.rmtree(task.local_date_path, ignore_errors=True)
            logger.debug(f"Removed local folder: {task.local_date_path}")
        else:
            logger.warning(
                f"Keeping local folder due to errors: {task.local_date_path}"
            )

        return FolderResult(
            task=task,
            success=True,
            error_details=error_details,
            untar_files=moved,
        )

    except Exception as e:
        broken = True
        logger.error(f"Error processing folder {tag}: {e}")
        error_details.append(ErrorDetail(
            date_path=task.remote_date_path,
            filename="<folder>",
            reason=str(e),
        ))
        return FolderResult(task=task, success=False, error_details=error_details)

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

    # Step 4: Summary of discovered folders
    total_folders = len(tasks)
    total_files   = sum(t.file_count for t in tasks)
    total_size    = sum(t.total_size  for t in tasks)
    logger.info("=" * 60)
    logger.info(f"Folders to process : {total_folders}")
    logger.info(f"Total tar.gz files : {total_files}")
    logger.info(f"Total size         : {human_size(total_size)}")
    logger.info("=" * 60)

    if total_folders == 0:
        logger.info("No folders to process. Exiting.")
        return

    # Step 5~10: Process concurrently using a shared download connection pool
    logger.info(f"Initialising download connection pool (size={concurrent}) ...")
    dl_pool = SFTPConnectionPool(cfg, concurrent, logger)

    failed_results:  List[FolderResult] = []
    success_results: List[FolderResult] = []
    completed = 0

    global_pbar = tqdm(
        total=total_folders,
        desc="Overall progress",
        unit="folder",
        dynamic_ncols=True,
    )

    wall_start = time.time()

    try:
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            future_map = {
                executor.submit(
                    process_folder, task, dl_pool, cfg, keywords, logger, global_pbar
                ): task
                for task in tasks
            }

            for fut in as_completed(future_map):
                result: FolderResult = fut.result()
                completed += 1
                elapsed     = time.time() - wall_start
                avg         = elapsed / completed
                remaining   = (total_folders - completed) * avg
                tag = f"{result.task.market_name}/{result.task.date_str}"

                if result.success:
                    success_results.append(result)
                    logger.info(
                        f"[{completed}/{total_folders}] OK : {tag}"
                        f" | ETA: {format_duration(remaining)}"
                    )
                else:
                    failed_results.append(result)
                    logger.warning(
                        f"[{completed}/{total_folders}] FAIL: {tag}"
                        f" | ETA: {format_duration(remaining)}"
                    )
    finally:
        dl_pool.close_all()
        logger.info("Download connection pool closed.")

    global_pbar.close()

    # Step 11: Final summary
    total_elapsed    = time.time() - total_start
    untar_files_all  = [f for r in success_results for f in r.untar_files]
    untar_total_size = sum(os.path.getsize(f) for f in untar_files_all if os.path.exists(f))
    all_errors       = [e for r in success_results + failed_results for e in r.error_details]

    logger.info("")
    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total folders      : {total_folders}")
    logger.info(f"  Success            : {len(success_results)}")
    logger.info(f"  Failed             : {len(failed_results)}")
    logger.info(f"  Kept files         : {len(untar_files_all)} / {human_size(untar_total_size)}")
    logger.info(f"  Total elapsed      : {format_duration(total_elapsed)}")
    logger.info("=" * 60)

    if all_errors:
        logger.warning("")
        logger.warning("ERROR DETAILS:")
        for e in all_errors:
            logger.warning(f"  [{e.date_path}] {e.filename}")
            logger.warning(f"    Reason: {e.reason}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
