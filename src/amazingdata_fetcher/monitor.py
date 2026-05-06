import time
from loguru import logger


def _read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except Exception:
        return None


def get_process_rss_mb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return None


def get_host_memory_gb():
    try:
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                key = parts[0].rstrip(":")
                info[key] = int(parts[1])
        total = info.get("MemTotal", 0) / 1024 / 1024
        available = info.get("MemAvailable", 0) / 1024 / 1024
        used = total - available
        return used, total
    except Exception:
        return None, None


def get_container_memory_mb():
    for used_path, limit_path in [
        ("/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
        ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"),
    ]:
        used_str = _read_file(used_path)
        limit_str = _read_file(limit_path)
        if used_str is not None and limit_str is not None:
            try:
                used = int(used_str.strip())
                limit_raw = limit_str.strip()
                limit = int(limit_raw) if limit_raw != "max" else None
                used_mb = used / 1024 / 1024
                if limit is not None:
                    limit_mb = limit / 1024 / 1024
                    return used_mb, limit_mb
                return used_mb, None
            except (ValueError, ZeroDivisionError):
                continue
    return None, None


def get_network_bytes():
    try:
        with open("/proc/net/dev") as f:
            lines = f.readlines()[2:]
            rx_total = 0
            tx_total = 0
            for line in lines:
                parts = line.split()
                iface = parts[0].rstrip(":")
                if iface == "lo":
                    continue
                rx_total += int(parts[1])
                tx_total += int(parts[9])
            return rx_total, tx_total
    except Exception:
        return None, None


class SystemMonitor:
    def __init__(self):
        self._last_rx = None
        self._last_tx = None
        self._last_time = None

    def snapshot(self, label=""):
        parts = []

        rss = get_process_rss_mb()
        if rss is not None:
            parts.append(f"进程RSS={rss:.0f}MB")

        c_used, c_limit = get_container_memory_mb()
        if c_used is not None:
            if c_limit is not None:
                pct = c_used / c_limit * 100
                parts.append(f"容器内存={c_used:.0f}/{c_limit:.0f}MB({pct:.0f}%)")
            else:
                parts.append(f"容器内存={c_used:.0f}MB")

        h_used, h_total = get_host_memory_gb()
        if h_used is not None:
            pct = h_used / h_total * 100
            parts.append(f"主机内存={h_used:.1f}/{h_total:.1f}GB({pct:.0f}%)")

        now = time.time()
        rx, tx = get_network_bytes()
        if rx is not None:
            if self._last_rx is not None and self._last_time is not None:
                dt = now - self._last_time
                if dt > 0:
                    rx_rate = (rx - self._last_rx) / dt
                    tx_rate = (tx - self._last_tx) / dt
                    parts.append(f"带宽=↓{rx_rate/1024/1024:.1f}↑{tx_rate/1024/1024:.1f}MB/s")
            rx_gb = rx / 1024 / 1024 / 1024
            tx_gb = tx / 1024 / 1024 / 1024
            parts.append(f"累计流量=↓{rx_gb:.2f}↑{tx_gb:.2f}GB")
            self._last_rx = rx
            self._last_tx = tx
        self._last_time = now

        if parts:
            prefix = f"[Monitor:{label}] " if label else "[Monitor] "
            logger.info(prefix + " | ".join(parts))
