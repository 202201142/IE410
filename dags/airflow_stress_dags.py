"""
===========================================================================================
  AIRFLOW 3.x STRESS TEST DAG GENERATOR
===========================================================================================
  Dynamically generates 65 DAGs designed to stress-test:
    - AWS EC2 CPU        → prime sieve, matrix math, SHA hashing loops, sorting
    - Memory             → large list allocations, numpy arrays, string buffers
    - I/O (disk)         → temp file read/write/seek cycles, recursive dir walk
    - I/O (network-sim)  → /dev/urandom reads, DNS resolution via socket
    - Concurrency        → all 65 DAGs share the same schedule so they fire together

  Components exercised:
    • default-worker      – executes every task
    • scheduler           – parses 65 DAGs, triggers runs simultaneously
    • dag-processor       – serializes all DAGs on startup
    • api-server          – polled by Airflow UI / CLI during the run

  Usage:
    1. Drop this file into your $AIRFLOW_HOME/dags/ folder.
    2. Wait ~30 s for the DAG Processor to pick it up (check dag-processor logs).
    3. Trigger manually or let the schedule fire.
    4. Watch CPU/Memory on your EC2 instance with:
         watch -n2 "free -h && uptime && df -h /tmp"

  Airflow version: 3.x  (tested on 3.1.x dev mode)
===========================================================================================
"""

from __future__ import annotations

import hashlib
import math
import os
import random
import socket
import string
import tempfile
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────────────────────────────────────
#  GLOBAL CONFIG
# ──────────────────────────────────────────────────────────────────────────────

STRESS_SCHEDULE = "0 2 * * *"  # All DAGs fire together at 02:00 UTC daily.
# Change to None and trigger manually if preferred.
TOTAL_DAGS = 65  # 65 DAGs → ~10 k tasks across the cluster
MAX_ACTIVE_RUNS = 1
CATCHUP = False

# Default args applied to every generated DAG
DEFAULT_ARGS: dict[str, Any] = {
    "owner": "stress-test",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),
}

# ──────────────────────────────────────────────────────────────────────────────
#  TASK WORKLOAD FUNCTIONS  (PythonOperator callables)
# ──────────────────────────────────────────────────────────────────────────────


def cpu_prime_sieve(limit: int = 200_000, **_: Any) -> dict:
    """Sieve of Eratosthenes – pure CPU burn."""
    sieve = bytearray([1]) * (limit + 1)
    sieve[0] = sieve[1] = 0
    for i in range(2, int(limit**0.5) + 1):
        if sieve[i]:
            sieve[i * i :: i] = bytearray(len(sieve[i * i :: i]))
    count = sum(sieve)
    print(f"[prime_sieve] found {count} primes up to {limit}")
    return {"prime_count": count}


def cpu_matrix_multiply(size: int = 200, **_: Any) -> dict:
    """Pure-Python matrix multiply (no numpy) – heavy CPU."""

    def make_matrix(n: int) -> list[list[float]]:
        return [[random.random() for _ in range(n)] for _ in range(n)]

    A, B = make_matrix(size), make_matrix(size)
    C = [[sum(A[i][k] * B[k][j] for k in range(size)) for j in range(size)] for i in range(size)]
    checksum = sum(C[0])
    print(f"[matrix_multiply] size={size}×{size}, checksum={checksum:.4f}")
    return {"checksum": checksum}


def cpu_sha_hash_loop(iterations: int = 50_000, **_: Any) -> dict:
    """Repeated SHA-256 hashing – CPU + memory bus stress."""
    data = os.urandom(4096)
    digest = data
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    result = digest.hex()
    print(f"[sha_hash_loop] {iterations} iterations, final={result[:16]}…")
    return {"final_hash_prefix": result[:16]}


def cpu_sorting_stress(n: int = 500_000, **_: Any) -> dict:
    """Sort a large random list multiple times."""
    data = [random.random() for _ in range(n)]
    for _ in range(3):
        data.sort()
        data.reverse()
    print(f"[sorting_stress] sorted {n} elements 3×")
    return {"first": data[0], "last": data[-1]}


def cpu_fibonacci_recursive(n: int = 35, **_: Any) -> dict:
    """Exponential recursive Fibonacci – worst-case CPU."""

    def fib(k: int) -> int:
        return k if k <= 1 else fib(k - 1) + fib(k - 2)

    result = fib(n)
    print(f"[fibonacci] fib({n}) = {result}")
    return {"fib": result}


def memory_large_allocation(mb: int = 256, **_: Any) -> dict:
    """Allocate, write, and read a large in-memory buffer."""
    chunk = b"X" * (1024 * 1024)  # 1 MB chunk
    buf = []
    for _ in range(mb):
        buf.append(bytearray(chunk))  # allocate
    # Read all bytes to prevent optimisation
    total = sum(b[0] for b in buf)
    del buf
    print(f"[memory_alloc] allocated & freed {mb} MB, checksum={total}")
    return {"mb_allocated": mb}


def memory_string_processing(count: int = 100_000, **_: Any) -> dict:
    """Build and search large strings – heap pressure."""
    alphabet = string.ascii_letters + string.digits
    corpus = "".join(random.choices(alphabet, k=count))
    matches = corpus.count("abc")
    words = corpus.split("z")
    print(f"[string_processing] len={len(corpus)}, 'abc' hits={matches}, splits={len(words)}")
    return {"length": len(corpus), "matches": matches}


def memory_dict_thrash(entries: int = 500_000, **_: Any) -> dict:
    """Fill, iterate, and clear a large dict – GC + allocator stress."""
    store: dict[int, str] = {}
    for i in range(entries):
        store[i] = str(i * 7)
    total = sum(len(v) for v in store.values())
    store.clear()
    print(f"[dict_thrash] {entries} entries, total value chars={total}")
    return {"entries": entries}


def io_disk_stress(size_mb: int = 64, **_: Any) -> dict:
    """Write, seek-read, and delete a temp file – disk I/O."""
    chunk = os.urandom(1024 * 1024)  # 1 MB random block
    with tempfile.NamedTemporaryFile(delete=False, suffix=".stress") as fh:
        path = fh.name
        for _ in range(size_mb):
            fh.write(chunk)
    # Random seek reads
    reads = 0
    with open(path, "rb") as fh:
        file_size = os.path.getsize(path)
        for _ in range(200):
            pos = random.randint(0, file_size - 4096)
            fh.seek(pos)
            fh.read(4096)
            reads += 1
    os.unlink(path)
    print(f"[disk_stress] wrote {size_mb} MB, did {reads} random reads")
    return {"size_mb": size_mb, "reads": reads}


def io_temp_file_pipeline(**_: Any) -> dict:
    """Chain of temp files – simulate ETL staging."""
    stages = []
    prev_data = os.urandom(512 * 1024)  # 512 KB seed
    for stage in range(5):
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".stage{stage}") as fh:
            # "transform": XOR each byte with stage index
            transformed = bytes(b ^ stage for b in prev_data)
            fh.write(transformed)
            stages.append(fh.name)
        with open(stages[-1], "rb") as fh:
            prev_data = fh.read()
    for p in stages:
        os.unlink(p)
    print(f"[file_pipeline] 5-stage file pipeline complete, final checksum={sum(prev_data) % 65536}")
    return {"stages": 5}


def io_recursive_dir_walk(**_: Any) -> dict:
    """Walk /proc or /sys – many small I/O calls."""
    root = "/proc" if os.path.isdir("/proc") else tempfile.gettempdir()
    file_count = 0
    byte_count = 0
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            try:
                fpath = os.path.join(dirpath, fname)
                byte_count += os.path.getsize(fpath)
                file_count += 1
            except OSError:
                pass
        if file_count > 10_000:  # guard: stop after 10 k files
            break
    print(f"[dir_walk] walked {file_count} files, ~{byte_count // 1024} KB total")
    return {"files": file_count}


def io_network_dns_resolve(**_: Any) -> dict:
    """Resolve a handful of hostnames – real network I/O."""
    hosts = [
        "google.com",
        "amazon.com",
        "github.com",
        "cloudflare.com",
        "fastly.com",
        "awsstatic.com",
    ]
    results: dict[str, str] = {}
    for host in hosts:
        try:
            ip = socket.gethostbyname(host)
            results[host] = ip
        except OSError as exc:
            results[host] = f"ERR:{exc}"
    print(f"[dns_resolve] {results}")
    return results


def mixed_cpu_io_pipeline(**_: Any) -> dict:
    """Interleave CPU work with file I/O – realistic DAG task simulation."""
    output = {}
    for step in range(4):
        # CPU phase
        data = [math.sin(i * 0.001) * math.cos(i * 0.002) for i in range(30_000)]
        checksum = sum(data)

        # I/O phase
        with tempfile.NamedTemporaryFile(delete=True, suffix=f".mixed{step}", mode="w") as fh:
            fh.write("\n".join(f"{v:.6f}" for v in data[:1000]))
            fh.flush()
            size = os.path.getsize(fh.name)

        output[f"step_{step}"] = {"checksum": checksum, "file_size": size}
        print(f"[mixed_pipeline] step={step}, checksum={checksum:.4f}, file_size={size}B")
    return output


def xcom_producer(**context: Any) -> list[int]:
    """Produce a large XCom payload to stress the metadata DB."""
    payload = list(range(5_000))  # ~40 KB when serialised
    print(f"[xcom_producer] pushing {len(payload)} ints via XCom")
    return payload


def xcom_consumer(**context: Any) -> dict:
    """Pull the XCom from xcom_producer and do work on it."""
    ti = context["ti"]
    data: list[int] = ti.xcom_pull(task_ids="xcom_producer")
    if not data:
        data = []
    total = sum(data)
    print(f"[xcom_consumer] received {len(data)} items, sum={total}")
    return {"sum": total, "count": len(data)}


def sleep_io_wait(seconds: float = 5.0, **_: Any) -> dict:
    """Simulate a slow external API / database poll."""
    time.sleep(seconds)
    print(f"[sleep_io_wait] slept {seconds}s (simulated I/O wait)")
    return {"slept_seconds": seconds}


# ──────────────────────────────────────────────────────────────────────────────
#  BASH SNIPPETS (BashOperator)
# ──────────────────────────────────────────────────────────────────────────────

BASH_CPU_STRESS = """\
echo "[bash_cpu] Starting CPU stress for 20s"
# use yes + dd as a portable CPU burner; bc for float math
end=$((SECONDS + 20))
count=0
while [ $SECONDS -lt $end ]; do
    echo "scale=10; 4*a(1)" | bc -l > /dev/null 2>&1
    count=$((count + 1))
done
echo "[bash_cpu] Finished $count iterations"
"""

BASH_MEMORY_STRESS = """\
echo "[bash_mem] Allocating ~128 MB via /dev/urandom → tmpfile"
TMP=$(mktemp /tmp/stress_mem.XXXXXX)
dd if=/dev/urandom of="$TMP" bs=1M count=128 status=progress 2>&1
SIZE=$(stat -c%s "$TMP")
echo "[bash_mem] File size: $SIZE bytes"
rm -f "$TMP"
echo "[bash_mem] Done"
"""

BASH_DISK_IO = """\
echo "[bash_disk] Sequential write + read test"
TMP=$(mktemp /tmp/stress_disk.XXXXXX)
dd if=/dev/zero of="$TMP" bs=1M count=256 conv=fdatasync status=progress 2>&1
echo "[bash_disk] Write done, now reading..."
dd if="$TMP" of=/dev/null bs=1M status=progress 2>&1
rm -f "$TMP"
echo "[bash_disk] Disk I/O complete"
"""

BASH_SORT_LARGE = """\
echo "[bash_sort] Generating + sorting 500k random lines"
TMP_IN=$(mktemp)
TMP_OUT=$(mktemp)
cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -500000 > "$TMP_IN" 2>/dev/null || true
sort "$TMP_IN" > "$TMP_OUT"
LINE_COUNT=$(wc -l < "$TMP_OUT")
echo "[bash_sort] Sorted $LINE_COUNT lines"
rm -f "$TMP_IN" "$TMP_OUT"
"""

BASH_NETWORK_CURL = """\
echo "[bash_network] HTTP HEAD requests"
for HOST in google.com amazon.com github.com; do
    CODE=$(curl -o /dev/null -s -w "%{http_code}" --max-time 5 "https://$HOST" || echo "ERR")
    echo "[bash_network] $HOST → $CODE"
done
"""

BASH_COMPRESS_STRESS = """\
echo "[bash_compress] Compress + decompress random data"
TMP=$(mktemp /tmp/stress_compress.XXXXXX)
TMP_GZ="$TMP.gz"
dd if=/dev/urandom bs=1M count=64 2>/dev/null | gzip -9 > "$TMP_GZ"
SIZE_GZ=$(stat -c%s "$TMP_GZ")
gunzip -c "$TMP_GZ" > "$TMP"
SIZE_RAW=$(stat -c%s "$TMP")
echo "[bash_compress] raw=${SIZE_RAW}B gz=${SIZE_GZ}B ratio=$(echo "scale=2; $SIZE_GZ * 100 / $SIZE_RAW" | bc)%"
rm -f "$TMP" "$TMP_GZ"
"""

BASH_PROCESS_FORK = """\
echo "[bash_fork] Spawning 20 background subshells"
pids=()
for i in $(seq 1 20); do
    (
        SUM=0
        for j in $(seq 1 10000); do SUM=$((SUM + j)); done
        echo "[bash_fork] worker $i done, sum=$SUM"
    ) &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done
echo "[bash_fork] All subshells joined"
"""

BASH_FIND_STRESS = """\
echo "[bash_find] Recursive find on filesystem"
COUNT=$(find /usr /lib /etc -type f 2>/dev/null | wc -l || echo 0)
echo "[bash_find] Found $COUNT files"
"""

# ──────────────���───────────────────────────────────────────────────────────────
#  DAG PROFILE CATALOGUE
#  Each profile drives a unique DAG topology with different task mixes.
# ──────────────────────────────────────────────────────────────────────────────


def _make_dag(dag_id: str, profile: int) -> DAG:
    """Build and return one stress-test DAG based on the profile index."""

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule=STRESS_SCHEDULE,
        start_date=datetime(2025, 1, 1),
        max_active_runs=MAX_ACTIVE_RUNS,
        catchup=CATCHUP,
        tags=["stress-test", f"profile-{profile % 13}", "ec2-benchmark"],
        description=f"Stress-test DAG #{dag_id} – profile {profile}",
    )

    with dag:
        # ── PROFILE 0 : CPU heavy fan-out ────────────────────────────────────
        if profile % 13 == 0:
            t1 = PythonOperator(task_id="prime_sieve", python_callable=cpu_prime_sieve, op_kwargs={"limit": 300_000})
            t2 = PythonOperator(task_id="matrix_multiply", python_callable=cpu_matrix_multiply, op_kwargs={"size": 180})
            t3 = PythonOperator(task_id="sha_hash_loop", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 80_000})
            t4 = PythonOperator(task_id="sorting_stress", python_callable=cpu_sorting_stress, op_kwargs={"n": 600_000})
            t5 = PythonOperator(task_id="fibonacci", python_callable=cpu_fibonacci_recursive, op_kwargs={"n": 35})
            t6 = BashOperator(task_id="bash_cpu_stress", bash_command=BASH_CPU_STRESS)
            t7 = PythonOperator(task_id="memory_alloc", python_callable=memory_large_allocation, op_kwargs={"mb": 200})
            t8 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)
            t9 = PythonOperator(task_id="dict_thrash", python_callable=memory_dict_thrash, op_kwargs={"entries": 400_000})
            t10 = PythonOperator(task_id="xcom_producer", python_callable=xcom_producer)
            t11 = PythonOperator(task_id="xcom_consumer", python_callable=xcom_consumer)
            t12 = BashOperator(task_id="bash_network", bash_command=BASH_NETWORK_CURL)
            t13 = PythonOperator(task_id="disk_stress", python_callable=io_disk_stress, op_kwargs={"size_mb": 64})

            t1 >> [t2, t3, t4]
            t2 >> t5
            t3 >> t6
            t4 >> t7
            [t5, t6, t7] >> t8
            t8 >> [t9, t10]
            t10 >> t11
            [t9, t11] >> t12
            t12 >> t13

        # ── PROFILE 1 : I/O heavy pipeline ───────────────────────────────────
        elif profile % 13 == 1:
            t1 = PythonOperator(task_id="disk_stress_lg", python_callable=io_disk_stress, op_kwargs={"size_mb": 128})
            t2 = BashOperator(task_id="bash_disk_io", bash_command=BASH_DISK_IO)
            t3 = PythonOperator(task_id="file_pipeline", python_callable=io_temp_file_pipeline)
            t4 = PythonOperator(task_id="dir_walk", python_callable=io_recursive_dir_walk)
            t5 = PythonOperator(task_id="dns_resolve", python_callable=io_network_dns_resolve)
            t6 = BashOperator(task_id="bash_find", bash_command=BASH_FIND_STRESS)
            t7 = BashOperator(task_id="bash_compress", bash_command=BASH_COMPRESS_STRESS)
            t8 = PythonOperator(task_id="sleep_wait", python_callable=sleep_io_wait, op_kwargs={"seconds": 8})
            t9 = PythonOperator(task_id="mixed_pipeline", python_callable=mixed_cpu_io_pipeline)
            t10 = BashOperator(task_id="bash_network", bash_command=BASH_NETWORK_CURL)
            t11 = PythonOperator(task_id="disk_stress_sm", python_callable=io_disk_stress, op_kwargs={"size_mb": 32})
            t12 = PythonOperator(task_id="string_processing", python_callable=memory_string_processing, op_kwargs={"count": 200_000})
            t13 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)

            t1 >> t2 >> t3
            t3 >> [t4, t5]
            t4 >> t6
            t5 >> t7
            [t6, t7] >> t8
            t8 >> [t9, t10]
            t9 >> t11
            t10 >> t12
            [t11, t12] >> t13

        # ── PROFILE 2 : Memory pressure ───────────────────────────────────────
        elif profile % 13 == 2:
            t1 = PythonOperator(task_id="mem_alloc_256", python_callable=memory_large_allocation, op_kwargs={"mb": 256})
            t2 = PythonOperator(task_id="dict_thrash_lg", python_callable=memory_dict_thrash, op_kwargs={"entries": 600_000})
            t3 = PythonOperator(task_id="string_proc", python_callable=memory_string_processing, op_kwargs={"count": 300_000})
            t4 = PythonOperator(task_id="mem_alloc_128", python_callable=memory_large_allocation, op_kwargs={"mb": 128})
            t5 = BashOperator(task_id="bash_mem_stress", bash_command=BASH_MEMORY_STRESS)
            t6 = PythonOperator(task_id="sha_hash", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 50_000})
            t7 = PythonOperator(task_id="sorting_600k", python_callable=cpu_sorting_stress, op_kwargs={"n": 600_000})
            t8 = PythonOperator(task_id="xcom_producer", python_callable=xcom_producer)
            t9 = PythonOperator(task_id="xcom_consumer", python_callable=xcom_consumer)
            t10 = PythonOperator(task_id="dict_thrash_sm", python_callable=memory_dict_thrash, op_kwargs={"entries": 200_000})
            t11 = BashOperator(task_id="bash_compress", bash_command=BASH_COMPRESS_STRESS)
            t12 = PythonOperator(task_id="disk_stress", python_callable=io_disk_stress, op_kwargs={"size_mb": 48})
            t13 = PythonOperator(task_id="dns_resolve", python_callable=io_network_dns_resolve)

            [t1, t2] >> t3
            t3 >> [t4, t5]
            t4 >> t6
            t5 >> t7
            [t6, t7] >> t8
            t8 >> t9
            t9 >> [t10, t11]
            t10 >> t12
            [t11, t12] >> t13

        # ── PROFILE 3 : Bash-dominant ─────────────────────────────────────────
        elif profile % 13 == 3:
            t1 = BashOperator(task_id="bash_cpu_a", bash_command=BASH_CPU_STRESS)
            t2 = BashOperator(task_id="bash_cpu_b", bash_command=BASH_CPU_STRESS)
            t3 = BashOperator(task_id="bash_mem", bash_command=BASH_MEMORY_STRESS)
            t4 = BashOperator(task_id="bash_disk", bash_command=BASH_DISK_IO)
            t5 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)
            t6 = BashOperator(task_id="bash_fork", bash_command=BASH_PROCESS_FORK)
            t7 = BashOperator(task_id="bash_compress", bash_command=BASH_COMPRESS_STRESS)
            t8 = BashOperator(task_id="bash_find", bash_command=BASH_FIND_STRESS)
            t9 = BashOperator(task_id="bash_network", bash_command=BASH_NETWORK_CURL)
            t10 = PythonOperator(task_id="python_cpu", python_callable=cpu_prime_sieve)
            t11 = PythonOperator(task_id="python_mem", python_callable=memory_large_allocation, op_kwargs={"mb": 128})
            t12 = PythonOperator(task_id="python_io", python_callable=io_disk_stress)
            t13 = PythonOperator(task_id="mixed_pipeline", python_callable=mixed_cpu_io_pipeline)

            [t1, t2] >> t3
            t3 >> [t4, t5]
            [t4, t5] >> t6
            t6 >> [t7, t8]
            [t7, t8] >> t9
            t9 >> [t10, t11]
            t10 >> t12
            t11 >> t13

        # ── PROFILE 4 : Mixed realistic ETL ───────────────────────────────────
        elif profile % 13 == 4:
            t1 = PythonOperator(task_id="extract_simulate", python_callable=io_disk_stress, op_kwargs={"size_mb": 96})
            t2 = PythonOperator(task_id="transform_cpu", python_callable=cpu_matrix_multiply, op_kwargs={"size": 150})
            t3 = PythonOperator(task_id="transform_hash", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 60_000})
            t4 = PythonOperator(task_id="transform_sort", python_callable=cpu_sorting_stress, op_kwargs={"n": 400_000})
            t5 = PythonOperator(task_id="load_simulate", python_callable=io_temp_file_pipeline)
            t6 = PythonOperator(task_id="validate_dns", python_callable=io_network_dns_resolve)
            t7 = BashOperator(task_id="bash_compress", bash_command=BASH_COMPRESS_STRESS)
            t8 = PythonOperator(task_id="xcom_producer", python_callable=xcom_producer)
            t9 = PythonOperator(task_id="xcom_consumer", python_callable=xcom_consumer)
            t10 = PythonOperator(task_id="mem_buffer", python_callable=memory_large_allocation, op_kwargs={"mb": 192})
            t11 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)
            t12 = PythonOperator(task_id="dict_thrash", python_callable=memory_dict_thrash, op_kwargs={"entries": 350_000})
            t13 = BashOperator(task_id="bash_find", bash_command=BASH_FIND_STRESS)

            t1 >> [t2, t3, t4]
            t2 >> t5
            t3 >> t6
            t4 >> t7
            [t5, t6, t7] >> t8
            t8 >> t9
            t9 >> [t10, t11]
            [t10, t11] >> t12
            t12 >> t13

        # ── PROFILE 5 : Deep sequential chain ────────────────────────────────
        elif profile % 13 == 5:
            tasks = [
                PythonOperator(task_id="step_01_prime", python_callable=cpu_prime_sieve, op_kwargs={"limit": 150_000}),
                BashOperator(task_id="step_02_cpu", bash_command=BASH_CPU_STRESS),
                PythonOperator(task_id="step_03_mem", python_callable=memory_large_allocation, op_kwargs={"mb": 128}),
                BashOperator(task_id="step_04_disk", bash_command=BASH_DISK_IO),
                PythonOperator(task_id="step_05_hash", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 40_000}),
                PythonOperator(task_id="step_06_sort", python_callable=cpu_sorting_stress, op_kwargs={"n": 300_000}),
                BashOperator(task_id="step_07_fork", bash_command=BASH_PROCESS_FORK),
                PythonOperator(task_id="step_08_dict", python_callable=memory_dict_thrash, op_kwargs={"entries": 300_000}),
                PythonOperator(task_id="step_09_io_walk", python_callable=io_recursive_dir_walk),
                BashOperator(task_id="step_10_compress", bash_command=BASH_COMPRESS_STRESS),
                PythonOperator(task_id="step_11_dns", python_callable=io_network_dns_resolve),
                PythonOperator(task_id="step_12_xcom_p", python_callable=xcom_producer),
                PythonOperator(task_id="step_13_xcom_c", python_callable=xcom_consumer),
            ]
            for i in range(len(tasks) - 1):
                tasks[i] >> tasks[i + 1]

        # ── PROFILE 6 : Wide fan-out then join ───────────────────────────────
        elif profile % 13 == 6:
            start = BashOperator(task_id="kickoff", bash_command="echo 'Stress fan-out start'")
            parallel = [
                PythonOperator(
                    task_id=f"parallel_prime_{i}",
                    python_callable=cpu_prime_sieve,
                    op_kwargs={"limit": 80_000 + i * 5_000},
                )
                for i in range(5)
            ] + [
                PythonOperator(
                    task_id=f"parallel_mem_{i}",
                    python_callable=memory_large_allocation,
                    op_kwargs={"mb": 64 + i * 16},
                )
                for i in range(4)
            ]
            join = PythonOperator(task_id="join_hash", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 30_000})
            end1 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)
            end2 = PythonOperator(task_id="disk_stress", python_callable=io_disk_stress, op_kwargs={"size_mb": 48})
            end3 = BashOperator(task_id="bash_network", bash_command=BASH_NETWORK_CURL)

            start >> parallel
            for t in parallel:
                t >> join
            join >> [end1, end2, end3]

        # ── PROFILE 7 : Sleep / IO wait simulation ────────────────────────────
        elif profile % 13 == 7:
            t1 = PythonOperator(task_id="cpu_warmup", python_callable=cpu_prime_sieve, op_kwargs={"limit": 100_000})
            t2 = PythonOperator(task_id="sleep_3s", python_callable=sleep_io_wait, op_kwargs={"seconds": 3})
            t3 = PythonOperator(task_id="disk_while_wait", python_callable=io_disk_stress, op_kwargs={"size_mb": 48})
            t4 = PythonOperator(task_id="sleep_5s", python_callable=sleep_io_wait, op_kwargs={"seconds": 5})
            t5 = PythonOperator(task_id="mem_alloc", python_callable=memory_large_allocation, op_kwargs={"mb": 160})
            t6 = BashOperator(task_id="bash_compress", bash_command=BASH_COMPRESS_STRESS)
            t7 = PythonOperator(task_id="sleep_7s", python_callable=sleep_io_wait, op_kwargs={"seconds": 7})
            t8 = PythonOperator(task_id="dns_resolve", python_callable=io_network_dns_resolve)
            t9 = BashOperator(task_id="bash_fork", bash_command=BASH_PROCESS_FORK)
            t10 = PythonOperator(task_id="sleep_4s", python_callable=sleep_io_wait, op_kwargs={"seconds": 4})
            t11 = PythonOperator(task_id="xcom_producer", python_callable=xcom_producer)
            t12 = PythonOperator(task_id="xcom_consumer", python_callable=xcom_consumer)
            t13 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)

            t1 >> [t2, t3]
            t2 >> t4
            t3 >> t5
            [t4, t5] >> t6
            t6 >> t7
            t7 >> [t8, t9]
            [t8, t9] >> t10
            t10 >> t11
            t11 >> t12
            t12 >> t13

        # ── PROFILE 8 : XCom-heavy metadata stress ────────────────────────────
        elif profile % 13 == 8:
            producers = [
                PythonOperator(task_id=f"xcom_prod_{i}", python_callable=xcom_producer) for i in range(5)
            ]
            consumers = [
                PythonOperator(task_id=f"xcom_cons_{i}", python_callable=xcom_consumer) for i in range(5)
            ]
            mid1 = BashOperator(task_id="bash_cpu", bash_command=BASH_CPU_STRESS)
            mid2 = PythonOperator(task_id="hash_loop", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 30_000})
            mid3 = PythonOperator(task_id="disk_stress", python_callable=io_disk_stress, op_kwargs={"size_mb": 48})

            for p, c in zip(producers, consumers):
                p >> c
            producers[0] >> mid1
            producers[2] >> mid2
            mid1 >> mid3
            mid2 >> mid3
            for c in consumers:
                c >> mid1

        # ── PROFILE 9 : CPU + compress marathon ───────────────────────────────
        elif profile % 13 == 9:
            t1 = BashOperator(task_id="bash_compress_a", bash_command=BASH_COMPRESS_STRESS)
            t2 = BashOperator(task_id="bash_compress_b", bash_command=BASH_COMPRESS_STRESS)
            t3 = PythonOperator(task_id="matrix_200", python_callable=cpu_matrix_multiply, op_kwargs={"size": 200})
            t4 = PythonOperator(task_id="sha_100k", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 100_000})
            t5 = BashOperator(task_id="bash_sort", bash_command=BASH_SORT_LARGE)
            t6 = PythonOperator(task_id="prime_400k", python_callable=cpu_prime_sieve, op_kwargs={"limit": 400_000})
            t7 = PythonOperator(task_id="fib_36", python_callable=cpu_fibonacci_recursive, op_kwargs={"n": 36})
            t8 = BashOperator(task_id="bash_fork", bash_command=BASH_PROCESS_FORK)
            t9 = PythonOperator(task_id="mem_alloc", python_callable=memory_large_allocation, op_kwargs={"mb": 224})
            t10 = PythonOperator(task_id="disk_96mb", python_callable=io_disk_stress, op_kwargs={"size_mb": 96})
            t11 = PythonOperator(task_id="dict_thrash", python_callable=memory_dict_thrash, op_kwargs={"entries": 500_000})
            t12 = PythonOperator(task_id="dns_resolve", python_callable=io_network_dns_resolve)
            t13 = PythonOperator(task_id="mixed_pipeline", python_callable=mixed_cpu_io_pipeline)

            [t1, t2] >> [t3, t4]
            t3 >> t5
            t4 >> t6
            t5 >> t7
            t6 >> t8
            [t7, t8] >> t9
            t9 >> [t10, t11]
            [t10, t11] >> t12
            t12 >> t13

        # ── PROFILE 10 : Network + disk I/O dominant ─────────────────────────
        elif profile % 13 == 10:
            t1 = PythonOperator(task_id="dns_a", python_callable=io_network_dns_resolve)
            t2 = BashOperator(task_id="bash_network", bash_command=BASH_NETWORK_CURL)
            t3 = PythonOperator(task_id="file_pipeline", python_callable=io_temp_file_pipeline)
            t4 = PythonOperator(task_id="disk_128mb", python_callable=io_disk_stress, op_kwargs={"size_mb": 128})
            t5 = PythonOperator(task_id="dir_walk", python_callable=io_recursive_dir_walk)
            t6 = BashOperator(task_id="bash_find", bash_command=BASH_FIND_STRESS)
            t7 = BashOperator(task_id="bash_disk", bash_command=BASH_DISK_IO)
            t8 = PythonOperator(task_id="sleep_6s", python_callable=sleep_io_wait, op_kwargs={"seconds": 6})
            t9 = PythonOperator(task_id="mixed_pipeline", python_callable=mixed_cpu_io_pipeline)
            t10 = PythonOperator(task_id="prime_sieve", python_callable=cpu_prime_sieve)
            t11 = BashOperator(task_id="bash_compress", bash_command=BASH_COMPRESS_STRESS)
            t12 = PythonOperator(task_id="mem_alloc", python_callable=memory_large_allocation, op_kwargs={"mb": 192})
            t13 = PythonOperator(task_id="dict_thrash", python_callable=memory_dict_thrash, op_kwargs={"entries": 400_000})

            [t1, t2] >> t3
            t3 >> [t4, t5]
            t4 >> t6
            t5 >> t7
            [t6, t7] >> t8
            t8 >> [t9, t10]
            t9 >> t11
            t10 >> t12
            [t11, t12] >> t13

        # ── PROFILE 11 : Short burst + high parallelism ───────────────────────
        elif profile % 13 == 11:
            start = PythonOperator(task_id="start_hash", python_callable=cpu_sha_hash_loop, op_kwargs={"iterations": 20_000})
            burst = [
                BashOperator(
                    task_id=f"burst_bash_{i}",
                    bash_command=f"echo 'burst {i}'; sleep 2; dd if=/dev/urandom bs=4M count=1 2>/dev/null | md5sum",
                )
                for i in range(6)
            ] + [
                PythonOperator(
                    task_id=f"burst_py_{i}",
                    python_callable=cpu_sorting_stress,
                    op_kwargs={"n": 100_000 + i * 10_000},
                )
                for i in range(5)
            ]
            end = BashOperator(task_id="final_compress", bash_command=BASH_COMPRESS_STRESS)

            start >> burst
            for t in burst:
                t >> end

        # ── PROFILE 12 : Fully random mix ─────────────────────────────────────
        else:
            py_callables = [
                (cpu_prime_sieve, {"limit": random.randint(100_000, 300_000)}),
                (cpu_matrix_multiply, {"size": random.randint(120, 200)}),
                (cpu_sha_hash_loop, {"iterations": random.randint(30_000, 100_000)}),
                (cpu_sorting_stress, {"n": random.randint(200_000, 600_000)}),
                (memory_large_allocation, {"mb": random.randint(64, 256)}),
                (memory_string_processing, {"count": random.randint(50_000, 300_000)}),
                (memory_dict_thrash, {"entries": random.randint(100_000, 500_000)}),
                (io_disk_stress, {"size_mb": random.randint(32, 128)}),
                (io_temp_file_pipeline, {}),
                (io_recursive_dir_walk, {}),
                (io_network_dns_resolve, {}),
                (mixed_cpu_io_pipeline, {}),
                (sleep_io_wait, {"seconds": random.uniform(2, 8)}),
            ]
            random.shuffle(py_callables)

            bash_cmds = [
                BASH_CPU_STRESS,
                BASH_DISK_IO,
                BASH_SORT_LARGE,
                BASH_COMPRESS_STRESS,
                BASH_PROCESS_FORK,
            ]
            random.shuffle(bash_cmds)

            tasks: list = []
            for idx, (fn, kw) in enumerate(py_callables[:8]):
                tasks.append(PythonOperator(task_id=f"py_task_{idx:02d}", python_callable=fn, op_kwargs=kw))
            for idx, cmd in enumerate(bash_cmds[:5]):
                tasks.append(BashOperator(task_id=f"bash_task_{idx:02d}", bash_command=cmd))

            # Build a random but connected DAG: chain + random cross-edges
            for i in range(1, len(tasks)):
                tasks[i - 1] >> tasks[i]
            # Add a few extra edges for branching
            if len(tasks) >= 6:
                tasks[0] >> tasks[3]
                tasks[2] >> tasks[5]

    return dag


# ──────────────────────────────────────────────────────────────────────────────
#  DYNAMIC DAG REGISTRATION
#  Airflow discovers any DAG object in module global scope.
# ──────────────────────────────────────────────────────────────────────────────

for _idx in range(TOTAL_DAGS):
    _dag_id = f"stress_test_{_idx:03d}"
    _profile = _idx  # cycles through 13 profiles
    _dag_obj = _make_dag(_dag_id, _profile)
    globals()[_dag_id] = _dag_obj  # register in module scope
