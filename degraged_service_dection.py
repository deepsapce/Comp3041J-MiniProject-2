import ray
import sys
from collections import defaultdict
from utils import parse_log_line
import oss2
from oss2.credentials import EnvironmentVariableCredentialsProvider
import time


def write_time(output_file, elapsed_seconds):
    time_file = output_file + ".time.txt"
    with open(time_file, 'w') as f:
        f.write(f"{elapsed_seconds:.3f}\n")


def read_log_from_oss(bucket_name, object_key):
    auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())
    endpoint = 'oss-cn-beijing.aliyuncs.com'
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    content = bucket.get_object(object_key).read().decode('utf-8')
    lines = content.splitlines()
    if lines and ('timestamp' in lines[0].lower() or 'request_id' in lines[0].lower()):
        lines = lines[1:]
    return lines


def split_data_from_lines(lines, num_partitions=4):
    lines = [l for l in lines if l.strip()]
    if not lines:
        return []
    chunk_size = max(1, len(lines) // num_partitions)
    shards = []
    for i in range(0, len(lines), chunk_size):
        shards.append(lines[i:i + chunk_size])
    return shards


@ray.remote
def process_shard(shard_lines):
    local_stats = defaultdict(lambda: {'total': 0, 'slow': 0, 'err500': 0, 'timeout': 0})
    for line in shard_lines:
        rec = parse_log_line(line)
        if not rec:
            continue
        svc = rec['service_name']
        local_stats[svc]['total'] += 1
        if rec['response_time_ms'] > 800:
            local_stats[svc]['slow'] += 1
        try:
            status = int(rec['status_code'])
        except (ValueError, TypeError):
            status = 0
        if status >= 500:
            local_stats[svc]['err500'] += 1
        if rec['error_type'] == 'Timeout':
            local_stats[svc]['timeout'] += 1
    return {k: dict(v) for k, v in local_stats.items()}


def aggregate_results(partials):
    global_stats = defaultdict(lambda: {'total': 0, 'slow': 0, 'err500': 0, 'timeout': 0})
    for part in partials:
        for svc, stats in part.items():
            global_stats[svc]['total'] += stats['total']
            global_stats[svc]['slow'] += stats['slow']
            global_stats[svc]['err500'] += stats['err500']
            global_stats[svc]['timeout'] += stats['timeout']
    return global_stats


def detect_degraded(global_stats):
    degraded = []
    for svc, stats in global_stats.items():
        total = stats['total']
        if total == 0:
            continue
        slow_rate = stats['slow'] / total
        err_rate = stats['err500'] / total
        timeout_cnt = stats['timeout']
        reason = None
        if slow_rate > 0.20:
            reason = "high slow request rate"
        elif err_rate > 0.10:
            reason = "high server error rate"
        elif timeout_cnt >= 5:
            reason = "repeated timeout errors"
        if reason:
            degraded.append((svc, reason))
    return degraded


def main(bucket_name, object_key, output_file, num_partitions=4):
    ray.init(ignore_reinit_error=True)
    start_time = time.time()
    all_lines = read_log_from_oss(bucket_name, object_key)
    if not all_lines:
        print("errrrrrror")
        ray.shutdown()
        sys.exit(1)

    shards = split_data_from_lines(all_lines, num_partitions)

    futures = [process_shard.remote(shard) for shard in shards]
    partial_results = ray.get(futures)

    global_stats = aggregate_results(partial_results)

    degraded_services = detect_degraded(global_stats)

    with open(output_file, 'w') as f:
        for svc, reason in degraded_services:
            f.write(f"{svc}, {reason}\n")

    elapsed = time.time() - start_time
    write_time(output_file, elapsed)

    ray.shutdown()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit(1)

    bucket_name = sys.argv[1]
    object_key = sys.argv[2]
    output_file = sys.argv[3]
    num_parts = int(sys.argv[4]) if len(sys.argv) > 4 else 4

    main(bucket_name, object_key, output_file, num_parts)