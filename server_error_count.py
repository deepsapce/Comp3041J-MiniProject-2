import sys
from collections import defaultdict
from utils import parse_log_line
import oss2
from oss2.credentials import EnvironmentVariableCredentialsProvider
import time


def read_log_from_oss(bucket_name, object_key):
    auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())
    endpoint = 'oss-cn-beijing.aliyuncs.com'
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    content = bucket.get_object(object_key).read().decode('utf-8')
    lines = content.splitlines()
    if lines and ('timestamp' in lines[0].lower() or 'request_id' in lines[0].lower()):
        lines = lines[1:]
    return lines


def mapper(line):
    record = parse_log_line(line)
    if record:
        try:
            status = int(record['status_code'])
        except ValueError:
            status = 0
        if status >= 500:
            return (record['service_name'], 1)
    return None


def reducer(shuffled):
    return {k: sum(v) for k, v in shuffled.items()}

def main(bucket_name, object_key, output_file):
    start_time = time.time()
    lines = read_log_from_oss(bucket_name, object_key)
    mapped = []
    for line in lines:
        kv = mapper(line)
        if kv:
            mapped.append(kv)
    shuffle_dict = defaultdict(list)
    for k, v in mapped:
        shuffle_dict[k].append(v)
    result = reducer(shuffle_dict)
    with open(output_file, 'w') as f:
        for service in sorted(result.keys()):
            f.write(f"{service} {result[service]}\n")
    elapsed = time.time() - start_time
    print(elapsed)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(1)

    bucket_name = sys.argv[1]
    object_key = sys.argv[2]
    output_file = sys.argv[3]

    main(bucket_name, object_key, output_file)