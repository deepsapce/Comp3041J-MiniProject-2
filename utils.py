def parse_log_line(line):
    """解析一行日志，返回字典，字段不存在时返回None"""
    fields = line.strip().split(',')
    if len(fields) < 10:
        return None
    return {
        'timestamp': fields[0],
        'request_id': fields[1],
        'user_id': fields[2],
        'service_name': fields[3],
        'endpoint': fields[4],
        'http_method': fields[5],
        'status_code': fields[6],
        'response_time_ms': int(fields[7]) if fields[7].isdigit() else 0,
        'region': fields[8],
        'error_type': fields[9] if len(fields) > 9 else ''
    }