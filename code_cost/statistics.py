import redis


redis_config = {
    "host": "r-bp1121c34842c834470.redis.rds.aliyuncs.com",
    "port": 6379,
    "password": "skD87d718RcuJBnDyvBbM9xx",
    "db": 1,
}

redis_client = redis.Redis(
    host=redis_config["host"],
    port=redis_config["port"],
    password=redis_config["password"],
    db=redis_config["db"],
)

def get_redis_client():
    return redis_client

def get_all_keys_with_prefix(prefix):
    return redis_client.keys(f"{prefix}*")

def delete_all_keys_with_prefix(prefix):
    keys = get_all_keys_with_prefix(prefix)
    for key in keys:
        redis_client.delete(key)
        print(f"Deleted key: {key}")

def get_all_keys_with_prefix(prefix):
    return redis_client.keys(f"{prefix}*")

# file submission_id.csv stores the submission_id
def get_submission_id_from_file(file_path):
    with open(file_path, "r") as file:
        return [line.strip() for line in file.readlines()]

def get_cost_time_from_redis(submission_id):
    key = f"creation_judger_submit_{submission_id}"
    value = redis_client.hget(key,"costTime")
    if value is None:
        return None
    try:
        value = int(value)
        return value
    except ValueError:
        return None

def get_all_cost_time_from_file(file_path):
    submission_ids = get_submission_id_from_file(file_path)
    print(f"Submission ids: {submission_ids}")
    cost_times = []
    for submission_id in submission_ids:
        cost_time = get_cost_time_from_redis(submission_id)
        if cost_time is None:
            print(f"Cost time is None for submission_id: {submission_id}")
            continue
        try:
            cost_time = int(cost_time)
            cost_times.append(cost_time)
        except ValueError:
            print(f"Cost time is not a number for submission_id: {submission_id}")
            continue
    
    return cost_times

# cost_times是整数数组
def get_max_and_p95(cost_times):
    if not cost_times:
        return None, None
    max_value = max(cost_times)
    sorted_times = sorted(cost_times)
    n = len(sorted_times)
    # p95 index: the smallest k such that k/n >= 0.95 (1-based)
    p95_index = int(n * 0.95) - 1
    if p95_index < 0:
        p95_index = 0
    elif p95_index >= n:
        p95_index = n - 1
    p95_value = sorted_times[p95_index]
    return max_value, p95_value

def main():
    # get all cost time from submission_id.csv
    # cost_times = get_all_cost_time_from_file("/Users/fanny/projects/stress/code_cost/submission_id.csv")
    # 删除所有以creation_judger_submit_开头的key
    # delete_all_keys_with_prefix("creation_judger_submit_")
    # 获取submission_id.csv中的submission_id再redis中对应的costTime
    cost_times = get_all_cost_time_from_file("/Users/fanny/projects/stress/code_cost/submission_id.csv")
    print(cost_times)
    max,p95 = get_max_and_p95(cost_times)
    print(f"max: {max}, p95: {p95}")

if __name__ == "__main__":
    main()

