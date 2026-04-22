import math
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from redis import Redis, WatchError

from ._internal import reversed_dotted_parts

UNIT_MAP = {
    's': 1.0,
    'm': 60.0,
    'h': 3600.0,
    'd': 86400.0,
}

RATE_LIMIT_RE = re.compile(r'^(\d+)/(\d*)(s|m|h|d)$')

REJECTION_RETENTION_SECONDS = 86400


def parse_rate_limit(rate_limit_str: str) -> Tuple[int, float]:
    match = RATE_LIMIT_RE.match(rate_limit_str)
    if not match:
        raise ValueError(f'Invalid rate limit format: {rate_limit_str}')

    count = int(match.group(1))
    multiplier_str = match.group(2)
    unit = match.group(3)

    if count <= 0:
        raise ValueError(f'Rate limit count must be positive: {count}')

    base = UNIT_MAP[unit]
    if multiplier_str:
        multiplier = int(multiplier_str)
        if multiplier <= 0:
            raise ValueError(f'Rate limit window multiplier must be positive: {multiplier}')
        window = base * multiplier
    else:
        window = base

    if window <= 0:
        raise ValueError(f'Rate limit window must be positive: {window}')

    return (count, window)


def format_rate_limit(count: int, window: float) -> str:
    if window == 1.0:
        return f'{count}/s'
    if window == 60.0:
        return f'{count}/m'
    if window == 3600.0:
        return f'{count}/h'
    if window == 86400.0:
        return f'{count}/d'
    if window == int(window):
        iw = int(window)
        if iw % 86400 == 0:
            return f'{count}/{iw // 86400}d'
        if iw % 3600 == 0:
            return f'{count}/{iw // 3600}h'
        if iw % 60 == 0:
            return f'{count}/{iw // 60}m'
        return f'{count}/{iw}s'
    return f'{count}/{int(math.ceil(window))}s'


class RateLimitInfo:
    __slots__ = ('limit', 'window', 'remaining', 'used', 'reset_at', 'key', 'burst_limit')

    def __init__(
        self,
        limit: int,
        window: float,
        remaining: int,
        used: int,
        reset_at: float,
        key: str,
        burst_limit: int,
    ) -> None:
        self.limit = limit
        self.window = window
        self.remaining = remaining
        self.used = used
        self.reset_at = reset_at
        self.key = key
        self.burst_limit = burst_limit

    def to_dict(self) -> Dict[str, Any]:
        return {
            'limit': self.limit,
            'window': self.window,
            'remaining': self.remaining,
            'used': self.used,
            'reset_at': self.reset_at,
            'key': self.key,
            'burst_limit': self.burst_limit,
        }

    @property
    def is_exhausted(self) -> bool:
        return self.remaining <= 0

    @property
    def utilization(self) -> float:
        if self.limit == 0:
            return 1.0
        return self.used / self.limit

    def __repr__(self) -> str:
        return (
            f'RateLimitInfo(limit={self.limit}, window={self.window}, '
            f'used={self.used}, remaining={self.remaining})'
        )


class RateLimiter:
    def __init__(self, redis: Redis, key_prefix: str) -> None:
        self._redis = redis
        self._key_prefix = key_prefix

    def _rate_limit_key(self, name: str) -> str:
        return f'{self._key_prefix}:rate_limit:{name}'

    def _config_key(self, name: str) -> str:
        return f'{self._key_prefix}:rate_limit_config:{name}'

    def _penalty_key(self, name: str) -> str:
        return f'{self._key_prefix}:rate_limit_penalty:{name}'

    def _history_key(self, name: str) -> str:
        return f'{self._key_prefix}:rate_limit_history:{name}'

    def resolve_queue_config(
        self, queue: str, rate_limits_config: Dict[str, str]
    ) -> Tuple[Optional[str], str]:
        config_val = self._redis.get(self._config_key(queue))
        if config_val:
            return (config_val, queue)

        for part in reversed_dotted_parts(queue):
            config_val = self._redis.get(self._config_key(part))
            if config_val:
                return (config_val, part)

        for part in reversed_dotted_parts(queue):
            if part in rate_limits_config:
                return (rate_limits_config[part], part)

        return (None, queue)

    def _resolve_queue_rate_limit(
        self, queue: str, rate_limits_config: Dict[str, str]
    ) -> Optional[Tuple[str, int, float, str]]:
        config_val, effective_queue = self.resolve_queue_config(queue, rate_limits_config)
        if config_val is None:
            return None
        count, window = parse_rate_limit(config_val)
        return (self._rate_limit_key(effective_queue), count, window, effective_queue)

    def _resolve_task_rate_limit(
        self, serialized_func: str, task_rate_limit_str: Optional[str]
    ) -> Optional[Tuple[str, int, float]]:
        if not task_rate_limit_str:
            return None
        count, window = parse_rate_limit(task_rate_limit_str)
        key = self._rate_limit_key(f'func:{serialized_func}')
        return (key, count, window)

    def get_effective_limits(
        self,
        queue: str,
        serialized_func: str,
        task_rate_limit: Optional[str],
        rate_limits_config: Dict[str, str],
        burst_config: Optional[Dict[str, int]] = None,
    ) -> List[Tuple[str, int, float]]:
        limits: List[Tuple[str, int, float]] = []

        queue_limit = self._resolve_queue_rate_limit(queue, rate_limits_config)
        if queue_limit:
            key, count, window, effective_queue = queue_limit
            if burst_config:
                count += burst_config.get(effective_queue, 0)
            limits.append((key, count, window))

        task_limit = self._resolve_task_rate_limit(serialized_func, task_rate_limit)
        if task_limit:
            limits.append(task_limit)

        return limits

    def _compute_retry_after(self, key: str, window: float, now: float) -> float:
        oldest = self._redis.zrange(key, 0, 0, withscores=True)
        retry_after = 0.0
        if oldest:
            oldest_score = oldest[0][1]
            retry_after = max(0.0, oldest_score + window - now)
        return retry_after

    def consume(
        self, key: str, count: int, window: float, amount: int = 1
    ) -> Tuple[bool, float]:
        return self.consume_multi([(key, count, window)], amount)

    def consume_with_burst(
        self,
        key: str,
        count: int,
        window: float,
        burst: int,
        amount: int = 1,
    ) -> Tuple[bool, float]:
        return self.consume_multi([(key, count + burst, window)], amount)

    def consume_multi(
        self, limits: List[Tuple[str, int, float]], amount: int = 1
    ) -> Tuple[bool, float]:
        if not limits:
            return (True, 0.0)

        keys = [key for key, _, _ in limits]

        for _ in range(5):
            now = time.time()

            clean_pipe = self._redis.pipeline(False)
            for key, _, window in limits:
                clean_pipe.zremrangebyscore(key, 0, now - window)
            clean_pipe.execute()

            try:
                pipe = self._redis.pipeline(True)
                pipe.watch(*keys)

                counts = {}
                for key, _, _ in limits:
                    counts[key] = pipe.zcard(key)

                max_retry_after = 0.0
                all_allowed = True
                for key, count, window in limits:
                    if counts[key] + amount > count:
                        all_allowed = False
                        oldest = pipe.zrange(key, 0, 0, withscores=True)
                        if oldest:
                            oldest_score = oldest[0][1]
                            max_retry_after = max(
                                max_retry_after,
                                max(0.0, oldest_score + window - now),
                            )
                        break

                if not all_allowed:
                    pipe.unwatch()
                    return (False, max_retry_after)

                pipe.multi()
                for key, count, window in limits:
                    for i in range(amount):
                        member = f'{now}:{counts[key] + i + 1}:{random.randint(0, 999999)}'
                        pipe.zadd(key, {member: now})
                    pipe.expire(key, int(window) + 10)
                pipe.execute()
                return (True, 0.0)

            except WatchError:
                continue

        return (False, 0.0)

    def get_status(self, key: str, count: int, window: float) -> Dict[str, Any]:
        now = time.time()
        window_start = now - window
        self._redis.zremrangebyscore(key, 0, window_start)
        used = self._redis.zcard(key)
        remaining = max(0, count - used)
        reset_at = now + window
        return {
            'limit': count,
            'window': window,
            'remaining': remaining,
            'used': used,
            'reset_at': reset_at,
        }

    def get_detailed_status(
        self, key: str, count: int, window: float, burst: int = 0
    ) -> RateLimitInfo:
        now = time.time()
        window_start = now - window
        self._redis.zremrangebyscore(key, 0, window_start)
        used = self._redis.zcard(key)
        effective = count + burst
        remaining = max(0, effective - used)
        reset_at = now + window
        return RateLimitInfo(
            limit=count,
            window=window,
            remaining=remaining,
            used=used,
            reset_at=reset_at,
            key=key,
            burst_limit=burst,
        )

    def set_rate_limit(self, name: str, rate_limit_str: str) -> None:
        parse_rate_limit(rate_limit_str)
        self._redis.set(self._config_key(name), rate_limit_str)

    def get_rate_limit(self, name: str) -> Optional[str]:
        val = self._redis.get(self._config_key(name))
        if val is None:
            return None
        return val

    def clear_rate_limit(self, name: str) -> None:
        self._redis.delete(self._config_key(name))

    def set_bulk_rate_limits(self, limits: Dict[str, str]) -> None:
        for rate_limit_str in limits.values():
            parse_rate_limit(rate_limit_str)
        pipe = self._redis.pipeline(True)
        for name, rate_limit_str in limits.items():
            pipe.set(self._config_key(name), rate_limit_str)
        pipe.execute()

    def get_bulk_rate_limits(self, names: List[str]) -> Dict[str, Optional[str]]:
        pipe = self._redis.pipeline(True)
        for name in names:
            pipe.get(self._config_key(name))
        results = pipe.execute()
        return {name: val for name, val in zip(names, results)}

    def clear_bulk_rate_limits(self, names: List[str]) -> None:
        pipe = self._redis.pipeline(True)
        for name in names:
            pipe.delete(self._config_key(name))
        pipe.execute()

    def record_penalty(self, name: str, ttl: int = 300) -> int:
        key = self._penalty_key(name)
        pipe = self._redis.pipeline(True)
        pipe.incr(key)
        pipe.expire(key, ttl)
        results = pipe.execute()
        return results[0]

    def get_penalty_count(self, name: str) -> int:
        val = self._redis.get(self._penalty_key(name))
        if val is None:
            return 0
        return int(val)

    def compute_backoff_delay(
        self,
        name: str,
        base_delay: float,
        max_delay: float = 60.0,
        factor: float = 2.0,
    ) -> float:
        violations = self.get_penalty_count(name)
        if violations <= 0:
            return base_delay
        delay = base_delay * (factor ** min(violations, 10))
        return min(delay, max_delay)

    def record_rejection(self, name: str, now: Optional[float] = None) -> None:
        if now is None:
            now = time.time()
        key = self._history_key(name)
        member = f'{now}:{random.randint(0, 999999)}'
        pipe = self._redis.pipeline(True)
        pipe.zadd(key, {member: now})
        pipe.zremrangebyscore(key, 0, now - REJECTION_RETENTION_SECONDS)
        pipe.expire(key, REJECTION_RETENTION_SECONDS + 60)
        pipe.execute()

    def get_rejection_count(self, name: str, window: float = 3600.0) -> int:
        now = time.time()
        window_start = now - window
        key = self._history_key(name)
        return self._redis.zcount(key, window_start, '+inf')

    def get_rejection_rate(self, name: str, window: float = 60.0) -> float:
        count = self.get_rejection_count(name, window)
        if window <= 0:
            return 0.0
        return count / window

    def estimate_wait_time(self, key: str, count: int, window: float) -> float:
        now = time.time()
        window_start = now - window
        self._redis.zremrangebyscore(key, 0, window_start)
        current = self._redis.zcard(key)
        if current < count:
            return 0.0
        return self._compute_retry_after(key, window, now)

    def peek(self, key: str, count: int, window: float, amount: int = 1) -> bool:
        now = time.time()
        window_start = now - window
        pipe = self._redis.pipeline(True)
        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zcard(key)
        results = pipe.execute()
        current = results[1]
        return current + amount <= count

    def reset_window(self, key: str) -> None:
        self._redis.delete(key)

    def get_all_configured_limits(self) -> Dict[str, str]:
        pattern = f'{self._key_prefix}:rate_limit_config:*'
        result: Dict[str, str] = {}
        prefix_len = len(f'{self._key_prefix}:rate_limit_config:')
        cursor = 0
        while True:
            cursor, keys = self._redis.scan(cursor, match=pattern, count=100)
            if keys:
                pipe = self._redis.pipeline(True)
                for k in keys:
                    pipe.get(k)
                values = pipe.execute()
                for k, v in zip(keys, values):
                    if v is not None:
                        name = k[prefix_len:]
                        result[name] = v
            if cursor == 0:
                break
        return result

    def validate_rate_limit_str(self, rate_limit_str: str) -> bool:
        try:
            parse_rate_limit(rate_limit_str)
            return True
        except ValueError:
            return False
