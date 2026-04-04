import time

import pytest

from tasktiger import RateLimitedException, Task, Worker
from tasktiger._internal import ACTIVE, ERROR, QUEUED, SCHEDULED
from tasktiger.rate_limiter import (
    RateLimitInfo,
    RateLimiter,
    format_rate_limit,
    parse_rate_limit,
)

from .tasks import simple_task
from .utils import external_worker, get_tiger

_tiger = get_tiger()


@_tiger.task(rate_limit='2/s')
def rate_limited_task():
    pass


@_tiger.task(rate_limit='5/m')
def rate_limited_slow_task():
    pass


@_tiger.task(queue='rate_limited_queue')
def queue_rate_limited_task():
    pass


class TestParseRateLimit:
    def test_per_second(self):
        assert parse_rate_limit('5/s') == (5, 1.0)

    def test_per_minute(self):
        assert parse_rate_limit('100/m') == (100, 60.0)

    def test_per_hour(self):
        assert parse_rate_limit('1000/h') == (1000, 3600.0)

    def test_per_day(self):
        assert parse_rate_limit('10000/d') == (10000, 86400.0)

    def test_custom_window_seconds(self):
        assert parse_rate_limit('10/30s') == (10, 30.0)

    def test_custom_window_minutes(self):
        assert parse_rate_limit('50/5m') == (50, 300.0)

    def test_custom_window_hours(self):
        assert parse_rate_limit('500/2h') == (500, 7200.0)

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            parse_rate_limit('invalid')

    def test_zero_count(self):
        with pytest.raises(ValueError):
            parse_rate_limit('0/s')

    def test_negative_count(self):
        with pytest.raises(ValueError):
            parse_rate_limit('-1/s')

    def test_missing_slash(self):
        with pytest.raises(ValueError):
            parse_rate_limit('100s')

    def test_empty_string(self):
        with pytest.raises(ValueError):
            parse_rate_limit('')

    def test_only_slash(self):
        with pytest.raises(ValueError):
            parse_rate_limit('/')

    def test_invalid_unit(self):
        with pytest.raises(ValueError):
            parse_rate_limit('10/w')

    def test_large_count(self):
        assert parse_rate_limit('1000000/s') == (1000000, 1.0)

    def test_large_custom_window(self):
        assert parse_rate_limit('10/3600s') == (10, 3600.0)


class TestFormatRateLimit:
    def test_format_per_second(self):
        assert format_rate_limit(5, 1.0) == '5/s'

    def test_format_per_minute(self):
        assert format_rate_limit(100, 60.0) == '100/m'

    def test_format_per_hour(self):
        assert format_rate_limit(1000, 3600.0) == '1000/h'

    def test_format_per_day(self):
        assert format_rate_limit(10000, 86400.0) == '10000/d'

    def test_format_custom_seconds(self):
        assert format_rate_limit(10, 30.0) == '10/30s'

    def test_format_custom_minutes(self):
        assert format_rate_limit(50, 300.0) == '50/5m'

    def test_format_custom_hours(self):
        assert format_rate_limit(500, 7200.0) == '500/2h'

    def test_roundtrip_per_second(self):
        count, window = parse_rate_limit('5/s')
        assert format_rate_limit(count, window) == '5/s'

    def test_roundtrip_per_minute(self):
        count, window = parse_rate_limit('100/m')
        assert format_rate_limit(count, window) == '100/m'

    def test_roundtrip_custom(self):
        count, window = parse_rate_limit('10/30s')
        assert format_rate_limit(count, window) == '10/30s'


class TestRateLimiter:
    @pytest.fixture(autouse=True)
    def setup(self, tiger):
        self.tiger = tiger
        self.conn = tiger.connection
        self.limiter = tiger.rate_limiter

    def test_consume_within_limit(self):
        allowed, retry_after = self.limiter.consume(
            self.limiter._rate_limit_key('test'), 5, 60.0
        )
        assert allowed is True
        assert retry_after == 0.0

    def test_consume_exceeds_limit(self):
        key = self.limiter._rate_limit_key('test_exceed')
        for _ in range(3):
            self.limiter.consume(key, 3, 60.0)
        allowed, retry_after = self.limiter.consume(key, 3, 60.0)
        assert allowed is False
        assert retry_after > 0

    def test_sliding_window_expiry(self):
        key = self.limiter._rate_limit_key('test_expiry')
        for _ in range(3):
            self.limiter.consume(key, 3, 0.5)
        allowed, _ = self.limiter.consume(key, 3, 0.5)
        assert allowed is False
        time.sleep(0.6)
        allowed, _ = self.limiter.consume(key, 3, 0.5)
        assert allowed is True

    def test_get_status(self):
        key = self.limiter._rate_limit_key('test_status')
        self.limiter.consume(key, 10, 60.0)
        self.limiter.consume(key, 10, 60.0)
        status = self.limiter.get_status(key, 10, 60.0)
        assert status['limit'] == 10
        assert status['used'] == 2
        assert status['remaining'] == 8
        assert status['window'] == 60.0

    def test_set_and_get_rate_limit(self):
        self.limiter.set_rate_limit('myqueue', '100/m')
        assert self.limiter.get_rate_limit('myqueue') == '100/m'

    def test_clear_rate_limit_removes_config(self):
        self.limiter.set_rate_limit('myqueue', '100/m')
        self.limiter.clear_rate_limit('myqueue')
        assert self.limiter.get_rate_limit('myqueue') is None

    def test_clear_rate_limit_preserves_window(self):
        self.limiter.set_rate_limit('myqueue', '100/m')
        key = self.limiter._rate_limit_key('myqueue')
        self.limiter.consume(key, 100, 60.0)
        self.limiter.consume(key, 100, 60.0)
        self.limiter.clear_rate_limit('myqueue')
        assert self.conn.exists(key) == 1

    def test_consume_amount_greater_than_one(self):
        key = self.limiter._rate_limit_key('test_amount')
        allowed, _ = self.limiter.consume(key, 5, 60.0, amount=3)
        assert allowed is True
        allowed, _ = self.limiter.consume(key, 5, 60.0, amount=3)
        assert allowed is False

    def test_consume_exact_limit(self):
        key = self.limiter._rate_limit_key('test_exact')
        for _ in range(5):
            allowed, _ = self.limiter.consume(key, 5, 60.0)
            assert allowed is True
        allowed, retry_after = self.limiter.consume(key, 5, 60.0)
        assert allowed is False
        assert retry_after > 0

    def test_consume_amount_equals_limit(self):
        key = self.limiter._rate_limit_key('test_amount_eq')
        allowed, _ = self.limiter.consume(key, 5, 60.0, amount=5)
        assert allowed is True
        allowed, _ = self.limiter.consume(key, 5, 60.0, amount=1)
        assert allowed is False

    def test_consume_amount_exceeds_limit(self):
        key = self.limiter._rate_limit_key('test_amount_over')
        allowed, retry_after = self.limiter.consume(key, 3, 60.0, amount=4)
        assert allowed is False

    def test_retry_after_decreases_over_time(self):
        key = self.limiter._rate_limit_key('test_retry_decay')
        for _ in range(3):
            self.limiter.consume(key, 3, 1.0)
        _, retry1 = self.limiter.consume(key, 3, 1.0)
        time.sleep(0.3)
        _, retry2 = self.limiter.consume(key, 3, 1.0)
        assert retry2 < retry1

    def test_consume_with_burst(self):
        key = self.limiter._rate_limit_key('test_burst')
        for _ in range(5):
            allowed, _ = self.limiter.consume_with_burst(key, 3, 60.0, burst=2)
            assert allowed is True
        allowed, _ = self.limiter.consume_with_burst(key, 3, 60.0, burst=2)
        assert allowed is False

    def test_consume_with_burst_zero(self):
        key = self.limiter._rate_limit_key('test_burst_zero')
        for _ in range(3):
            self.limiter.consume_with_burst(key, 3, 60.0, burst=0)
        allowed, _ = self.limiter.consume_with_burst(key, 3, 60.0, burst=0)
        assert allowed is False

    def test_consume_multi_atomic_all_or_nothing(self):
        key_a = self.limiter._rate_limit_key('multi_a')
        key_b = self.limiter._rate_limit_key('multi_b')
        for _ in range(3):
            self.limiter.consume(key_b, 3, 60.0)
        allowed, _ = self.limiter.consume_multi([
            (key_a, 5, 60.0),
            (key_b, 3, 60.0),
        ])
        assert allowed is False
        status_a = self.limiter.get_status(key_a, 5, 60.0)
        assert status_a['used'] == 0

    def test_consume_multi_commits_all_on_success(self):
        key_a = self.limiter._rate_limit_key('multi_ok_a')
        key_b = self.limiter._rate_limit_key('multi_ok_b')
        allowed, _ = self.limiter.consume_multi([
            (key_a, 5, 60.0),
            (key_b, 5, 60.0),
        ])
        assert allowed is True
        assert self.limiter.get_status(key_a, 5, 60.0)['used'] == 1
        assert self.limiter.get_status(key_b, 5, 60.0)['used'] == 1

    def test_set_invalid_rate_limit(self):
        with pytest.raises(ValueError):
            self.limiter.set_rate_limit('q', 'bad')

    def test_get_nonexistent_rate_limit(self):
        assert self.limiter.get_rate_limit('nonexistent') is None

    def test_clear_nonexistent_rate_limit(self):
        self.limiter.clear_rate_limit('nonexistent')

    def test_set_bulk_rate_limits(self):
        self.limiter.set_bulk_rate_limits({
            'queue_a': '10/s',
            'queue_b': '20/m',
            'queue_c': '100/h',
        })
        assert self.limiter.get_rate_limit('queue_a') == '10/s'
        assert self.limiter.get_rate_limit('queue_b') == '20/m'
        assert self.limiter.get_rate_limit('queue_c') == '100/h'

    def test_set_bulk_rate_limits_invalid(self):
        with pytest.raises(ValueError):
            self.limiter.set_bulk_rate_limits({
                'queue_a': '10/s',
                'queue_b': 'invalid',
            })

    def test_get_bulk_rate_limits(self):
        self.limiter.set_rate_limit('q1', '5/s')
        self.limiter.set_rate_limit('q2', '10/m')
        result = self.limiter.get_bulk_rate_limits(['q1', 'q2', 'q3'])
        assert result['q1'] == '5/s'
        assert result['q2'] == '10/m'
        assert result['q3'] is None

    def test_clear_bulk_rate_limits(self):
        self.limiter.set_bulk_rate_limits({'x': '1/s', 'y': '2/s'})
        self.limiter.clear_bulk_rate_limits(['x', 'y'])
        assert self.limiter.get_rate_limit('x') is None
        assert self.limiter.get_rate_limit('y') is None

    def test_penalty_tracking(self):
        count = self.limiter.record_penalty('myq')
        assert count == 1
        count = self.limiter.record_penalty('myq')
        assert count == 2
        assert self.limiter.get_penalty_count('myq') == 2

    def test_penalty_count_zero_default(self):
        assert self.limiter.get_penalty_count('nothing') == 0

    def test_compute_backoff_delay(self):
        base = 1.0
        delay = self.limiter.compute_backoff_delay('nopenalty', base)
        assert delay == base

        self.limiter.record_penalty('backoff_q')
        delay = self.limiter.compute_backoff_delay('backoff_q', base)
        assert delay == 2.0

        self.limiter.record_penalty('backoff_q')
        delay = self.limiter.compute_backoff_delay('backoff_q', base)
        assert delay == 4.0

    def test_compute_backoff_max_cap(self):
        for _ in range(20):
            self.limiter.record_penalty('cap_q')
        delay = self.limiter.compute_backoff_delay('cap_q', 1.0, max_delay=10.0)
        assert delay == 10.0

    def test_rejection_recording(self):
        self.limiter.record_rejection('rejq')
        self.limiter.record_rejection('rejq')
        self.limiter.record_rejection('rejq')
        assert self.limiter.get_rejection_count('rejq', 60.0) == 3

    def test_rejection_rate(self):
        for _ in range(10):
            self.limiter.record_rejection('rate_q')
        rate = self.limiter.get_rejection_rate('rate_q', 60.0)
        assert abs(rate - 10.0 / 60.0) < 0.01

    def test_rejection_rate_zero_window(self):
        assert self.limiter.get_rejection_rate('any', 0.0) == 0.0

    def test_estimate_wait_time_no_entries(self):
        key = self.limiter._rate_limit_key('empty')
        wait = self.limiter.estimate_wait_time(key, 5, 60.0)
        assert wait == 0.0

    def test_estimate_wait_time_under_limit(self):
        key = self.limiter._rate_limit_key('partial')
        self.limiter.consume(key, 5, 60.0)
        wait = self.limiter.estimate_wait_time(key, 5, 60.0)
        assert wait == 0.0

    def test_estimate_wait_time_at_limit(self):
        key = self.limiter._rate_limit_key('full')
        for _ in range(5):
            self.limiter.consume(key, 5, 60.0)
        wait = self.limiter.estimate_wait_time(key, 5, 60.0)
        assert wait > 0

    def test_peek_under_limit(self):
        key = self.limiter._rate_limit_key('peek_ok')
        assert self.limiter.peek(key, 5, 60.0) is True
        self.limiter.consume(key, 5, 60.0)
        assert self.limiter.peek(key, 5, 60.0) is True

    def test_peek_at_limit(self):
        key = self.limiter._rate_limit_key('peek_full')
        for _ in range(5):
            self.limiter.consume(key, 5, 60.0)
        assert self.limiter.peek(key, 5, 60.0) is False

    def test_peek_does_not_consume(self):
        key = self.limiter._rate_limit_key('peek_nomod')
        self.limiter.peek(key, 5, 60.0)
        self.limiter.peek(key, 5, 60.0)
        self.limiter.peek(key, 5, 60.0)
        status = self.limiter.get_status(key, 5, 60.0)
        assert status['used'] == 0

    def test_reset_window(self):
        key = self.limiter._rate_limit_key('reset')
        for _ in range(5):
            self.limiter.consume(key, 5, 60.0)
        assert self.limiter.peek(key, 5, 60.0) is False
        self.limiter.reset_window(key)
        assert self.limiter.peek(key, 5, 60.0) is True

    def test_get_detailed_status(self):
        key = self.limiter._rate_limit_key('detail')
        self.limiter.consume(key, 10, 60.0)
        self.limiter.consume(key, 10, 60.0)
        info = self.limiter.get_detailed_status(key, 10, 60.0)
        assert isinstance(info, RateLimitInfo)
        assert info.limit == 10
        assert info.used == 2
        assert info.remaining == 8
        assert info.burst_limit == 0
        assert info.is_exhausted is False
        assert 0 < info.utilization < 1.0

    def test_get_detailed_status_with_burst(self):
        key = self.limiter._rate_limit_key('detail_burst')
        for _ in range(3):
            self.limiter.consume(key, 3, 60.0)
        info = self.limiter.get_detailed_status(key, 3, 60.0, burst=5)
        assert info.burst_limit == 5
        assert info.remaining == 5

    def test_rate_limit_info_exhausted(self):
        key = self.limiter._rate_limit_key('exhausted')
        for _ in range(5):
            self.limiter.consume(key, 5, 60.0)
        info = self.limiter.get_detailed_status(key, 5, 60.0)
        assert info.is_exhausted is True
        assert info.utilization == 1.0

    def test_rate_limit_info_repr(self):
        info = RateLimitInfo(10, 60.0, 8, 2, time.time() + 60, 'k', 0)
        r = repr(info)
        assert 'limit=10' in r
        assert 'used=2' in r

    def test_rate_limit_info_to_dict(self):
        info = RateLimitInfo(10, 60.0, 8, 2, time.time() + 60, 'k', 0)
        d = info.to_dict()
        assert d['limit'] == 10
        assert d['used'] == 2
        assert d['remaining'] == 8
        assert d['window'] == 60.0
        assert 'key' in d
        assert 'burst_limit' in d

    def test_get_all_configured_limits(self):
        self.limiter.set_bulk_rate_limits({
            'a': '1/s',
            'b': '2/m',
        })
        all_limits = self.limiter.get_all_configured_limits()
        assert all_limits['a'] == '1/s'
        assert all_limits['b'] == '2/m'

    def test_get_all_configured_limits_empty(self):
        assert self.limiter.get_all_configured_limits() == {}

    def test_validate_rate_limit_str(self):
        assert self.limiter.validate_rate_limit_str('10/s') is True
        assert self.limiter.validate_rate_limit_str('bad') is False
        assert self.limiter.validate_rate_limit_str('0/s') is False
        assert self.limiter.validate_rate_limit_str('100/5m') is True

    def test_key_isolation(self):
        key_a = self.limiter._rate_limit_key('a')
        key_b = self.limiter._rate_limit_key('b')
        for _ in range(5):
            self.limiter.consume(key_a, 5, 60.0)
        allowed, _ = self.limiter.consume(key_a, 5, 60.0)
        assert allowed is False
        allowed, _ = self.limiter.consume(key_b, 5, 60.0)
        assert allowed is True


class TestRateLimitIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, tiger, ensure_queues):
        self.tiger = tiger
        self.conn = tiger.connection
        self._ensure_queues = ensure_queues

    def test_task_rate_limit_property(self):
        task = Task(self.tiger, rate_limited_task)
        assert task.rate_limit == '2/s'

    def test_task_rate_limit_from_delay(self):
        task = Task(self.tiger, simple_task, rate_limit='10/m')
        assert task.rate_limit == '10/m'

    def test_task_no_rate_limit(self):
        task = Task(self.tiger, simple_task)
        assert task.rate_limit is None

    def test_delay_with_rate_limit_kwarg(self):
        task = self.tiger.delay(simple_task, rate_limit='10/m')
        assert task.rate_limit == '10/m'
        self._ensure_queues(queued={'default': 1})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_rate_limit_decorator(self):
        self.tiger.delay(rate_limited_task)
        self._ensure_queues(queued={'default': 1})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_rate_limited_tasks_rescheduled(self):
        for _ in range(5):
            self.tiger.delay(rate_limited_task)
        self._ensure_queues(queued={'default': 5})
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:default')
        assert scheduled_count > 0

    def test_queue_rate_limit_via_config(self):
        self.tiger.config['RATE_LIMITS'] = {'rate_limited_queue': '2/s'}
        for _ in range(5):
            self.tiger.delay(queue_rate_limited_task)
        self._ensure_queues(queued={'rate_limited_queue': 5})
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:rate_limited_queue')
        assert scheduled_count > 0

    def test_queue_rate_limit_subqueue_inheritance(self):
        self.tiger.config['RATE_LIMITS'] = {'api': '2/s'}
        for _ in range(5):
            self.tiger.delay(simple_task, queue='api.v1')
        self._ensure_queues(queued={'api.v1': 5})
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:api.v1')
        assert scheduled_count > 0

    def test_dynamic_parent_queue_limit_subqueue_inheritance(self):
        self.tiger.set_queue_rate_limit('api', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='api.v2')
        self._ensure_queues(queued={'api.v2': 5})
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:api.v2')
        assert scheduled_count > 0

    def test_dynamic_rate_limit(self):
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:default')
        assert scheduled_count > 0

    def test_clear_queue_rate_limit(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        self.tiger.clear_queue_rate_limit('default')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_get_rate_limit_status(self):
        self.tiger.set_queue_rate_limit('default', '10/m')
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        status = self.tiger.get_rate_limit_status('default')
        assert status is not None
        assert status['limit'] == 10
        assert status['used'] >= 1

    def test_get_rate_limit_status_no_limit(self):
        status = self.tiger.get_rate_limit_status('default')
        assert status is None

    def test_get_queue_rate_limit(self):
        self.tiger.set_queue_rate_limit('myq', '50/h')
        assert self.tiger.get_queue_rate_limit('myq') == '50/h'

    def test_get_queue_rate_limit_none(self):
        assert self.tiger.get_queue_rate_limit('nonexistent') is None

    def test_rate_limited_tasks_eventually_execute(self):
        for _ in range(3):
            self.tiger.delay(rate_limited_task)
        Worker(self.tiger).run(once=True)
        time.sleep(1.2)
        Worker(self.tiger).run(once=True)
        queued_count = self.conn.zcard('t:queued:default')
        scheduled_count = self.conn.zcard('t:scheduled:default')
        active_count = self.conn.zcard('t:active:default')
        total_remaining = queued_count + scheduled_count + active_count
        assert total_remaining < 3

    def test_rate_limit_retry_delay_config(self):
        self.tiger.config['RATE_LIMIT_RETRY_DELAY'] = 2.0
        self.tiger.config['RATE_LIMITS'] = {'default': '1/s'}
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        scheduled = self.conn.zrange('t:scheduled:default', 0, -1, withscores=True)
        if scheduled:
            now = time.time()
            for _, score in scheduled:
                assert score >= now + 1.5

    def test_dynamic_overrides_static_config(self):
        self.tiger.config['RATE_LIMITS'] = {'default': '100/s'}
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:default')
        assert scheduled_count > 0

    def test_set_bulk_queue_rate_limits(self):
        self.tiger.set_bulk_queue_rate_limits({
            'q1': '10/s',
            'q2': '20/m',
        })
        assert self.tiger.get_queue_rate_limit('q1') == '10/s'
        assert self.tiger.get_queue_rate_limit('q2') == '20/m'

    def test_get_bulk_queue_rate_limits(self):
        self.tiger.set_queue_rate_limit('a', '5/s')
        self.tiger.set_queue_rate_limit('b', '10/m')
        result = self.tiger.get_bulk_queue_rate_limits(['a', 'b', 'c'])
        assert result['a'] == '5/s'
        assert result['b'] == '10/m'
        assert result['c'] is None

    def test_clear_bulk_queue_rate_limits(self):
        self.tiger.set_bulk_queue_rate_limits({'x': '1/s', 'y': '2/s'})
        self.tiger.clear_bulk_queue_rate_limits(['x', 'y'])
        assert self.tiger.get_queue_rate_limit('x') is None
        assert self.tiger.get_queue_rate_limit('y') is None

    def test_get_rate_limit_detailed_status(self):
        self.tiger.set_queue_rate_limit('default', '10/m')
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        info = self.tiger.get_rate_limit_detailed_status('default')
        assert isinstance(info, RateLimitInfo)
        assert info.limit == 10
        assert info.used >= 1
        assert info.is_exhausted is False

    def test_get_rate_limit_detailed_status_no_limit(self):
        assert self.tiger.get_rate_limit_detailed_status('default') is None

    def test_get_rate_limit_detailed_status_with_burst(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'default': 5}
        self.tiger.set_queue_rate_limit('default', '3/m')
        info = self.tiger.get_rate_limit_detailed_status('default')
        assert info.burst_limit == 5

    def test_queue_rejection_count(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        count = self.tiger.get_queue_rejection_count('default', 60.0)
        assert count > 0

    def test_queue_rejection_count_no_rejections(self):
        count = self.tiger.get_queue_rejection_count('default', 60.0)
        assert count == 0

    def test_queue_rejection_rate(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rate = self.tiger.get_queue_rejection_rate('default', 60.0)
        assert rate > 0

    def test_estimate_queue_wait_time_no_limit(self):
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait == 0.0

    def test_estimate_queue_wait_time_under_limit(self):
        self.tiger.set_queue_rate_limit('default', '100/m')
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait == 0.0

    def test_peek_queue_rate_limit_no_limit(self):
        assert self.tiger.peek_queue_rate_limit('default') is True

    def test_peek_queue_rate_limit_under(self):
        self.tiger.set_queue_rate_limit('default', '100/m')
        assert self.tiger.peek_queue_rate_limit('default') is True

    def test_peek_queue_rate_limit_exhausted(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('default') is False

    def test_reset_queue_rate_limit_window(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('default') is False
        self.tiger.reset_queue_rate_limit_window('default')
        assert self.tiger.peek_queue_rate_limit('default') is True

    def test_get_all_queue_rate_limits(self):
        self.tiger.config['RATE_LIMITS'] = {'static_q': '10/s'}
        self.tiger.set_queue_rate_limit('dynamic_q', '20/m')
        all_limits = self.tiger.get_all_queue_rate_limits()
        assert all_limits['static_q'] == '10/s'
        assert all_limits['dynamic_q'] == '20/m'

    def test_get_all_queue_rate_limits_dynamic_overrides(self):
        self.tiger.config['RATE_LIMITS'] = {'q': '10/s'}
        self.tiger.set_queue_rate_limit('q', '20/m')
        all_limits = self.tiger.get_all_queue_rate_limits()
        assert all_limits['q'] == '20/m'

    def test_validate_rate_limit(self):
        assert self.tiger.validate_rate_limit('10/s') is True
        assert self.tiger.validate_rate_limit('bad') is False

    def test_burst_config_allows_more_than_base(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'default': 3}
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(8):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        scheduled_count = self.conn.zcard('t:scheduled:default')
        executed = 8 - scheduled_count
        assert executed > 2
        assert executed <= 5

    def test_backoff_produces_growing_delays(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 2.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 60.0
        self.tiger.config['RATE_LIMITS'] = {'default': '1/s'}
        for _ in range(4):
            self.tiger.delay(simple_task)
        now = time.time()
        Worker(self.tiger).run(once=True)
        scheduled = self.conn.zrange('t:scheduled:default', 0, -1, withscores=True)
        assert len(scheduled) >= 2
        delays = sorted([score - now for _, score in scheduled])
        assert delays[-1] > 3.0
        for i in range(1, len(delays)):
            assert delays[i] > delays[i - 1]

    def test_rate_limit_slow_task_decorator(self):
        task = Task(self.tiger, rate_limited_slow_task)
        assert task.rate_limit == '5/m'

    def test_rate_limit_preserved_in_task_data(self):
        task = Task(self.tiger, simple_task, rate_limit='50/h')
        assert task.data.get('rate_limit') == '50/h'

    def test_rate_limit_not_in_data_when_none(self):
        task = Task(self.tiger, simple_task)
        assert 'rate_limit' not in task.data

    def test_package_root_import(self):
        from tasktiger import RateLimitedException as Exc
        assert issubclass(Exc, Exception)
        from tasktiger import RateLimitInfo as Info
        assert Info is RateLimitInfo
        from tasktiger import RateLimiter as RL
        assert RL is RateLimiter
        from tasktiger import parse_rate_limit as prl
        assert prl is parse_rate_limit
