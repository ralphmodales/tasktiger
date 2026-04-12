import time

import pytest

from tasktiger import RateLimitedException, RateLimitInfo, RateLimiter, Task, Worker
from tasktiger.rate_limiter import format_rate_limit, parse_rate_limit

from .tasks import simple_task
from .utils import external_worker, get_tiger

_tiger = get_tiger()


@_tiger.task(rate_limit='2/s')
def rate_limited_task():
    pass


@_tiger.task(rate_limit='1/s')
def strict_rate_limited_task():
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


class TestRateLimitIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, tiger, ensure_queues):
        self.tiger = tiger
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
        assert self.tiger.get_queue_rejection_count('default', 60.0) > 0

    def test_queue_rate_limit_via_config(self):
        self.tiger.config['RATE_LIMITS'] = {'rate_limited_queue': '2/s'}
        for _ in range(5):
            self.tiger.delay(queue_rate_limited_task)
        self._ensure_queues(queued={'rate_limited_queue': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('rate_limited_queue') is False

    def test_queue_rate_limit_subqueue_inheritance(self):
        self.tiger.config['RATE_LIMITS'] = {'api': '2/s'}
        for _ in range(5):
            self.tiger.delay(simple_task, queue='api.v1')
        self._ensure_queues(queued={'api.v1': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('api.v1', 60.0) > 0

    def test_dynamic_parent_queue_limit_subqueue_inheritance(self):
        self.tiger.set_queue_rate_limit('api', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='api.v2')
        self._ensure_queues(queued={'api.v2': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('api.v2', 60.0) > 0

    def test_dynamic_rate_limit(self):
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('default') is False

    def test_clear_queue_rate_limit(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        self.tiger.clear_queue_rate_limit('default')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_clear_preserves_window_and_history(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_before = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_before > 0
        self.tiger.clear_queue_rate_limit('default')
        assert self.tiger.get_queue_rate_limit('default') is None
        rejections_after = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_after == rejections_before

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

    def test_status_remaining_decreases(self):
        self.tiger.set_queue_rate_limit('default', '10/m')
        status_before = self.tiger.get_rate_limit_status('default')
        assert status_before['remaining'] == 10
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        status_after = self.tiger.get_rate_limit_status('default')
        assert status_after['remaining'] < status_before['remaining']

    def test_get_queue_rate_limit(self):
        self.tiger.set_queue_rate_limit('myq', '50/h')
        assert self.tiger.get_queue_rate_limit('myq') == '50/h'

    def test_get_queue_rate_limit_none(self):
        assert self.tiger.get_queue_rate_limit('nonexistent') is None

    def test_sliding_window_expires(self):
        for _ in range(3):
            self.tiger.delay(rate_limited_task)
        Worker(self.tiger).run(once=True)
        wait_before = self.tiger.estimate_queue_wait_time('default')
        time.sleep(1.2)
        Worker(self.tiger).run(once=True)
        wait_after = self.tiger.estimate_queue_wait_time('default')
        assert wait_after < wait_before or wait_after == 0.0

    def test_rate_limit_retry_delay_config(self):
        self.tiger.config['RATE_LIMIT_RETRY_DELAY'] = 2.0
        self.tiger.config['RATE_LIMITS'] = {'default': '1/s'}
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait > 0

    def test_dynamic_overrides_static_config(self):
        self.tiger.config['RATE_LIMITS'] = {'default': '100/s'}
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('default') is False

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
        assert info is not None
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

    def test_detailed_status_shows_exhaustion(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        info = self.tiger.get_rate_limit_detailed_status('default')
        assert info.is_exhausted is True
        assert info.utilization >= 1.0

    def test_detailed_status_to_dict(self):
        self.tiger.set_queue_rate_limit('default', '10/m')
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        info = self.tiger.get_rate_limit_detailed_status('default')
        d = info.to_dict()
        assert d['limit'] == 10
        assert d['used'] >= 1
        assert 'remaining' in d
        assert 'window' in d

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

    def test_estimate_queue_wait_time_exhausted(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait > 0

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

    def test_peek_does_not_affect_capacity(self):
        self.tiger.set_queue_rate_limit('default', '100/m')
        self.tiger.peek_queue_rate_limit('default')
        self.tiger.peek_queue_rate_limit('default')
        self.tiger.peek_queue_rate_limit('default')
        status = self.tiger.get_rate_limit_status('default')
        assert status['used'] == 0

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

    def test_multi_limit_all_or_nothing(self):
        self.tiger.set_queue_rate_limit('default', '5/s')
        for _ in range(3):
            self.tiger.delay(strict_rate_limited_task)
        Worker(self.tiger).run(once=True)
        status = self.tiger.get_rate_limit_status('default')
        assert status['used'] == 1

    def test_burst_config_allows_more_than_base(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'default': 3}
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(8):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        status = self.tiger.get_rate_limit_status('default')
        assert status['used'] > 2
        assert status['used'] <= 5

    def test_backoff_produces_growing_delays(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 2.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 60.0
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections >= 2
        self.tiger.clear_queue_rate_limit('default')
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_after = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_after > rejections
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait > 0

    def test_backoff_respects_max_cap(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 100.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 5.0
        self.tiger.config['RATE_LIMITS'] = {'default': '1/s'}
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait <= 5.0 + 1.0

    def test_rate_limit_slow_task_decorator(self):
        task = Task(self.tiger, rate_limited_slow_task)
        assert task.rate_limit == '5/m'

    def test_rate_limit_stored_in_task_data(self):
        task = Task(self.tiger, simple_task, rate_limit='50/h')
        assert task.data.get('rate_limit') == '50/h'

    def test_rate_limit_absent_from_task_data_when_none(self):
        task = Task(self.tiger, simple_task)
        assert 'rate_limit' not in task.data

    def test_package_root_import(self):
        from tasktiger import RateLimitedException as Exc
        assert issubclass(Exc, Exception)
        from tasktiger import RateLimitInfo as Info
        from tasktiger import RateLimiter as RL
        from tasktiger import parse_rate_limit as prl
        assert prl('5/s') == (5, 1.0)

    def test_queue_isolation(self):
        self.tiger.set_queue_rate_limit('q_a', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='q_a')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='q_b')
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('q_a', 60.0) > 0
        assert self.tiger.get_queue_rejection_count('q_b', 60.0) == 0

    def test_set_queue_rate_limit_validates_input(self):
        with pytest.raises(ValueError):
            self.tiger.set_queue_rate_limit('default', 'garbage')
        with pytest.raises(ValueError):
            self.tiger.set_queue_rate_limit('default', '0/s')

    def test_delay_rate_limit_overrides_decorator(self):
        task = self.tiger.delay(rate_limited_task, rate_limit='100/m')
        assert task.rate_limit == '100/m'

    def test_peek_subqueue_inherits_parent_limit(self):
        self.tiger.set_queue_rate_limit('svc', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='svc.endpoint')
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('svc.endpoint') is False

    def test_status_remaining_equals_limit_minus_used(self):
        self.tiger.set_queue_rate_limit('default', '10/m')
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        status = self.tiger.get_rate_limit_status('default')
        assert status['remaining'] == status['limit'] - status['used']

    def test_rate_limit_info_repr(self):
        self.tiger.set_queue_rate_limit('default', '10/m')
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        info = self.tiger.get_rate_limit_detailed_status('default')
        r = repr(info)
        assert 'limit' in r
        assert 'used' in r

    def test_tasks_without_rate_limit_unaffected(self):
        self.tiger.config['RATE_LIMITS'] = {}
        for _ in range(5):
            self.tiger.delay(simple_task)
        self._ensure_queues(queued={'default': 5})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_backoff_disabled_by_default(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections >= 2
        self.tiger.clear_queue_rate_limit('default')
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_second = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_second > rejections

    def test_estimate_wait_time_subqueue_inherits(self):
        self.tiger.set_queue_rate_limit('parent', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='parent.child')
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('parent.child')
        assert wait > 0

    def test_validate_rate_limit_edge_cases(self):
        assert self.tiger.validate_rate_limit('1/s') is True
        assert self.tiger.validate_rate_limit('999/30s') is True
        assert self.tiger.validate_rate_limit('10/d') is True
        assert self.tiger.validate_rate_limit('') is False
        assert self.tiger.validate_rate_limit('0/s') is False
        assert self.tiger.validate_rate_limit('abc') is False
        assert self.tiger.validate_rate_limit('10/x') is False

    def test_get_all_queue_rate_limits_empty(self):
        self.tiger.config['RATE_LIMITS'] = {}
        result = self.tiger.get_all_queue_rate_limits()
        assert result == {}

    def test_burst_does_not_affect_other_queues(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'burst_q': 10}
        self.tiger.set_queue_rate_limit('burst_q', '2/s')
        self.tiger.set_queue_rate_limit('normal_q', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='burst_q')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='normal_q')
        Worker(self.tiger).run(once=True)
        status_burst = self.tiger.get_rate_limit_status('burst_q')
        status_normal = self.tiger.get_rate_limit_status('normal_q')
        assert status_burst['used'] > status_normal['used']

    def test_sliding_window_not_fixed_window(self):
        self.tiger.set_queue_rate_limit('default', '3/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})
        status = self.tiger.get_rate_limit_status('default')
        assert status['used'] == 3
        assert status['remaining'] == 0
        time.sleep(0.6)
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        status_mid = self.tiger.get_rate_limit_status('default')
        assert status_mid is not None
        assert status_mid['used'] > 0
        time.sleep(0.6)
        status_after = self.tiger.get_rate_limit_status('default')
        assert status_after['remaining'] > status['remaining']

    def test_sliding_window_partial_expiry(self):
        self.tiger.set_queue_rate_limit('default', '4/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('default') is False
        time.sleep(0.5)
        self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        time.sleep(0.6)
        assert self.tiger.peek_queue_rate_limit('default') is True
        status = self.tiger.get_rate_limit_status('default')
        assert status['remaining'] > 0

    def test_sliding_window_wait_time_decreases_over_time(self):
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait_t0 = self.tiger.estimate_queue_wait_time('default')
        assert wait_t0 > 0
        time.sleep(0.5)
        wait_t1 = self.tiger.estimate_queue_wait_time('default')
        assert wait_t1 < wait_t0 or wait_t1 == 0.0

    def test_backoff_disabled_uses_fixed_delay(self):
        self.tiger.config['RATE_LIMIT_RETRY_DELAY'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = False
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_first = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_first >= 1
        time.sleep(0.1)
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_second = self.tiger.get_queue_rejection_count('default', 60.0)
        wait_first = self.tiger.estimate_queue_wait_time('default')
        wait_second = self.tiger.estimate_queue_wait_time('default')
        assert abs(wait_first - wait_second) < 0.5

    def test_backoff_enabled_grows_delay_across_rejections(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 0.5
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 2.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 30.0
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_first = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_first >= 1
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_second = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_second > rejections_first

    def test_backoff_max_caps_delay(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 1000.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 3.0
        self.tiger.config['RATE_LIMITS'] = {'default': '1/s'}
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait <= 3.0 + 1.0

    def test_delay_kwarg_rate_limit_causes_rescheduling(self):
        for _ in range(5):
            self.tiger.delay(simple_task, rate_limit='1/s')
        Worker(self.tiger).run(once=True)
        rejections = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections > 0

    def test_task_constructor_rate_limit_causes_rescheduling(self):
        for _ in range(5):
            task = Task(self.tiger, simple_task, rate_limit='1/s')
            task.delay()
        Worker(self.tiger).run(once=True)
        rejections = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections > 0

    def test_reset_window_on_inherited_queue(self):
        self.tiger.set_queue_rate_limit('parent', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='parent.sub')
        Worker(self.tiger).run(once=True)
        assert self.tiger.peek_queue_rate_limit('parent.sub') is False
        self.tiger.reset_queue_rate_limit_window('parent.sub')
        assert self.tiger.peek_queue_rate_limit('parent.sub') is True

    def test_burst_wired_into_wait_time(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'default': 5}
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait == 0.0

    def test_format_rate_limit_multi_day(self):
        assert format_rate_limit(100, 172800.0) == '100/2d'

    def test_format_rate_limit_integer_window(self):
        assert format_rate_limit(10, 30) == '10/30s'
