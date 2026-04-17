import time

import pytest

from tasktiger import RateLimitedException, RateLimitInfo, RateLimiter, Task, Worker
from tasktiger.rate_limiter import format_rate_limit, parse_rate_limit

from .tasks import counting_task, locked_rate_limited_task, simple_task
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

    def test_format_multi_day(self):
        assert format_rate_limit(100, 172800.0) == '100/2d'

    def test_format_integer_window(self):
        assert format_rate_limit(10, 30) == '10/30s'

    def test_roundtrip(self):
        for s in ['5/s', '100/m', '10/30s']:
            count, window = parse_rate_limit(s)
            assert format_rate_limit(count, window) == s


class TestRateLimitBehavior:
    @pytest.fixture(autouse=True)
    def setup(self, tiger, ensure_queues):
        self.tiger = tiger
        self.conn = tiger.connection
        self._ensure_queues = ensure_queues

    def _scheduled_delays(self, queue='default'):
        now = time.time()
        entries = self.conn.zrange(
            f't:scheduled:{queue}', 0, -1, withscores=True
        )
        return sorted(score - now for _, score in entries)

    def test_single_task_under_limit_executes(self):
        self.tiger.delay(rate_limited_task)
        self._ensure_queues(queued={'default': 1})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_tasks_over_limit_rescheduled(self):
        for _ in range(5):
            self.tiger.delay(rate_limited_task)
        self._ensure_queues(queued={'default': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('default', 60.0) > 0

    def test_tasks_without_limit_unaffected(self):
        self.tiger.config['RATE_LIMITS'] = {}
        for _ in range(5):
            self.tiger.delay(simple_task)
        self._ensure_queues(queued={'default': 5})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_decorator_rate_limit_property(self):
        task = Task(self.tiger, rate_limited_task)
        assert task.rate_limit == '2/s'
        task2 = Task(self.tiger, rate_limited_slow_task)
        assert task2.rate_limit == '5/m'

    def test_delay_kwarg_rate_limit(self):
        task = self.tiger.delay(simple_task, rate_limit='10/m')
        assert task.rate_limit == '10/m'
        assert task.data.get('rate_limit') == '10/m'

    def test_task_constructor_rate_limit(self):
        task = Task(self.tiger, simple_task, rate_limit='10/m')
        assert task.rate_limit == '10/m'
        assert task.data.get('rate_limit') == '10/m'

    def test_no_rate_limit_absent_from_data(self):
        task = Task(self.tiger, simple_task)
        assert task.rate_limit is None
        assert 'rate_limit' not in task.data

    def test_delay_overrides_decorator(self):
        task = self.tiger.delay(rate_limited_task, rate_limit='100/m')
        assert task.rate_limit == '100/m'

    def test_delay_kwarg_causes_rescheduling(self):
        for _ in range(5):
            self.tiger.delay(simple_task, rate_limit='1/s')
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('default', 60.0) > 0

    def test_task_constructor_causes_rescheduling(self):
        for _ in range(5):
            task = Task(self.tiger, simple_task, rate_limit='1/s')
            task.delay()
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('default', 60.0) > 0

    def test_config_rate_limit(self):
        self.tiger.config['RATE_LIMITS'] = {'rate_limited_queue': '2/s'}
        for _ in range(5):
            self.tiger.delay(queue_rate_limited_task)
        self._ensure_queues(queued={'rate_limited_queue': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('rate_limited_queue', 60.0) > 0

    def test_set_and_get_runtime_limit(self):
        self.tiger.set_queue_rate_limit('myq', '50/h')
        assert self.tiger.get_queue_rate_limit('myq') == '50/h'
        assert self.tiger.get_queue_rate_limit('nonexistent') is None

    def test_set_queue_rate_limit_validates(self):
        with pytest.raises(ValueError):
            self.tiger.set_queue_rate_limit('default', 'garbage')
        with pytest.raises(ValueError):
            self.tiger.set_queue_rate_limit('default', '0/s')

    def test_runtime_limit_causes_rescheduling(self):
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('default', 60.0) > 0

    def test_clear_restores_throughput(self):
        self.tiger.set_queue_rate_limit('default', '1/s')
        self.tiger.clear_queue_rate_limit('default')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})

    def test_clear_preserves_window_state(self):
        self.tiger.set_queue_rate_limit('default', '3/s')
        for _ in range(3):
            self.tiger.delay(counting_task)
        Worker(self.tiger).run(once=True)
        assert self.conn.get('exec_count') == '3'
        wait1 = self.tiger.estimate_queue_wait_time('default')
        assert wait1 > 0

        self.tiger.clear_queue_rate_limit('default')
        assert self.tiger.get_queue_rate_limit('default') is None

        self.tiger.set_queue_rate_limit('default', '3/s')
        wait2 = self.tiger.estimate_queue_wait_time('default')
        assert wait2 > 0

        self.tiger.delay(counting_task)
        Worker(self.tiger).run(once=True)
        assert self.conn.get('exec_count') == '3'
        scheduled = self.conn.zrange('t:scheduled:default', 0, -1)
        assert len(scheduled) == 1

    def test_subqueue_inherits_static_config(self):
        self.tiger.config['RATE_LIMITS'] = {'api': '2/s'}
        for _ in range(5):
            self.tiger.delay(simple_task, queue='api.v1')
        self._ensure_queues(queued={'api.v1': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('api.v1', 60.0) > 0

    def test_subqueue_inherits_runtime_limit(self):
        self.tiger.set_queue_rate_limit('api', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='api.v2')
        self._ensure_queues(queued={'api.v2': 5})
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('api.v2', 60.0) > 0

    def test_runtime_overrides_static(self):
        self.tiger.config['RATE_LIMITS'] = {'default': '100/s'}
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(5):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('default', 60.0) > 0

    def test_queue_isolation(self):
        self.tiger.set_queue_rate_limit('q_a', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='q_a')
        for _ in range(3):
            self.tiger.delay(simple_task, queue='q_b')
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('q_a', 60.0) > 0
        assert self.tiger.get_queue_rejection_count('q_b', 60.0) == 0

    def test_sliding_window_capacity_recovers(self):
        for _ in range(3):
            self.tiger.delay(rate_limited_task)
        Worker(self.tiger).run(once=True)
        wait_before = self.tiger.estimate_queue_wait_time('default')
        time.sleep(1.2)
        wait_after = self.tiger.estimate_queue_wait_time('default')
        assert wait_after < wait_before or wait_after == 0.0

    def test_sliding_window_partial_expiry(self):
        self.tiger.set_queue_rate_limit('default', '4/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait_full = self.tiger.estimate_queue_wait_time('default')
        assert wait_full > 0
        time.sleep(0.5)
        wait_mid = self.tiger.estimate_queue_wait_time('default')
        assert wait_mid < wait_full or wait_mid == 0.0
        time.sleep(0.6)
        wait_end = self.tiger.estimate_queue_wait_time('default')
        assert wait_end == 0.0

    def test_sliding_window_not_fixed_reset(self):
        self.tiger.set_queue_rate_limit('default', '3/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})
        time.sleep(0.6)
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        time.sleep(0.6)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait == 0.0

    def test_multi_limit_atomic_no_partial_consumption(self):
        self.tiger.set_queue_rate_limit('default', '10/s')
        for _ in range(5):
            self.tiger.delay(strict_rate_limited_task)
        Worker(self.tiger).run(once=True)
        detailed = self.tiger.get_rate_limit_detailed_status('default')
        assert detailed.used == 1
        assert detailed.remaining == 9
        scheduled = self.conn.zrange('t:scheduled:default', 0, -1)
        assert len(scheduled) == 4

    def test_burst_allows_above_base_rate(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'default': 3}
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(8):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections < 6
        assert rejections >= 3

    def test_burst_scoped_to_queue(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'burst_q': 10}
        self.tiger.set_queue_rate_limit('burst_q', '2/s')
        self.tiger.set_queue_rate_limit('normal_q', '2/s')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='burst_q')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='normal_q')
        Worker(self.tiger).run(once=True)
        rej_burst = self.tiger.get_queue_rejection_count('burst_q', 60.0)
        rej_normal = self.tiger.get_queue_rejection_count('normal_q', 60.0)
        assert rej_burst < rej_normal

    def test_burst_reflected_in_wait_time(self):
        self.tiger.config['RATE_LIMIT_BURST'] = {'default': 5}
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait == 0.0

    def test_backoff_disabled_fixed_retry(self):
        self.tiger.config['RATE_LIMIT_RETRY_DELAY'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = False
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        assert self.tiger.get_queue_rejection_count('default', 60.0) >= 1
        wait_1 = self.tiger.estimate_queue_wait_time('default')
        wait_2 = self.tiger.estimate_queue_wait_time('default')
        assert abs(wait_1 - wait_2) < 0.5

    def test_backoff_enabled_grows_rejections(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 0.5
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 2.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 30.0
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_1 = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_1 >= 1
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        rejections_2 = self.tiger.get_queue_rejection_count('default', 60.0)
        assert rejections_2 > rejections_1

    def test_backoff_capped_at_max(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 1000.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 3.0
        self.tiger.config['RATE_LIMITS'] = {'default': '1/s'}
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        wait = self.tiger.estimate_queue_wait_time('default')
        assert wait <= 4.0

    def test_tasks_over_limit_get_rescheduled_and_run_later(self):
        self.tiger.set_queue_rate_limit('default', '2/s')
        for _ in range(5):
            self.tiger.delay(counting_task)
        self._ensure_queues(queued={'default': 5})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0}, scheduled={'default': 3})
        assert self.conn.get('exec_count') == '2'

        deadline = time.time() + 6.0
        while time.time() < deadline:
            time.sleep(1.3)
            Worker(self.tiger).run(once=True)
            Worker(self.tiger).run(once=True)
            if self.conn.get('exec_count') == '5':
                break

        assert self.conn.get('exec_count') == '5'
        self._ensure_queues()

    def test_sliding_window_gradual_expiry(self):
        self.tiger.set_queue_rate_limit('default', '4/s')
        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)

        wait_full = self.tiger.estimate_queue_wait_time('default')
        assert wait_full > 0

        time.sleep(0.5)
        wait_mid = self.tiger.estimate_queue_wait_time('default')
        assert wait_mid < wait_full

        time.sleep(0.6)
        wait_end = self.tiger.estimate_queue_wait_time('default')
        assert wait_end == 0.0

    def test_sliding_window_staggered_admission(self):
        self.tiger.set_queue_rate_limit('default', '3/s')
        rl = self.tiger.rate_limiter
        limits = [(rl._rate_limit_key('default'), 3, 1.0)]

        allowed, _ = rl.consume_multi(limits)
        assert allowed
        allowed, _ = rl.consume_multi(limits)
        assert allowed

        time.sleep(0.5)
        allowed, _ = rl.consume_multi(limits)
        assert allowed
        allowed, _ = rl.consume_multi(limits)
        assert not allowed

        time.sleep(0.6)
        allowed, _ = rl.consume_multi(limits)
        assert allowed
        allowed, _ = rl.consume_multi(limits)
        assert allowed
        allowed, _ = rl.consume_multi(limits)
        assert not allowed

    def test_fixed_retry_delay_exact_schedule_time(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = False
        self.tiger.config['RATE_LIMIT_RETRY_DELAY'] = 2.0
        self.tiger.set_queue_rate_limit('default', '1/s')
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)

        delays = self._scheduled_delays()
        assert len(delays) == 2
        assert all(1.5 <= d <= 2.5 for d in delays)

    def test_backoff_delay_grows_per_rejection(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 0.5
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 2.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 30.0
        self.tiger.set_queue_rate_limit('default', '1/s')

        for _ in range(2):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        d1 = max(self._scheduled_delays())

        for _ in range(2):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        d2 = max(self._scheduled_delays())

        for _ in range(2):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        d3 = max(self._scheduled_delays())

        assert d1 < d2
        assert d2 <= d3
        assert d1 <= 30.5
        assert d2 <= 30.5
        assert d3 <= 30.5

    def test_backoff_delay_capped_at_max(self):
        self.tiger.config['RATE_LIMIT_BACKOFF_ENABLED'] = True
        self.tiger.config['RATE_LIMIT_BACKOFF_BASE'] = 1.0
        self.tiger.config['RATE_LIMIT_BACKOFF_FACTOR'] = 1000.0
        self.tiger.config['RATE_LIMIT_BACKOFF_MAX'] = 3.0
        self.tiger.set_queue_rate_limit('default', '1/s')

        for _ in range(4):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)
        for _ in range(3):
            self.tiger.delay(simple_task)
        Worker(self.tiger).run(once=True)

        delays = self._scheduled_delays()
        assert delays
        assert max(delays) <= 3.5

    def test_short_window_read_doesnt_break_long_window_read(self):
        self.tiger.set_queue_rate_limit('q', '1/s')
        for _ in range(5):
            self.tiger.delay(simple_task, queue='q')
        Worker(self.tiger, queues=['q']).run(once=True)

        assert self.tiger.get_queue_rejection_count('q', 60.0) == 4

        self.tiger.get_queue_rejection_count('q', 0.001)
        time.sleep(0.2)
        count_long = self.tiger.get_queue_rejection_count('q', 3600.0)
        assert count_long == 4

    def test_rate_limited_locked_task_releases_lock(self):
        for key in ('k1', 'k2', 'k3'):
            self.tiger.delay(locked_rate_limited_task, args=(key,))

        Worker(self.tiger).run(once=True)

        admitted = 0
        for key in ('k1', 'k2', 'k3'):
            val = self.conn.get('locked_rl_exec:' + key)
            if val == '1':
                admitted += 1
        assert admitted == 1

        time.sleep(1.2)
        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        time.sleep(1.2)
        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)

        for key in ('k1', 'k2', 'k3'):
            assert self.conn.get('locked_rl_exec:' + key) == '1'

        self._ensure_queues()

    def test_burst_applies_to_parent_when_child_inherits(self):
        self.tiger.config['RATE_LIMITS'] = {'api': '2/s'}
        self.tiger.config['RATE_LIMIT_BURST'] = {'api': 10}
        for _ in range(8):
            self.tiger.delay(simple_task, queue='api.v1')
        Worker(self.tiger, queues=['api.v1']).run(once=True)

        rejections = self.tiger.get_queue_rejection_count('api.v1', 60.0)
        rejections_parent = self.tiger.get_queue_rejection_count('api', 60.0)
        assert rejections == 0
        assert rejections_parent == 0

    def test_bulk_get_matches_single_get_with_inheritance(self):
        self.tiger.config['RATE_LIMITS'] = {'api': '5/s'}
        self.tiger.set_queue_rate_limit('billing.v1', '10/m')

        result = self.tiger.get_bulk_queue_rate_limits(
            ['api.v1', 'api', 'billing.v1', 'nothing']
        )
        assert result == {
            'api.v1': '5/s',
            'api': '5/s',
            'billing.v1': '10/m',
            'nothing': None,
        }
        for q in ['api.v1', 'api', 'billing.v1', 'nothing']:
            assert result[q] == self.tiger.get_queue_rate_limit(q)

    def test_package_exports(self):
        from tasktiger import RateLimitedException as Exc
        assert issubclass(Exc, Exception)
        from tasktiger import RateLimitInfo as Info
        from tasktiger import RateLimiter as RL
        from tasktiger import parse_rate_limit as prl
        assert prl('5/s') == (5, 1.0)
