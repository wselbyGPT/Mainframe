from __future__ import annotations

from datetime import datetime, timezone
import unittest

from common.slo import compute_slo_report


class SLOMathEdgeCaseTests(unittest.TestCase):
    def test_no_traffic_keeps_objectives_met_with_full_budget(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        report = compute_slo_report(jobs=[], events_by_job_id={}, now=now)

        for objective in report['objectives']:
            self.assertEqual(objective['status'], 'met')
            self.assertEqual(objective['error_budget']['remaining_ratio'], 1.0)
            self.assertIsNone(objective['burn_rate']['short'])
            self.assertIsNone(objective['windows']['24h']['sli'])

    def test_sparse_samples_report_burn_only_where_samples_exist(self) -> None:
        now = datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc)
        jobs = [
            {
                'id': 'j1',
                'state': 'completed',
                'result': 'success',
                'stage': 'done',
                'created_at': '2026-01-01T00:00:00+00:00',
                'finished_at': '2026-01-01T00:01:00+00:00',
                'updated_at': '2026-01-01T00:01:00+00:00',
            }
        ]
        report = compute_slo_report(jobs=jobs, events_by_job_id={'j1': []}, now=now)
        availability = next(item for item in report['objectives'] if item['objective_id'] == 'availability')

        self.assertEqual(availability['windows']['24h']['total'], 1)
        self.assertEqual(availability['windows']['5m']['total'], 0)
        self.assertIsNone(availability['burn_rate']['short'])
        self.assertEqual(availability['burn_rate']['long'], 0.0)

    def test_clock_skew_negative_stage_duration_is_ignored(self) -> None:
        now = datetime(2026, 1, 1, 0, 20, tzinfo=timezone.utc)
        jobs = [
            {
                'id': 'j2',
                'state': 'completed',
                'result': 'success',
                'stage': 'done',
                'created_at': '2026-01-01T00:00:00+00:00',
                'finished_at': '2026-01-01T00:10:00+00:00',
                'updated_at': '2026-01-01T00:10:00+00:00',
            }
        ]
        events = {
            'j2': [
                {'ts': '2026-01-01T00:05:00+00:00', 'payload': {'state': 'waiting_for_completion', 'stage': 'waiting_for_completion'}},
                {'ts': '2026-01-01T00:04:00+00:00', 'payload': {'state': 'reading_spool', 'stage': 'reading_spool'}},
            ]
        }
        report = compute_slo_report(jobs=jobs, events_by_job_id=events, now=now)
        latency = next(item for item in report['objectives'] if item['objective_id'] == 'latency_poll')

        self.assertEqual(latency['windows']['24h']['total'], 0)
        self.assertEqual(latency['status'], 'met')

    def test_partial_windows_only_count_recent_events(self) -> None:
        now = datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)
        jobs = [
            {
                'id': 'old',
                'state': 'failed',
                'result': 'error',
                'stage': 'unexpected',
                'created_at': '2025-12-30T20:00:00+00:00',
                'finished_at': '2025-12-30T21:00:00+00:00',
                'updated_at': '2025-12-30T21:00:00+00:00',
            },
            {
                'id': 'new',
                'state': 'completed',
                'result': 'success',
                'stage': 'done',
                'created_at': '2026-01-01T23:30:00+00:00',
                'finished_at': '2026-01-01T23:45:00+00:00',
                'updated_at': '2026-01-01T23:45:00+00:00',
            },
        ]

        report = compute_slo_report(jobs=jobs, events_by_job_id={'old': [], 'new': []}, now=now)
        availability = next(item for item in report['objectives'] if item['objective_id'] == 'availability')

        self.assertEqual(availability['windows']['24h']['total'], 1)
        self.assertEqual(availability['windows']['24h']['good'], 1)
        self.assertEqual(availability['status'], 'met')


if __name__ == '__main__':
    unittest.main()
