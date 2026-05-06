import unittest

from src.procnexus import nexus


def add(a: int, b: int) -> int:
    return a + b


class ProcNexusTests(unittest.TestCase):
    def test_submit_after_start_with_process_pool(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        self.assertIsNone(job.start())
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15, 7])

    def test_submit_after_start_in_process(self) -> None:
        job = nexus(add, processes=0)
        job.submit(1, 2)
        self.assertIsNone(job.start())
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15, 7])

    def test_get_waits_for_running_process_pool(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        job.start()

        self.assertEqual(job.get(), [3])

    def test_run_does_not_change_pending_state_or_consume_tasks(self) -> None:
        job = nexus(add, processes=0)
        job.submit(1, 2)
        job.submit(10, 5)

        self.assertEqual(job.run(), [3, 15])
        self.assertEqual(job.run(), [3, 15])

        self.assertIsNone(job.start())
        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15])

    def test_run_uses_process_pool_without_changing_state(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        job.submit(10, 5)

        self.assertEqual(job.run(), [3, 15])

        self.assertIsNone(job.start())
        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15])

    def test_run_preserves_keyword_arguments_with_process_pool(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, b=2)
        job.submit(a=10, b=5)

        self.assertEqual(job.run(), [3, 15])

    def test_run_after_start_is_rejected(self) -> None:
        job = nexus(add, processes=0)
        job.start()

        with self.assertRaisesRegex(RuntimeError, "cannot run a nexus"):
            job.run()

    def test_get_before_start_is_rejected(self) -> None:
        job = nexus(add, processes=0)

        with self.assertRaisesRegex(RuntimeError, "cannot get results before"):
            job.get()

    def test_submit_after_join_is_rejected(self) -> None:
        job = nexus(add, processes=0)
        job.start()
        job.join()

        with self.assertRaisesRegex(RuntimeError, "cannot submit tasks after"):
            job.submit(1, 2)


if __name__ == "__main__":
    unittest.main()
