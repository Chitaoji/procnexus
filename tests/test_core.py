import unittest
from multiprocessing import TimeoutError
from time import sleep

from src.procnexus import ProcNexus, ThreadNexus, nexus


def add(a: int, b: int) -> int:
    return a + b


def wait_then_return(seconds: float, value: int) -> int:
    sleep(seconds)
    return value


class ProcNexusTests(unittest.TestCase):
    def test_nexus_can_create_thread_nexus(self) -> None:
        job = nexus(add, processes=2, threaded=True)
        job.submit(1, 2)
        job.submit(10, 5)

        self.assertIsInstance(job, ThreadNexus)
        self.assertEqual(job.run(), [3, 15])

    def test_nexus_creates_proc_nexus_by_default(self) -> None:
        job = nexus(add, processes=0)

        self.assertIsInstance(job, ProcNexus)
        self.assertNotIsInstance(job, ThreadNexus)

    def test_thread_nexus_supports_non_picklable_callables(self) -> None:
        offset = 5
        job = nexus(lambda value: value + offset, processes=2, threaded=True)
        job.submit(1)
        job.submit(10)

        self.assertEqual(job.run(), [6, 15])

    def test_submit_after_start_with_thread_pool(self) -> None:
        job = nexus(add, processes=2, threaded=True)
        job.submit(1, 2)
        self.assertIsNone(job.start())
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15, 7])

    def test_nexus_rejects_non_bool_threaded_option(self) -> None:
        with self.assertRaisesRegex(TypeError, "invalid type for threaded"):
            nexus(add, threaded=1)

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

    def test_get_while_running_process_pool_is_rejected(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        job.start()

        with self.assertRaisesRegex(RuntimeError, "cannot get results before join"):
            job.get()

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3])

    def test_get_while_running_in_process_is_rejected(self) -> None:
        job = nexus(add, processes=0)
        job.submit(1, 2)
        job.start()

        with self.assertRaisesRegex(RuntimeError, "cannot get results before join"):
            job.get()

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3])

    def test_join_accepts_timeout_for_process_pool(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        job.submit(10, 5)
        job.start()

        self.assertIsNone(job.join(timeout=1))
        self.assertEqual(job.get(), [3, 15])

    def test_join_timeout_raises_for_process_pool(self) -> None:
        job = nexus(wait_then_return, processes=1)
        job.submit(1, 3)
        job.start()

        with self.assertRaises(TimeoutError):
            job.join(timeout=0.01)

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
