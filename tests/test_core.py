import inspect
import unittest
from multiprocessing import TimeoutError
from time import sleep

import src.procnexus as procnexus
from src.procnexus import core, nexus


def add(a: int, b: int) -> int:
    return a + b


def wait_then_return(seconds: float, value: int) -> int:
    sleep(seconds)
    return value


class NexusTests(unittest.TestCase):
    def test_public_package_exports_only_nexus(self) -> None:
        self.assertEqual(procnexus.__all__, ["nexus"])
        self.assertIs(procnexus.nexus, nexus)
        self.assertFalse(hasattr(procnexus, "ParallelNexus"))
        self.assertFalse(hasattr(procnexus, "SerialNexus"))
        self.assertFalse(hasattr(procnexus, "ProcNexus"))
        self.assertFalse(hasattr(procnexus, "ThreadNexus"))

    def test_nexus_has_explicit_worker_count_parameters(self) -> None:
        signature = inspect.signature(nexus)

        self.assertEqual(list(signature.parameters), ["func", "processes", "threads"])
        self.assertIsNone(signature.parameters["processes"].default)
        self.assertIsNone(signature.parameters["threads"].default)

    def test_nexus_can_create_thread_nexus(self) -> None:
        job = nexus(add, threads=2)
        job.submit(1, 2)
        job.submit(10, 5)

        self.assertIsInstance(job, core.ThreadNexus)
        self.assertEqual(job.run(), [3, 15])

    def test_nexus_creates_serial_nexus_by_default(self) -> None:
        job = nexus(add)

        self.assertIsInstance(job, core.SerialNexus)
        self.assertNotIsInstance(job, core.ProcNexus)
        self.assertNotIsInstance(job, core.ThreadNexus)

    def test_nexus_creates_serial_nexus_when_worker_counts_are_none(self) -> None:
        job = nexus(add, processes=None, threads=None)

        self.assertIsInstance(job, core.SerialNexus)
        self.assertNotIsInstance(job, core.ProcNexus)
        self.assertNotIsInstance(job, core.ThreadNexus)

    def test_nexus_can_create_proc_nexus(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        job.submit(10, 5)

        self.assertIsInstance(job, core.ProcNexus)
        self.assertEqual(job.run(), [3, 15])

    def test_nexus_classes_share_base_parent(self) -> None:
        serial_job = nexus(add)
        process_job = nexus(add, processes=2)
        thread_job = nexus(add, threads=2)

        self.assertIsInstance(serial_job, core.ParallelNexus)
        self.assertIsInstance(process_job, core.ParallelNexus)
        self.assertIsInstance(thread_job, core.ParallelNexus)
        self.assertTrue(issubclass(core.SerialNexus, core.ParallelNexus))
        self.assertTrue(issubclass(core.ProcNexus, core.ParallelNexus))
        self.assertTrue(issubclass(core.ThreadNexus, core.ParallelNexus))
        self.assertFalse(issubclass(core.ThreadNexus, core.ProcNexus))

    def test_thread_nexus_supports_non_picklable_callables(self) -> None:
        offset = 5
        job = nexus(lambda value: value + offset, threads=2)
        job.submit(1)
        job.submit(10)

        self.assertEqual(job.run(), [6, 15])

    def test_submit_after_start_with_thread_pool(self) -> None:
        job = nexus(add, threads=2)
        job.submit(1, 2)
        self.assertIsNone(job.start())
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15, 7])

    def test_nexus_rejects_non_int_or_none_threads_option(self) -> None:
        with self.assertRaisesRegex(TypeError, "invalid type for threads"):
            nexus(add, threads="2")

    def test_nexus_rejects_processes_with_threads_option(self) -> None:
        with self.assertRaisesRegex(TypeError, "mutually exclusive"):
            nexus(add, processes=2, threads=2)

    def test_nexus_normalizes_zero_worker_counts_before_selecting_runner(self) -> None:
        serial_job = nexus(add, processes=0, threads=0)
        process_job = nexus(add, processes=2, threads=0)
        thread_job = nexus(add, processes=0, threads=2)

        self.assertIsInstance(serial_job, core.SerialNexus)
        self.assertIsInstance(process_job, core.ProcNexus)
        self.assertIsInstance(thread_job, core.ThreadNexus)
        self.assertFalse(hasattr(serial_job, "workers"))
        self.assertEqual(process_job.workers, 2)
        self.assertEqual(thread_job.workers, 2)

    def test_submit_after_start_with_process_pool(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        self.assertIsNone(job.start())
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertIsNone(job.join())
        self.assertEqual(job.get(), [3, 15, 7])

    def test_submit_after_start_in_process(self) -> None:
        job = nexus(add)
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
        job = nexus(add)
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
        job = nexus(add)
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
        job = nexus(add)
        job.start()

        with self.assertRaisesRegex(RuntimeError, "cannot run a nexus"):
            job.run()

    def test_get_before_start_is_rejected(self) -> None:
        job = nexus(add)

        with self.assertRaisesRegex(RuntimeError, "cannot get results before"):
            job.get()

    def test_submit_after_join_is_rejected(self) -> None:
        job = nexus(add)
        job.start()
        job.join()

        with self.assertRaisesRegex(RuntimeError, "cannot submit tasks after"):
            job.submit(1, 2)


if __name__ == "__main__":
    unittest.main()
