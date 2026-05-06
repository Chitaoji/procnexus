import unittest

from procnexus import nexus


def add(a: int, b: int) -> int:
    return a + b


class ProcNexusTests(unittest.TestCase):
    def test_submit_after_start_with_process_pool(self) -> None:
        job = nexus(add, processes=2)
        job.submit(1, 2)
        job.start()
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertEqual(job.join(), [3, 15, 7])

    def test_submit_after_start_in_process(self) -> None:
        job = nexus(add, processes=0)
        job.submit(1, 2)
        job.start()
        job.submit(10, 5)
        job.submit(-1, 8)

        self.assertEqual(job.join(), [3, 15, 7])

    def test_submit_after_join_is_rejected(self) -> None:
        job = nexus(add, processes=0)
        job.start().join()

        with self.assertRaisesRegex(RuntimeError, "cannot submit tasks after"):
            job.submit(1, 2)


if __name__ == "__main__":
    unittest.main()
