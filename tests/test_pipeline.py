"""增量接入流程测试（占位，骨架阶段）。"""

import unittest


class PipelineSkeletonTest(unittest.TestCase):
    def test_imports(self):
        from app import models
        from app.pipeline import Pipeline
        from app.api import create_app
        self.assertTrue(callable(create_app))
        self.assertTrue(hasattr(models, "RejectReason"))


if __name__ == "__main__":
    unittest.main()
