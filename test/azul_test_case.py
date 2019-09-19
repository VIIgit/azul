from unittest import TestCase

import boto3.session


class AlwaysTearDownTestCase(TestCase):
    """
    SafeTestCase makes sure that tearDown / cleanup methods are always run when
    they should be.

    This means that

    - if a KeyboardInterrupt is raised in a test, then tearDown, tearDownClass,
      and tearDownModule will all still run
    - if any exception is raised in a setUp, then tearDown, tearDownClass,
      and tearDownModule will all still run

    Caveats:

    - All tearDown methods should pass even if their corresponding setUps don't
      run at all, as in the case of a KeyboardInterrupt or other exception.
    - If an exception is raised in setUpClass or setUpModule, the corresponding
      tearDown will not be run.
    """

    def run(self, result=None):
        test_method = getattr(self, self._testMethodName)
        wrapped_test = self._cleanup_wrapper(test_method, KeyboardInterrupt)
        setattr(self, self._testMethodName, wrapped_test)

        self.setUp = self._cleanup_wrapper(self.setUp, BaseException)

        return super().run(result)

    def _cleanup_wrapper(self, method, exception_cls):
        def wrapped(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except exception_cls:
                self.tearDown()
                self.doCleanups()
                raise

        return wrapped


class AzulTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.get_credentials_orig = boto3.session.Session.get_credentials
        # This ensures that we don't accidentally use actual cloud resources in unit tests. Furthermore,
        # Boto3/botocore cache credentials which can lead to credentials from an unmocked use of boto3 in one test to
        # leak into a mocked use of boto3. The latter was the reason for #668.
        boto3.session.Session.get_credentials = lambda self: None
        assert boto3.session.Session().get_credentials() is None

    @classmethod
    def tearDownClass(cls) -> None:
        boto3.session.Session.get_credentials = cls.get_credentials_orig
        super().tearDownClass()


class Test:
    # Keep this test case out of the main namespace

    class LoggingTestCase(TestCase):
        """A test case which logs its calls."""

        def __init__(self, events):
            super(Test.LoggingTestCase, self).__init__('test')
            self.events = events

        def setUp(self):
            self.events.append('setUp')

        def test(self):
            self.events.append('test')

        def tearDown(self):
            self.events.append('tearDown')


class TestAlwaysTearDownTestCase(TestCase):

    def test_regular_execution_order(self):
        for always_tear_down in (False, True):
            events = []
            tear_down_parent = [AlwaysTearDownTestCase] if always_tear_down else []
            with self.subTest(always_tear_down=always_tear_down):

                class TC(*([Test.LoggingTestCase] + tear_down_parent)):
                    pass

                TC(events).run()
                expected = ['setUp', 'test', 'tearDown']
                self.assertEqual(events, expected)

    def test_keyboard_interrupt_in_test(self):
        for always_tear_down in (False, True):
            events = []
            tear_down_parent = [AlwaysTearDownTestCase] if always_tear_down else []
            with self.subTest(always_tear_down=always_tear_down):

                class TC(*([Test.LoggingTestCase] + tear_down_parent)):
                    def test(self):
                        super().test()
                        raise KeyboardInterrupt()

                with self.assertRaises(KeyboardInterrupt):
                    TC(events).run()
                expected = ['setUp', 'test'] + (['tearDown'] if always_tear_down else [])
                self.assertEqual(events, expected)

    def test_exception_in_setup(self):
        for always_tear_down in (False, True):
            events = []
            tear_down_parent = [AlwaysTearDownTestCase] if always_tear_down else []
            with self.subTest(always_tear_down=always_tear_down):

                class TC(*([Test.LoggingTestCase] + tear_down_parent)):
                    def setUp(self):
                        super().setUp()
                        raise RuntimeError('Exception in setUp')

                TC(events).run()
                expected = ['setUp'] + (['tearDown'] if always_tear_down else [])
                self.assertEqual(events, expected)

    def test_keyboard_interrupt_in_setup(self):
        for always_tear_down in (False, True):
            events = []
            tear_down_parent = [AlwaysTearDownTestCase] if always_tear_down else []
            with self.subTest(always_tear_down=always_tear_down):

                class TC(*([Test.LoggingTestCase] + tear_down_parent)):
                    def setUp(self):
                        super().setUp()
                        raise KeyboardInterrupt()

                with self.assertRaises(KeyboardInterrupt):
                    TC(events).run()
                expected = ['setUp'] + (['tearDown'] if always_tear_down else [])
                self.assertEqual(events, expected)
