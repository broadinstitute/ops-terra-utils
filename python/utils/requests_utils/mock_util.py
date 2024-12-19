
import functools
import inspect
from pathlib import Path
from typing import Any
import responses
import responses._recorder
import git


def make_filename(func: Any) -> Path:
    module = inspect.getmodule(func)

    file_path = Path(module.__file__)  # type: ignore[union-attr, arg-type]
    repo = git.Repo(file_path, search_parent_directories=True)
    git_root = repo.git.rev_parse("--show-toplevel")
    return Path(git_root).joinpath("mock_output.yaml")


def activate_responses() -> Any:
    def outer_decorator(func: Any) -> Any:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with responses.RequestsMock() as rsp:
                rsp._add_from_file(file_path=make_filename(func))
                return func(*args, **kwargs)

        return wrapper

    return outer_decorator


def activate_recorder() -> Any:
    def outer_decorator(func: Any) -> Any:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            recorder = responses._recorder.Recorder()
            with recorder:
                try:
                    result = func(*args, **kwargs)
                finally:
                    recorder.dump_to_file(
                        file_path=make_filename(func),
                        registered=recorder.get_registry().registered,
                    )
                return result

        return wrapper

    return outer_decorator


def mock_responses(activate: bool = False, update_results: bool = False) -> Any:
    """Decorator to record then mock requests made with the requests module.

    When update_results is True, will store requests to a yaml file. When it
    is false, it will retrieve the results, allowing to run tests offline.

    Usage:
        import requests
        from python.tests.utils.mock_responses import mock_responses


        class MyTestCase(TestCase):
            @mock_responses(update_results=settings.TESTS_UPDATE_STORED_RESULTS)
            def test_mytest(self):
                request.get("https://example.com)
                ...
    """
    def conditional_decorator(func: Any) -> Any:
        if activate:
            if update_results:
                return activate_recorder()
            else:
                return activate_responses()
        else:
            return func
    return conditional_decorator
