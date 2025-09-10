import pytest

try:
    from infrastructure.app import main
except ModuleNotFoundError:  # aws_cdk may be missing
    main = None


def test_cdk_main():
    if main is None:
        pytest.skip('aws_cdk not installed')
    main()
