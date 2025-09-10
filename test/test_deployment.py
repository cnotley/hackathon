import subprocess, sys

def test_cdk_synth_cli():
    # Verify CDK app module runs without error
    cmd = [sys.executable, "-c", "import aws_cdk as cdk, sys; sys.path.append('infrastructure'); import app"]
    subprocess.run(cmd, check=True)
