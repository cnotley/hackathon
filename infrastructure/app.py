#!/usr/bin/env python3
from aws_cdk import App, Stack
from constructs import Construct
from .ingestion_stack import IngestionStack


class AuditFullStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        state_machine_arn = self.node.try_get_context('state_machine_arn') or 'arn:aws:states:region:acct:stateMachine:dummy'
        IngestionStack(self, 'Ingestion', state_machine_arn=state_machine_arn)


def main() -> None:
    app = App()
    AuditFullStack(app, 'AuditFullStack')
    app.synth()


if __name__ == '__main__':
    main()
